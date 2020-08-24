## 相关配置
	
`具体官方配置请转移[动态分配](http://spark.apache.org/docs/latest/configuration.html#dynamic-allocation）`

**下面看下简单的介绍**

	spark.dynamicAllocation.enabled：Whether to use dynamic resource allocation, which scales the number of executors registered with this application up and down based on the workload. For more detail, see the description here.
    该配置项用于配置是否使用动态资源分配，根据工作负载调整应用程序注册的executor的数量。默认为false。
    
    与之相关的还有如下配置：
      spark.dynamicAllocation.minExecutors
      spark.dynamicAllocation.maxExecutors 
      spark.dynamicAllocation.initialExecutors
      spark.dynamicAllocation.executorAllocationRatio

    如果启用动态分配，在executor空闲spark.dynamicAllocation.executorIdleTimeout(默认60s)之后将被释放。

    spark.dynamicAllocation.minExecutors和spark.dynamicAllocation.maxExecutors分别为分配的最小及最大值，spark.dynamicAllocation.initialExecutors为初始分配的值，默认取值为minExecutors。在--num-executors参数设置后，将使用此设置的值作为动态分配executor数的初始值。

    在spark1.6中，如果同时设置dynamicAllocation及num-executors，启动时会有
    
    WARN spark.SparkContext: Dynamic Allocation and num executors both set, thus dynamic allocation disabled。
    
    在spark2中已经不再禁用了，如果num-executors也设置的话只是作为初始值存在而已。
    如果启用dynamicAllocation则spark.shuffle.service.enable必须设置为true，此选项用于启动外部的shuffle服务，免得在executor释放时造成数据丢失。外部的shuffle服务运行在NodeManager节点中，独立于spark的executor，在spark配置中通过spark.shuffle.service.port指定其端口，默认为7337。
  
针对动态资源调整，我搜索到了脸书对其的应用：

,,,,

**下面从源码进行简单的解析**

>### 1.入口类ExecutorAllocationManager

`An agent that dynamically allocates and removes executors based on the workload.---一种根据工作负荷动态分配和删除执行程序的代理。`

OK，从这个注释就可以理解，这个类会根据当前应用的运行状态来动态添加和移除executor资源，以此保证更好的使用集群资源。

那么具体是怎么实现的呢？我们自定义的配置又是如何生效的呢？

首先当然是读取配置了，具体如下：

     // Lower and upper bounds on the number of executors.
      private val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
      private val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
      private val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)

      // How long there must be backlogged tasks for before an addition is triggered (seconds)
      private val schedulerBacklogTimeoutS = conf.getTimeAsSeconds(
        "spark.dynamicAllocation.schedulerBacklogTimeout", "1s")

      // Same as above, but used only after `schedulerBacklogTimeoutS` is exceeded
      private val sustainedSchedulerBacklogTimeoutS = conf.getTimeAsSeconds(
        "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", s"${schedulerBacklogTimeoutS}s")

      // How long an executor must be idle for before it is removed (seconds)
      private val executorIdleTimeoutS = conf.getTimeAsSeconds(
        "spark.dynamicAllocation.executorIdleTimeout", "60s")

      private val cachedExecutorIdleTimeoutS = conf.getTimeAsSeconds(
        "spark.dynamicAllocation.cachedExecutorIdleTimeout", s"${Integer.MAX_VALUE}s")

这些对应的变量或常量，跟进去就可以看到，其实读取的都是我们submit提交的conf参数，比如

    private[spark] val DYN_ALLOCATION_MIN_EXECUTORS =
        ConfigBuilder("spark.dynamicAllocation.minExecutors").intConf.createWithDefault(0)
        
可以看到动态资源最小取值逻辑以及默认值分配等，其他配置都可以在追进去查看。
针对初始化executor数值，源码中可以看到是spark.dynamicAllocation.initialExecutors、spark.dynamicAllocation.minExecutors和spark.executor.instances配置的最大值。

>### 2.配置参数合法性校验

validateSettings()方法提供的配置的一些合法性校验，具体看源码

>### 3.调起方法

 #### **start()方法** | ****重要****
  
还是先看下注释：

`Register for scheduler callbacks to decide when to add and remove executors, and start the scheduling task.
---注册调度程序回调，以决定何时添加和删除执行程序，并启动调度任务。`

首先要知道谁在哪里调用的start()方法?

毫无疑问，如果了解spark应用提交流程，那么你一定能够想到，这个是在sparkcontext初始化时实现的（学习spark必看源码），sc创建时会初始化_executorAllocationManager，在这个过程中执行了_executorAllocationManager.foreach(_.start())，以此启动了动态调整资源。

那么start里具体干了啥？看下源码

    def start(): Unit = {
        listenerBus.addToManagementQueue(listener)

        val scheduleTask = new Runnable() {
          override def run(): Unit = {
            try {
              schedule()
            } catch {
              case ct: ControlThrowable =>
                throw ct
              case t: Throwable =>
                logWarning(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
            }
          }
        }
        executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)

        client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
      }
      
`executor.scheduleWithFixedDelay`守护形式的调度执行器（单线程的线程池）每隔intervalMillis毫秒执行scheduleTask任务，任务具体就是执行schedule()方法，之后client向集群管理器发起一次rpc请求，方法结束。这里的client是ExecutorAllocationClient对象，提供的方法用途就是和集群管理器交互，发送crud请求。那为啥这里要有一次请求呢？主要就是为了告诉集群管理器去创建初始目标数量的executors，具体看schedule()方法，里面有个初始化的过程，如果这里不请求一次，那么资源不会到达初始目标值，可以看下第一次numExecutorsTarget是啥来加强理解。

#### 下面看下schedule()这个周期性被调用的方法

    /**
       * This is called at a fixed interval to regulate the number of pending executor requests
       * and number of executors running.
       *
       * First, adjust our requested executors based on the add time and our current needs.
       * Then, if the remove time for an existing executor has expired, kill the executor.
       *
       * This is factored out into its own method for testing.
       */
      private def schedule(): Unit = synchronized {
          val now = clock.getTimeMillis

          val executorIdsToBeRemoved = ArrayBuffer[String]()
          removeTimes.retain { case (executorId, expireTime) =>
            val expired = now >= expireTime
            if (expired) {
              initializing = false
              executorIdsToBeRemoved += executorId
            }
            !expired
          }
          // Update executor target number only after initializing flag is unset
          updateAndSyncNumExecutorsTarget(now)
          if (executorIdsToBeRemoved.nonEmpty) {
            removeExecutors(executorIdsToBeRemoved)
          }
        }


**看注释足矣**

首先是收集待移除的executor，那么待移除什么时候被添加进removeTimes这个map呢，从源码可以看到是onExecutorIdle()方法里，而如何判断当前的executor是不是idle呢，其实是在onTaskEnd这里判断的，当executor所有的task都结束时并且该executor闲置超过executorIdleTimeoutS时，这个executorId会被添加到removeTimes里，并且会在removeExecutors(executorIdsToBeRemoved)方法里被client.killExecutors方法回收（rpc）。


#### schedule()方法判断移除和执行中间还有一步updateAndSyncNumExecutorsTarget(now) ####

从方法命名可以推断，这个方法是用来更新和同步资源需求数量的，也就是判断当前有多少资源，是不是要新加，是不是要减少等。
 
具体看下updateAndSyncNumExecutorsTarget方法。

    private def updateAndSyncNumExecutorsTarget(now: Long): Int = synchronized {
        val maxNeeded = maxNumExecutorsNeeded
        if (initializing) {
          0
        } else if (maxNeeded < numExecutorsTarget) {
          val oldNumExecutorsTarget = numExecutorsTarget
          numExecutorsTarget = math.max(maxNeeded, minNumExecutors)
          numExecutorsToAdd = 1

          if (numExecutorsTarget < oldNumExecutorsTarget) {
            client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
            ...
          }
          numExecutorsTarget - oldNumExecutorsTarget
        } else if (addTime != NOT_SET && now >= addTime) {
          val delta = addExecutors(maxNeeded)
          ...
          addTime = now + (sustainedSchedulerBacklogTimeoutS * 1000)
          delta
        } else {
          0
        }
      }

a.首先一个分支是判断是否还在初始化阶段，如果是的话，直接返回了，不会做任何操作，所以上面才会讲要执行一次rpc请求会创建executors，否则不会有初始的executors，直接GG了。当开始发生executor idle或有stage提交时，跳出初始化阶段。

b.其次计算应用此时maxNeeded数量(需求值)，计算方法如下：
 
    private def maxNumExecutorsNeeded(): Int = {
          val numRunningOrPendingTasks = listener.totalPendingTasks + listener.totalRunningTasks
          math.ceil(numRunningOrPendingTasks * executorAllocationRatio /
                    tasksPerExecutorForFullParallelism)
            .toInt
        }
 
这里是使用当前每个阶段所运行的task和剩余待运行的task的数量来粗算的，通过一个ExecutorAllocationListener监听器来保存executor和stage、task（task状态等）中间的实时关系（内部数据结构都是mutable.HashMap），通过监听stage的提交、结束和task的开始、结束等事件来实时维护所需要的线程信息，以此来实时获取当前运行时运行的线程和等待的线程，最终计算出当前所需要的executor数量（所需总线程数/每个executor线程池最大线程数）。

c.接下来判断maxNeeded < numExecutorsTarget是否成立，当需求值小于现有值成立，执行`numExecutorsTarget = math.max(maxNeeded, minNumExecutors)`将现有值修改为此次计算的maxNeeded同时当新的需求值比上一次的需求值小，那么向集群发送rpc请求更新executor的需求值。具体实现如下：

d.最后如果需求值大于了当前值，那么将执行addExecutors(maxNeeded)方法来新增executor。

对于添加方法，首先会判断需求值是否大于了配置的自动分配最大值，如果大于了，直接return。实现如下：

    // Do not request more executors if it would put our target over the upper bound
        if (numExecutorsTarget >= maxNumExecutors) {
          logDebug(s"Not adding executors because our current target total " +
            s"is already $numExecutorsTarget (limit $maxNumExecutors)")
          numExecutorsToAdd = 1
          return 0
        }
        
否则的化也比较简单，计算出numExecutorsTarget需求值并以此值去向资源管理器发送rpc请求，来动态增加executor数量。这里需要注意下，我们首次新增的executor数量numExecutorsToAdd为1，但是当发送rpc请求成功后判断增量与numExecutorsToAdd当前值相等时，numExecutorsToAdd会变为2倍，即`numExecutorsToAdd * 2`，所以增量的化每次都是以2倍的速度在增加。具体体现如下

    if (addRequestAcknowledged) {
          val executorsString = "executor" + { if (delta > 1) "s" else "" }
          logInfo(s"Requesting $delta new $executorsString because tasks are backlogged" +
            s" (new desired total will be $numExecutorsTarget)")
          numExecutorsToAdd = if (delta == numExecutorsToAdd) {
            numExecutorsToAdd * 2
          } else {
            1
          }
          delta
        }
        
总结：本文直接讲解了executor动态调整的大致流程，里面还有很多具体细节，比如提供的各种backend的方法来维护task、stage、executor等信息，有兴趣可以具体读一下。本文不做具体讲解。最后的最后说明以下，如果关闭动态分配，那么会根据读取配置来创建executors，并且在整个job执行过程中，executor数量不会改变（这里不是绝对的），也就是资源无法释放，当你的任务只需要一两个thread就end了，但是配置了几百上千的executor，会发生什么呢。。。。
