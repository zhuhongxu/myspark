#### 一、什么是spark RDD？
    RDD(Resilient Distributed Dataset)弹性分布式数据集是spark对数据的核心抽象，可以将其理解为分布式的元素集合。
    在spark中，对数据的所有 操作不外乎创建RDD、转换已有RDD以及调用RDD操作进行求值，而在这一切背后，spark会自动将
    RDD中的数据分发到集群上，并将操作并行化执行。
#### 二、什么是spark驱动器程序？
    从上层来看，每个spark应用都由一个驱动器程序(driver program)来发起集群上的各种并行操作。驱动器程序包含应用的
    main函数，并且定义了集群上的分布式数据集，还对这些分布式数据集应用了相关操作，在shell操作中，驱动器程序就是
    spark shell本身，你只需要在终端输入想运行的操作就可以了，驱动器程序通过SparkContext对象来访问spark，这个
    对象代表对计算集群对一个连接，shell启动时已经自动创建了一个SparkContext的对象，是一个叫做sc的变量，一旦有
    了SparkContext，就可以用它来创建RDD。