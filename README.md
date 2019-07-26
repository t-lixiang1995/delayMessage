# delayMessage

## 延迟任务处理方案

### 方案一：数据库轮询

小型项目常用方式，通过一个线程去扫描数据库或数据库定时任务，通过订单时间判断超时的订单，进行更新状态或其他操作。

### 方案二：JDK延迟队列

DelayQueue是一个无界阻塞队列，只有在延迟期满时才能从中获取元素，放入DelayQueue中的对象需要实现Delayed接口。

#### 实现
```java
public class DelayQueuePlan {

	public static void main(String[] args) {
		DelayQueue<MyDelayed> delayQueue = new DelayQueue<MyDelayed>();
		//生产者生产一个5秒的延时任务
		new Thread(new ProducerDelay(delayQueue, 5)).start();
		//开启消费者轮询
		new Thread(new ConsumerDelay(delayQueue)).start();
	}
	
	/**
	 * 延时任务生产者 
	 **/
	public static class ProducerDelay implements Runnable{
		DelayQueue<MyDelayed> delayQueue;
		int delaySecond;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		public ProducerDelay(DelayQueue<MyDelayed> delayQueue, int delaySecond){
			this.delayQueue = delayQueue;
			this.delaySecond = delaySecond;
		}
		
		public void run() {
			String orderId = "1010101";
			for (int i = 0; i < 10; i++) {
				//定义一个Delay, 放入到DelayQueue队列中
				MyDelayed delay = new MyDelayed(this.delaySecond, orderId+i);
				delayQueue.add(delay);//向队列中插入一个元素（延时任务）
				System.out.println(sdf.format(new Date())+ " Thread "+Thread.currentThread()+" 添加了一个delay. orderId:"+orderId+i);
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	/**
	 * 延时任务消费者
	 **/
	public static class ConsumerDelay implements Runnable{
		
		DelayQueue<MyDelayed> delayQueue;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		public ConsumerDelay(DelayQueue<MyDelayed> delayQueue){
			this.delayQueue = delayQueue;
		}
		public void run() {
			//轮询获取DelayQueue队列中当前超时的Delay元素
			while(true){
				MyDelayed delayed=null;
				try {
					delayed = delayQueue.take();
				} catch (Exception e) {
					e.printStackTrace();
				}
				//如果Delay元素存在,则任务到达超时时间
				if(delayed!=null){
					//处理任务
					System.out.println(sdf.format(new Date())+" Thread "+Thread.currentThread()+" 消费了一个delay. orderId:"+delayed.getOrderId());
				}else{
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("....");
				}
			}
		}
	}
}
```

#### 结果

```tiki wiki
2019-07-26 15:06:51 Thread Thread[Thread-3,5,main] 添加了一个delay. orderId:10101010
2019-07-26 15:06:51 Thread Thread[Thread-3,5,main] 添加了一个delay. orderId:10101011
2019-07-26 15:06:51 Thread Thread[Thread-3,5,main] 添加了一个delay. orderId:10101012
2019-07-26 15:06:51 Thread Thread[Thread-3,5,main] 添加了一个delay. orderId:10101013
2019-07-26 15:06:52 Thread Thread[Thread-3,5,main] 添加了一个delay. orderId:10101014
2019-07-26 15:06:52 Thread Thread[Thread-3,5,main] 添加了一个delay. orderId:10101015
2019-07-26 15:06:52 Thread Thread[Thread-3,5,main] 添加了一个delay. orderId:10101016
2019-07-26 15:06:52 Thread Thread[Thread-3,5,main] 添加了一个delay. orderId:10101017
2019-07-26 15:06:52 Thread Thread[Thread-3,5,main] 添加了一个delay. orderId:10101018
2019-07-26 15:06:53 Thread Thread[Thread-3,5,main] 添加了一个delay. orderId:10101019
2019-07-26 15:06:56 Thread Thread[Thread-4,5,main] 消费了一个delay. orderId:10101010
2019-07-26 15:06:56 Thread Thread[Thread-4,5,main] 消费了一个delay. orderId:10101011
2019-07-26 15:06:56 Thread Thread[Thread-4,5,main] 消费了一个delay. orderId:10101012
2019-07-26 15:06:56 Thread Thread[Thread-4,5,main] 消费了一个delay. orderId:10101013
2019-07-26 15:06:57 Thread Thread[Thread-4,5,main] 消费了一个delay. orderId:10101014
2019-07-26 15:06:57 Thread Thread[Thread-4,5,main] 消费了一个delay. orderId:10101015
2019-07-26 15:06:57 Thread Thread[Thread-4,5,main] 消费了一个delay. orderId:10101016
2019-07-26 15:06:57 Thread Thread[Thread-4,5,main] 消费了一个delay. orderId:10101017
2019-07-26 15:06:57 Thread Thread[Thread-4,5,main] 消费了一个delay. orderId:10101018
2019-07-26 15:06:58 Thread Thread[Thread-4,5,main] 消费了一个delay. orderId:10101019
```



### 方案三：Redis 有序集合

将订单超时时间戳与订单号分别设置为score与member，系统扫描第一个元素判断是否超时。

#### 实现


```java
//扫描redis 判断订单是否超时需要处理
public void dofind(String key){
    //拿到redis客户端
    Jedis jedis = jedisPool.getResource();
    while(true){
        Set<Tuple> zrangeWithScores = jedis.zrangeWithScores(key, 0, 0);
        //判断元素是否超时  根据超时时间戳
        if(zrangeWithScores !=null && !zrangeWithScores.isEmpty()){
            //score  ===  订单的超时时间戳       与当前时间戳对比 判断是否超时
            double score = ((Tuple)(zrangeWithScores.toArray()[0])).getScore();//订单的超时时间戳
            long currentTimeMillis = System.currentTimeMillis();
            if(currentTimeMillis>=score){
                //订单超时
                String element = ((Tuple)(zrangeWithScores.toArray()[0])).getElement();//订单ID
                //删除元素
                Long zrem = jedis.zrem(key, element); //关键点：redis单线程机制解决并发场景安全问题。
                if(zrem!=null && zrem>0){
                    //处理超时订单
                    System.out.println(sdf.format(new Date())+"["+Thread.currentThread()+"] 从redis中拿到一个超时任务[key:"+key+", score:"+score+", member:"+element+"]");
                }else{
                    //						System.out.println(sdf.format(new Date())+"["+Thread.currentThread()+"] 任务被其他服务消费了");
                }
            }else{
                //					System.out.println(sdf.format(new Date())+"["+Thread.currentThread()+"] 当前没有超时的订单");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }else{
            //				System.out.println("当前redis中没有可以操作的数据");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
```



#### 结果

##### 生产者

```tiki wiki
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034031E12, member:OIDNO100000]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034055E12, member:OIDNO100001]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034056E12, member:OIDNO100002]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034058E12, member:OIDNO100003]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.56412503406E12, member:OIDNO100004]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034061E12, member:OIDNO100005]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034062E12, member:OIDNO100006]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034063E12, member:OIDNO100007]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034064E12, member:OIDNO100008]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034066E12, member:OIDNO100009]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034067E12, member:OIDNO100010]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034068E12, member:OIDNO100011]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034069E12, member:OIDNO100012]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034071E12, member:OIDNO100013]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034074E12, member:OIDNO100014]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034076E12, member:OIDNO100015]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034079E12, member:OIDNO100016]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034081E12, member:OIDNO100017]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034083E12, member:OIDNO100018]
2019-07-26 15:10:24 向redis中添加了一个任务[key:ORDER_KEY, score:1.564125034084E12, member:OIDNO100019]
```

##### 消费者

```tiki wiki
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034031E12, member:OIDNO100000]
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034055E12, member:OIDNO100001]
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034056E12, member:OIDNO100002]
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034058E12, member:OIDNO100003]
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.56412503406E12, member:OIDNO100004]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034061E12, member:OIDNO100005]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034062E12, member:OIDNO100006]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034063E12, member:OIDNO100007]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034064E12, member:OIDNO100008]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034066E12, member:OIDNO100009]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034067E12, member:OIDNO100010]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034068E12, member:OIDNO100011]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034069E12, member:OIDNO100012]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034071E12, member:OIDNO100013]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034074E12, member:OIDNO100014]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034076E12, member:OIDNO100015]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034079E12, member:OIDNO100016]
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034081E12, member:OIDNO100017]
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034083E12, member:OIDNO100018]
2019-07-26 15:11:15[Thread[Thread-11,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-3,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-4,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-5,5,main]] 从redis中拿到一个超时任务[key:ORDER_KEY, score:1.564125034084E12, member:OIDNO100019]
2019-07-26 15:11:15[Thread[Thread-9,5,main]] 任务被其他服务消费了
2019-07-26 15:11:15[Thread[Thread-10,5,main]] 任务被其他服务消费了
```



### 方案四：RabbitMQ TTL+DLX

RabbitMQ可设置消息过期时间（TTL），当消息过期后可以将该消息投递到队列上设置的死信交换器（DLX）上，再次投递到死信队列中，重新消费。

#### 实现

##### 生产者


```java
public void product() {
    String orderId = "1010101";
    for (int i = 0; i < 10; i++) {
        //创建订单
        amqpTemplate.convertAndSend(MQProperties.EXCHANGE_NAME, MQProperties.ROUTE_KEY, orderId+i);

        System.out.println(CalendarUtils.getCurrentTimeByStr(0)+" 生成了一个订单，订单ID："+orderId+i);
        if(i%3==0){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
```

##### 消费者


```java
@Component
public class MQBusiness {
	
	@RabbitListener(queues=MQProperties.DEAD_QUEUE_NAME)
	public void process(String message) throws IOException{
		System.out.println(CalendarUtils.getCurrentTimeByStr(0)+" 消费了一个超时订单，订单ID："+message);
		
//		basicAck() //确认消费成功，并删除RabbitMQ中对应数据
	}
}
```

#### 结果

```tiki wiki
16:49:51 生成了一个订单，订单ID：10101010
16:49:52 生成了一个订单，订单ID：10101011
16:49:52 生成了一个订单，订单ID：10101012
16:49:52 生成了一个订单，订单ID：10101013
16:49:53 生成了一个订单，订单ID：10101014
16:49:53 生成了一个订单，订单ID：10101015
16:49:53 生成了一个订单，订单ID：10101016
16:49:54 生成了一个订单，订单ID：10101017
16:49:54 生成了一个订单，订单ID：10101018
16:49:54 生成了一个订单，订单ID：10101019
16:50:1 消费了一个超时订单，订单ID：10101010
16:50:2 消费了一个超时订单，订单ID：10101011
16:50:2 消费了一个超时订单，订单ID：10101012
16:50:2 消费了一个超时订单，订单ID：10101013
16:50:3 消费了一个超时订单，订单ID：10101014
16:50:3 消费了一个超时订单，订单ID：10101015
16:50:3 消费了一个超时订单，订单ID：10101016
16:50:4 消费了一个超时订单，订单ID：10101017
16:50:4 消费了一个超时订单，订单ID：10101018
16:50:4 消费了一个超时订单，订单ID：10101019
```



## 各方案总结：

### DB轮询

**优点：**
实现简单、无技术难点、异常恢复、支持分布式/集群环境；
**缺点：**
影响数据库性能；

### DelayedQueue

**优点：**
实现简单、性能较好；  
**缺点：**
异常恢复困难、只适用于单机环境，分布式/集群实现困难；

### Redis

**优点：**
解耦、异常恢复、支持分布式/集群环境；  
**缺点：**
增加Redis维护、占用宽带、增加异常处理；

### RabbitMQ

**优点：**
解耦、异常恢复、扩展性强、支持分布式/集群环境；  
**缺点：**
增加RabbitMQ维护、占用宽带；



