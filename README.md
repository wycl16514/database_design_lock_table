在前面章节，我们描述的并发控制的一些基本原理。其中一个重要原则就是“序列化”，也就数据库引擎要对交易提交的请求进行调度，调度的结果要使得每个交易就好像独占了引擎那样。要实现这样的效果就必须进行相应的加锁。但是加锁必然会降低高并发的效率，因此改进办法是实现两种锁，一种是互斥锁，他用于保证区块写入的安全性，一个区块加锁后其他任何操作，无论是读还是写，都不能执行，必须要等到互斥锁释放。另一种是共享锁，他运行多个读操作同时进行，但是不允许执行写操作，必须等到共享锁全部释放后才可以。

本节的目的就在于如何实现两种锁机制。尽管go语言提供了很多并发机制，他也有共享锁和互斥锁，但还不足以满足一个数据库的并发要求，这也是我们需要进行相应设计的原因。我们所设计的锁机制作用的对象是区块，因此我们需要一种对应机制，不同的区块要对应不同的锁，因此我们要实现一个类似map的对象，他也称为LockTable。

他要实现的目的有，一，让不同的区块对应不同的锁，因此引擎在读写区块1时，不影响对区块2的操作。第二，他要有超时回退机制，当引擎通过他读写某个区块，发现长时间得不到操作所需要的锁时，这个时候极有可能发生了死锁，因此他要能检测此种情况并实现操作的回滚。下面我们看看代码的实现，首先在tx模块中添加新文件命名为lock_table.go，然后先输入如下代码：

```go
package tx

import (
	"errors"
	fm "file_manager"
	"sync"
	"time"
)

const (
	MAX_WAITING_TIME = 3 //3用于测试，在正式使用时设置为10
)

func NewLockTable() *LockTable {
	/*
		如果给定blk对应的值为-1，表明有互斥锁,如果大于0表明有相应数量的共享锁加在对应区块上，
		如果是0则表示没有锁
	*/
	lock_table := &LockTable{
		lock_map:    make(map[*fm.BlockId]int64),
		notify_chan: make(map[*fm.BlockId]chan struct{}),
		notify_wg:   make(map[*fm.BlockId]*sync.WaitGroup),
	}

	return lock_table
}
```

接下来我们要实现这样的功能，假设有3个线程编号分别为1,2,3，第一个线程要写入区块1，第二，三个线程要读取区块1，如果线程1先获得了互斥锁，那么线程2,3就必须挂起，等待线程1释放锁。如果在时间MAX_WAITING_TIME范围内线程1完成操作释放了互斥锁，那么线程2,3就能被唤醒，他们将获得共享锁，然后同时读取区块1的数据。如果在MAX_WAITING_TIME所表达的时间范围内依然无法读取区块1，那么他们就需要唤醒，然后放弃读取操作。

确切的说这里需要实现WaitGivenTimeOut功能，当调用这个函数时，对应的线程会在给定时间段内挂起，一旦超时后才被唤醒。同时我们还需要实现NotifyAll接口，一旦某个线程调用这个函数后，所有因为执行WaitGivenTimeOut而被挂起的线程都要被唤醒，然后往下执行。

如果了解go语言的同学可能知道，sync包中有个Cond类，他的Wait接口能实现调用线程挂起功能，同时他对应的Broadcast能实现唤醒所有因为调用Wait而挂起的线程。但是问题在于Wait接口不能实现挂起特定时间，因此一旦调用该接口后，必须等待其他线程调用Signal接口或是Broadcast接口才能实现唤醒。

由此我们要自己实现WaitGivenTimeOut对应的功能，相关代码如下：

```go
type LockTable struct {
	lock_map    map[*fm.BlockId]int64           //将锁和区块对应起来
	notify_chan map[*fm.BlockId]chan struct{}   //用于实现超时回退的管道
	notify_wg   map[*fm.BlockId]*sync.WaitGroup //用于实现唤醒通知
	method_lock sync.Mutex                      //实现方法调用的线程安全，相当于java的synchronize关键字
}

func (l *LockTable) waitGivenTimeOut(blk *fm.BlockId) {
	wg, ok := l.notify_wg[blk]
	if !ok {
		var new_wg sync.WaitGroup
		l.notify_wg[blk] = &new_wg
		wg = &new_wg
	}
	wg.Add(1)
	defer wg.Done()
    l.method_lock.Unlock() //挂起前释放方法锁
	select {
	case <-time.After(MAX_WAITING_TIME * time.Second):
	case <-l.notify_chan[blk]:
	}
	l.method_lock.Lock() //唤起后加上方法锁
}

func (l *LockTable) notifyAll(blk *fm.BlockId) {
	go func() {
		//等待所有线程返回后再重新设置channel
		l.notify_wg[blk].Wait()
		l.notify_chan[blk] = make(chan struct{})
	}()

	close(l.notify_chan[blk])
}
```

函数waitGivenTimeOut需要输入模块对应的BlockId对象。notify_wg是一个map对象，他将WaitGoup和特定的区块号对应起来，如果有多个线程要访问同一个区块，那么对应的WaitGroup就会执行Add(1)操作。接下来的Select 语句用于实现线程挂起，第一个case用于将线程挂起给定时间，第二个case用于将线程唤醒。

假设线程2,3因为执行waitGivenTimeOut函数而被挂起，那么这两个线程会因为两种情况会被重新唤起，第一种情况就是超时，也就是time.After对应的管道会启动，从而将两个线程唤起。第二种情况是线程1调用了notifyAll，注意到这里关闭了对应区块的管道，于是select语句中第二个case得到执行，于是挂起的线程被唤醒。注意到notifyAll还启动了一个线程，他的作用是等待给定区块对应的WaitGroup能完成，当l.notify_wg[blk].Wait返回后，那意味着所有挂起的线程都完成了唤醒操作，这时他就重新给区块对应的管道重新赋值，以便于下次使用。

并发设计难度很大，也很容易出错，上面的做法可能存在一些问题，以后发现时我们再进行修改。接下来我们需要设计互斥锁和共享锁，互斥锁对应的接口为XLock, 共享锁对应的接口为SLock:

```go

func (l *LockTable) initWaitingOnBlk(blk *fm.BlockId) {
	_, ok := l.notify_chan[blk]
	if !ok {
		l.notify_chan[blk] = make(chan struct{})
	}

	_, ok = l.notify_wg[blk]
	if !ok {
		l.notify_wg[blk] = &sync.WaitGroup{}
	}
}

func (l *LockTable) SLock(blk *fm.BlockId) error {
	l.method_lock.Lock()
	defer l.method_lock.Unlock()
    l.initWaitingOnBlk(blk)
	start := time.Now()
	for l.hasXlock(blk) && !l.waitingTooLong(start) {
		l.waitGivenTimeOut(blk)
	}
	//如果等待过长时间，有可能是产生了死锁
	if l.hasXlock(blk) {
		return errors.New("SLock Exception: XLock on given blk")
	}

	val := l.getLockVal(blk)
	l.lock_map[blk] = val + 1
	return nil
}

func (l *LockTable) XLock(blk *fm.BlockId) error {
	l.method_lock.Lock()
	defer l.method_lock.Unlock()
    l.initWaitingOnBlk(blk)
	start := time.Now()
	for l.hasOtherSLocks(blk) && !l.waitingTooLong(start) {
		l.waitGivenTimeOut(blk)
	}

	if l.hasOtherSLocks(blk) {
		return errors.New("XLock error: SLock on given blk")
	}

	//-1表示区块被加上互斥锁
	l.lock_map[blk] = -1

	return nil
}

func (l *LockTable) UnLock(blk *fm.BlockId) {
	l.method_lock.Lock()
	defer l.method_lock.Unlock()

	val := l.getLockVal(blk)
	if val >= 1 {
		l.lock_map[blk] = val - 1
	} else {
		delete(l.lock_map, blk)
		//通知所有等待给定区块的线程从Wait中恢复
		l.notifyAll(blk)
	}
}

func (l *LockTable) hasXlock(blk *fm.BlockId) bool {
	return l.getLockVal(blk) < 0
}

func (l *LockTable) hasOtherSLocks(blk *fm.BlockId) bool {
	return l.getLockVal(blk) >= 1
}

func (l *LockTable) waitingTooLong(start time.Time) bool {
	elapsed := time.Since(start).Seconds()
	if elapsed >= MAX_WAITING_TIME {
		return true
	}

	return false
}

func (l *LockTable) getLockVal(blk *fm.BlockId) int64 {
	val, ok := l.lock_map[blk]
	if !ok {
		return 0
	}

	return val
}
```
在上面的代码实现中，需要注意的是，在调用XLock或是SLock时，首先需要判断给定区块是否被加上其他锁，也就是调用XLock时需要判断区块是否已经被加了共享锁，调用SLock时判断区块是否已经被加了互斥锁。

如果区块已经被加上了其他锁，那么线程就会先挂起一段时间，例如某个线程调用SLock给区块1加上共享锁，此时他发现区块1已经被加上了互斥锁，那么他就会调用waitGivenTimeOut来挂起一段时间，超时后他会自动唤醒，然后再次判断互斥锁是否已经解除，如果还没解除，那么他需要返回一个错误，于是对应的交易就要放弃读取给定区块的内容。如果它在挂起期间，另一个线程已经完成了写入操作，于是就会调用notifyAll接口，然后该线程就会从挂起中恢复，接下来他就会对区块加上共享锁，然后读取区块的数据。

这里我们实现共享锁和互斥锁的机制很简单，我们使用一个map来实现。如果给定区块被加上互斥锁，那么该区块对应数值就是-1，如果被加上共享锁，那么对应的数值就是一个大于0的数，如果有三个线程针对同一区块获取共享锁，那么该区块对应的数值就是3，当给定区块数值对应为0时，表示没有锁加在给定区块上。

下面我们需要对上面实现的逻辑进行检测，首先要检验waitGivenTimeOut和notifyAll的正确性，测试用例这么做，首先创建区块1，然后启动4个线程，第一个线程先在区块1上获取互斥锁，接下来启动线程2,3,4，后面三个线程针对区块1获得共享锁，由于此时区块1已经被加上互斥锁，因此后面三个线程会被挂起给定时长MAX_WAITING_TIME，一旦超时后他们被唤醒，然后发现区块1依然被加上了互斥锁，于是返回错误，下面是测试用例的实现：

```go

func TestRoutinesWithSLockTimeout(t *testing.T) {
	var err_array []error
	var err_array_lock sync.Mutex
	blk := fm.NewBlockId("testfile", 1)
	lock_table := NewLockTable()
	lock_table.XLock(blk)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			err_array_lock.Lock()
			defer err_array_lock.Unlock()
			err := lock_table.SLock(blk)
			if err == nil {
				fmt.Println("access slock ok")
			}
			err_array = append(err_array, err)
		}()
	}
	time.Sleep(1 * time.Second) //让线程都运行起来
	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	require.Equal(t, elapsed >= MAX_WAITING_TIME, true)
	require.Equal(t, len(err_array), 3)
	for i := 0; i < 3; i++ {
		require.Equal(t, err_array[i], errors.New("SLock Exception: XLock on given blk"))
	}
}
```
我们用如下命令执行上面的用例：

```go
go test tx -run TestRoutinesWithSLockTimeout
```
运行后返回结果为：
```
ok      tx      9.753s
```
也就是说测试用例的执行是成功的。我们再看一个用例，线程1先获取互斥锁，然后启动3个线程去获取共享锁并进入挂起状态，线程1在挂起超时前释放互斥锁，调用notifyAll唤起所有挂起的线程，被唤起的线程都能获得共享锁并读取区块数据，代码如下：
```

func TestRoutinesWithSLockAfterXLockRelease(t *testing.T) {
	var err_array []error
	var err_array_lock sync.Mutex
	blk := fm.NewBlockId("testfile", 1)
	lock_table := NewLockTable()
	lock_table.XLock(blk)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			err_array_lock.Lock()
			defer err_array_lock.Unlock()
			err := lock_table.SLock(blk)
			if err == nil {
				fmt.Println("access slock ok")
			}
			err_array = append(err_array, err)
		}()
	}
	time.Sleep(1 * time.Second) //让线程都运行起来
	lock_table.UnLock(blk)      //释放加在区块上的互斥锁
	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	require.Equal(t, elapsed < MAX_WAITING_TIME, true)
	require.Equal(t, len(err_array), 3)
	for i := 0; i < 3; i++ {
		require.Nil(t, err_array[i]) //所有线程能获得共享锁然后读取数据
	}

	require.Equal(t, lock_table.lock_map[blk], int64(3))
}
```
上面的用例经过多次运行均能通过。更多更详细的视频演示和讲解请参看b站，搜索Coding迪斯尼。

