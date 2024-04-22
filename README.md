### lab2 2A部分内容
#### 实现领导者选举功能
electionLoop() 方法使得所有follower实例不断的检测是否定时器过期，过期后晋升为候选者开始新term的投票
#### 实现心跳包功能
appendEntriesLoop() 方法使得leader实例，不断的向follower发送心跳包（后续的日志也是这个rpc发送的），来维持或者更新leader状态。

测试代码
```sh
git clone git@github.com:nullptroot/raft-go.git
cd raft-go/raft
go test -run 2A
```
输出
```sh
Test (2A): initial election ...
  ... Passed --   3.0  3   60   12160    0
Test (2A): election after network failure ...
  ... Passed --   7.4  3  178   28466    0
PASS
ok      raft    10.458s
```
### lab2 2B部分内容
#### 实现日志复制过程
测试代码
```sh
git clone git@github.com:nullptroot/raft-go.git
cd raft-go/raft
go test -run 2B
```
输出
```sh
Test (2B): basic agreement ...
  ... Passed --   0.8  3   16    4366    3
Test (2B): RPC byte count ...
  ... Passed --   2.6  3   48  114902   11
Test (2B): agreement despite follower disconnection ...
  ... Passed --   4.3  3   86   24282    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.7  5  156   39137    4
Test (2B): concurrent Start()s ...
  ... Passed --   0.8  3   16    4422    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   6.3  3  184   47198    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  34.5  5 2232 2006792  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.2  3   40   12266   12
PASS
ok  	raft	55.309s
```

### lab2 2C部分内容
#### 实现raft状态的持久化
测试代码
```sh
git clone git@github.com:nullptroot/raft-go.git
cd raft-go/raft
go test -run 2C
```
输出
```sh
Test (2C): basic persistence ...
  ... Passed --   3.8  3   80   20283    6
Test (2C): more persistence ...
  ... Passed --  31.5  5 1424  300498   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.8  3   34    8571    4
Test (2C): Figure 8 ...
  ... Passed --  31.6  5  396   77691   11
Test (2C): unreliable agreement ...
  ... Passed --  16.5  5  728  206809  271
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  39.1  5 2640 8377457  146
Test (2C): churn ...
  ... Passed --  17.0  5  668  572842  280
Test (2C): unreliable churn ...
  ... Passed --  16.9  5  612  272743  156
PASS
ok  	raft	158.281s
```