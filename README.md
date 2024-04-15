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