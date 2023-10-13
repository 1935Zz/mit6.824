# mit6.824 lab2
mit6.824 lab2的实现,在此记录一下心得。目前2D还没做。本人比较菜，可能有些地方说的不是很准确或是有误，还请见谅

虽说是个人实现，但自己做完后一直过不了测试，lab官方文档看了半天也不知道问题在哪，最后还是去参考了下网上其他人的实现，不过根本原因还是没有弄清楚raft的具体细节

虽然最后还是通过了所有测试，但感觉写的完全是shi山（

[mit6.824 lab2链接](http://nil.csail.mit.edu/6.824/2021/labs/lab-raft.html)


## lab2A
2A要实现的就是一个由leader，candidate，follower三种状态构成的选举投票系统
每一个raft实体都有一个选举超时计时器
当计时器归0后，如果raft目前处于follower状态，转变为candidate状态，同时发起选举（向其它raft实体发送请求投票rpc）
这里有几个小细节（也都是我踩的坑）
1. 每个raft实体的计时器不能设置为同一时间，需要随机化为一个范围，否则所有raft同时发起投票导致票数分均始终无法选出leader
2. 请求投票rpc最好采取并行发送，否则很可能导致选不上leader（我也不知道为什么，可能是因为还在选举的过程中又有某个raft发起选举，和上一点类似）
3. 发送rpc前要释放锁，否则可能会出现两个rpc都占据着锁，但同时都向对方发送了rpc，而rpc处理也需要锁，说白了就是互相持有锁，又互相要对方的锁，导致发生死锁
4. leader发送appendentries rpc也要并行发送，因为lab采用的labrpc模拟了多种失败情况（具体可参考labrpc.go中的processReq函数），当某个raft不在网络中时，其响应的rpc会延迟发送，如果不采取并行发送，leader发送appendentries rpc的函数会阻塞
导致延迟发送下一个rpc，这样会导致一系列问题(之前因为这里第二个test一直有概率通不过)

如果获得大部分的投票，转变为leader，定时向其它raft实体发送心跳rpc重置它们的计时器避免他们发起选举投票


## lab2B  
2B要实现的是日志的复制和统一
leader从客户端接受log，存入自己的状态中，而leader定时发送的appendentries rpc便是为了让其它raft复制它的log，保持日志的一致性

为此，leader需要为每个其它raft实体保存1.nextindex，表示将要发送给其log的下标开始处 2，matchindex，表示匹配的log最大的下标

如果leader最后一个log的下标>=对某个raft的nextindex，那么就表示leader可以向该raft发送请求复制log的rpc，否则发送的就是空log的心跳rpc

appendentries rpc具体实现便是在args存入prevLogIndex表示新添加的log前一个log的下标，收到这个rpc的raft如果在该下标的log和leader的一致
就表示这个log以及该log之前的log都保持一致，便表示leader发送过来的log可以复制，否则表示无法复制。

可以复制之后，如果leader发送过来的的log和自己的某个log冲突，便删去该log及其之后的log，将leader发送过来的log复制到自己的log中

而leader收到成功复制的回应后，更新nextindex和matchindex

如果收到失败的回应，递减nextindex，继续发送appendentries rpc尝试复制直到成功

leader要做的另一件事便是不断检查是否可以更新commitindex（表示已提交的log最大的下标），如果大多数的raft的matchindex大于某个值，那么leader的commitindex便可以更新到这个值

当然2B还需要实现term任期，term只有在发起选举的时候才会递增

当某个raft成为leader后，它便在这个任期内持续任职leader直到它发生崩溃，而log也需要带上任期这个参数以便区分某个log是过期的leader所接受的还是目前任职的leader接收的

因为目前任职的leader具有更高的任期，所以它所接收的log任期更高，同时它的任期也会通过appendentries rpc来同步更新其余raft的任期，所以当某个raft收到任期比自己还低的appendentries，它就可以判断对方是过期的leader，不会复制过期的log

而raft的状态改变以及rpc的响应都需要根据任期大小来实现

