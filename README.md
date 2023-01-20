
Kafka EOS and Transactions for Go Rule Engine
----------------------------------
----------------------------------


Demo for eos consuming and producing transactionally from kafka in Golang.


Abstract
--------

This app uses NewGroupTransactSession.

Quote from the source code:
https://github.com/twmb/franz-go/blob/bfcfa0847e03933f371a4ec813d8dd4e0f5780fd/pkg/kgo/txn.go#L75


// NewGroupTransactSession is exactly the same as NewClient, but wraps the

// client's OnPartitionsRevoked / OnPartitionsLost to ensure that transactions

// are correctly aborted whenever necessary so as to properly provide EOS.


More elaborate exaplanation is founded there.


Install Golang on Windows:
-------------------------

1. Go to: https://go.dev/doc/install

2. Download Windows msi installer

3. Click on the Windows tab for further instructions



Install application + dependencies:
------------------------------------------


$ git clone https://github.com/boomerlang/go_kafka_txn_eos


Build the application:
--------------------

$ cd go_kafka_txn_eos


$ go build


Run an instance:
----------------

The default kafka broker is looked up on localhost:9092 address.

If you have other configuration use the flag -brokers.

The flag -brokers expects a comma separated list of kafka brokers. 

Do not leave comma at the end of the list!


$ ./go_kafka_txn_eos -brokers broker-host-or-ip-address:9092 -debug-host localhost-or-ip-address:9999 -topic-in GoRuleEngineServiceIn -eos-topic-out GoRuleEngineServiceOutTransformationServiceIn  > run_inst1.log 2>&1 &


To see the flow the in action:

$ tail -f run_inst1.log


Run another instance:
--------------------

One must change the debug-host port. This is the only requirement to run distinct instances.

$ ./go_kafka_txn_eos -brokers broker-host-or-ip-address:9092 -debug-host localhost-or-ip-address:9998 -topic-in GoRuleEngineServiceIn -eos-topic-out GoRuleEngineServiceOutTransformationServiceIn  > run_inst2.log 2>&1 &


Stop an instance:
-----------------

$ kill -TERM pid_of_instance


Caveats
-------

Kafka broker is dumb producer/smart consumer kind of broker.
For this reason it is important that the client performs correctly
the cleanup at shutdown, namely the transactions a closed correctly or rolled dback as 
a result that the running context was signaled to stop.

It is very important that the transactions do not remain in an unconsistent state.
For this reason the application must be send the TERM (-15) terminate UNIX signal in order to 
shutdown cleanly all the transactions. Do NOT send the KILL (-9) UNIX signal because the process is simply killed
and the cleanup is not performed.

This is bad because we could lose messages and when the instance is started it performes some kind of
verifications that takes some time until it becomes operational.

In Windows must be taken into account this.


Crash it!
---------

If an instance is crashed with SIGKILL UNIX signal the other running instances will have a 
delay in taking over the ownership of the transactions, because if the transactions
are not cleanly studown the other instances does not get notified about this and there is
involved a timeout after which the rebalance occurs.

A SIGTERM UNIX signal will shutdown cleanly the transcations and another running instance 
will start immedaiately processing the kafla topic because the closing instance announces this
to the other instances and one of them takes over and rebalance immediately


Metrics
-------

To see what is going on in the guts of the app run this command:

curl http://ip-addr-or-host:9999/metrics

This uses the -debug-host command line switch.

