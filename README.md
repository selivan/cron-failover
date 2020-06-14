# cron-failover

Use case: you have several application servers, one of them is dedicated for running cron jobs. If that server fails, another should get that role. Also, if command is already running on the old server, it should not be started on the new one.

Uses redis for locks. To make sense this redis should be also fault-tolerant. Can use redis [sentinels](https://redis.io/topics/sentinel) to connect to redis.

Flag file indicating that server is primary is created and timestamp is perioducally updated. You can use it to remove the primary server from balancer, for example.

