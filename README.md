# Cron failover

Use case: you have several application servers, one of them is dedicated for running cron jobs. If that server fails, another should get that role. Also, if command is already running on the old server, it should not be started on the new one.

Uses [redis](https://redis.io) for keeping locks. To make sense this redis should be also fault-tolerant. Can use redis [sentinels](https://redis.io/topics/sentinel) to connect to redis.

Flag file indicating that server is primary is created and timestamp is periodically updated. You can use it to remove the primary server from balancer, for example.

# Example

Start on each server:

`python cron-ha.py --hold-primary-lock`

First server to start this command will become primary. Now add to each server crontab or systemd times:

`python cron-ha.py --command 'foo --bar --baz' --lock-key sleep`

The command will run only on primary server. Script exit code will be the same as command exit code.

Use `--debug` option to see in details what's going on.

**P.S.** If this code is useful for you - don't forget to put a star on it's [github repo](https://github.com/selivan/cron-failover).
