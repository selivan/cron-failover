# Cron failover

Use case: you have several application servers, one of them is dedicated for running cron jobs. If that server fails, another should get that role. Also, if command is already running on the old server, it should not be started on the new one.

Uses [redis](https://redis.io) for keeping locks. To make sense this redis should be also fault-tolerant. Can use redis [sentinels](https://redis.io/topics/sentinel) to connect to redis.

Flag file indicating that server is primary is created and timestamp is periodically updated. You can use it to remove the primary server from balancer, for example.

# Example

`cron-ha.yml`:

```yaml
sentinels:
  - 'redis1:26379'
  - 'redis2:26379'
  - 'redis3:26379'
sentinel_master_name: mymaster
redis_db_num: 0
timeout_sec: 5
server_key_name: 'cron:server_name'
lock_key_prefix: 'cron:lock:'

```

Start on each server:

`python cron-ha.py --config cron-ha.yml --hold-primary-lock`

First server that gets lock in redis will become primary. Now add to each server crontab or systemd timer:

`python cron-ha.py --config cron-ha.yml --command 'foo --bar --baz' --lock-key foo-bar-baz`

The command will run only on primary server. Script exit code will be the same as command exit code.

Use `--debug` option to see in details what's going on.

**P.S.** If this code is useful for you - don't forget to put a star on it's [github repo](https://github.com/selivan/cron-failover).
