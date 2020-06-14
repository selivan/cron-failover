# Cron failover

Use case: you have several application servers, one of them is dedicated for running cron jobs. If that server fails, another should get that role. Also, if command is already running on the old server, it should not be started on the new one.

Uses single Redis instance to keep locks. Of course it should be also fault-tolerant. Can use [sentinels](https://redis.io/topics/sentinel) to connect to Redis.

Flag file indicating that server is primary is created and its timestamp is periodically updated. File is removed when server becomes non-primary. You can use it to remove the primary server from balancer, for example.

## Config

`cron-ha.yml`:

```yaml
sentinels:
  - 'redis1:26379'
  - 'redis2:26379'
  - 'redis3:26379'
sentinel_master_name: mymaster
redis_db_num: 0
timeout_sec: 5 # expiration time for primary server lock and command lock
server_key_name: 'cron:server_name'
lock_key_prefix: 'cron:lock:'
flag_file_is_primary: /tmp/cron-ha-primary-flag # this file is created and updated on primary server and deleted on non-primary
```

## Usage

Note: `--debug` option allows to see in details what's going on.

### Starting

`python cron-ha.py --config cron-ha.yml --cycle-try-get-primary-lock`

First server that gets the lock in redis will become primary. On that server file `/tmp/cron-ha-primary-flag` will be created if not exist and it's modification time will be updated every `timeout_sec`.

### Running commands/jobs

Add to each server crontab or systemd timer:

`python cron-ha.py --config cron-ha.yml --command 'foo --bar --baz' --lock-key foo-bar-baz`

The command will run only on primary server. Script exit code will be the same as command exit code.

If redis becomes unavailable, for example server got offline, command will continue running. If you want to stop the command in that case, use these options:

`python cron-ha.py --config cron-ha.yml --command 'foo --bar --baz' --lock-key foo-bar-baz --stop-command-on-lock-fail --stop-signal 15 --stop-timeout-sec 30 --kill-signal 9` 

### Force server to become primary

`python cron-ha.py --config cron-ha.yml --force-get-primary-lock`

That server will become primary. Flag file will be created on it and deleted on old server. Command will not start on the new server before command with the same `--lock-key` on the old server finishes and releases the lock.

**P.S.** If this code is useful for you - don't forget to put a star on it's [github repo](https://github.com/selivan/cron-failover).
