* [x] logging - use timestamps
* [x] logging - use something instead of 'root', probably script filename
* [x] logging - use stderr
* [x] touch flag file if server is primary and delete if when is not primary
* [x] manual switch to become a primary server
* [x] remove flag file if server is not primary
* [x] Use hostname + IP instead of just hostname
* [x] strict mode: kill command if failed to update lock in redis. --stop-command-on-redis-fail
  * --stop-signal
  * --stop-timeout-sec
  * --kill-signal (not necessary, always SIGKILL)
* [x] refactor: move common things to functions
