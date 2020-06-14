* [ ] logging - use timestamps
* [ ] logging - use something instead of 'root', probably script filename
* [ ] logging - use stderr  
* [ ] touch flag file if server is primary and delete if when is not primary
* [ ] strict mode: kill command if failed to update lock in redis. --stop-command-on-redis-fail
  * --stop-signal
  * --stop-timeout-sec
  * --kill-signal (not necessary, always SIGKILL)
* [ ] manual switch to become a primary server
