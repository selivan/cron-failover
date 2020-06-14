#!/usr/bin/env python3

import argparse
import time
import logging
import sys
from socket import gethostname
from subprocess import Popen, PIPE

import yaml
import redis
from redis.sentinel import Sentinel

default_config = {
    'redis': '127.0.0.1:6379',
    'sentinels': [],
    'sentinel_master_name': 'mymaster',
    'redis_db_num': 0,
    'timeout_sec': 5,
    'server_key_name': 'cron:server_name',
    'lock_key_prefix': 'cron:lock:',
}

class ObjectView(object):
    def __init__(self, d):
        self.__dict__ = d

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run commands only on selected server, with failover to new if old one became offline. After switching servers new command will not run until the old one is working and holding lock. Uses locks in a single redis instance. Can use sentinels to connect to redis.")
    parser.add_argument('--config', default='cron-ha.yml', help='Configuration in yaml format.')
    parser.add_argument('--debug', action='store_true', default=False, help='Print debug messages')
    parser.add_argument('--hold-primary-lock', action='store_true', default=False, help='Run daemon holding lock in redis saying this server should be used to run commands. If redis connection fails it infinitely tries to reconnect and get lock.')
    parser.add_argument('--command', help='Run this command holding lock in redis. Exit code is the same as command exit code.')
    parser.add_argument('--lock-key', help='Unique key used for this command lock')
    args = parser.parse_args()

    conf = default_config
    with open(args.config, 'r') as config_file:
        conf.update(yaml.safe_load(config_file))
    # Convenience: conf.debug instead of conf['debug']
    conf = ObjectView(conf)

    if args.debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(level=log_level,
                        format='%(asctime)s %(levelname)s %(message)s',  # ISO 8601 time format
                        datefmt='%Y-%m-%dT%H:%M:%S%z')

    if len(conf.sentinels) != 0:
        # support for IPv6 addresses: {::1}:6379
        sentinels = list((''.join(i.split(':')[0:-1]), int(i.split(':')[-1])) for i in conf.sentinels)
    else:
        redis_host, redis_port = ''.join(conf.redis.split(':')[0:-1]), int(conf.redis.split(':')[-1])

    if args.hold_primary_lock:
        while True:
            # Not doing this before because hostname may change while the program is running
            hostname = gethostname()
            try:
                # Get redis master from sentinels
                if 'sentinels' in locals():
                    logging.debug('Asking sentinels for master address')
                    sentinel_conn = Sentinel(sentinels, socket_timeout=0.2)
                    redis_host, redis_port = sentinel_conn.discover_master(conf.sentinel_master_name)
                logging.debug('Connecting to redis')
                redis_conn = redis.Redis(host=redis_host, port=redis_port, db=conf.redis_db_num)
            except redis.RedisError:
                logging.debug('Failed to connect to Redis, sleeping')
                time.sleep(conf.timeout_sec)
            try:
                logging.debug('Trying to set key value to current hostname with expiration if key does not exist')
                # https://redis.io/commands/set
                # nx    do not set value if already set
                # ex    expire time in seconds
                redis_conn.set(name=conf.server_key_name, value=hostname, nx=True, ex=conf.timeout_sec)
                if redis_conn.get(name=conf.server_key_name).decode('utf-8') == hostname:
                    logging.debug('Key value equals hostname, updating expiration period')
                    redis_conn.expire(name=conf.server_key_name, time=conf.timeout_sec)
                logging.debug('Sleeping')
                time.sleep(conf.timeout_sec*0.8)
                redis_conn.close()
            except redis.RedisError:
                pass
    elif hasattr(args, 'command') and hasattr(args, 'lock_key'):
        hostname = gethostname()
        lock_key_name = conf.lock_key_prefix + args.lock_key
        try:
            # Get redis master from sentinels
            if 'sentinels' in locals():
                logging.debug('Asking sentinels for master address')
                sentinel_conn = Sentinel(sentinels, socket_timeout=0.2)
                redis_host, redis_port = sentinel_conn.discover_master(conf.sentinel_master_name)
            logging.debug('Connecting to redis')
            redis_conn = redis.Redis(host=redis_host, port=redis_port, db=conf.redis_db_num)
        except redis.RedisError:
            logging.debug('Failed to connect to Redis')
            raise
        server_key_value = redis_conn.get(conf.server_key_name)
        # If we are on primary server
        if server_key_value is not None and server_key_value.decode('utf-8') == hostname:
            if redis_conn.get(lock_key_name) is None:
                logging.debug('Starting command')
                process = Popen(args.command, shell=True)
                # Run the command holding lock in redis
                while True:
                    if process.poll() is not None:
                        logging.debug('Process finished')
                        redis_conn.close()
                        sys.exit(process.returncode)
                    else:
                        logging.debug('Process still running, reset lock expiration time and sleep')
                        # Do not stop if failed to reset lock expiration time
                        try:
                            redis_conn.set(name=lock_key_name, value=gethostname(), ex=conf.timeout_sec)
                        except redis.RedisError:
                            pass
                        time.sleep(conf.timeout_sec * 0.8)
            else:
                logging.debug('Lock key ' + lock_key_name + ' exists in redis, not starting command')
        else:
            logging.warning('Key ' + conf.server_key_name + ' does not match, not a primary server, so not doing anything')
    else:
        parser.print_help()
        logging.error('Should use one of --hold-primary-lock or --command and --lock-key')
        sys.exit(1)
