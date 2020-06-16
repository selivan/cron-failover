#!/usr/bin/env python3

import argparse
import time
import logging
import sys
import os.path
import socket
from subprocess import Popen, TimeoutExpired
import signal

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
    'flag_file_is_primary': None
}

config_search_paths = [
    '/etc/cron-ha/cron-ha.yml',
    'cron-ha.yml'
]


class ObjectView(object):
    def __init__(self, d):
        self.__dict__ = d


def get_cmdline_args():
    """Parse command line arguments and return namespace"""
    parser = argparse.ArgumentParser(
        description="Run commands only on selected server, with failover to new if old one became offline. After switching servers new command will not run until the old one is working and holding lock. Uses locks in a single redis instance. Can use sentinels to connect to redis.")
    parser.add_argument('--config', default='',
                        help='Configuration in yaml format. Default: ' + ' '.join(config_search_paths))
    parser.add_argument('--debug', action='store_true',
                        default=False, help='Print debug messages')
    parser.add_argument('--cycle-try-get-primary-lock', action='store_true', default=False,
                        help='Run daemon holding lock in redis saying this server should be used to run commands. If redis connection fails it infinitely tries to reconnect and get lock.')
    parser.add_argument('--force-get-primary-lock', action='store_true', default=False,
                        help='Get primary lock for this server.')
    parser.add_argument('--check-is-primary', action='store_true', default=False,
                        help='Check if this server is primary.')
    parser.add_argument(
        '--command', help='Run this command holding lock in redis. Exit code is the same as command exit code.')
    parser.add_argument(
        '--lock-key', help='Unique key used for this command lock')
    parser.add_argument('--stop-command-on-lock-fail', action='store_true', default=False,
                        help='Strict mode: stop command if failed to check the lock in redis')
    parser.add_argument('--stop-signal', type=int, default=15,
                        help='Signal to stop command. Default: 15(SIGTERM)')
    parser.add_argument('--stop-timeout-sec', type=int, default=1,
                        help='Timeout to wait for command to stop. Default: 1')
    parser.add_argument('--kill-signal', type=int, default=9,
                        help='Signal to kill command if it did not stop. Default: 9(SIGKILL)')
    return parser.parse_args()


def get_config(config_file_path, default_config_dict):
    """Return object with configuration values in attributes:
        conf.debug instead conf['debug']"""
    logging.info('Parsing configuration file: ' + config_file_path)

    conf = default_config_dict
    with open(config_file_path, 'r') as config_file:
        conf.update(yaml.safe_load(config_file))

    # support for IPv6 addresses: ::1:6379
    if len(conf['sentinels']) != 0:
        sentinels = list(
            (''.join(i.split(':')[0:-1]), int(i.split(':')[-1])) for i in conf['sentinels'])
        conf['sentinels'] = sentinels
        conf['redis_host'], conf['redis_port'] = None, None
    else:
        conf['redis_host'], conf['redis_port'] = ''.join(
            conf.redis.split(':')[0:-1]), int(conf.redis.split(':')[-1])
        conf['sentinels'] = None
    return ObjectView(conf)


def get_redis_connection(sentinels=None, host=None, port=None, db_num=0):
    """ Connect to redis using sentinels if defined or directly using given host and port.
        :arg sentinels  list of tuples (host, port)
        :arg host   redis host
        :arg port   redis port
        :arg db_num     redis db number
        :return redis.Redis
        :raise redis.RedisError"""
    if sentinels is not None:
        logging.debug('Asking sentinels for master address')
        sentinel_conn = Sentinel(sentinels, socket_timeout=0.2)
        host, port = sentinel_conn.discover_master(conf.sentinel_master_name)
    logging.debug('Connecting to redis at ' + str(host) +
                  ':' + str(port) + ' db=' + str(db_num))
    return redis.Redis(host=host, port=port, db=db_num)


def get_system_id():
    """ Return concatenated hostname and IPv4/IPv6 addresses used for default route.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # nothing is sent over network
        sock.connect(('1.1.1.1', 0))
        ipv4 = sock.getsockname()[0]
        sock.close()
    except Exception as e:
        ipv4 = '127.0.0.1'
    try:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        # nothing is sent over network
        sock.connect(('2606:4700:4700::1001', 0))
        ipv6 = sock.getsockname()[0]
        sock.close()
    except Exception as e:
        ipv6 = '::1'
    return socket.gethostname() + '-' + ipv4 + '-' + ipv6


if __name__ == '__main__':

    args = get_cmdline_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level,
                        format='%(asctime)s %(levelname)s %(message)s',
                        # ISO 8601 time format
                        datefmt='%Y-%m-%dT%H:%M:%S%z',
                        stream=sys.stderr)
    conf = None

    if args.config != '':
        conf = get_config(config_file_path=args.config,
                          default_config_dict=default_config)
    else:
        for i in config_search_paths:
            if os.path.isfile(i):
                conf = get_config(config_file_path=i,
                                  default_config_dict=default_config)
    if conf is None:
        logging.error('Failed to find a config file.')
        sys.exit(1)

    if args.check_is_primary:
        try:
            redis_conn = get_redis_connection(
                sentinels=conf.sentinels, host=conf.redis_host, port=conf.redis_port, db_num=conf.redis_db_num)
        except Exception as e:
            logging.error('Failed to connect to Redis')
            sys.exit(1)

        if redis_conn.get(name=conf.server_key_name).decode('utf-8') == get_system_id():
            print('YES')
            sys.exit(0)
        else:
            print('NO')
            sys.exit(1)
    elif args.force_get_primary_lock:
        try:
            redis_conn = get_redis_connection(
                sentinels=conf.sentinels, host=conf.redis_host, port=conf.redis_port, db_num=conf.redis_db_num)
        except Exception as e:
            logging.error('Failed to connect to Redis')
            sys.exit(1)

        logging.debug(
            'Force set key value to current system id with expiration')
        # https://redis.io/commands/set
        # nx    do not set value if already set
        # ex    expire time in seconds
        redis_conn.set(name=conf.server_key_name,
                       value=get_system_id(), nx=False, ex=conf.timeout_sec)
        redis_conn.close()
        # NOTE: script will not fail if flag_file is not updated
        try:
            if os.path.exists(conf.flag_file_is_primary):
                os.utime(conf.flag_file_is_primary)
            else:
                flag_file = open(conf.flag_file_is_primary, 'w')
                flag_file.write('')
                flag_file.close()
        except Exception as e:
            logging.error('Failed to update flag file modification time')
            logging.error(str(e))
    elif args.cycle_try_get_primary_lock:
        while True:
            # Not doing this before because hostname may change while the program is running
            system_id = get_system_id()
            try:
                redis_conn = get_redis_connection(
                    sentinels=conf.sentinels, host=conf.redis_host, port=conf.redis_port, db_num=conf.redis_db_num)
            except Exception as e:
                logging.debug('Failed to connect to Redis, sleeping')
                time.sleep(conf.timeout_sec)
                continue
            try:
                logging.debug(
                    'Trying to set key value to current system id with expiration if key does not exist')
                # https://redis.io/commands/set
                # nx    do not set value if already set
                # ex    expire time in seconds
                redis_conn.set(name=conf.server_key_name,
                               value=system_id, nx=True, ex=conf.timeout_sec)
                if redis_conn.get(name=conf.server_key_name).decode('utf-8') == system_id:
                    logging.debug(
                        'Key value equals current system id, updating lock expiration period')
                    redis_conn.expire(
                        name=conf.server_key_name, time=conf.timeout_sec)
                    if conf.flag_file_is_primary is not None:
                        logging.debug(
                            'Key value equals current system id, updating modification time for flag file ' + conf.flag_file_is_primary)
                        # NOTE: script will not fail if flag_file is not updated
                        try:
                            if os.path.exists(conf.flag_file_is_primary):
                                os.utime(conf.flag_file_is_primary)
                            else:
                                flag_file = open(
                                    conf.flag_file_is_primary, 'w')
                                flag_file.write('')
                                flag_file.close()
                        except Exception as e:
                            logging.error(
                                'Failed to update flag file modification time')
                            logging.error(str(e))
                else:
                    logging.debug(
                        'Key value does not point to this server as primary')
                    try:
                        if os.path.exists(conf.flag_file_is_primary):
                            logging.debug('Removing flag file ' +
                                          conf.flag_file_is_primary)
                            os.remove(conf.flag_file_is_primary)
                    except Exception as e:
                        pass
                logging.debug('Sleeping')
                time.sleep(conf.timeout_sec*0.8)
                redis_conn.close()
            except redis.RedisError:
                logging.error('Failed to update key in redis. Sleeping before next try.')
                time.sleep(conf.timeout_sec * 0.8)
    elif hasattr(args, 'command') and hasattr(args, 'lock_key'):
        system_id = get_system_id()
        lock_key_name = conf.lock_key_prefix + args.lock_key
        try:
            redis_conn = get_redis_connection(
                sentinels=conf.sentinels, host=conf.redis_host, port=conf.redis_port, db_num=conf.redis_db_num)
        except redis.RedisError:
            logging.debug('Failed to connect to Redis')
            raise
        server_key_value = redis_conn.get(conf.server_key_name)
        # If we are on primary server
        if server_key_value is not None and server_key_value.decode('utf-8') == system_id:
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
                        logging.debug(
                            'Process still running, reset lock expiration time and sleep')
                        # Do not stop if failed to reset lock expiration time
                        try:
                            redis_conn.set(
                                name=lock_key_name, value=get_system_id(), ex=conf.timeout_sec)
                        except redis.RedisError:
                            if args.stop_command_on_lock_fail:
                                logging.error(
                                    'Failed to update lock in redis, strict mode: terminating command with signal ' + str(args.stop_signal))
                                process.send_signal(args.stop_signal)
                                try:
                                    process.wait(timeout=args.stop_timeout_sec)
                                except TimeoutExpired as e:
                                    logging.error(
                                        'Command did not stop after timeout ' + str(args.stop_timeout_sec))
                                    logging.error(
                                        'Trying to kill it with signal ' + str(args.kill_signal))
                                    process.send_signal(args.kill_signal)
                                sys.exit(process.returncode)
                            else:
                                logging.error(
                                    'Failed to update lock in redis, not a strict mode: continue')
                        time.sleep(conf.timeout_sec * 0.8)
            else:
                logging.debug('Lock key ' + lock_key_name +
                              ' exists in redis, not starting command')
                redis_conn.close()
        else:
            logging.warning('Key ' + conf.server_key_name +
                            ' does not match, not a primary server, so not doing anything')
            redis_conn.close()
    else:
        logging.error('Incorrect options. Check --help')
        sys.exit(1)
