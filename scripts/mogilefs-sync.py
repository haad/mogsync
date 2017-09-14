#!/usr/bin/env python

# This file will read list of keys from $1 and fetch them from local mogilefs server and
# then upload them to different mogilefs server at $2
#
# /usr/local/bin/mogsync.py -r 10.3.16.102:7001 -l localhost:6001 -d reality.sk -j 1 -f /srv/backup/mogile/backup_diff_1504259996
#
# diff file
#
# 123805937;/public/data/110/RE0000425/ads/288/RE0000425-17-007345/img/45586615.jpeg;85857;2
#

import os
import signal
import sys
import csv
import subprocess
import argparse
import uuid
import logging
import tempfile

from multiprocessing import Pool

import retrying
from retrying import retry

from pymogile import Client, MogileFSError

MOGSYNC_VERSION='1.0.0'
MOGILE_SERVER_VERSION='2.72'

BASE_PATH='/dev/shm/'
BASE_BACKUP_FILE='/srv/backup/mogile/base_backup'

def signal_handler(signal, frame):
        logging.info('Mogsync tool exiting')
        sys.exit(0)

class MogSync(object):
    def __init__(self, domain, rnode, lnode, threads, remote_check):
        self.domain = domain
        self.threads = threads

        self.rnode = rnode
        self.rclient = self.get_mogile_client(rnode)

        self.lnode = lnode
        self.lclient = self.get_mogile_client(lnode)

        self.remote_check = remote_check

    @retry(wait_fixed=2000, stop_max_attempt_number=3)
    def get_mogile_client(self, node):
        ''' Initialize Mogilefs client against given node:port combination '''
        logging.debug('Initializing mogilefs client for domain: %s, tracker: %s', self.domain, node)
        client = Client(domain=self.domain, trackers=node.split(','))

        # Test if we can execute simple request on mogilefs server
        if client.sleep(1):
            logging.info('Successfuly connected to mogilefs server %s', node)
            return client
        else:
            raise Exception('Connection to mogilefs server {} failed'.format(node))

    @retry(wait_fixed=1000, stop_max_attempt_number=5)
    def fetch_mogile_file(self, client, key, path):
        ''' Fetch key from a client and save it to file path '''
        logging.debug('Fetching key: %s to a file: %s from tracker %s', key, path,
                      client.last_tracker)
        file_data = client.read_file(key)

        if file_data:
            with open(path, 'a') as mogfile:
                mogfile.write(file_data.read())

            file_data.close()
        else:
            raise Exception('Fetching key: {0} failed retrying...'.format(key))

    @retry(wait_fixed=3000, stop_max_attempt_number=7)
    def upload_mogile_file(self, client, key, path):
        ''' Upload file located on path as a key on a mogilefs server defined by client '''
        logging.debug('Uploading file: %s as a key: %s to tracker %s', path, key,
                      client.last_tracker)
        fp = client.new_file(key)
        with open(path, 'r') as mogfile:
            fp.write(mogfile.read())
        fp.close()

    def unlink_mogile_file(self, path):
        ''' Remove removed file from fs '''
        logging.debug('Removing file %s', path)
        if os.path.isfile(path):
            os.unlink(path)

    def sync_mogile_file(self, key):
        ''' Fetch key from lclient and push it to remote client '''
        logging.debug('Running sync on key: %s', key)

        #
        # remote_check flag means that we will check if file is present on
        # remote server and only if it is not we will upload.
        if self.remote_check and self.rclient.get_paths(key):
            logging.debug('Remote check key: {0}, already present on a remote_server'.format(key))
            return key

        tmppath = tempfile.mkdtemp(prefix=BASE_PATH)
        file_name = '{0}/{1}'.format(tmppath, key.split('/')[-1])
#       os.chdir(tmppath)

        try:
            self.fetch_mogile_file(self.lclient, key, file_name)
            self.upload_mogile_file(self.rclient, key, file_name)
            self.unlink_mogile_file(file_name)
        except Exception as e:
            logging.error('Syncing file %s failed...', key)
            logging.exception('Exception received:')
            sys.stderr.write(key+'\n')

        os.rmdir(tmppath)
        return key

    def go(self, keys):
        p = Pool(self.threads)
        logging.info(p.map(self, keys))

    def __call__(self, x):
        return self.sync_mogile_file(x)


def load_keys_from_file(key_file):
    ''' Load keys from file and extract them to array '''
    keys = []

    logging.info('Loading keys from file %s', key_file)

    with open(key_file, 'r') as mogfile:
        keys = [row[1] for row in csv.reader(mogfile, delimiter=';')]

    return keys


def update_base_backup(key_file):
    ''' update base_backup file to newer version '''
    fn = key_file.split('/')[-1]
    with open(BASE_BACKUP_FILE, 'w') as bfile:
        bfile.write(fn)


def get_version():
    print('MogSync version: {0}, MogileFS Server Version: {1}'.format(
        MOGSYNC_VERSION, MOGILE_SERVER_VERSION))
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--remote-node", type=str, required=True,
                        help='Remote Mogilefs node address:port,address:port')
    parser.add_argument("-l", "--local-node", type=str, default='localhost:6001',
                        help='Local Mogilefs node address:port')
    parser.add_argument("-d", "--domain", type=str, required=True,
                        help='Mogilefs domain used for download and upload')
    parser.add_argument("-j", "--threads", type=int, default=1,
                        help='Number of threads run sync with.')
    parser.add_argument("-f", "--file", type=str, required=True,
                        help='File path to file containing list of keys to fetch/upload.')
    parser.add_argument("-L", "--loglevel", type=str, default='INFO',
                        help='Set logging level to given string.')
    parser.add_argument("--log-file", type=str, default='/tmp/mogsync.log',
                        help='Set log file path.')
    parser.add_argument("-R", "--remote-check", default=False, action='store_true',
                        help='Remotely check if file is present on remote location first then fetch/upload files.')
    parser.add_argument("-b", "--no-update-backup", default=True, action='store_false',
                        help='Update backup file stored at {0}.'.format(BASE_BACKUP_FILE))
    parser.add_argument("-v", "--version", default=False, action='store_true',
                        help='Returns program and server version used.')


    args = parser.parse_args()

    if args.version:
        get_version()

    signal.signal(signal.SIGINT, signal_handler)

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)
    logging.basicConfig(level=numeric_level, filename=args.log_file)

    if not os.path.isfile(args.file):
        logging.error('File containing list of keys to sync is missing %s', args.file)

    mogsync = MogSync(args.domain, args.remote_node, args.local_node,
                      args.threads, args.remote_check)

    keys = load_keys_from_file(args.file)

    mogsync.go(keys)

    if args.no_update_backup:
        update_base_backup(args.file)

if __name__ == "__main__":
    main()
