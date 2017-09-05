#!/usr/bin/env python

# This file will read list of keys from $1 and fetch them from local mogilefs server and
# then upload them to different mogilefs server at $2
#
# mogilefs-sync.py -r 10.3.16.102:7001 -l localhost:6001 -d reality.sk -f /srv/backup/mogile/backup_diff_1503647724
#
# diff file
#
# 123805937;/public/data/110/RE0000425/ads/288/RE0000425-17-007345/img/45586615.jpeg;85857;2
#

import os
import csv
import subprocess
import argparse
import uuid
import logging
import tempfile

from multiprocessing import Pool

import retrying

from pymogile import Client, MogileFSError

BASE_PATH='/dev/shm/'

class MogSync(object):
    def __init__(self, domain, rnode, lnode):
        self.domain = domain

        self.rnode = rnode
        self.rclient = self.get_mogile_client(rnode)

        self.lnode = lnode
        self.lclient = self.get_mogile_client(lnode)


    @retry(wait_fixed=2000, stop_max_attempt_number=5)
    def get_mogile_client(self, node):
        logging.debug('Initializing mogilefs client for domain: %s, tracker: %s', domain, node)
        return Client(domain=self.domain, trackers=[node])

    @retry(wait_fixed=2000, stop_max_attempt_number=5)
    def fetch_mogile_file(self, client, key, path):
        ''' Fetch key from a client and save it to file path '''
        logging.debug('Fetching key: %s to a file: %s from tracker %s:%s', key, path,
                      client.last_tracker[0], client.last_tracker[1])
        file_data = client.read_file(key)
        with open(path, 'a') as mogfile:
            mogfile.write(file_data.read())

        file_data.close()

    @retry(wait_fixed=2000, stop_max_attempt_number=5)
    def upload_mogile_file(self, client, key, path):
        ''' Upload file located on path as a key on a mogilefs server defined by client '''
        logging.debug('Uploading file: %s as a key: %s to tracker %s:%s', path, key,
                      client.last_tracker[0], client.last_tracker[1])
        fp = client.new_file(key)
        with open(path, 'r') as mogfile:
            fp.write(mogfile.read())
        fp.close()

    def unlink_mogile_file(self, path):
        ''' Remove removed file from fs '''
        logging.debug('Removing file %s', path)
        os.unlink(path)

    def sync_mogile_file(self, key):
        ''' Fetch key from lclient and push it to remote client '''
        file_name = key.split('/')[-1]
        logging.debug('Running sync on key: %s', key)

        tmppath = tempfile.mkdtemp(prefix=BASE_PATH)
        os.chdir(tmppath)

        self.fetch_mogile_file(self.lclient, key, file_name)
        self.upload_mogile_file(self.rclient, key, file_name)
        self.unlink_mogile_file(file_name)

        os.rmdir(tmppath)
        return key

def load_keys_from_file(key_file):
    ''' Load keys from file and extract them to array '''
    keys = []

    with open(key_file, 'r') as mogfile:
        keys = [row[1] for row in csv.reader(mogfile, delimiter=';')]

    return keys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--remote-node", type=str, required=True,
                        help='Remote Mogilefs node address:port')
    parser.add_argument("-l", "--local-node", type=str, default='localhost:6001',
                        help='Local Mogilefs node address:port')
    parser.add_argument("-d", "--domain", type=str, required=True,
                        help='Mogilefs domain used for download and upload')
    parser.add_argument("-j", "--threads", type=int, default=1,
                        help='Number of threads run sync with.')
    parser.add_argument("-f", "--file", type=str, required=True,
                        help='File path to file containing list of keys to fetch/upload.')
    parser.add_argument("-L", "--loglevel", type=str, default='DEBUG',
                        help='Set logging level to given string.')
    parser.add_argument("--log-file", type=str, default='/tmp/mogsync.log',
                        help='Set log file path.')

    args = parser.parse_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)
    logging.basicConfig(level=numeric_level, filename=args.log_file)

    mogsync = MogSync(args.domain, args.rnode, args.lnode)

    if not os.path.isfile(args.file):
        logging.error('File containing list of keys to sync is missing %s', args.file)

    keys = load_keys_from_file(args.file)

    p = Pool(args.threads)
    print(p.map(mogsync.sync_mogile_file, keys))



if __name__ == "__main__":
    main()
