#!/bin/bash

# This file will read list of keys from $1 and fetch them from local mogilefs server and
# then upload them to different mogilefs server at $2
#
# mogilefs-sync.sh /srv/backup/mogile/backup_diff_1503647724 10.3.16.102
#
# diff file
#
# 123805937;/public/data/110/RE0000425/ads/288/RE0000425-17-007345/img/45586615.jpeg;85857;2
#
trap "echo Exited!; exit;" SIGINT SIGTERM

BASE_DIR="/dev/shm/$(uuid)"
DIFF_FILE_PATH="/srv/backup/mogile/backup_diff_$(cat /srv/backup/mogile/backup_info)"

REMOTE_SERVER="${1}:7001"
LOCAL_SERVER="localhost:6001"

DOMAIN="reality.sk"

function retry {
  local n=1
  local max=5
  local delay=2

  echo "====> Running command $@ in retry loop"

  while true; do
    "$@" && break || {
      if [[ $n -lt $max ]]; then
        ((n++))
        echo "Command $@ failed. Retrying $n/$max:"
        sleep $delay;
      else
        echo "Command $@ failed exiting with return code 1."
        return 1
      fi
    }
  done
}

if [ "$#" -ne 1 ]; then
    echo "mogilefs-sync.sh 10.3.16.102"
    exit 1
fi

mkdir -p ${BASE_DIR}
cd ${BASE_DIR}

for key in $(cut -d';' -f 2 ${DIFF_FILE_PATH}); do
    file=$(basename ${key})

    retry mogfileinfo --domain=${DOMAIN} --key=${key} || continue

    echo "==> Fetching file with ${key} to ${file}."
    retry mogfetch --domain=${DOMAIN} --key=${key} --file=${file}

    echo "==> Uploading file ${file} to ${REMOTE_SERVER}/${DOMAIN} under ${key}."
    retry mogupload --tracker=${REMOTE_SERVER} --domain=${DOMAIN} --key=${key} --file=${file}

    unlink ${file};
    echo "====== ====== ======"
done

cp /srv/backup/mogile/base_backup /srv/backup/mogile/base_backup.old
echo "$(basename ${DIFF_FILE_PATH})" > /srv/backup/mogile/base_backup