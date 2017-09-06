#!/bin/bash

#
# mogilefs-rsync.sh 10.3.16.101 dev1
#

# Trap interrupts and exit instead of continuing the loop
trap "echo Exited!; exit;" SIGINT SIGTERM

MAX_RETRIES=50
i=0

# Set the initial return value to failure
false

while [ $? -ne 0 -a $i -lt $MAX_RETRIES ]
do
 i=$(($i+1))
 rsync -acv -e 'ssh' /srv/mogile/mogilefs-data/${2}/* root@${1}:/srv/mogilefs-data/
done

if [ $i -eq $MAX_RETRIES ]
then
  echo "Hit maximum number of retries, giving up."
fi