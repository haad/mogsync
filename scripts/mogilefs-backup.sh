#!/bin/bash

export BACKUP_BASE_DIR="/srv/backup/mogile"
export PGPASSWORD="M0gi1eFS"
export DATE=$(date '+%s')

export BACKUP_MOGILEFS_DB="${BACKUP_BASE_DIR}/mogilefs_backup_${DATE}.sql.gz"
export BACKUP_MOGILEFS_TABLE_FILE="${BACKUP_BASE_DIR}/mogilefs_backup_table_file_${DATE}.data"
export BACKUP_MOGILEFS_TABLE_FILE_ON="${BACKUP_BASE_DIR}/mogilefs_backup_table_file_on_${DATE}.data"

#
# BACKUP_DIFF_FILE contains all diferences between previous backup and currently taken one.
export BACKUP_DIFF_FILE="${BACKUP_BASE_DIR}/backup_diff_${DATE}"

#
# BACKUP_INFO_FILE contains last successfull backup DATE string so we know to what we can compare table dumps to.
export BACKUP_INFO_FILE="${BACKUP_BASE_DIR}/backup_info"

echo "=> Taking backup from Mogilefs DB to file: ${BACKUP_MOGILEFS_DB}"
pg_dump -c -h 192.168.106.100 -p 5432 -U mogilefs | gzip -c > ${BACKUP_MOGILEFS_DB}
echo "====== ====== ======"

#
# Order list of files by fid to make sure we always have those added from last backup at the end.
#
echo "=> Taking backup from Mogilefs DB table: file, to file: ${BACKUP_MOGILEFS_TABLE_FILE}"
psql -h 192.168.106.100 -p 5432 -U mogilefs -c "copy(SELECT fid, dkey, length, devcount FROM file ORDER BY fid) to stdout With CSV DELIMITER ';'" > ${BACKUP_MOGILEFS_TABLE_FILE}
#pg_dump -j 2 -h 192.168.106.100 -p 5432 -U mogilefs -a -t file | gzip -c > ${BACKUP_BASE_DIR}/mogilefs_backup_table_file_${DATE}.sql.gz
echo "====== ====== ======"

echo "=> Taking backup from Mogilefs DB table: file_on, to file: ${BACKUP_MOGILEFS_TABLE_FILE_ON}"
psql -h 192.168.106.100 -p 5432 -U mogilefs -c "copy(SELECT fid, devid FROM file_on ORDER BY fid) to stdout With CSV DELIMITER ';'" > ${BACKUP_MOGILEFS_TABLE_FILE_ON}
#pg_dump -h 192.168.106.100 -p 5432 -U mogilefs -a -t file_on | gzip -c > ${BACKUP_BASE_DIR}/mogilefs_backup_table_file_on_${DATE}.sql.gz
echo "====== ====== ======"

if [ -f ${BACKUP_INFO_FILE} ]; then
    export BACKUP_MOGILEFS_TABLE_FILE_LAST="${BACKUP_BASE_DIR}/mogilefs_backup_table_file_$(cat ${BACKUP_INFO_FILE}).data"
    echo "=> Checking number of lines in Mogilefs db table: file backups"
    wc -l ${BACKUP_MOGILEFS_TABLE_FILE_LAST} ${BACKUP_MOGILEFS_TABLE_FILE}
    echo "====== ====== ======"

    echo "=> Creating difference file between: ${BACKUP_MOGILEFS_TABLE_FILE_LAST} - ${BACKUP_MOGILEFS_TABLE_FILE}"
    comm -13 ${BACKUP_MOGILEFS_TABLE_FILE_LAST} ${BACKUP_MOGILEFS_TABLE_FILE} > ${BACKUP_DIFF_FILE}
    echo "====== ====== ======"
fi

echo ${DATE} > ${BACKUP_INFO_FILE}
