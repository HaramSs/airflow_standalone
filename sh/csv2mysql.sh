#!/bin/bash

CSV_PATH=$1
DEL_DT=$2
password="q123"
user="root"

MYSQL_PWD='q123' mysql --local-infile=1  -u"$user" -p"$password" "$database" <<EOF
DELETE FROM history_db.tmp_cmd_usage WHERE dt='${DEL_DT}';

-- LOAD DATA INFILE '/var/lib/mysql-files/csv.csv'
LOAD DATA LOCAL INFILE '$CSV_PATH'
INTO TABLE history_db.tmp_cmd_usage
CHARACTER SET latin1
FIELDS TERMINATED BY ',' ENCLOSED BY '^' ESCAPED BY '\b'
LINES TERMINATED BY '\n';
EOF
