#!/bin/sh
YDBD_PATH=${YDBD_PATH:-`pwd`/ydbd/bin/ydbd}
ps aux | grep "${YDBD_PATH}" | grep -v "grep" | awk '{print $2}' | while read line
do
    kill $line
done
