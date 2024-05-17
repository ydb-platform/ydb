#!/bin/bash
TABLE=$1
IFS="|"

while read line
do
	set $line
	count=1
	while [ $# -gt 0 ]
	do
		echo $1 >> ${TABLE}$count
		count=`expr $count + 1`
		shift
	done
done
