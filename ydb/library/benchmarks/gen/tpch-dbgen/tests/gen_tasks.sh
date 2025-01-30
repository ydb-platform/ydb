#!/bin/bash
COMMAND="./last_row.sh"
if [ -d "c:/" ]
then COMMAND="./no_tail.sh"
fi

if [ $# -eq 0 ]
then
	echo "USAGE: `basename $0` <SF> [<DOP>...]"
	echo "    generate the commands for the load balancer"
	echo "    if DOP is ommitted, {1,2,3,4,7,8,11,13,16,17,32,64} is used"
	exit
fi

SF=$1
DOP="1 2 3 4 7 8 11 13 16 17 32 64"
shift
[ $# -ge 1 ] && DOP="$*"
for D in $DOP
do
	echo "$COMMAND c $SF $D"
	echo "$COMMAND L $SF $D"
	echo "$COMMAND O $SF $D"
	echo "$COMMAND P $SF $D"
	echo "$COMMAND S $SF $D"
	echo "$COMMAND s $SF $D"
done

