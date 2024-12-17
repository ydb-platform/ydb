#!/bin/bash
DOP=4
#
lock()
{
	while [ true ]
	do
		sleep 1
		[ -f $LOCK ] && continue
		echo $$ > $LOCK
		[ `cat $LOCK` -ne $$ ] && continue
		return
	done
}
unlock()
{
	rm -f $LOCK
}
usage()
{
	echo "USAGE: `basename $0` <task list> [<DOP=4>]"
	echo "	work through commands in <task list>, keeping <DOP> tasks active"
	exit
}
#
###
# MAIN
###
case $# in 
0)	# no args, summarize usage and exit
	usage
	;;
1)	# default DOP
	[ $1 = "-h" ] && usage
	[ $1 = "--help" ] && usage
	TASKS=$1
	;;
2) 	# set DOP and tasks
	TASKS=$1
	DOP=$2
	;;
3)	# worker invocation
	TASKS=$1
	DOP=$2
	JOBID=$3
	;;
esac
# validate commands
if [ ! -f "$TASKS" ]
then 
	echo "ERROR: no list of tasks found"
	exit
fi
# launch children or execute next command
if [ -z "$JOBID" ]
then
	JOBID=$$
	LOCK=$JOBID.lck
	TID=$JOBID.task
	echo 1 > $TID
	unlock
	while [ $DOP -gt 0  ]
	do
		./`basename $0` $TASKS $DOP $JOBID  &
		DOP=`expr $DOP - 1`
	done
	wait
	rm -f $TID $LOCK
else
	LOCK=$JOBID.lck
	TID=$JOBID.task
	while [ true ]
	do
		lock
		CMD_NUM=`cat $TID`
		echo `expr $CMD_NUM + 1` > $TID
		unlock
		CMD=`sed -n ${CMD_NUM}p $TASKS`
		[ -z "$CMD" ] && break
		echo -n "."
		$CMD >> lb_${JOBID}_$DOP.out 2>&1
	done
fi

