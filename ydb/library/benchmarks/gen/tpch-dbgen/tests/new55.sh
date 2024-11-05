#!/bin/bash
##
# verify data set at different DOP levels
##
usage()
{
	[ $# -gt 1 ] && echo "ERROR: $2"
	echo "USAGE: `basename $1` <SF> <DOP> [<PROCS=4>]"
	echo "     collect last rows for each table at a set of SF and DOP values"
	echo "     and try to keep <PROCS> tasks running at all times"
	echo "     NOTE: multiple values for SF/DOP must be enclosed in quotation marks and separated by spaces"
	exit
}
##
# MAIN
##
PROCS=4
[ -z "$DSS_PATH" ] && DSS_PATH="."
export DSS_PATH

# parse command line
case $# in
	3)	
		PROCS=$3
		DOP=$2
		SF=$1
		;;
	2)	
		DOP=$2
		SF=$1
		;;
	*)	usage $0;;
esac

# generate a task list
CMDS=$$.cmds
rm -f $CMDS
for s in $SF
do
	./gen_tasks.sh $s $DOP >> $CMDS
done

# run parallel tasks to get last rows
echo "Begining `wc -l $CMDS|cut -f1 -d\ ` tasks with PROCS=$PROCS"

if [ ! -x ./dbgen ]
then
	echo "ERROR: dbgen not found in `pwd`"
	exit
fi
./load_balance.sh $CMDS $PROCS
echo "done"

# gather the results by table
for f in *.last_row.*
do
	t=`echo $f | cut -f1 -d\.`
	s=`echo $f | cut -f3 -d\.`
	d=`echo $f | cut -f4 -d\.`
	echo -n "$d	" >> ${DSS_PATH}/$s/results.$t
	cat $f >> ${DSS_PATH}/$s/results.$t
	rm $f
done

# clean up
rm $CMDS
for f in lb_*.out
do
	if [ -s $f ]
	then
		echo "There is output in $f. Not removed"
	else
		rm $f
	fi
done
