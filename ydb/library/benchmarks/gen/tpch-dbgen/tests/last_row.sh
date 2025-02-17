#!/bin/bash
##
# use named pipes to get the last row of a given DBGEN data chunk
##
usage()
{
	[ $# -gt 1 ] && echo "ERROR: $2"
	echo "USAGE: `basename $1` <T> <SF=1> <DOP=1>"
	echo "	gather final row of table (e.g., -T <T>) at named <SF> and <DOP>"
	exit
}
##
# MAIN
##
SF=1
DOP=1
# parse command line
case $# in
1)
	[ $1 = "-h" ] && usage $0
	[ $1 = "--help" ] && usage $0
	t=$1
	;;
2)
	t=$1
	SF=$2
	;;
3)
	t=$1
	SF=$2
	DOP=$3
	;;
*)
	usage $0 "invalid argument count"
	;;
esac

# assure a setting for DSS_PATH
[ -z "$DSS_PATH" ] && DSS_PATH="."
[ ! -d "$DSS_PATH/$SF" ] && mkdir $DSS_PATH/$SF
chmod 777 $DSS_PATH
DSS_PATH=$DSS_PATH/$SF
export DSS_PATH

# set the other args
case $t in 
	n) f="nation"; DOP=1;;	# special case for tiny table
	r) f="region"; DOP=1;;	# special case for tiny table
	c) f="customer";;
	s) f="supplier";;
	P) f="part";;
	S) f="partsupp";;
	O) f="orders";;
	L) f="lineitem";;
	*) usage "bad table abreviation: $t"
esac
if [ $DOP -eq 1 ]
then PIPE="$DSS_PATH/${f}.tbl"
else PIPE="$DSS_PATH/${f}.tbl.${DOP}"
fi

# create a named pipe for each table
rm -rf $PIPE
mknod $PIPE p
# generate data into it
./dbgen -q -f -s $SF -T $t -S $DOP -C $DOP &
# assure that data is being written to the pipe
sleep 30 
# read the last row from the pipe
tail -1 $PIPE > $f.last_row.$SF.$DOP &
wait
rm $PIPE
