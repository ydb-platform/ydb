#!/bin/bash
SF="1 100 300 1000 3000 10000 30000 100000"
case $# in 
2) 
	SF=$2; 
	dop=$1; 
	;;
1) 
	dop=$1; 
	;;
*)
	echo "USAGE: $0 <proc> [sf]"
	echo "  work through each SF value using <proc> parallel processes"
	echo "  and the defined DOP settings for bug 55 verification"
	exit
	;;
esac
for sf in $SF
do
./new55.sh $sf "`./dop.sh $sf`" $dop 
done
