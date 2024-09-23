#!/bin/bash
SF=$1
[ -z "$SF" ] && SF = 100

base=`expr $SF \* 2`

for x in 1 2 3 4 7 8 11 13 16 17 32 64
do
	dop=`expr $base \* $x`
	dop=`expr $dop + $x`
	echo -n "$dop "
done
