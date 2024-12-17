#!/bin/bash

pairs()
{
x=$1
shift 1
for y in $*;do
    echo Comparing $x to $y
    for ((i=1; i<23; i++));do
       ./cmpq.pl $i ./${x}/q${i}.out ./${y}/q${i}.out
    done
    mkdir -p ${x}_${y}
    mv analysis* ${x}_${y}
done
if [ $# -gt 1 ]; then
  pairs $*
fi
}

if [ $# -eq 0 ]; then
  pairs oracle ingres microsoft ibm
else
  pairs $*
fi

