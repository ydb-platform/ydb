#!/bin/bash
echo "Executing program with following trace query:"
cat example_query.tr
echo -n "Press any key to start program"
read
LWTRACE="example_query.tr" ./lwtrace-example3
