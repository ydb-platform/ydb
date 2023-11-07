#!/bin/bash

usage() {
    echo Usage: `basename $0` ya.make
    exit 0
}

fail() {
    echo Error: $*
    exit 1
}

[ -n "$1" ] || usage
[ "$1" != "-h" ] || usage
[ "$1" != "--help" ] || usage

sed --version > /dev/null 2> /dev/null
if [ $? -eq 1 ];then
  gsed --version > /dev/null 2> /dev/null
  if [ $? -ne 0 ];then
    echo You seem to have an old version of sed installed on your machine. If on MacOS, run \'brew install gnu-sed\' to run this script.
    exit
  fi
  sed=gsed
else
  sed=sed
fi

while [ -n "$1" ]; do

echo $1
[ -e $1 ] || fail "file not found" $1

# ya.make
grep -E -e '^UNITTEST_FOR\(.*\)$' $1 > /dev/null
if [ $? -eq 0 ]; then
    text=`grep -m 1 -E -e '^UNITTEST_FOR\(.*\)$' $1 | sed -r -e 's/.*UNITTEST_FOR\((.*)\).*/\1/'`
    num=`grep -n -m 1 -E -e '^PEERDIR\($' $1 | cut -d: -f1`
    if [[ $num -gt 0 ]]; then
      $sed -i -r -e "${num}a\    ${text}" $1
    fi
    grep -n -E -e '^[[:space:]]*SRCS\([[:space:]]*$' $1 | while read sr
    do
        nums1=$((`echo "$sr" | cut -d: -f1`+1))
        nums2=$((`$sed -n "$nums1,$ p" $1 | grep -n -m 1 -E -e '^[[:space:]]*)[[:space:]]*$' | cut -d: -f1`+$nums1-2))
        $sed -i "$nums1,$nums2 s/^[[:space:]]*//" $1
        $sed -i "$nums1,$nums2 s|^|    $text/|" $1
    done
fi
$sed -i -r -e '/library\/cpp\/testing\/gmock_in_unittest/d' $1
$sed -i -r -e 's/library\/cpp\/testing\/unittest/library\/cpp\/testing\/gtest\n    library\/cpp\/testing\/gmock/' $1
$sed -i -r -e 's/^UNITTEST\(\)$/GTEST()/' $1
$sed -i -r -e 's/^UNITTEST_FOR\(.*\)$/GTEST()/' $1

shift
done