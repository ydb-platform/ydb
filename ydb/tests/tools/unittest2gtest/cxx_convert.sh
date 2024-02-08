
#!/bin/bash

usage() {
    echo Usage: `basename $0` *.cpp
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

  $sed -i -r -e 's/<library\/cpp\/testing\/unittest\/registar.h>/<library\/cpp\/testing\/gtest\/gtest.h>\n#include <ydb\/library\/testlib\/unittest_gtest_macro_subst.h>/' $1
  $sed -i -r -e '/<library\/cpp\/testing\/gmock_in_unittest\/gmock.h>/d' $1

  grep -n -E -e 'Y_UNIT_TEST_SUITE *\(.*\)' $1 | while read suiteline
  do
      # echo suiteline: $suiteline
      line=`echo $suiteline | cut -d: -f1`
      first=$(($line+1))
      # echo first: $first
      name=`echo $suiteline | $sed -r -e 's/.*Y_UNIT_TEST_SUITE *\((.*)\).*/\1/'`
      # echo name: $name
      nextsuiteline=`$sed -n $first',$p' $1 | grep -n -E -m 1 -e 'Y_UNIT_TEST_SUITE *\(.*\)' | cut -d: -f1`
      # echo nextsuiteline: $nextsuiteline
      if [[ $nextsuiteline -gt 0 ]]; then
        next=$(($nextsuiteline+$line))
      else
        next='$'
      fi
      # echo next: $next
      $sed -i -r -e $line"s/Y_UNIT_TEST_SUITE *\(.*\)/namespace \/* Unittest suite ${name} *\//" $1
      $sed -i -r -e $first,$next"s/Y_UNIT_TEST *\((.*)\)/TEST(${name}, \1)/" $1
      $sed -i -r -e $first,$next's/Y_UNIT_TEST_F *\((.*), *(.*)\) */TEST_F(\2, \1)/' $1
      # echo
  done

  $sed -i -r -e 's/NUnitTest::TBaseFixture/::testing::Test/' $1

  shift
done