#! /bin/sh

rm -rf build
mkdir -p build/debug
cd build/debug

cmake -DCMAKE_BUILD_TYPE=coverage ../../
make

cd ..

lcov --base-directory .. --directory debug/CMakeFiles --capture --initial --output-file replxx-baseline.info

cd ..
./tests.py
cd -

lcov --base-directory .. --directory debug/CMakeFiles --capture --output-file replxx-test.info
lcov --add-tracefile replxx-baseline.info --add-tracefile replxx-test.info --output-file replxx-total.info
lcov --extract replxx-total.info '*/replxx/src/*' '*/replxx/include/*' --output-file replxx-coverage.info
genhtml replxx-coverage.info --legend --num-spaces=2 --output-directory replxx-coverage-html

