set -ex

mkdir -p temp

cc -c ../a/lib.cpp -o temp/lib_a.o
cc -c ../b/lib.cpp -o temp/lib_b.o
ar rcs temp/lib_a.a temp/lib_a.o
ar rcs temp/lib_b.a temp/lib_b.o
cc -c ../program/main.cpp -o temp/main.o
cc temp/main.o temp/lib_a.a temp/lib_b.a

