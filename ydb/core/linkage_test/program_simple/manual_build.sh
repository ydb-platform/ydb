cc -c ../a/lib.cpp -o lib_a.o
cc -c ../b/lib.cpp -o lib_b.o
ar rcs lib_a.a lib_a.o
ar rcs lib_b.a lib_b.o
cc -c ../program/main.cpp main.o
cc lib_a.a lib_b.a main.o 

