CXX ?= g++-13
CFLAGS = -I../src -std=c++17 -O3
EXE = kvsort qsortavx2 qsortavx512 qsortspr qsorticl

default: all
all : $(EXE)

kvsort: avx512-kv.cpp
	     $(CXX) -o kvsort -mavx512vl -mavx512dq $(CFLAGS) avx512-kv.cpp

qsortavx512: skx-avx2.cpp
	     $(CXX) -o qsortavx512 -mavx512vl -mavx512dq $(CFLAGS) skx-avx2.cpp

qsortavx2: skx-avx2.cpp
	     $(CXX) -o qsortavx2 -mavx2 $(CFLAGS) skx-avx2.cpp

qsorticl: icl-16bit.cpp
	     $(CXX) -o qsorticl -mavx512vl -mavx512bw -mavx512dq -mavx512vbmi2 $(CFLAGS) icl-16bit.cpp

qsortspr: spr-16bit.cpp
	     $(CXX) -o qsortspr -mavx512vl -mavx512dq -mavx512vbmi2 -mavx512fp16  $(CFLAGS) spr-16bit.cpp

clean:
	$(RM) $(EXE)
