# $Id$

CFLAGS	   = -DTHREADING -g
INCLUDE	   = -I/usr/local/include -I.
LIBS       = -L/usr/local/lib -lssl -lcrypto
#if PYTHON_VERSION
PYVER = $(PYTHON_VERSION)
#else
PYVER = 2.3
#endif
PYINCLUDE  = -DHAVE_CONFIG_H -I/usr/local/include/python$(PYVER) \
	-I/usr/local/lib/python$(PYVER)/config
PYLIB      = /usr/local/lib/python$(PYVER)/config

all:	_m2crypto

_m2crypto: 	_m2crypto.i
	swig -python -shadow _m2crypto.i
	cc -c -fpic $(CFLAGS) $(INCLUDE) $(PYINCLUDE) _m2crypto_wrap.c
	ld -Bshareable -o _m2crypto.so _m2crypto_wrap.o $(LIBS)
	cp m2crypto.py _m2crypto.so ../M2Crypto

clean:
	rm -f *_wrap* *.o *.so _*.py *.pyc

