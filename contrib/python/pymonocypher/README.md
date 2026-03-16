
# pymonocypher

This python package uses cython to wrap the Monocypher C library. 
Monocypher is an easy to use, easy to deploy, auditable crypto library
written in portable C.  

Normal Python installations have access to a wide
selection of quality crypto libraries.  This python binding is intended to
communicate with other implementations that do use the Monocypher library.
A typical application is to communicate with a microcontroller that 
uses Monocypher.

*   pymonocypher [Source code](https://github.com/jetperch/pymonocypher)
*   Monocypher [official site](https://monocypher.org/)

The Python binding API mirrors the underlying C API, but with 
simplifications to only pass bytes objects, not uint8_t * and length.


## Installation

You can install directly from pypi:

    pip install pymonocypher
    
You can then use pymonocypher:

    python
    >>> import monocypher
    >>> monocypher.blake2b(b'hello world')

###

You may also build a standalone, reproducible Debian package:
```
make deb
```
