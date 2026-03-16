===============================
LZF filter for HDF5, revision 3
===============================

The LZF filter provides high-speed compression with acceptable compression
performance, resulting in much faster performance than DEFLATE, at the
cost of a slightly lower compression ratio. It's appropriate for large
datasets of low to moderate complexity, for which some compression is
much better than none, but for which the speed of DEFLATE is unacceptable.

This filter has been tested against HDF5 versions 1.6.5 through 1.8.3.  It
is released under the BSD license (see LICENSE.txt for details).


Using the filter from HDF5
--------------------------

With HDF5 version 1.8.11 or later the filter can be loaded dynamically by the
HDF5 library.  The filter needs to be compiled as a plugin as described below
that is placed in the default plugin path /usr/local/hdf5/lib/plugin/.  The
plugin path can be overridden with the environment variable HDF5_PLUGIN_PATH.

With older HDF5 versions, or when statically linking the filter to your program,
the filter must be registered manually. There is exactly one new public function
declared in lzf_filter.h, with the following signature:

    int register_lzf(void)

Calling this will register the filter with the HDF5 library.  A non-negative
return value indicates success.  If the registration fails, an error is pushed
onto the current error stack and a negative value is returned.

It's strongly recommended to use the SHUFFLE filter with LZF, as it's
cheap, supported by all current versions of HDF5, and can significantly
improve the compression ratio.  An example C program ("example.c") is included
which demonstrates the proper use of the filter.


Compiling
---------

The filter consists of a single .c file and header, along with an embedded
version of the LZF compression library.  Since the filter is stateless, it's
recommended to statically link the entire thing into your program; for
example:

    $ gcc -O2 lzf/*.c lzf_filter.c myprog.c -lhdf5 -o myprog

It can also be built as a shared library, although you will have to install
the resulting library somewhere the runtime linker can find it:

    $ gcc -O2 -fPIC -shared lzf/*.c lzf_filter.c -lhdf5 -o liblzf_filter.so

A similar procedure should be used for building C++ code.  As in these
examples, using option -O1 or higher is strongly recommended for increased
performance.

With HDF5 version 1.8.11 or later the filter can be dynamically loaded as a
plugin.  The filter is built as a shared library that is *not* linked against
the HDF5 library:

    $ gcc -O2 -fPIC -shared lzf/*.c lzf_filter.c -o liblzf_filter.so


Contact
-------

This filter is maintained as part of the HDF5 for Python (h5py) project.  The
goal of h5py is to provide access to the majority of the HDF5 C API and feature
set from Python.  The most recent version of h5py (1.1) includes the LZF
filter by default.

* Downloads:  https://pypi.org/project/h5py/

* Issue tracker:  https://github.com/h5py/h5py

* Main web site and documentation:  http://h5py.org

* Discussion forum: https://forum.hdfgroup.org/c/hdf5/h5py


History of changes
------------------

Revision 3 (6/25/09)
    Fix issue with changed filter struct definition under HDF5 1.8.3.

Revision 2
    Minor speed enhancement.

Revision 1
    Initial release.
