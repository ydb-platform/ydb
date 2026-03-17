Getting and Building netCDF {#getting_and_building_netcdf}
=============================

[TOC]

This document is for getting and building the netCDF C library and
utilities, version 4.7.1 and later.  Other libraries that depend on the netCDF C
library, such as the Fortran and C++ libraries, are available as
separate distributions that can be built and installed after the C
library is successfully installed.  The netCDF-Java library is also a
separate distribution that is currently independent of the netCDF C
library.


Getting netCDF-C {#getting}
=========================

* For information regarding the netCDF-Fortran libraries, see \subpage building_netcdf_fortran.

Getting pre-built netCDF-C libraries. {#sec_get_pre_built}
-------------------------------------

The easiest way to get netCDF is through a package management program,
such as rpm, yum, adept, and others. NetCDF is available from many
different repositories, including the default Red Hat and Ubuntu
repositories.

When getting netCDF from a software repository, you will wish to get
the development version of the package ("netcdf-devel"). This includes
the netcdf.h header file.

Pre-release libraries for Windows may be found here: \ref winbin.

Getting the latest netCDF-C Source Code {#sec_get_source}
----------------------------------------

Starting with netCDF-C version 4.3.1, the netCDF-C source code is hosted at the
Unidata GitHub repository, available at http://github.com/Unidata/netcdf-c.

Two options are available for building from source:

- The latest release.
- The developer snapshot.

### The latest release {#sec_latest_release}

The latest release may be downloaded from github at the following location:

- http://github.com/Unidata/netcdf-c/releases

Source files are available in `.tar.gz` and `.zip` formats.

### The developer snapshot {#sec_dev_snapshot}

The developer snapshot may be cloned from github directly by using the `git` command.

> $ git clone http://github.com/Unidata/netcdf-c netcdf-c

**Note:**

> ***The developer snapshot release contains bug-fixes and new
features added since the last full release. It may also contain
portability bugs.***

Once you have downloaded and unpacked the distribution, see the
following section on \ref building.

Building netCDF-C {#building}
===========================

The netCDF-C library and utilities require third-party libraries for
full functionality. (See \ref architecture).
- \ref build_default
- \ref build_classic
- \ref build_hdf4
- \ref build_parallel
- \ref building_netcdf_fortran
- \ref configure_options

Requirements {#netcdf_requirements}
----------------------------------

* HDF5 1.8.15 (netcdf-4 support)
* zlib 1.2.5
* curl 7.18.0 (DAP support)

> Note: If you are working with a development version of the source code, you will need to generate the `configure` script by running the following command from the top-level `netcdf-c/` directory:

> **$ autoreconf -if**


CMake and Windows support {#sub}
--------------------------------

- \ref netCDF-CMake
- \subpage winbin

Building with netCDF-4 and the Remote Data Client {#build_default}
--------------------------------

The usual way of building netCDF requires the HDF5, zlib, and curl
libraries. (And, optionally, the szlib library). Versions required are
at least HDF5 1.8.15, zlib 1.2.5, and curl 7.18.0 or later.
(Optionally, if building with szlib, get szip 2.0 or later.)

These packages are available at:
https://resources.unidata.ucar.edu/netcdf/netcdf-4/

If you wish to use the remote data client code, then you
will also need libcurl, which can be obtained from the <a
href="http://curl.haxx.se/download.html">curl website</a>.

Make sure you run ``make check'' for the HDF5 and zlib
distributions. They are very well-behaved distributions, but sometimes
the build doesn't work (perhaps because of something subtly
misconfigured on the target machine). If one of these libraries is not
working, netCDF will have serious problems.

Note that for building netCDF, it is not necessary to build the HDF5
Fortran, C++, or Java API's.  Only the HDF5 C library is used.

Optionally, you can also build netCDF-4 with the szip library
(a.k.a. szlib). NetCDF cannot create szipped data files, but can read
HDF5 data files that have used szip.
8
There are license restrictions on the use of szip, see the section on
licensing terms in the <a
href="http://www.hdfgroup.org/doc_resource/SZIP/">web page on szip
compression in HDF products</a>. These license restrictions seem to
apply to commercial users who are writing data. (Data readers are not
restricted.) But here at NetCDF World Headquarters, in Sunny Boulder,
Colorado, there are no lawyers, only programmers, so please read the
szip documents for the license agreement to see how it applies to your
situation.

If ``make check'' fails for either zlib or HDF5, the problem must be
resolved before the netCDF-4 installation can continue. For HDF5
problems, see the <a
href="http://www.hdfgroup.org/services/support.html">HDF5 help
services</a>.

Build zlib like this:

~~~
$ ./configure --prefix=/home/username/local
$ make check install
~~~

Then you build HDF5, specifying the location of the zlib library:

~~~
$ ./configure --with-zlib=/home/username/local --prefix=/home/username/local
$ make check install
~~~

In all cases, the installation location specified with the <CODE>--prefix</CODE>
option must be different from the source directory where the software
is being built.

Note that for shared libraries, you may need to add the install
directory to the LD_LIBRARY_PATH environment variable. See
the <a href="https://docs.unidata.ucar.edu/netcdf-c/current/faq.html#Shared%20Libraries">netCDF
FAQ</a> for more details on using shared libraries.

If you are building HDF5 with szip, then include the <CODE>--with-szlib=</CODE>
option, with the directory holding the szip library.

After HDF5 is done, build netcdf, specifying the location of the
HDF5, zlib, and (if built into HDF5) the szip header files and
libraries in the CPPFLAGS and LDFLAGS environment variables. For example:

~~~
$ CPPFLAGS=-I/home/username/local/include LDFLAGS=-L/home/username/local/lib ./configure --prefix=/home/username/local
$ make check install
~~~

The configure script will try to find necessary tools in your
path. When you run configure you may optionally use the <CODE>--prefix</CODE>
argument to change the default installation directory. The above
examples install the zlib, HDF5, and netCDF-4 libraries in
/home/username/local/lib, the header file in /home/username/local/include, and the
utilities in /home/username/local/bin. If you don't provide a <CODE>--prefix</CODE>
option, installation will be in /usr/local/, in subdirectories lib/,
include/, and bin/.  The installation location specified with the
<CODE>--prefix</CODE> option must be different from the source directory where the
software is being built.

Building netCDF with Classic Library Only {#build_classic}
---------------------------------------

It is possible to build the netCDF C libraries and utilities so that
only the netCDF classic, 64-bit offset, and CDF-5 formats are supported, or
the remote data access client is not built. (See \ref netcdf_format)
for more information about the netCDF format variants.  See the <a
href="http://opendap.org/netCDF-DAP">netCDF-DAP site</a>
for more information about remote client access to data
on OPeNDAP servers.)

To build without support for the netCDF-4 formats or the additional
netCDF-4 functions, but with remote access, use:

~~~
$ ./configure --prefix=/home/username/local --disable-netcdf-4
$ make check install
~~~

(Replace `/home/username/local` with the name of the directory where
netCDF is to be installed.  The installation location specified with
the <CODE>--prefix</CODE> option must be different from the source directory where
the software is being built.)

Starting with version 4.1.1 the netCDF C libraries and utilities have
supported remote data access, using the OPeNDAP protocols.  To build
with full support for netCDF-4 APIs and format but without remote
client access, use:

~~~
$ ./configure --prefix=/home/username/local --disable-dap
$ make check install
~~~

To build without netCDF-4 support or remote client access, use:

~~~
$ ./configure --prefix=/home/username/local --disable-netcdf-4 --disable-dap
$ make check install
~~~

If you get the message that netCDF installed correctly, then you are
done!

Building with HDF4 Support {#build_hdf4}
---------------------

The netCDF-4 library can (since version 4.1) read HDF4 data files, if
they were created with the SD (Scientific Data) API.

For this to work, you must build the HDF4 library with the
configure option
~~~
  --disable-netcdf
~~~
to prevent it from building an HDF4 version of the netCDF-2 library
that conflicts with the netCDF-2 functions that are built into the Unidata
netCDF library.

Then, when building netCDF-4, use the
~~~
  --enable-hdf4
~~~
option to configure. The location for the HDF4 header files and
library must be set in the CPPFLAGS and LDFLAGS options.

For HDF4 access to work, the library must be build with netCDF-4
features.

Here's an example, assuming the HDF5 library has been built and
installed in H5DIR and you will build and install the HDF4 library in
H4DIR (which could be the same as H5DIR):

~~~
# Build and install HDF4
$ cd ${HDF4_SOURCE_DIRECTORY}
$ ./configure --enable-shared --disable-netcdf --disable-fortran --prefix=${H4DIR}
$ make
$ make install
$ # Build and install netCDF with HDF4 access enabled
$ cd ${NETCDF_SOURCE_DIRECTORY}
$ CPPFLAGS="-I${H5DIR}/include -I${H4DIR}/include" \
$          LDFLAGS="-L${H5DIR}/lib -L${H4DIR}/lib" \
$ ./configure --enable-hdf4 --enable-hdf4-file-tests
$ make check
$ make install
~~~

Building with Parallel I/O Support {#build_parallel}
--------------

For parallel I/O to work, HDF5 must be installed with
–enable-parallel, and an MPI library (and related libraries) must be
made available to the HDF5 configure. This can be accomplished with
the mpicc wrapper script, in the case of MPICH2.

The following works to build HDF5 with parallel I/O on our netCDF
testing system:

~~~
CC=mpicc ./configure --enable-parallel
make check install
~~~

If the HDF5 used by netCDF has been built with parallel I/O, then
netCDF will also be built with support for parallel I/O. This allows
parallel I/O access to netCDF-4/HDF5 files. Note that shared libraries
are not supported for parallel HDF5, which makes linking more
difficult to get right.  "LIBS=-ldl" is also sometimes needed to link
successfully with parallel HDF5 libraries.
(See /ref netcdf_formats for more information about the netCDF format
variants.)

The following works to build netCDF-4 with parallel I/O on our netCDF
testing system:

~~~
$ H5DIR=/where/parallel/HDF5/was/installed
$ CPPFLAGS="-I${H5DIR}/include"
$ CC=mpicc
$ LDFLAGS=-L${H5DIR}/lib
$ LIBS=-ldl
$ ./configure --disable-shared --enable-parallel-tests
$ make check install
~~~


If parallel I/O access to netCDF classic, 64-bit offset, CDF-5 files is
also needed, the PnetCDF library should also be installed.
(Note: the previously recommended <a
href="https://resources.unidata.ucar.edu/netcdf/contrib/pnetcdf.h">replacement
pnetcdf.h</a> should no longer be used.)  Then configure netCDF with the
"--enable-pnetcdf" option.

Linking to netCDF-C {#linking}
-------------------

For static build, to use netCDF-4 you must link to all the libraries,
netCDF, HDF5, zlib, szip (if used with HDF5 build), and curl (if the
remote access client has not been disabled). This will mean -L options
to your build for the locations of the libraries, and -l (lower-case
L) for the names of the libraries.

For example, one user reports that she can build other applications
with netCDF-4 by setting the LIBS environment variable:

~~~
LIBS='-L/X/netcdf-4.0/lib -lnetcdf -L/X/hdf5-1.8.15/lib -lhdf5_hl -lhdf5 -lz -lm -L/X/szip-2.1/lib -lsz'
~~~

For shared builds, only -lnetcdf is needed. All other libraries will
be found automatically.

The ``nc-config --all'' command can be used to learn what options are
needed for the local netCDF installation.

For example, this works for linking an application named myapp.c with
netCDF-4 libraries:

~~~
cc -o myapp myapp.c `nc-config --cflags --libs`
~~~

configure options {#configure_options}
-----------------------------

These options are used for `autotools`-based builds.  For `cmake` options, see

Note: --disable prefix indicates that the option is normally enabled.
<table>
<tr><th>Option<th>Description<th>Dependencies
<tr><td>--disable-doxygen<td>Disable generation of documentation.<td>doxygen
<tr><td>--disable-fsync<td>disable fsync support<td>kernel fsync support
<tr><td>--enable-valgrind-tests <td>build with valgrind-tests; static builds only<td>valgrind
<tr><td>--enable-netcdf-4<td>build with netcdf-4<td>HDF5 and zlib
<tr><td>--enable-netcdf4<td>synonym for enable-netcdf-4
<tr><td>--enable-hdf4<td>build netcdf-4 with HDF4 read capability<td>HDF4, HDF5 and zlib
<tr><td>--enable-hdf4-file-tests<td>test ability to read HDF4 files<td>selected HDF4 files from Unidata resources site
<tr><td>--enable-pnetcdf<td>build netcdf-4 with parallel I/O for classic, 64-bit offset, and CDF-5 files using PnetCDF
<tr><td>--enable-extra-example-tests<td>Run extra example tests<td>--enable-netcdf-4,GNU sed
<tr><td>--enable-parallel-tests <td>run extra parallel IO tests<td>--enable-netcdf-4, parallel IO support
<tr><td>--enable-logging<td>enable logging capability<td>--enable-netcdf-4
<tr><td>--disable-dap<td>build without DAP client support.<td>libcurl
<tr><td>--disable-dap-remote-tests<td>disable dap remote tests<td>--enable-dap
<tr><td>--enable-dap-long-tests<td>enable dap long tests<td>
<tr><td>--enable-extra-tests<td>run some extra tests that may not pass because of known issues<td>
<tr><td>--enable-ffio<td>use ffio instead of posixio (ex. on the Cray)<td>
<tr><td>--disable-examples<td>don't build the netCDF examples during make check
                          (examples are treated as extra tests by netCDF)<td>
<tr><td>--disable-v2<td>turn off the netCDF version 2 API<td>
<tr><td>--disable-utilities<td>don't build netCDF utilities ncgen, ncdump, and nccopy<td>
<tr><td>--disable-testsets<td>don't build or run netCDF tests<td>
<tr><td>--enable-large-file-tests <td>Run tests which create very large data
		          files<td>~13 GB disk space required, but recovered when
                          tests are complete). See option --with-temp-large to
                          specify temporary directory<td>
<tr><td>--enable-benchmarks<td>Run benchmarks. This is an experimental feature.
			  The benchmarks are a
                          bunch of extra tests, which are timed. We use these
                          tests to check netCDF performance.
    <td>sample data files from the Unidata resources site
<tr><td>--disable-extreme-numbers
<td>don't use extreme numbers during testing, such as MAX_INT - 1<td>
<tr><td>--enable-dll<td>build a win32 DLL<td>mingw compiler
<tr><td>--disable-shared<td>build shared libraries<td>
<tr><td>--disable-static<td>build static libraries<td>
<tr><td>--disable-largefile<td>omit support for large files<td>
<tr><td>--enable-mmap<td>Use mmap to implement NC_DISKLESS<td>
</table>

Build Instructions for netCDF-C using CMake {#netCDF-CMake}
===========================================

## Overview {#cmake_overview}

Starting with netCDF-C 4.3.0, we are happy to announce the inclusion of CMake support.  CMake will allow for building netCDF on a wider range of platforms, include Microsoft Windows with Visual Studio.  CMake support also provides robust unit and regression testing tools.  We will also maintain the standard autotools-based build system in parallel.

In addition to providing new build options for netCDF-C, we will also provide pre-built binary downloads for the shared versions of netCDF for use with Visual Studio.


##  Requirements {#cmake_requirements}
The following packages are required to build netCDF-C using CMake.

* netCDF-C Source Code
* CMake version 2.8.12 or greater.
* Optional Requirements:
	* HDF5 Libraries for netCDF4/HDF5 support.
	* libcurl for DAP support.

<center>
<img src="deptree.jpg" height="250px" />
</center>

## The CMake Build Process {#cmake_build}

There are four steps in the Build Process when using CMake

1. Configuration: Before compiling, the software is configured based on the desired options.
2. Building: Once configuration is complete, the libraries are compiled.
3. Testing: Post-build, it is possible to run tests to ensure the functionality of the netCDF-C libraries.
4. Installation: If all tests pass, the libraries can be installed in the location specified during configuration.

For users who prefer pre-built binaries, installation packages are available at \ref winbin

### Configuration {#cmake_configuration}

The output of the configuration step is a project file based on the appropriate configurator specified.  Common configurators include:

* Unix Makefiles
* Visual Studio
* CodeBlocks
* ... and others

### Common CMake Options {#cmake_common_options}

| **Option** | **Autotools** | **CMake** |
| :------- | :---- | :----- |
Specify Install Location | --prefix=PREFIX | -D"CMAKE\_INSTALL\_PREFIX=PREFIX"
Enable/Disable netCDF-4 | --enable-netcdf-4<br>--disable-netcdf-4 | -D"ENABLE\_NETCDF\_4=ON" <br> -D"ENABLE\_NETCDF\_4=OFF"
Enable/Disable DAP | --enable-dap <br> --disable-dap | -D"ENABLE\_DAP=ON" <br> -D"ENABLE\_DAP=OFF"
Enable/Disable Utilities | --enable-utilities <br> --disable-utilities | -D"BUILD\_UTILITIES=ON" <br> -D"BUILD\_UTILITIES=OFF"
Specify shared/Static Libraries | --enable-shared <br> --enable-static | -D"BUILD\_SHARED\_LIBS=ON" <br> -D"BUILD\_SHARED\_LIBS=OFF"
Enable/Disable Tests | --enable-testsets <br> --disable-testsets | -D"ENABLE\_TESTS=ON" <br> -D"ENABLE\_TESTS=OFF"
Specify a custom library location | Use *CFLAGS* and *LDFLAGS* | -D"CMAKE\_PREFIX\_PATH=/usr/custom_libs/"

A full list of *basic* options can be found by invoking `cmake [Source Directory] -L`. To enable a list of *basic* and *advanced* options, one would invoke `cmake [Source Directory] -LA`.

### Configuring your build from the command line. {#cmake_command_line}

The easiest configuration case would be one in which all of the dependent libraries are installed on the system path (in either Unix/Linux or Windows) and all the default options are desired. From the build directory (often, but not required to be located within the source directory):

> $ cmake [Source Directory]

If you have libraries installed in a custom directory, you may need to specify the **CMAKE\_PREFIX_PATH** variable to tell cmake where the libraries are installed. For example:

> $ cmake [Source Directory] -DCMAKE\_PREFIX\_PATH=/usr/custom_libraries/

## Building {#cmake_building}

The compiler can be executed directly with 'make' or the appropriate command for the configurator which was used.

> $ make

Building can also be executed indirectly via cmake:

> $ cmake --build [Build Directory]

## Testing {#cmake_testing}

Testing can be executed several different ways:

> $ make test

or

> $ ctest

or

> $ cmake --build [Build Directory] --target test

### Installation {#cmake_installation}

Once netCDF has been built and tested, it may be installed using the following commands:

> $ make install

or

> $ cmake --build [Build Directory] --target install

## See Also {#cmake_see_also}

For further information regarding netCDF and CMake, see \ref cmake_faq
