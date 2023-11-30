libpqxx
=======

Welcome to libpqxx, the C++ API to the PostgreSQL database management system.

Compiling this package requires PostgreSQL to be installed -- including the C
headers for client development.  The library builds on top of PostgreSQL's
standard C API, libpq, though this fact is almost completely hidden from
programmes which use libpqxx.

As of release 6.0, C++11 is the minimum supported C++ version.  Make sure your
compiler supports this, and if necessary, that you have support for C++11
configured.

**Version 7.0 will require C++17.**  However, it's probably not a problem if
your compiler does not implement C++17 fully.  Initially the 7.x series will
only require some basic C++17 features such as `std::string_view`.  More
advanced use may follow later.

Also, **7.0 will make some breaking changes in rarely used APIs:**
 * All `connection` classes will be folded into a single class.
 * Custom `connection` classes will no longer be supported.
 * Connection reactivation will be explicit: _you_ call `activate()` if needed.
 * "String traits" (for string conversions) will have a slightly different API.

Find libpqxx on Github: https://github.com/jtv/libpqxx


Building libpqxx
----------------

There are three very different ways of building libpqxx:
 1. Using CMake, on any system which supports it.
 2. On Unix-like systems, using a `configure` script.
 3. With Visual Studio on Windows, using supplied project files and headers.

The CMake build should work on any system where CMake is supported.  This is a
recently contributed alternative build, so if you run into problems, your help
could be crucial in fixing them.

The "Unix-like" section applies to systems that look like Unix: GNU/Linux,
Apple OSX and the BSD family, AIX, HP-UX, Irix, Solaris, etc.  Microsoft
Windows with a Unix-like environment such as Cygwin or MinGW installed should
also work in the same way.

There is a separate section below for a Visual C++ build on Windows.  It takes
a bit of work, and if the CMake build works well, we may drop support for the
Windows/Visual C++ build later.


### Using CMake

On CMake the standard way of working is to have the source tree in one
directory, and build in another.  (The `configure` script supports this as
well, but that build is enough work that I didn't bother documenting it.)
Let's say you have the libpqxx source tree in a location `$SOURCE`, and are
building in a different location `$BUILD`.

CMake also lets you choose whether to run the ultimate build through `make`,
or some other tool.  The default on Unix-like systems is `make`, but you may
have to look in the CMake documentation what works well on your system.

For a default build, using those two directories, go into `$BUILD` and run:

```shell
    cmake $SOURCE
```

This sets up the build, in your current directory.

Stay in the `$SOURCE` directory, and run:

```shell
    make
```

If you have multiple cores that you want to put to good use, use the `-j`
option to make it run multiple jobs in parallel.  For instance, if you have 8
CPU cores, you'll probably want to be compiling about 8 files simultaneously:

```shell
    make -j8
```


### On Unix-like systems

For the Unix-like systems the procedure is the standard "configure, make, make
install" sequence.  In order to run the test suite, you'll also need to set up a
database for the tests to play with.

Run the "configure" script with the `--help` option to see build and
installation options.  You need to get these right before you compile.  Then:

```shell
    ./configure	# (plus any options you find appropriate)
    make
```

This will compile the library.  You'll also want to run the test suite to make
sure that everything works.  To prepare for that, you need to set up a
disposable test database that the test suite to play with.  You'll want
password-less authentication so that you won't need to log in for every test.

In this example, the test database is called pqxx-test and runs on a server at
IP address 192.168.1.99.  Before running the test, make sure you can log into
your test database with psql, the command-line SQL shell that comes with
PostgreSQL:

```shell
    PGHOST=192.168.1.99 PGDATฺABASE=pqxx-test psql
```

Once you have that working, use the same login parameters to run the libpqxx
test suite:

```shell
    make PGHOST=192.168.1.99 PGDATABASE=pqxx-test check
```


Assuming that the test suite runs successfully, you are now ready to install.
You'll typically need superuser privileges to run this command:

```shell
    make install
```

Now you should be able to link your own programs with libpqxx.

If something went wrong along the way, or what you have isn't quite what you
wanted, it's time to move on to that fineprint that we hinted at earlier.


#### 1. Configure

A word on the configure script.  It needs to find the C header and the binary
for libpq, the C-level client library, so that the libpqxx build procedure can
make use of them.

The configure script finds these files by running a script called pg\_config
that comes with PostgresQL.  If you have postgres installed, pg\_config should
be somewhere on your system.  It will "know" where the relevant files are.  The
configure script just needs to run it.

Make sure that the folder containing pg\_config is in your executable path
before you run the configure script, or it will fail with a message like:

```
configure: error: PostgreSQL configuration script pg_config was not found.
```

If you don't want to have pg\_config in your path for whatever reason, or you
have multiple PostgreSQL installations on your system (each with their own copy
of pg\_config) and wish to override the default version, add an option like
this to your "configure" command line:

```shell
	PG_CONFIG=/home/me/postgres/bin/pg_config
```

Here, "/home/me/postgres/bin/pg\_config" is just an example of where your
preferred copy of pg\_config might be.  This would tell the configure script
that you wish to build a libpqxx based on the postgres version found in
/home/me/postgres.

About installing: if you wish to install libpqxx in a custom location, such as
your home directory /home/me, you can specify this to the configure script
before you build libpqxx.  You select the installation location using the
configure script's --prefix option, e.g.: 

```shell
	./configure --prefix=/home/me
```

A custom location can be useful to keep locally-build software separate from
packaged software.  Conventional installation locations for custom software on
Unix-like systems are /usr/local and /opt.

Custom installation locations can also be handy if you don't have administrator
rights on the machine you're working on!

The configure scripts supports many other options to tweak how and where
libpqxx is to be built and installed; try the --help option to get an overview
if you're interested.

If configuration just plain won't work for whatever reason: take a look in the
config/sample-headers/ directory.  Here you will find configuration headers for
various compilers and libpq versions.  Pick the config-internal-\*.h and
config-public-\*.h headers for the compiler and libpq version most closely
matching your own, and see if they work for you.  You may also want to tweak
them manually.


#### 2. Make

One problem some people have run into at this stage is that the header files
for PostgreSQL need the OpenSSL header files to be installed.  If this happens
to you, make sure openssl is installed and its headers are in your compiler's
include path.


#### 3. Make Check

"Make check" is where you compile and run the test suite that verifies the
library's functionality.

The "make check" procedure needs a database to play with.  It will create and
drop various tables in that database.  Use a throwaway database for this or
risk losing data!

(Actually the test only manipulates tables whose names start with "pqxx" so in
practice the risk will be small.  But better safe than sorry: use a disposable
test database separate from your own data.)

To direct the test suite to the right database, set some or all of the
following environment variables as needed for "make check":

```
	PGDATABASE	(name of database; defaults to your user name)
	PGHOST		(database server; defaults to local machine)
	PGPORT		(TCP port to connect to; default is 5432)
	PGUSER		(your PostgreSQL user ID; defaults to your login name)
	PGPASSWORD	(your PostgreSQL password, if needed)
```

Further environment variables that may be of use to you are documented in the
libpq documentation and in the manpage for Postgres' command-line client, psql.

Setting environment variables works differently depending on your shell, but
try one of these:

```shell
    VARIABLE=value
    export VARIABLE
```

or

```shell
    set VARIABLE=value
```

Try printing the variable afterwards to make sure.  The command is normally

```shell
    echo $VARIABLE
```

If you set the variable successfully, it should print the value you assigned.
It will print nothing if you failed to set the variable.

On Unix-like systems, postgres may be listening on a Unix domain socket instead
of a TCP port.  The socket will appear as a file somewhere in the filesystem
with a name like .s.PGSQL.5432.  To connect to this type of socket, set PGHOST
to the directory where you find this file, as an absolute path.  For example,
it may be "/tmp" or "/var/run" or "/var/run/postgresql".  The leading slash
tells libpq that this is not a network address but a local Unix socket.


#### 4. Make Install

This is where you install the libpqxx library and header files to your system.

Assuming this succeeds, you should now be able to build your own programs by
adding the location of the header files (e.g. /usr/local/pqxx/include) to your
compiler's include path when compiling your application.  Similarly, add the
location of the library binary (e.g. /usr/local/pqxx/lib) to your library
search path when linking your application.  See the documentation and the test
programs for more information on using libpqxx.

If you link with the dynamic version of the library, you may find that your
program fails to run because the run-time loader cannot find the library.

There are several ways around that.  Pick the first option that works for you:
1. by linking to the static version of the library, or
2. adding a link to the dynamic libpqxx library somewhere in your system's
   standard library locations, or
3. adding libpqxx's lib/ directory to your loader's search path before
   running your program.

On Unix-like systems including GNU/Linux, the loader's search path can be
extended by setting the LD\_LIBRARY\_PATH variable.

Enjoy!


### On Microsoft Windows

Project files for Visual C++ are provided in the win32 directory, along with
some other Windows-specific material.  These are very old, so if you run into
problems, please let us know what we can do to fix them.  One known problem is
that _folder names with spaces in them_ cause trouble.  If you run into
trouble, try using the alternative build using CMake!

As yet another alternative, if you are running a Unix-like environment such as
Cygwin, you may want to try if the Unix build procedure works for you.  In
theory it should be possible to run the configure script and build with Visual
C++ or any other compiler, so long as you have a reasonably Unix-like shell
environment.

If you do proceed with the Visual C++ files, you'll need to copy the most
appropriate compile-time configuration files from various subdirectories in
config/example-headers/ to include/pqxx.  You'll want to tweak them manually
to define the exact features your system, compiler, and PostgreSQL versions
support.  On a Unix-like system the configure script would do this for you.

Before trying to compile with Visual C++, you'll at least need to copy the file
win32/common-sample to win32/common, and edit the latter to reflect the proper
paths to your PostgreSQL headers and the libpq library.  See the win32
subdirectory for more documentation.


#### Manual Configuration: config-\*-\*.h

Normally, on any vaguely Unix-like system, the configuration headers (called
config-internal-\*.h for the library's internal use, config-public-\*.h for
both the library and client programs) are generated from config.h.in.  All
these files, once generated, are situated in the include/pqxx/ directory.

The configitems file lists all configuration items and where they go; but see
win32/INSTALL.txt for a detailed description of how these files work.

Getting the compiler-related configuration right can take several stages of
trying to build, looking at error messages, looking for configuration items
that may be related, changing them, and building again.  If nothing seems to
help, register an issue on Github.  Be sure to read the FAQ though, because
there are some known problems.


#### Windows-Specific Build Problems

One problem specific to Windows is that apparently it doesn't let you free
memory in a DLL that was allocated in the main program or in another DLL, or
vice versa.  This can cause trouble when setting your own notice handlers to
process error or warning output.  Recommended practice is to build libpqxx as
a static library, not a DLL.


Documentation
-------------

The doc/ directory contains API reference documentation and a tutorial, both in
HTML format.  These are also available online.

For more detailed information, look at the header files themselves.  These are
in the include/pqxx/ directory.  The reference documentation is extracted from
the headers using a program called Doxygen.

When learning about programming with libpqxx, you'll want to start off by
reading about the `connection_base` class and its children, as well as the
`transaction_base` class.

For programming examples, take a look at the test programs in the test/
directory.  If you don't know how a certain function or class is used, try
searching the test programs for that name.


Programming with libpqxx
------------------------

Your first program will involve the libpqxx classes "connection" (see headers
`pqxx/connection_base.hxx` and `pqxx/connection.hxx`), and `work` (a
convenience alias for `transaction<>` which conforms to the interface defined
in `pqxx/transaction_base.hxx`).

These `*.hxx` headers are not the ones you include in your program.  Instead,
include the versions without filename suffix (e.g. `pqxx/connection_base`).
Those will include the actual .hxx files for you.  This was done so that
includes are in standard C++ style (as in `<iostream>` etc.), but an editor
will still recognize them as files containing C++ code.

Continuing the list of classes, you will most likely also need the result class
(`pqxx/result.hxx`).  In a nutshell, you create a `connection` based on a
Postgres connection string (see below), create a `work` in the context of that
connection, and run one or more queries on the work which return `result`
objects.  The results are containers of rows of data, each of which you can
treat as an array of strings: one for each field in the row.  It's that simple.

Here is a simple example program to get you going, with full error handling:

```c++
#include <iostream>
#include <pqxx/pqxx>

int main()
{
    try
    {
        pqxx::connection C;
        std::cout << "Connected to " << C.dbname() << std::endl;
        pqxx::work W(C);

        pqxx::result R = W.exec("SELECT name FROM employee");

        std::cout << "Found " << R.size() << "employees:" << std::endl;
        for (auto row: R)
            std::cout << row[0].c_str() << std::endl;

        std::cout << "Doubling all employees' salaries..." << std::endl;
        W.exec("UPDATE employee SET salary = salary*2");

        std::cout << "Making changes definite: ";
        W.commit();
        std::cout << "OK." << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
        return 1;
    }
    return 0;
}
```


Connection strings
------------------

Postgres connection strings state which database server you wish to connect to,
under which username, using which password, and so on.  Their format is defined
in the documentation for libpq, the C client interface for PostgreSQL.
Alternatively, these values may be defined by setting certain environment
variables as documented in e.g. the manual for psql, the command line interface
to PostgreSQL.  Again the definitions are the same for libpqxx-based programs.

The connection strings and variables are not fully and definitively documented
here; this document will tell you just enough to get going.  Check the
PostgreSQL documentation for authoritative information.

The connection string consists of attribute=value pairs separated by spaces,
e.g. "user=john password=1x2y3z4".  The valid attributes include:

- `host`
	Name of server to connect to, or the full file path (beginning with a
	slash) to a Unix-domain socket on the local machine.  Defaults to
	"/tmp".  Equivalent to (but overrides) environment variable PGHOST.

- `hostaddr`
	IP address of a server to connect to; mutually exclusive with "host".

- `port`
	Port number at the server host to connect to, or socket file name
	extension for Unix-domain connections.  Equivalent to (but overrides)
	environment variable PGPORT.

- `dbname`
	Name of the database to connect to.  A single server may host multiple
	databases.  Defaults to the same name as the current user's name.
	Equivalent to (but overrides) environment variable PGDATABASE.

- `user`
	User name to connect under.  This defaults to the name of the current
	user, although PostgreSQL users are not necessarily the same thing as
	system users.

- `requiressl`
	If set to 1, demands an encrypted SSL connection (and fails if no SSL
	connection can be created).

Settings in the connection strings override the environment variables, which in
turn override the default, on a variable-by-variable basis.  You only need to
define those variables that require non-default values.


Linking with libpqxx
--------------------

To link your final program, make sure you link to both the C-level libpq library
and the actual C++ library, libpqxx.  With most Unix-style compilers, you'd do
this using the options

```
	-lpqxx -lpq
```

while linking.  Both libraries must be in your link path, so the linker knows
where to find them.  Any dynamic libraries you use must also be in a place
where the loader can find them when loading your program at runtime.

Some users have reported problems using the above syntax, however, particularly
when multiple versions of libpqxx are partially or incorrectly installed on the
system.  If you get massive link errors, try removing the "-lpqxx" argument from
the command line and replacing it with the name of the libpqxx library binary
instead.  That's typically libpqxx.a, but you'll have to add the path to its
location as well, e.g. /usr/local/pqxx/lib/libpqxx.a.  This will ensure that the
linker will use that exact version of the library rather than one found
elsewhere on the system, and eliminate worries about the exact right version of
the library being installed with your program..
