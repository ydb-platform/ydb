## Building `numactl`

TL;DR:

```shell
$ ./autogen.sh
$ ./configure
$ make
# make install
```

Start by configuring the build running the configure script:

```shell
$ ./configure
```

You can pass options to configure to define build options, to pass it
compiler paths, compiler flags and to define the installation layout. Run
`./configure --help` for more details on how to customize the build.

Once build is completed, build `numactl` with:

```shell
$ make
```

If you would like to increase verbosity by printing the full build command
lines, pass `make` the `V=1` parameter:

```shell
$ make V=1
```

You can build and run the tests included with numactl with the following
command:

```shell
$ make check
```

The results will be saved in `test/*.log` files and a `test-suite.log` will be
generated with the summary of test passes and failures.

Install numactl to the system by running the following command as root:

```shell
# make install
```

You can also install it to a staging directory, in which case it is not
required to be root while running the install steps. Just pass a DESTDIR
variable while running `make install` with the path to the staging
directory.

```shell
$ make install DESTDIR=/path/to/staging/numactl
```

## Using a snapshot from the Git repository

First, the build system files need to be generated using the `./autogen.sh`
script, which calls `autoreconf` with the appropriate options to generate the
configure script and the templates for `Makefile`, `config.h`, etc.

Once those files are generated, follow the normal steps to configure and
build numactl.

In order to create a distribution tarball, use `make dist` from a configured
build tree. Use `make distcheck` to build a distribution tarball and confirm
that rebuilding from that archive works as expected, that building from
out-of-tree works, that test cases pass.

