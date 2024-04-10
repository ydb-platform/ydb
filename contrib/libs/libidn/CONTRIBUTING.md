# Contributing to Libidn

This file contains instructions for developers and advanced users that
wants to build from version controlled sources.

We rely on several tools to build the software, including:

- Make <https://www.gnu.org/software/make/>
- C compiler <https://www.gnu.org/software/gcc/>
- Automake <https://www.gnu.org/software/automake/>
- Autoconf <https://www.gnu.org/software/autoconf/>
- Libtool <https://www.gnu.org/software/libtool/>
- Gettext <https://www.gnu.org/software/gettext/>
- Texinfo <https://www.gnu.org/software/texinfo/>
- Gperf <https://www.gnu.org/software/gperf/>
- help2man <https://www.gnu.org/software/help2man/>
- Gengetopt <https://www.gnu.org/software/gengetopt/>
- Tar <https://www.gnu.org/software/tar/>
- Gzip <https://www.gnu.org/software/gzip/>
- Texlive & epsf <https://tug.org/texlive/> (for PDF manual)
- GTK-DOC <https://gitlab.gnome.org/GNOME/gtk-doc> (for API manual)
- Git <https://git-scm.com/>
- Perl <https://www.cpan.org/src/http://www.cpan.org/>
- Valgrind <https://valgrind.org/> (optional)
- OpenJDK (for java port)
- Mono mcs <https://www.mono-project.com/> (for C# port)
- fig2dev <https://sourceforge.net/projects/mcj/>

The software is typically distributed with your operating system, and
the instructions for installing them differ.  Here are some hints:

APT/DPKG-based distributions:
```
apt-get install make git autoconf automake libtool gettext autopoint cvs
apt-get install texinfo texlive texlive-plain-generic texlive-extra-utils
apt-get install help2man gtk-doc-tools dblatex valgrind gengetopt
apt-get install transfig mono-mcs gperf default-jdk-headless
```

DNF/RPM-based distributions:
```
dnf install -y make git autoconf automake libtool gettext-devel cvs
dnf install -y texinfo texinfo-tex texlive
dnf install -y help2man gtk-doc gengetopt dblatex valgrind
dnf install -y gperf java-latest-openjdk-devel
```

On macOS with Xcode and Homebrew:
```
brew install autoconf automake libtool gengetopt help2man texinfo fig2dev
```

To download the version controlled sources:

```
git clone https://git.savannah.gnu.org/git/libidn.git
cd libidn
```

The next step is to import gnulib files, run autoreconf etc:

```
./bootstrap
```

If you have a local checkout of gnulib and wants to avoid download
another copy, you may want to use:

```
./bootstrap --gnulib-refdir=../gnulib
```

Then configure the project as you would normally, for example:

```
./configure --enable-java --enable-gtk-doc-pdf
```

Then build the project:

```
make
make check
```

To prepare releases you need some additional tools:

- Mingw (to produce Windows binaries)
- Wine (to self-check Windows binaries)
- Lcov (to produce coverage HTML pages)
- Zip (to pack Windows binaries)
- Clang (to produce clang analysis)
- Doxygen (to produce doxygen manual)
- pmccabe (to produce cyclomatic code complexity report)
- ncftpput (to upload source tarballs)

APT/DPKG-based distributions:
```
apt-get install mingw-w64 wine binfmt-support lcov zip
apt-get install clang doxygen pmccabe ncftp
```

See README-release on how to make a release.

Happy hacking!

----------------------------------------------------------------------
Copyright (C) 2009-2024 Simon Josefsson
Copying and distribution of this file, with or without modification,
are permitted in any medium without royalty provided the copyright
notice and this notice are preserved.
