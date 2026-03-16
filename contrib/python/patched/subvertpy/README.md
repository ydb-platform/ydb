[![Build Status](https://travis-ci.org/jelmer/subvertpy.png?branch=master)](https://travis-ci.org/jelmer/subvertpy)

Subvertpy
=========

Homepage: https://jelmer.uk/subvertpy/

Python bindings for the Subversion version control system that are aimed to be
complete, fast and feel native to Python programmers.

Bindings are provided for the working copy, client, delta, remote access and
repository APIs. A hookable server side implementation of the custom Subversion
protocol (svn_ra) is also provided.

Differences with similar packages
---------------------------------
subvertpy covers more of the APIs than python-svn. It provides a more
"Pythonic" API than python-subversion, which wraps the Subversion C API pretty
much directly. Neither provide a hookable server-side.

Dependencies
------------
Subvertpy depends on Python 2.7 or 3.5, and Subversion 1.4 or later. It should
work on Windows as well as most POSIX-based platforms (including Linux, BSDs
and Mac OS X).

Installation
------------
Standard distutils are used - use "setup.py build" to build and "setup.install"
to install. On most platforms, setup will find the Python and Subversion
development libraries by itself. On Windows you may have to set various
environment variables, see the next section for details.

Build instructions for Windows
==============================

* Install the SVN dev kit ZIP for Windows from
  http://sourceforge.net/projects/win32svn/files/
  E.g. svn-win32-1.4.6_dev.zip
* Find the SVN binary ZIP file with the binaries for your dev kit.
  E.g. svn-win32-1.4.6.zip
  Unzip this in the *same directory* as the dev kit - README.txt will be
  overwritten, but that is all. This is the default location the .ZIP file
  will suggest (ie, the directory embedded in both .zip files are the same)
* Set SVN_DEV to point at this directory.
* Install BDB.
  For Subversion 1.7.0 and later:
  http://www.oracle.com/technetwork/database/berkeleydb/downloads/index-082944.html
  download Berkeley DB 4.8.30.msi Windows installer and install it.
  For Subversion 1.6.17 and earlier:
  http://subversion.tigris.org/servlets/ProjectDocumentList?folderID=688
  download "db-4.4.20-win32.zip" or earlier version of BDB and extract it.
* Set SVN_BDB to the installed directory or extracted directory.
* Install SVN libintl.
  http://subversion.tigris.org/servlets/ProjectDocumentList?folderID=2627
  Download svn-win32-libintl.zip.
  extract it to the directory that you want.
* Set SVN_LIBINTL to the extract dir.

Development
-----------
If using GCC it might be useful to disable the deprecation warnings when
compiling to see if there are any more serious warnings:

make CFLAGS="-Wno-deprecated-declarations"
