# Libidn2 DEPENDENCIES -- Related packages

The following packages should be installed before GNU Libidn is
installed (runtime dependencies that are also build dependencies):

* libintl, part of GNU gettext
  + Not needed on systems with glibc.
    But recommended on all other systems.
    Needed for localization of messages.
  + Homepage:
    https://www.gnu.org/software/gettext/
  + Download:
    https://ftp.gnu.org/gnu/gettext/
  + Pre-built package name:
    - On Debian and Debian-based systems: --,
    - On Red Hat distributions: --.
    - Other: https://repology.org/project/gettext/versions
  + If it is installed in a nonstandard directory, pass the option
    --with-libintl-prefix=DIR to 'configure'.

The following packages should be installed when GNU Libidn is installed
(runtime dependencies, but not build dependencies):

* The Gnulib localizations.
  + Recommended.
    Needed for localization of some of the programs to the user's language.
  + Documentation:
    https://www.gnu.org/software/gnulib/manual/html_node/Localization.html
  + Download:
    https://ftp.gnu.org/gnu/gnulib/gnulib-l10n-*

The following should be installed when GNU libidn is built, but are not
needed later, once it is installed (build dependencies, but not runtime
dependencies):

* A C runtime, compiler, linker, etc.
  + Mandatory.
    Either the platform's native 'cc', or GCC.
  + GCC Homepage:
    https://www.gnu.org/software/gcc/
  + Download:
    https://ftp.gnu.org/gnu/gcc/

* A POSIX-like 'make' utility.
  + Mandatory.
    Either the platform's native 'make' (for in-tree builds only),
    or GNU Make 3.79.1 or newer.
  + GNU Make Homepage:
    https://www.gnu.org/software/make/
  + Download:
    https://ftp.gnu.org/gnu/make/

* A POSIX-like shell
  + Mandatory.
    Either the platform's native 'sh', or Bash.
  + Homepage:
    https://www.gnu.org/software/bash/
  + Download:
    https://ftp.gnu.org/gnu/bash/

* Core POSIX utilities, including:
    [ basename cat chgrp chmod chown cp dd echo expand expr
    false hostname install kill ln ls md5sum mkdir mkfifo
    mknod mv printenv pwd rm rmdir sleep sort tee test touch
    true uname
  + Mandatory.
    Either the platform's native utilities, or GNU coreutils.
  + Homepage:
    https://www.gnu.org/software/coreutils/
  + Download:
    https://ftp.gnu.org/gnu/coreutils/

* The comparison utilities 'cmp' and 'diff'.
  + Mandatory.
    Either the platform's native utilities, or GNU diffutils.
  + Homepage:
    https://www.gnu.org/software/diffutils/
  + Download:
    https://ftp.gnu.org/gnu/diffutils/

* Grep.
  + Mandatory.
    Either the platform's native grep, or GNU grep.
  + Homepage:
    https://www.gnu.org/software/grep/
  + Download:
    https://ftp.gnu.org/gnu/grep/
