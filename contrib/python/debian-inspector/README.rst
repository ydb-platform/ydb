=================================
debian-inspector
=================================

Utilities to parse Debian package, copyright and control files

The Python package `debian_inspector` is a collection of utilities to parse
various Debian package manifests, machine readable copyright and control files
collectively known as the Debian 822 format (based on the RFC822 email format).

It was originally named "debut" as a portmanteau of DEBian and UTilities.


Origin
------

This library is based on a heavily modified and remixed combination of original
code with code from other origins:

* python-deb-pkg-tools from @xolox for its handling of Debian packages and the
  parsing of versions, dependencies and other package-to-package relationship
  fields and for its ability to process the syntax of Debian versions and
  compare them. See https://github.com/xolox/python-deb-pkg-tools

* dlt (Debian license tools) from @agustinhenze for its check of the coverage of
  a license file Files sections and a lot of inspiration. See
  https://github.com/agustinhenze/dlt/

* PGPy from @SecurityInnovation and @Commod0re for its ability to remove a PGP
  signature from an email. dsc (Debian Source Control) files are typically
  PGP-signed and are email-like. See https://github.com/SecurityInnovation/PGPy

* python-dpkg by @TheClimateCorporation and @memory for its ability to process
  the syntax of Debian versions and to compare them according to the spec.
  See https://github.com/TheClimateCorporation/python-dpkg


Why?
----

Why create this seemingly redundant library? The official python-debian tools
and several other utilities already provide similar capabilities!

On the surface this is correct, but there are several reasons for a new utility:

* Existing tools parse control files strictly. This library tries to be more
  flexible. For instance it can recognize and fix some almost correct copyright
  files that are not fully "machine readable" but close enough to the spec to be
  worthy of recovery.

* Several of these tools have to deal with legacy compatibility. We do not have
  such a need. For instance, the Python standard library email module and one
  line of code is enough to parse a Debian 822 file fields. I doubt that this
  feature was available in the standard library when python-debian was started
  but it is here now and this vastly simplifies the code.

* The focus of this library is to parse and inspect control files, not to emit
  and create them, so the code and tests can be much simpler. For instance,
  rather than using more complex case-insensitive dictionary keys while
  preserving case for Deb822-like objects, this library uses lower case keys
  throughout.

* The official python-debian library is GPL-licensed making it it hard to
  combine with Apache-licensed libraries. I tried to work out a licensing change
  for python-debian with all its authors such that it could be integrated in
  permissive-licensed Python tools. Even though most current maintainers and
  contributors were OK with that relicensing to a permissive or an LGPL license,
  I could not get a reply and agreement from some important legacy authors:
  therefore the relicensing was not possible.


License
-------

SPDX-License-Identifier: Apache-2.0 AND BSD-3-Clause AND MIT

This software combines software from several origins with different licenses.
All of these licenses apply as all original files have been refactored and remixed
significantly::

    Copyright (c) nexB Inc. and others.
    Copyright (c) Peter Odding.
    Copyright (c) The Climate Corporation (https://climate.com)
    Copyright (c) Security Innovation, Inc
    Copyright (c) Agustin Henze <tin@sluc.org.ar>
