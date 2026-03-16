hurry.filesize
==============

hurry.filesize a simple Python library that can take a number of bytes and
returns a human-readable string with the size in it, in kilobytes (K),
megabytes (M), etc.

The default system it uses is "traditional", where multipliers of 1024
increase the unit size::

  >>> from hurry.filesize import size
  >>> size(1024)
  '1K'

An alternative, slightly more verbose system::

  >>> from hurry.filesize import alternative
  >>> size(1, system=alternative)
  '1 byte'
  >>> size(10, system=alternative)
  '10 bytes'
  >>> size(1024, system=alternative)
  '1 KB'

A verbose system::

  >>> from hurry.filesize import verbose
  >>> size(10, system=verbose)
  '10 bytes'
  >>> size(1024, system=verbose)
  '1 kilobyte'
  >>> size(2000, system=verbose)
  '1 kilobyte'
  >>> size(3000, system=verbose)
  '2 kilobytes'
  >>> size(1024 * 1024, system=verbose)
  '1 megabyte'
  >>> size(1024 * 1024 * 3, system=verbose)
  '3 megabytes'

You can also use the SI system, where multipliers of 1000 increase the unit
size::

  >>> from hurry.filesize import si
  >>> size(1000, system=si)
  '1K'

