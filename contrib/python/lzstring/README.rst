lz-string-python
================

lz-string for python 2/3

Based on the LZ-String javascript found here: http://pieroxy.net/blog/pages/lz-string/index.html

Example
-------
::

  >>> import lzstring
  >>> x = lzstring.LZString()
  >>> compressed = x.compressToBase64(u'你好') # 'gbyl9NI='
  >>> x.decompressFromBase64(compressed) # '你好'

Installation
------------
::

  $ pip install lzstring
