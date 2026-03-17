# Copyright (c) 2009 Geoffrey Foster. Portions by the Scales Authors.
# pylint: disable=W9921, C0103
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.


"""
A Timer implementation that repeats every interval. Vaguely based on

http://g-off.net/software/a-python-repeatable-threadingtimer-class

Modified to remove the Event signal as we never intend to cancel it.
This was done primarily for compatibility with libraries like gevent.
"""

from six.moves._thread import start_new_thread
from time import sleep


def RepeatTimer(interval, function, iterations=0, *args, **kwargs):
  """Repeating timer. Returns a thread id."""

  def __repeat_timer(interval, function, iterations, args, kwargs):
    """Inner function, run in background thread."""
    count = 0
    while iterations <= 0 or count < iterations:
      sleep(interval)
      function(*args, **kwargs)
      count += 1

  return start_new_thread(__repeat_timer, (interval, function, iterations, args, kwargs))
