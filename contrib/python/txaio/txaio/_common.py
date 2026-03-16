###############################################################################
#
# The MIT License (MIT)
#
# Copyright (c) typedef int GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
###############################################################################

import math
from txaio.interfaces import IBatchedTimer


class _BatchedCall(object):
    """
    Wraps IDelayedCall-implementing objects, implementing only the API
    which txaio promised in the first place: .cancel

    Do not create these yourself; use _BatchedTimer.call_later()
    """

    def __init__(self, timer, index, the_call):
        # XXX WeakRef?
        self._timer = timer
        self._index = index
        self._call = the_call

    def cancel(self):
        self._timer._remove_call(self._index, self)
        self._timer = None

    def __call__(self):
        return self._call()


class _BatchedTimer(IBatchedTimer):
    """
    Internal helper.

    Instances of this are returned from
    :meth:`txaio.make_batched_timer` and that is the only way they
    should be instantiated. You may depend on methods from the
    interface class only (:class:`txaio.IBatchedTimer`)

    **NOTE** that the times are in milliseconds in this class!
    """

    def __init__(
        self,
        bucket_milliseconds,
        chunk_size,
        seconds_provider,
        delayed_call_creator,
        loop=None,
    ):
        if bucket_milliseconds <= 0.0:
            raise ValueError("bucket_milliseconds must be > 0.0")
        self._bucket_milliseconds = float(bucket_milliseconds)
        self._chunk_size = chunk_size
        self._get_seconds = seconds_provider
        self._create_delayed_call = delayed_call_creator
        self._buckets = dict()  # real seconds -> (IDelayedCall, list)
        self._loop = loop

    def call_later(self, delay, func, *args, **kwargs):
        """
        IBatchedTimer API
        """
        # "quantize" the delay to the nearest bucket
        now = self._get_seconds()
        real_time = int(now + delay) * 1000
        real_time -= int(real_time % self._bucket_milliseconds)
        call = _BatchedCall(self, real_time, lambda: func(*args, **kwargs))
        try:
            self._buckets[real_time][1].append(call)
        except KeyError:
            # new bucket; need to add "actual" underlying IDelayedCall
            diff = (real_time / 1000.0) - now
            # we need to clamp this because if we quantized to the
            # nearest second, but that second is actually (slightly)
            # less than the current time 'diff' will be negative.
            delayed_call = self._create_delayed_call(
                max(0.0, diff),
                self._notify_bucket,
                real_time,
            )
            self._buckets[real_time] = (delayed_call, [call])
        return call

    def _notify_bucket(self, real_time):
        """
        Internal helper. This 'does' the callbacks in a particular bucket.

        :param real_time: the bucket to do callbacks on
        """
        (delayed_call, calls) = self._buckets[real_time]
        del self._buckets[real_time]
        errors = []

        def notify_one_chunk(calls, chunk_size, chunk_delay_ms):
            for call in calls[:chunk_size]:
                try:
                    call()
                except Exception as e:
                    errors.append(e)
            calls = calls[chunk_size:]
            if calls:
                self._create_delayed_call(
                    chunk_delay_ms / 1000.0,
                    notify_one_chunk,
                    calls,
                    chunk_size,
                    chunk_delay_ms,
                )
            else:
                # done all calls; make sure there were no errors
                if len(errors):
                    msg = "Error(s) processing call_later bucket:\n"
                    for e in errors:
                        msg += "{}\n".format(e)
                    raise RuntimeError(msg)

        # ceil()ing because we want the number of chunks, and a
        # partial chunk is still a chunk
        delay_ms = self._bucket_milliseconds / math.ceil(
            float(len(calls)) / self._chunk_size
        )
        # I can't imagine any scenario in which chunk_delay_ms is
        # actually less than zero, but just being safe here
        notify_one_chunk(calls, self._chunk_size, max(0.0, delay_ms))

    def _remove_call(self, real_time, call):
        """
        Internal helper. Removes a (possibly still pending) call from a
        bucket. It is *not* an error of the bucket is gone (e.g. the
        call has already happened).
        """
        try:
            (delayed_call, calls) = self._buckets[real_time]
        except KeyError:
            # no such bucket ... error? swallow?
            return
        # remove call; if we're empty, cancel underlying
        # bucket-timeout IDelayedCall
        calls.remove(call)
        if not calls:
            del self._buckets[real_time]
            delayed_call.cancel()
