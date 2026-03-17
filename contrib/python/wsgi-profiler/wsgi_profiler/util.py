# -*- coding: utf-8 -*-


class MergeStream(object):
    """An object that redirects `write` calls to multiple streams.
    Use this to log to both `sys.stdout` and a file::

        f = open('profiler.log', 'w')
        stream = MergeStream(sys.stdout, f)
        profiler = ProfilerMiddleware(app, stream)
    """

    def __init__(self, *streams):
        if not streams:
            raise TypeError('at least one stream must be given')
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)
