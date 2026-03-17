Blosc supports threading
========================

Threads are the most efficient way to program parallel code for
multi-core processors, but also the more difficult to program well.
Also, they has a non-negligible start-up time that does not fit well
with a high-performance compressor as Blosc tries to be.

In order to reduce the overhead of threads as much as possible, I've
decided to implement a pool of threads (the workers) that are waiting
for the main process (the master) to send them jobs (basically,
compressing and decompressing small blocks of the initial buffer).

Despite this and many other internal optimizations in the threaded
code, it does not work faster than the serial version for buffer sizes
around 64/128 KB or less.  This is for Intel Quad Core2 (Q8400 @ 2.66
GHz) / Linux (openSUSE 11.2, 64 bit), but your mileage may vary (and
will vary!) for other processors / operating systems.

In contrast, for buffers larger than 64/128 KB, the threaded version
starts to perform significantly better, being the sweet point at 1 MB
(again, this is with my setup).  For larger buffer sizes than 1 MB,
the threaded code slows down again, but it is probably due to a cache
size issue and besides, it is still considerably faster than serial
code.

This is why Blosc falls back to use the serial version for such a
'small' buffers.  So, you don't have to worry too much about deciding
whether you should set the number of threads to 1 (serial) or more
(parallel).  Just set it to the number of cores in your processor and
your are done!

Francesc Alted
