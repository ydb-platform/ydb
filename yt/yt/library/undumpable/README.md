# undumpable

The undumpable library allows excluding certain memory regions from the core dump.

Linux allows excluding memory from core dumps using the `madvise(DONTNEED)` system call.
But this system call is too expensive to use on every allocation.

Instead, we maintain a list of undumpable regions in memory, and mark them with `madvise(DONTNEED)`
at crash time, from the signal handler.
