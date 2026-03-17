/*
This is a sample version of system_info.h. See building.md for more information
about why you might find it interesting.

In addition to the values documented below, you can also add this one --

#define SYSV_IPC_DEBUG

Doing so will cause sysv_ipc to print messages to stderr as it runs. Use this
with care; it's a developer-only feature and the implementation isn't very
robust.
*/

/* SYSV_IPC_VERSION should be a quoted string. It's what the module will
report in its VERSION and __version__ attributes.
*/
#define SYSV_IPC_VERSION "1.2.0"

/* PAGE_SIZE is the size (in bytes) of a block of memory. When allocating
shared memory, some systems will round odd sizes up to the next PAGE_SIZE
unit. (e.g. if PAGE_SIZE is 16384, a block that is nominally 10k would still
occupy 16k of shared memory.)

You should be able to get this value via Python using this command --

    python -c "import os; print(os.sysconf('SC_PAGE_SIZE'))"

PAGE_SIZE is already #defined in system header files on many systems, so
it should be surrounded with #ifndef/#endif here.
*/
#ifndef PAGE_SIZE
#define PAGE_SIZE                       16384
#endif

/* _SEM_SEMUN_UNDEFINED should be #defined if the semun union is *not* defined
in a system header file somewhere. If the system doesn't define it, then it's
the responsibility of sysv_ipc's code to do so (which it does in semaphore.c).
*/
#define _SEM_SEMUN_UNDEFINED

/* SEMTIMEDOP_EXISTS should be #defined if and only if the host OS supports
semtimedop(). When semtimedop() is implemented, sysv_ipc enables non-infinite
timeouts for acquire() on Semaphore instances.
(It's not supported on Mac.)
*/
#define SEMTIMEDOP_EXISTS

/* SEMVMX is the maximum value of a semaphore. It's already #defined in
system header files on some systems, so it should be surrounded with
#ifndef/#endif here.

This value only provides information to module users. The module reports it as
sysv_ipc.SEMAPHORE_VALUE_MAX, but that value isn't used anywhere in the
module code, so if it's wrong, that might not matter to you.
*/
#ifndef SEMVMX
#define SEMVMX                          32767
#endif
