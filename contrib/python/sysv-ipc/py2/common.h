#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "structmember.h"

#define PY_STRING_LENGTH_MAX  PY_SSIZE_T_MAX

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/msg.h>

#include "probe_results.h"

/* Struct to contain a key which can be None */
typedef struct {
    int is_none;
    key_t value;
} NoneableKey;


/* These identifiers are prefixed with SVIFP_ which stands for SysV Ipc
For Python. It's really just a random string of letters to prevent clashes
with other constants (as happens with SHM_SIZE on AIX).
*/
enum GET_SET_IDENTIFIERS {
    SVIFP_IPC_PERM_UID = 1,
    SVIFP_IPC_PERM_GID,
    SVIFP_IPC_PERM_CUID,
    SVIFP_IPC_PERM_CGID,
    SVIFP_IPC_PERM_MODE,
    SVIFP_SEM_OTIME,
    SVIFP_SHM_SIZE,
    SVIFP_SHM_LAST_ATTACH_TIME,
    SVIFP_SHM_LAST_DETACH_TIME,
    SVIFP_SHM_LAST_CHANGE_TIME,
    SVIFP_SHM_CREATOR_PID,
    SVIFP_SHM_LAST_AT_DT_PID,
    SVIFP_SHM_NUMBER_ATTACHED,
    SVIFP_MQ_LAST_SEND_TIME,
    SVIFP_MQ_LAST_RECEIVE_TIME,
    SVIFP_MQ_LAST_CHANGE_TIME,
    SVIFP_MQ_CURRENT_MESSAGES,
    SVIFP_MQ_QUEUE_BYTES_MAX,
    SVIFP_MQ_LAST_SEND_PID,
    SVIFP_MQ_LAST_RECEIVE_PID
};

// Shorthand for lazy typists like me
#define IPC_CREX  (IPC_CREAT | IPC_EXCL)

#ifdef SYSV_IPC_DEBUG
#define DPRINTF(fmt, args...) fprintf(stderr, "+++ " fmt, ## args)
#else
#define DPRINTF(fmt, args...)
#endif

/* **************************************************************************
I have to do some guessing about types, mostly with regard to key_t. Since I
schlep these values back and forth between C and Python, I need to know
approximately what types these represent so that I can call the appropriate
Python function to convert them to Python values (e.g. PyInt_FromLong(),
PyFloat_FromDouble(), etc.)

For most of these types the SUSv3 specification states that they're
integer-ish which means I can stuff them into a long and not worry. The macros
below safely convert each type and above each macro I document why my type
assumptions are safe.

Unfortunately, key_t is an exception. The SUSv3 specification doesn't get more
detailed than saying it is "arithmetic", which includes signed and unsigned
short, int, long, long long, float and double (but thankfully excludes
pointers). I feel I can safely ignore long long since support for it is far
from ubiquitous.

Ideally I would cast key_t to double and call PyWhatever_FromDouble() when
sending values to Python. Representing it as a double would ensure no data
loss regardless of whether it is typedef-ed as int, long, float, etc.

In practice, this would be awkward. I have yet to see a platform where
key_t is not integer-ish, so most or all users of this library would be
surprised if it returned keys as floats.

So I store keys as key_t types and cast them to long when Python forces
me to be specific. The two disadvantages to this are, (1) if any platforms
typedef key_t as a float or double, this code will break, and (2) KEY_MIN
and KEY_MAX don't represent the real min and max of keys.

Point #1 is mitigated somewhat because if any OS typedefs key_t as float or
double, this code should complain loudly during compilation.

Typedefs of key_t (for 32 bit systems):
OpenSolaris 2008.11 - int
OS X 10.5.8 - __int32_t (int)
Ubuntu 9.04 - int
Freebsd - l_int (int)
************************************************************************** */


// key_t is guaranteed to be an arithmetic type. Some earlier versions
// of the standard didn't guarantee that it was arithmetic; the standard was
// changed to guarantee that it *is* arithmetic.
// ref: http://pubs.opengroup.org/onlinepubs/009696899/functions/xsh_chap02_12.html
// I assume it is a long; see comment above.
// Some functions return (key_t)-1, so I guess this has to be a signed type.
// ref: http://www.opengroup.org/austin/interps/doc.tpl?gdid=6226
#if PY_MAJOR_VERSION > 2
    #define KEY_T_TO_PY(key)   PyLong_FromLong(key)
#else
    #define KEY_T_TO_PY(key)   PyInt_FromLong(key)
#endif
// SUSv3 guarantees a uid_t to be an integer type. Some functions return
// (uid_t)-1, so I guess this has to be a signed type.
// ref: http://www.opengroup.org/onlinepubs/009695399/basedefs/sys/types.h.html
// ref: http://www.opengroup.org/onlinepubs/9699919799/functions/chown.html
#if PY_MAJOR_VERSION > 2
    #define UID_T_TO_PY(uid)   PyLong_FromLong(uid)
#else
    #define UID_T_TO_PY(uid)   PyInt_FromLong(uid)
#endif

// SUSv3 guarantees a gid_t to be an integer type. Some functions return
// (gid_t)-1, so I guess this has to be a signed type.
// ref: http://www.opengroup.org/onlinepubs/009695399/basedefs/sys/types.h.html
#if PY_MAJOR_VERSION > 2
    #define GID_T_TO_PY(gid)   PyLong_FromLong(gid)
#else
    #define GID_T_TO_PY(gid)   PyInt_FromLong(gid)
#endif

// I'm not sure what guarantees SUSv3 makes about a mode_t, but param 3 of
// shmget() is an int that contains flags in addition to the mode, so
// mode must be able to fit into an int.
// ref: http://www.opengroup.org/onlinepubs/009695399/functions/shmget.html
#if PY_MAJOR_VERSION > 2
    #define MODE_T_TO_PY(mode)   PyLong_FromLong(mode)
#else
    #define MODE_T_TO_PY(mode)   PyInt_FromLong(mode)
#endif

// I'm not sure what guarantees SUSv3 makes about a time_t, but the times
// I deal with here are all guaranteed to be after 1 Jan 1970 which means
// they'll always be positive numbers. A ulong sounds appropriate to me,
// and Python agrees in posixmodule.c.
#if PY_MAJOR_VERSION > 2
    #define TIME_T_TO_PY(time)   PyLong_FromUnsignedLong(time)
#else
    #define TIME_T_TO_PY(time)   py_int_or_long_from_ulong(time)
#endif

// C89 guarantees a size_t to be unsigned and fit into a ulong or smaller.
#if PY_MAJOR_VERSION > 2
    #define SIZE_T_TO_PY(size)   PyLong_FromUnsignedLong(size)
#else
    #define SIZE_T_TO_PY(size)   py_int_or_long_from_ulong(size)
#endif

// SUSv3 guarantees a pid_t to be a signed integer type. Some functions
// return (pid_t)-1 so I guess this has to be signed.
// ref: http://www.opengroup.org/onlinepubs/000095399/basedefs/sys/types.h.html#tag_13_67
#if PY_MAJOR_VERSION > 2
    #define PID_T_TO_PY(pid)   PyLong_FromLong(pid)
#else
    #define PID_T_TO_PY(pid)   PyInt_FromLong(pid)
#endif

// The SUS guarantees a msglen_t to be an unsigned integer type.
// Ditto: msgqnum_t.
// ref: http://www.opengroup.org/onlinepubs/000095399/basedefs/sys/msg.h.html
#if PY_MAJOR_VERSION > 2
    #define MSGLEN_T_TO_PY(msglen)     PyLong_FromUnsignedLong(msglen)
    #define MSGQNUM_T_TO_PY(msgqnum)   PyLong_FromUnsignedLong(msgqnum)
#else
    #define MSGLEN_T_TO_PY(msglen)     py_int_or_long_from_ulong(msglen)
    #define MSGQNUM_T_TO_PY(msgqnum)   py_int_or_long_from_ulong(msgqnum)
#endif

/* Utility functions */
key_t get_random_key(void);
int convert_key_param(PyObject *, void *);
#if PY_MAJOR_VERSION < 3
PyObject *py_int_or_long_from_ulong(unsigned long);
#endif

/* Custom Exceptions/Errors */
extern PyObject *pBaseException;
extern PyObject *pInternalException;
extern PyObject *pPermissionsException;
extern PyObject *pExistentialException;
extern PyObject *pBusyException;
extern PyObject *pNotAttachedException;
