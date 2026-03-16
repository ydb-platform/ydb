#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "structmember.h"

#include "common.h"
#include "semaphore.h"

#define ONE_BILLION 1000000000

// This enum has to start at zero because its values are used as an
// arry index in sem_perform_semop().
enum SEMOP_TYPE {
    SEMOP_P = 0,
    SEMOP_V,
    SEMOP_Z
};

/* Struct to contain a timeout which can be None */
typedef struct {
    int is_none;
    int is_zero;
    struct timespec timestamp;
} NoneableTimeout;


// It is recommended practice to define this union here in the .c module, but
// it's been common practice for platforms to define it themselves in header
// files. For instance, BSD and OS X do so (provisionally) in sem.h. As a
// result, I need to surround this with an #ifdef. The value _SEM_SEMUN_UNDEFINED
// is written to system_info.h as necessary.
#ifdef _SEM_SEMUN_UNDEFINED
union semun {
    int val;                    /* used for SETVAL only */
    struct semid_ds *buf;       /* for IPC_STAT and IPC_SET */
    unsigned short *array;      /* used for GETALL and SETALL */
#ifdef __linux__
	struct seminfo  *__buf;  	/* Buffer for IPC_INFO (Linux-specific) */
#endif
};
#endif


static int
convert_timeout(PyObject *py_timeout, void *converted_timeout) {
    // Converts a PyObject into a timeout if possible. The PyObject should
    // be None or some sort of numeric value (e.g. int, float, etc.)
    // converted_timeout should point to a NoneableTimeout. When this function
    // returns, if the NoneableTimeout's is_none is true, then the rest of the
    // struct is undefined. Otherwise, the rest of the struct is populated.
    int rc = 0;
    double simple_timeout = 0;
    NoneableTimeout *p_timeout = (NoneableTimeout *)converted_timeout;

    // The timeout can be None or any Python numeric type (float,
    // int, long).
    if (py_timeout == Py_None)
        rc = 1;
    else if (PyFloat_Check(py_timeout)) {
        rc = 1;
        simple_timeout = PyFloat_AsDouble(py_timeout);
    }
    else if (PyLong_Check(py_timeout)) {
        rc = 1;
        simple_timeout = (double)PyLong_AsLong(py_timeout);
    }

    // The timeout may not be negative.
    if ((rc) && (simple_timeout < 0))
        rc = 0;

    if (!rc)
        PyErr_SetString(PyExc_TypeError,
                        "The timeout must be None or a non-negative number");
    else {
        if (py_timeout == Py_None)
            p_timeout->is_none = 1;
        else {
            p_timeout->is_none = 0;

            p_timeout->is_zero = (!simple_timeout);

            // Note the difference between this and POSIX timeouts. System V
            // timeouts expect tv_sec to represent a delta from the current
            // time whereas POSIX semaphores expect an absolute value.
            p_timeout->timestamp.tv_sec = (time_t)floor(simple_timeout);
            p_timeout->timestamp.tv_nsec = (long)((simple_timeout - floor(simple_timeout)) * ONE_BILLION);
        }
    }

    return rc;
}


PyObject *
sem_str(Semaphore *self) {
    return PyUnicode_FromFormat("Key=%ld, id=%d", (long)self->key, self->id);
}


PyObject *
sem_repr(Semaphore *self) {
    return PyUnicode_FromFormat("sysv_ipc.Semaphore(%ld)", (long)self->key);
}


static void
sem_set_error(void) {
    switch (errno) {
        case ENOENT:
        case EINVAL:
            PyErr_SetString(pExistentialException,
                                "No semaphore exists with the specified key");
        break;

        case EEXIST:
            PyErr_SetString(pExistentialException,
                        "A semaphore with the specified key already exists");
        break;

        case EACCES:
            PyErr_SetString(pPermissionsException, "Permission denied");
        break;

        case ERANGE:
            PyErr_Format(PyExc_ValueError,
                "The semaphore's value must remain between 0 and SEMVMX");
        break;

        case EAGAIN:
            PyErr_SetString(pBusyException, "The semaphore is busy");
        break;

        case EIDRM:
            PyErr_SetString(pExistentialException, "The semaphore was removed");
        break;

        case EINTR:
            PyErr_SetString(pBaseException, "Signaled while waiting");
        break;

        case ENOMEM:
            PyErr_SetString(PyExc_MemoryError, "Not enough memory");
        break;

        default:
            PyErr_SetFromErrno(PyExc_OSError);
        break;
    }
}


static PyObject *
sem_perform_semop(enum SEMOP_TYPE op_type, Semaphore *self, PyObject *args, PyObject *keywords) {
    int rc = 0;
    NoneableTimeout timeout;
    struct sembuf op[1];
    /* delta (a.k.a. struct sembuf.sem_op) is a short
       ref: http://www.opengroup.org/onlinepubs/000095399/functions/semop.html
    */
    short int delta;
    char *keyword_list[3][3] = {
                    {"timeout", "delta", NULL},     // P == acquire
                    {"delta", NULL},                // V == release
                    {"timeout", NULL}               // Z == zero test
                };


    /* Initialize this to the default value. If the user doesn't pass a
       timeout, Python won't call convert_timeout() and so the timeout
       will be otherwise uninitialized.
    */
    timeout.is_none = 1;

    /* op_type is P, V or Z corresponding to the 3 Semaphore methods
       that call that call semop(). */
    switch (op_type) {
        case SEMOP_P:
            // P == acquire
            delta = -1;
            rc = PyArg_ParseTupleAndKeywords(args, keywords, "|O&h",
                                             keyword_list[SEMOP_P],
                                             convert_timeout, &timeout,
                                             &delta);

            if (rc && !delta) {
                rc = 0;
                PyErr_SetString(PyExc_ValueError, "The delta must be non-zero");
            }
            else
                delta = -abs(delta);
        break;

        case SEMOP_V:
            // V == release
            delta = 1;
            rc = PyArg_ParseTupleAndKeywords(args, keywords, "|h",
                                             keyword_list[SEMOP_V],
                                             &delta);

            if (rc && !delta) {
                rc = 0;
                PyErr_SetString(PyExc_ValueError, "The delta must be non-zero");
            }
            else
                delta = abs(delta);
        break;

        case SEMOP_Z:
            // Z = Zero test
            delta = 0;
            rc = PyArg_ParseTupleAndKeywords(args, keywords, "|O&",
                                             keyword_list[SEMOP_Z],
                                             convert_timeout, &timeout);
        break;

        default:
            PyErr_Format(pInternalException, "Bad op_type (%d)", op_type);
            rc = 0;
        break;
    }

    if (!rc)
        goto error_return;

    // Now that the caller's params have been vetted, I set up the op struct
    // that I'm going to pass to semop().
    op[0].sem_num = 0;
    op[0].sem_op = delta;
    op[0].sem_flg = self->op_flags;

    Py_BEGIN_ALLOW_THREADS;
#ifdef SEMTIMEDOP_EXISTS
    // Call semtimedop() if appropriate, otherwise call semop()
    if (!timeout.is_none) {
        DPRINTF("calling semtimedop on id %d, op.sem_op=%d, op.flags=0x%x\n",
                self->id, op[0].sem_op, op[0].sem_flg);
        DPRINTF("timeout tv_sec = %ld; timeout tv_nsec = %ld\n",
                timeout.timestamp.tv_sec, timeout.timestamp.tv_nsec);
        rc = semtimedop(self->id, op, 1, &timeout.timestamp);
    }
    else {
        DPRINTF("calling semop on id %d, op.sem_op = %d, op.flags=%x\n",
                self->id, op[0].sem_op, op[0].sem_flg);
        rc = semop(self->id, op, 1);
    }
#else
    // no support for semtimedop(), always call semop() instead.
    DPRINTF("calling semop on id %d, op.sem_op = %d, op.flags=%x\n",
            self->id, op[0].sem_op, op[0].sem_flg);
    rc = semop(self->id, op, 1);
#endif
    Py_END_ALLOW_THREADS;

    if (rc == -1) {
        sem_set_error();
        goto error_return;
    }

    Py_RETURN_NONE;

    error_return:
    return NULL;
}


// cmd can be any of the values defined in the documentation for semctl().
static PyObject *
sem_get_semctl_value(int semaphore_id, int cmd) {
    int rc;

    // semctl() returns an int
    // ref: http://www.opengroup.org/onlinepubs/000095399/functions/semctl.html
    rc = semctl(semaphore_id, 0, cmd);

    if (-1 == rc) {
        sem_set_error();
        goto error_return;
    }

    return PyLong_FromLong(rc);

    error_return:
    return NULL;
}


static PyObject *
sem_get_ipc_perm_value(int id, enum GET_SET_IDENTIFIERS field) {
    struct semid_ds sem_info;
    union semun arg;
    PyObject *py_value = NULL;

    arg.buf = &sem_info;

    // Here I get the values currently associated with the semaphore.
    if (-1 == semctl(id, 0, IPC_STAT, arg)) {
        sem_set_error();
        goto error_return;
    }

    switch (field) {
        case SVIFP_IPC_PERM_UID:
            py_value = UID_T_TO_PY(sem_info.sem_perm.uid);
        break;

        case SVIFP_IPC_PERM_GID:
            py_value = GID_T_TO_PY(sem_info.sem_perm.gid);
        break;

        case SVIFP_IPC_PERM_CUID:
            py_value = UID_T_TO_PY(sem_info.sem_perm.cuid);
        break;

        case SVIFP_IPC_PERM_CGID:
            py_value = GID_T_TO_PY(sem_info.sem_perm.cgid);
        break;

        case SVIFP_IPC_PERM_MODE:
            py_value = MODE_T_TO_PY(sem_info.sem_perm.mode);
        break;

        // This isn't an ipc_perm value but it fits here anyway.
        case SVIFP_SEM_OTIME:
            py_value = TIME_T_TO_PY(sem_info.sem_otime);
        break;

        default:
            PyErr_Format(pInternalException,
                "Bad field %d passed to sem_get_ipc_perm_value", field);
            goto error_return;
        break;
    }

    return py_value;

    error_return:
    return NULL;
}


static int
sem_set_ipc_perm_value(int id, enum GET_SET_IDENTIFIERS field, PyObject *py_value) {
    struct semid_ds sem_info;
    union semun arg;

    arg.buf = &sem_info;

    if (!PyLong_Check(py_value))
    {
        PyErr_Format(PyExc_TypeError, "The attribute must be an integer");
        goto error_return;
    }

    arg.buf = &sem_info;

    /* Here I get the current values associated with the semaphore. It's
       critical to populate sem_info with current values here (rather than
       just using the struct filled with whatever garbage it acquired from
       being declared on the stack) because the call to semctl(...IPC_SET...)
       below will copy uid, gid and mode to the kernel's data structure.
    */
    if (-1 == semctl(id, 0, IPC_STAT, arg)) {
        sem_set_error();
        goto error_return;
    }

    // Below I'm stuffing a Python int converted to a C long into a
    // uid_t, gid_t or mode_t. A long might not fit, hence the explicit
    // cast. If the user passes a value that's too big, tough cookies.
    switch (field) {
        case SVIFP_IPC_PERM_UID:
            sem_info.sem_perm.uid = (uid_t)PyLong_AsLong(py_value);
        break;

        case SVIFP_IPC_PERM_GID:
            sem_info.sem_perm.gid = (gid_t)PyLong_AsLong(py_value);
        break;

        case SVIFP_IPC_PERM_MODE:
            sem_info.sem_perm.mode = (mode_t)PyLong_AsLong(py_value);
        break;

        default:
            PyErr_Format(pInternalException,
                "Bad field %d passed to sem_set_ipc_perm_value", field);
            goto error_return;
        break;
    }

    if (-1 == semctl(id, 0, IPC_SET, arg)) {
        sem_set_error();
        goto error_return;
    }

    return 0;

    error_return:
    return -1;
}


PyObject *
sem_remove(int id) {
    if (NULL == sem_get_semctl_value(id, IPC_RMID))
        return NULL;
    else
        Py_RETURN_NONE;
}


void
Semaphore_dealloc(Semaphore *self) {
    Py_TYPE(self)->tp_free((PyObject*)self);
}

PyObject *
Semaphore_new(PyTypeObject *type, PyObject *args, PyObject *keywords) {
    Semaphore *self;

    self = (Semaphore *)type->tp_alloc(type, 0);

    return (PyObject *)self;
}


int
Semaphore_init(Semaphore *self, PyObject *args, PyObject *keywords) {
    int mode = 0600;
    int initial_value = 0;
    int flags = 0;
    union semun arg;
    char *keyword_list[ ] = {"key", "flags", "mode", "initial_value", NULL};
    NoneableKey key;

    //Semaphore(key, [flags = 0, [mode = 0600, [initial_value = 0]]])

    if (!PyArg_ParseTupleAndKeywords(args, keywords, "O&|iii", keyword_list,
                                     &convert_key_param, &key, &flags,
                                     &mode, &initial_value))
        goto error_return;

    DPRINTF("key is none = %d, key value = %ld\n", key.is_none, (long)key.value);

    if ( !(flags & IPC_CREAT) && (flags & IPC_EXCL) ) {
		PyErr_SetString(PyExc_ValueError,
                "IPC_EXCL must be combined with IPC_CREAT");
        goto error_return;
    }

    if (key.is_none && ((flags & IPC_EXCL) != IPC_EXCL)) {
		PyErr_SetString(PyExc_ValueError,
                "Key can only be None if IPC_EXCL is set");
        goto error_return;
    }

    self->op_flags = 0;

    // I mask the caller's flags against the two IPC_* flags to ensure that
    // nothing funky sneaks into the flags.
    flags &= (IPC_CREAT | IPC_EXCL);

    // Note that Sys V sems can be in "sets" (arrays) but I hardcode this
    // to always be a set with just one member.
    // Permissions and flags (i.e. IPC_CREAT | IPC_EXCL) are both crammed
    // into the 3rd param.
    if (key.is_none) {
        // (key == None) ==> generate a key for the caller
        do {
            errno = 0;
            self->key = get_random_key();

            DPRINTF("Calling semget, key=%ld, mode=%o, flags=%x\n",
                        (long)self->key, mode, flags);
            self->id = semget(self->key, 1, mode | flags);
        } while ( (-1 == self->id) && (EEXIST == errno) );
    }
    else {
        // (key != None) ==> use key supplied by the caller
        self->key = key.value;

        DPRINTF("Calling semget, key=%ld, mode=%o, flags=%x\n",
                    (long)self->key, mode, flags);
        self->id = semget(self->key, 1, mode | flags);
    }

    DPRINTF("id == %d\n", self->id);

    if (self->id == -1) {
        sem_set_error();
        goto error_return;
    }

    // Before attempting to set the initial value, I have to be sure that
    // I created this semaphore and that I have write access to it.
    if ((flags & IPC_CREX) && (mode & 0200)) {
        DPRINTF("setting initial value to %d\n", initial_value);
        arg.val = initial_value;

        if (-1 == semctl(self->id, 0, SETVAL, arg)) {
            sem_set_error();
            goto error_return;
        }
    }

    return 0;

    error_return:
    return -1;
}


PyObject *
Semaphore_P(Semaphore *self, PyObject *args, PyObject *keywords) {
    return sem_perform_semop(SEMOP_P, self, args, keywords);
}


PyObject *
Semaphore_acquire(Semaphore *self, PyObject *args, PyObject *keywords) {
    return Semaphore_P(self, args, keywords);
}


PyObject *
Semaphore_V(Semaphore *self, PyObject *args, PyObject *keywords) {
    return sem_perform_semop(SEMOP_V, self, args, keywords);
}


PyObject *
Semaphore_release(Semaphore *self, PyObject *args, PyObject *keywords) {
    return Semaphore_V(self, args, keywords);
}


PyObject *
Semaphore_Z(Semaphore *self, PyObject *args, PyObject *keywords) {
    return sem_perform_semop(SEMOP_Z, self, args, keywords);
}


PyObject *
Semaphore_remove(Semaphore *self) {
    return sem_remove(self->id);
}

PyObject *
Semaphore_enter(Semaphore *self) {
    PyObject *args = PyTuple_New(0);
    PyObject *retval = NULL;

    if (Semaphore_acquire(self, args, NULL)) {
        retval = (PyObject *)self;
        Py_INCREF(self);
    }

    Py_DECREF(args);

    return retval;
}

PyObject *
Semaphore_exit(Semaphore *self, PyObject *args) {
    PyObject *release_args = PyTuple_New(0);
    PyObject *retval = NULL;

    DPRINTF("exiting context and releasing semaphore %ld\n", (long)self->key);

    retval = Semaphore_release(self, release_args, NULL);

    Py_DECREF(release_args);

    return retval;
}

PyObject *
sem_get_key(Semaphore *self) {
    return KEY_T_TO_PY(self->key);
}

PyObject *
sem_get_value(Semaphore *self) {
    return sem_get_semctl_value(self->id, GETVAL);
}


int
sem_set_value(Semaphore *self, PyObject *py_value)
{
    union semun arg;
    long value;

    if (!PyLong_Check(py_value))
    {
		PyErr_Format(PyExc_TypeError, "Attribute 'value' must be an integer");
        goto error_return;
    }

    value = PyLong_AsLong(py_value);

    DPRINTF("C value is %ld\n", value);

    if ((-1 == value) && PyErr_Occurred()) {
        // No idea what could cause this -- just raise it to the caller.
        goto error_return;
    }

    arg.val = value;

    if (-1 == semctl(self->id, 0, SETVAL, arg)) {
        sem_set_error();
        goto error_return;
    }

    return 0;

    error_return:
    return -1;
}


PyObject *
sem_get_block(Semaphore *self) {
    DPRINTF("op_flags: %x\n", self->op_flags);
    return PyBool_FromLong( (self->op_flags & IPC_NOWAIT) ? 0 : 1);
}


int
sem_set_block(Semaphore *self, PyObject *py_value)
{
    DPRINTF("op_flags before: %x\n", self->op_flags);

    if (PyObject_IsTrue(py_value))
        self->op_flags &= ~IPC_NOWAIT;
    else
        self->op_flags |= IPC_NOWAIT;

    DPRINTF("op_flags after: %x\n", self->op_flags);

    return 0;
}


PyObject *
sem_get_mode(Semaphore *self) {
    return sem_get_ipc_perm_value(self->id, SVIFP_IPC_PERM_MODE);
}


int
sem_set_mode(Semaphore *self, PyObject *py_value) {
    return sem_set_ipc_perm_value(self->id, SVIFP_IPC_PERM_MODE, py_value);
}


PyObject *
sem_get_undo(Semaphore *self) {
    return PyBool_FromLong( (self->op_flags & SEM_UNDO) ? 1 : 0 );
}


int
sem_set_undo(Semaphore *self, PyObject *py_value)
{
    DPRINTF("op_flags before: %x\n", self->op_flags);

    if (PyObject_IsTrue(py_value))
        self->op_flags |= SEM_UNDO;
    else
        self->op_flags &= ~SEM_UNDO;

    DPRINTF("op_flags after: %x\n", self->op_flags);

    return 0;
}


PyObject *
sem_get_uid(Semaphore *self) {
    return sem_get_ipc_perm_value(self->id, SVIFP_IPC_PERM_UID);
}

int
sem_set_uid(Semaphore *self, PyObject *py_value) {
    return sem_set_ipc_perm_value(self->id, SVIFP_IPC_PERM_UID, py_value);
}

PyObject *
sem_get_gid(Semaphore *self) {
    return sem_get_ipc_perm_value(self->id, SVIFP_IPC_PERM_GID);
}

int
sem_set_gid(Semaphore *self, PyObject *py_value) {
    return sem_set_ipc_perm_value(self->id, SVIFP_IPC_PERM_GID, py_value);
}

PyObject *
sem_get_c_uid(Semaphore *self) {
    return sem_get_ipc_perm_value(self->id, SVIFP_IPC_PERM_CUID);
}

PyObject *
sem_get_c_gid(Semaphore *self) {
    return sem_get_ipc_perm_value(self->id, SVIFP_IPC_PERM_CGID);
}

PyObject *
sem_get_last_pid(Semaphore *self) {
    return sem_get_semctl_value(self->id, GETPID);
}

PyObject *
sem_get_waiting_for_nonzero(Semaphore *self) {
    return sem_get_semctl_value(self->id, GETNCNT);
}

PyObject *
sem_get_waiting_for_zero(Semaphore *self) {
    return sem_get_semctl_value(self->id, GETZCNT);
}

PyObject *
sem_get_o_time(Semaphore *self) {
    return sem_get_ipc_perm_value(self->id, SVIFP_SEM_OTIME);
}
