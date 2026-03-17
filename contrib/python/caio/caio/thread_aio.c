#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#include "src/threadpool/threadpool.h"


static const unsigned CTX_POOL_SIZE_DEFAULT = 8;
static const unsigned CTX_MAX_REQUESTS_DEFAULT = 512;


static PyTypeObject AIOOperationType;
static PyTypeObject AIOContextType;

typedef struct {
    PyObject_HEAD
    threadpool_t* pool;
    uint16_t max_requests;
    uint8_t pool_size;
} AIOContext;


typedef struct {
    PyObject_HEAD
    PyObject* py_buffer;
    PyObject* callback;
    int opcode;
    unsigned int fileno;
    off_t offset;
    int result;
    uint8_t error;
    uint8_t in_progress;
    Py_ssize_t buf_size;
    char* buf;
    PyObject* ctx;
} AIOOperation;


enum THAIO_OP_CODE {
    THAIO_READ,
    THAIO_WRITE,
    THAIO_FSYNC,
    THAIO_FDSYNC,
    THAIO_NOOP,
};


static void
AIOContext_dealloc(AIOContext *self) {
    if (self->pool != 0) {
        threadpool_t* pool = self->pool;
        self->pool = 0;

        threadpool_destroy(pool, 0);
    }

    Py_TYPE(self)->tp_free((PyObject *) self);
}

/*
    AIOContext.__new__ classmethod definition
*/
static PyObject *
AIOContext_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    AIOContext *self;

    self = (AIOContext *) type->tp_alloc(type, 0);
    return (PyObject *) self;
}

static int
AIOContext_init(AIOContext *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"max_requests", "pool_size", NULL};

    self->pool = NULL;
    self->max_requests = 0;

    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "|HH", kwlist,
            &self->max_requests, &self->pool_size
    )) return -1;

    if (self->max_requests <= 0) {
        self->max_requests = CTX_MAX_REQUESTS_DEFAULT;
    }

    if (self->pool_size <= 0) {
        self->pool_size = CTX_POOL_SIZE_DEFAULT;
    }

    if (self->pool_size > MAX_THREADS) {
        PyErr_Format(
            PyExc_ValueError,
            "pool_size too large. Allowed lower then %d",
            MAX_THREADS
        );
        return -1;
    }

    if (self->max_requests >= (MAX_QUEUE - 1)) {
        PyErr_Format(
            PyExc_ValueError,
            "max_requests too large. Allowed lower then %d",
            MAX_QUEUE - 1
        );
        return -1;
    }

    self->pool = threadpool_create(self->pool_size, self->max_requests, 0);

    if (self->pool == NULL) {
        PyErr_Format(
            PyExc_RuntimeError,
            "Pool initialization failed size=%d max_requests=%d",
            self->pool_size, self->max_requests
        );
        return -1;
    }

    return 0;
}

static PyObject* AIOContext_repr(AIOContext *self) {
    if (self->pool == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Pool not initialized");
        return NULL;
    }
    return PyUnicode_FromFormat(
        "<%s as %p: max_requests=%i, pool_size=%i, ctx=%lli>",
        Py_TYPE(self)->tp_name, self, self->max_requests,
        self->pool_size, self->pool
    );
}


void worker(void *arg) {
    PyGILState_STATE state;

    AIOOperation* op = arg;
    PyObject* ctx = op->ctx;
    op->ctx = NULL;
    op->error = 0;

    if (op->opcode == THAIO_NOOP) {
        state = PyGILState_Ensure();
        op->ctx = NULL;
        Py_DECREF(ctx);
        Py_DECREF(op);
        PyGILState_Release(state);
        return;
    }

    int fileno = op->fileno;
    off_t offset = op->offset;
    int buf_size = op->buf_size;
    char* buf = op->buf;

    int result;

    switch (op->opcode) {
        case THAIO_WRITE:
            result = pwrite(fileno, (const char*) buf, buf_size, offset);
            break;
        case THAIO_FSYNC:
            result = fsync(fileno);
            break;
        case THAIO_FDSYNC:
#ifdef HAVE_FDATASYNC
            result = fdatasync(fileno);
#else
            result = fsync(fileno);
#endif
            break;

        case THAIO_READ:
            result = pread(fileno, buf, buf_size, offset);
            break;
    }

    op->ctx = NULL;
    op->result = result;

    if (result < 0) op->error = errno;

    if (op->opcode == THAIO_READ) {
        op->buf_size = result;
    }
    state = PyGILState_Ensure();
    if (op->callback != NULL) {
        PyObject_CallFunction(op->callback, "i", result);
    }

    if (op->opcode == THAIO_WRITE) {
        Py_DECREF(op->py_buffer);
        op->py_buffer = NULL;
    }

    Py_DECREF(ctx);
    Py_DECREF(op);

    PyGILState_Release(state);
}


inline static int process_pool_error(int code) {
    switch (code) {
        case threadpool_invalid:
            PyErr_SetString(
                PyExc_RuntimeError,
                "Thread pool pointer is invalid"
            );
            return code;
        case threadpool_lock_failure:
            PyErr_SetString(
                PyExc_RuntimeError,
                "Failed to lock thread pool"
            );
            return code;
        case threadpool_queue_full:
            PyErr_Format(
                PyExc_RuntimeError,
                "Thread pool queue full"
            );
            return code;
        case threadpool_shutdown:
            PyErr_SetString(
                PyExc_RuntimeError,
                "Thread pool is shutdown"
            );
            return code;
        case threadpool_thread_failure:
            PyErr_SetString(
                PyExc_RuntimeError,
                "Thread failure"
            );
            return code;
    }

    if (code < 0) PyErr_SetString(PyExc_RuntimeError, "Unknown error");
    return code;
}



PyDoc_STRVAR(AIOContext_submit_docstring,
    "Accepts multiple Operations. Returns \n\n"
    "    Operation.submit(aio_op1, aio_op2, aio_opN, ...) -> int"
);
static PyObject* AIOContext_submit(
    AIOContext *self, PyObject *args
) {
    if (self == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "self is NULL");
        return NULL;
    }

    if (self->pool == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "self->pool is NULL");
        return NULL;
    }

    if (!PyTuple_Check(args)) {
        PyErr_SetNone(PyExc_ValueError);
        return NULL;
    }

    unsigned int nr = PyTuple_Size(args);

    PyObject* obj;
    AIOOperation* ops[nr];
    unsigned int i;

    for (i=0; i < nr; i++) {
        obj = PyTuple_GetItem(args, i);
        if (PyObject_TypeCheck(obj, &AIOOperationType) == 0) {
            PyErr_Format(
                PyExc_TypeError,
                "Wrong type for argument %d", i
            );

            return NULL;
        }

        ops[i] = (AIOOperation*) obj;
        ops[i]->ctx = (void*) self;
    }

    unsigned int j=0;
    int result = 0;

    for (i=0; i < nr; i++) {
        if (ops[i]->in_progress) continue;
        ops[i]->in_progress = 1;
        Py_INCREF(ops[i]);
        Py_INCREF(ops[i]->ctx);
        result = threadpool_add(self->pool, worker, (void*) ops[i], 0);
        if (process_pool_error(result) < 0) return NULL;
        j++;
    }

    return (PyObject*) PyLong_FromSsize_t(j);
}


PyDoc_STRVAR(AIOContext_cancel_docstring,
    "Cancels multiple Operations. Returns \n\n"
    "    Operation.cancel(aio_op1, aio_op2, aio_opN, ...) -> int\n\n"
    "(Always returns zero, this method exists for compatibility reasons)"
);
static PyObject* AIOContext_cancel(
    AIOContext *self, PyObject *args
) {
    return (PyObject*) PyLong_FromSsize_t(0);
}


/*
    AIOContext properties
*/
static PyMemberDef AIOContext_members[] = {
    {
        "pool_size",
        T_INT,
        offsetof(AIOContext, pool_size),
        READONLY,
        "pool_size"
    },
    {
        "max_requests",
        T_USHORT,
        offsetof(AIOContext, max_requests),
        READONLY,
        "max requests"
    },
    {NULL}  /* Sentinel */
};

static PyMethodDef AIOContext_methods[] = {
    {
        "submit",
        (PyCFunction) AIOContext_submit, METH_VARARGS,
        AIOContext_submit_docstring
    },
    {
        "cancel",
        (PyCFunction) AIOContext_cancel, METH_VARARGS,
        AIOContext_cancel_docstring
    },
    {NULL}  /* Sentinel */
};

static PyTypeObject
AIOContextType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "Context",
    .tp_doc = "thread aio context",
    .tp_basicsize = sizeof(AIOContext),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = AIOContext_new,
    .tp_init = (initproc) AIOContext_init,
    .tp_dealloc = (destructor) AIOContext_dealloc,
    .tp_members = AIOContext_members,
    .tp_methods = AIOContext_methods,
    .tp_repr = (reprfunc) AIOContext_repr
};


static void
AIOOperation_dealloc(AIOOperation *self) {
    Py_CLEAR(self->callback);

    if ((self->opcode == THAIO_READ) && self->buf != NULL) {
        PyMem_Free(self->buf);
        self->buf = NULL;
    }

    Py_CLEAR(self->py_buffer);
    Py_TYPE(self)->tp_free((PyObject *) self);
}


static PyObject* AIOOperation_repr(AIOOperation *self) {
    char* mode;

    switch (self->opcode) {
        case THAIO_READ:
            mode = "read";
            break;

        case THAIO_WRITE:
            mode = "write";
            break;

        case THAIO_FSYNC:
            mode = "fsync";
            break;

        case THAIO_FDSYNC:
            mode = "fdsync";
            break;
        default:
            mode = "noop";
            break;
    }

    return PyUnicode_FromFormat(
        "<%s at %p: mode=\"%s\", fd=%i, offset=%i, result=%i, buffer=%p>",
        Py_TYPE(self)->tp_name, self, mode,
        self->fileno, self->offset, self->result, self->buf
    );
}


/*
    AIOOperation.read classmethod definition
*/
PyDoc_STRVAR(AIOOperation_read_docstring,
    "Creates a new instance of Operation on read mode.\n\n"
    "    Operation.read(\n"
    "        nbytes: int,\n"
    "        aio_context: Context,\n"
    "        fd: int, \n"
    "        offset: int,\n"
    "        priority=0\n"
    "    )"
);

static PyObject* AIOOperation_read(
    PyTypeObject *type, PyObject *args, PyObject *kwds
) {
    AIOOperation *self = (AIOOperation *) type->tp_alloc(type, 0);

    static char *kwlist[] = {"nbytes", "fd", "offset", "priority", NULL};

    if (self == NULL) {
        PyErr_SetString(PyExc_MemoryError, "can not allocate memory");
        return NULL;
    }

    self->buf = NULL;
    self->py_buffer = NULL;
    self->in_progress = 0;

    uint64_t nbytes = 0;
    uint16_t priority;

    int argIsOk = PyArg_ParseTupleAndKeywords(
        args, kwds, "KI|LH", kwlist,
        &nbytes,
        &(self->fileno),
        &(self->offset),
        &priority
    );

    if (!argIsOk) return NULL;

    self->buf = PyMem_Calloc(nbytes, sizeof(char));
    self->buf_size = nbytes;

    self->py_buffer = PyMemoryView_FromMemory(
        self->buf,
        self->buf_size,
        PyBUF_READ
    );

    self->opcode = THAIO_READ;

	return (PyObject*) self;
}

/*
    AIOOperation.write classmethod definition
*/
PyDoc_STRVAR(AIOOperation_write_docstring,
    "Creates a new instance of Operation on write mode.\n\n"
    "    Operation.write(\n"
    "        payload_bytes: bytes,\n"
    "        fd: int, \n"
    "        offset: int,\n"
    "        priority=0\n"
    "    )"
);

static PyObject* AIOOperation_write(
    PyTypeObject *type, PyObject *args, PyObject *kwds
) {
    AIOOperation *self = (AIOOperation *) type->tp_alloc(type, 0);

    static char *kwlist[] = {"payload_bytes", "fd", "offset", "priority", NULL};

    if (self == NULL) {
        PyErr_SetString(PyExc_MemoryError, "can not allocate memory");
        return NULL;
    }

    // unused
    uint16_t priority;

    self->buf = NULL;
    self->py_buffer = NULL;
    self->in_progress = 0;

    int argIsOk = PyArg_ParseTupleAndKeywords(
        args, kwds, "OI|LH", kwlist,
        &(self->py_buffer),
        &(self->fileno),
        &(self->offset),
        &priority
    );

    if (!argIsOk) return NULL;

    if (!PyBytes_Check(self->py_buffer)) {
        Py_XDECREF(self);
        PyErr_SetString(
            PyExc_ValueError,
            "payload_bytes argument must be bytes"
        );
        return NULL;
    }

    self->opcode = THAIO_WRITE;

    if (PyBytes_AsStringAndSize(
            self->py_buffer,
            &self->buf,
            &self->buf_size
    )) {
        Py_XDECREF(self);
        PyErr_SetString(
            PyExc_RuntimeError,
            "Can not convert bytes to c string"
        );
        return NULL;
    }

    Py_INCREF(self->py_buffer);

	return (PyObject*) self;
}


/*
    AIOOperation.fsync classmethod definition
*/
PyDoc_STRVAR(AIOOperation_fsync_docstring,
    "Creates a new instance of Operation on fsync mode.\n\n"
    "    Operation.fsync(\n"
    "        aio_context: AIOContext,\n"
    "        fd: int, \n"
    "        priority=0\n"
    "    )"
);

static PyObject* AIOOperation_fsync(
    PyTypeObject *type, PyObject *args, PyObject *kwds
) {
    AIOOperation *self = (AIOOperation *) type->tp_alloc(type, 0);

    static char *kwlist[] = {"fd", "priority", NULL};

    if (self == NULL) {
        PyErr_SetString(PyExc_MemoryError, "can not allocate memory");
        return NULL;
    }

    uint16_t priority;

    self->buf = NULL;
    self->py_buffer = NULL;
    self->in_progress = 0;

    int argIsOk = PyArg_ParseTupleAndKeywords(
        args, kwds, "I|H", kwlist,
        &(self->fileno),
        &priority
    );

    if (!argIsOk) return NULL;

    self->opcode = THAIO_FSYNC;

	return (PyObject*) self;
}


/*
    AIOOperation.fdsync classmethod definition
*/
PyDoc_STRVAR(AIOOperation_fdsync_docstring,
    "Creates a new instance of Operation on fdsync mode.\n\n"
    "    Operation.fdsync(\n"
    "        aio_context: AIOContext,\n"
    "        fd: int, \n"
    "        priority=0\n"
    "    )"
);

static PyObject* AIOOperation_fdsync(
    PyTypeObject *type, PyObject *args, PyObject *kwds
) {
    AIOOperation *self = (AIOOperation *) type->tp_alloc(type, 0);

    static char *kwlist[] = {"fd", "priority", NULL};

    if (self == NULL) {
        PyErr_SetString(PyExc_MemoryError, "can not allocate memory");
        return NULL;
    }

    self->buf = NULL;
    self->py_buffer = NULL;
    self->in_progress = 0;
    uint16_t priority;

    int argIsOk = PyArg_ParseTupleAndKeywords(
        args, kwds, "I|H", kwlist,
        &(self->fileno),
        &priority
    );

    if (!argIsOk) return NULL;

    self->opcode = THAIO_FDSYNC;

	return (PyObject*) self;
}

/*
    AIOOperation.get_value method definition
*/
PyDoc_STRVAR(AIOOperation_get_value_docstring,
    "Method returns a bytes value of Operation's result or None.\n\n"
    "    Operation.get_value() -> Optional[bytes]"
);

static PyObject* AIOOperation_get_value(
    AIOOperation *self, PyObject *args, PyObject *kwds
) {
    if (self->error != 0) {
        PyErr_SetString(
            PyExc_SystemError,
            strerror(self->error)
        );

        return NULL;
    }

    switch (self->opcode) {
        case THAIO_READ:
            return PyBytes_FromStringAndSize(
                self->buf, self->buf_size
            );

        case THAIO_WRITE:
            return PyLong_FromSsize_t(self->result);
    }

    return Py_None;
}


/*
    AIOOperation.get_value method definition
*/
PyDoc_STRVAR(AIOOperation_set_callback_docstring,
    "Set callback which will be called after Operation will be finished.\n\n"
    "    Operation.get_value() -> Optional[bytes]"
);

static PyObject* AIOOperation_set_callback(
    AIOOperation *self, PyObject *args, PyObject *kwds
) {
    static char *kwlist[] = {"callback", NULL};

    PyObject* callback;

    int argIsOk = PyArg_ParseTupleAndKeywords(
        args, kwds, "O", kwlist,
        &callback
    );

    if (!argIsOk) return NULL;

    if (!PyCallable_Check(callback)) {
        PyErr_Format(
            PyExc_ValueError,
            "object %r is not callable",
            callback
        );
        return NULL;
    }

    Py_INCREF(callback);
    self->callback = callback;

    Py_RETURN_TRUE;
}

/*
    AIOOperation properties
*/
static PyMemberDef AIOOperation_members[] = {
    {
        "fileno", T_UINT,
        offsetof(AIOOperation, fileno),
        READONLY, "file descriptor"
    },
    {
        "offset", T_ULONGLONG,
        offsetof(AIOOperation, offset),
        READONLY, "offset"
    },
    {
        "payload", T_OBJECT,
        offsetof(AIOOperation, py_buffer),
        READONLY, "payload"
    },
    {
        "nbytes", T_ULONGLONG,
        offsetof(AIOOperation, buf_size),
        READONLY, "nbytes"
    },
    {
        "result", T_INT,
        offsetof(AIOOperation, result),
        READONLY, "result"
    },
    {
        "error", T_INT,
        offsetof(AIOOperation, error),
        READONLY, "error"
    },
    {NULL}  /* Sentinel */
};

/*
    AIOOperation methods
*/
static PyMethodDef AIOOperation_methods[] = {
    {
        "read",
        (PyCFunction) AIOOperation_read,
        METH_CLASS | METH_VARARGS | METH_KEYWORDS,
        AIOOperation_read_docstring
    },
    {
        "write",
        (PyCFunction) AIOOperation_write,
        METH_CLASS | METH_VARARGS | METH_KEYWORDS,
        AIOOperation_write_docstring
    },
    {
        "fsync",
        (PyCFunction) AIOOperation_fsync,
        METH_CLASS | METH_VARARGS | METH_KEYWORDS,
        AIOOperation_fsync_docstring
    },
    {
        "fdsync",
        (PyCFunction) AIOOperation_fdsync,
        METH_CLASS | METH_VARARGS | METH_KEYWORDS,
        AIOOperation_fdsync_docstring
    },
    {
        "get_value",
        (PyCFunction) AIOOperation_get_value, METH_NOARGS,
        AIOOperation_get_value_docstring
    },
    {
        "set_callback",
        (PyCFunction) AIOOperation_set_callback, METH_VARARGS | METH_KEYWORDS,
        AIOOperation_set_callback_docstring
    },
    {NULL}  /* Sentinel */
};

/*
    AIOOperation class
*/
static PyTypeObject
AIOOperationType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "aio.AIOOperation",
    .tp_doc = "thread aio operation representation",
    .tp_basicsize = sizeof(AIOOperation),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor) AIOOperation_dealloc,
    .tp_members = AIOOperation_members,
    .tp_methods = AIOOperation_methods,
    .tp_repr = (reprfunc) AIOOperation_repr
};


static PyModuleDef thread_aio_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "thread_aio",
    .m_doc = "Thread based AIO.",
    .m_size = -1,
};


PyMODINIT_FUNC PyInit_thread_aio(void) {
    Py_Initialize();

    PyObject *m;

    m = PyModule_Create(&thread_aio_module);

    if (m == NULL) return NULL;

    if (PyType_Ready(&AIOContextType) < 0) return NULL;

    Py_INCREF(&AIOContextType);

    if (PyModule_AddObject(m, "Context", (PyObject *) &AIOContextType) < 0) {
        Py_XDECREF(&AIOContextType);
        Py_XDECREF(m);
        return NULL;
    }

    if (PyType_Ready(&AIOOperationType) < 0) return NULL;

    Py_INCREF(&AIOOperationType);

    if (PyModule_AddObject(m, "Operation", (PyObject *) &AIOOperationType) < 0) {
        Py_XDECREF(&AIOOperationType);
        Py_XDECREF(m);
        return NULL;
    }

    return m;
}
