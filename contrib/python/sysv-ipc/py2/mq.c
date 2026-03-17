#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "structmember.h"

#include "common.h"
#include "mq.h"


PyObject *
mq_str(MessageQueue *self) {
#if PY_MAJOR_VERSION > 2
    return PyUnicode_FromFormat("Key=%ld, id=%d", (long)self->key, self->id);
#else
    return PyString_FromFormat("Key=%ld, id=%d", (long)self->key, self->id);
#endif
}


PyObject *
mq_repr(MessageQueue *self) {
#if PY_MAJOR_VERSION > 2
    return PyUnicode_FromFormat("sysv_ipc.MessageQueue(%ld)", (long)self->key);
#else
    return PyString_FromFormat("sysv_ipc.MessageQueue(%ld)", (long)self->key);
#endif
}


static PyObject *
get_a_value(int queue_id, enum GET_SET_IDENTIFIERS field) {
    struct msqid_ds q_info;
    PyObject *py_value = NULL;

    DPRINTF("Calling msgctl(...IPC_STAT...), field = %d\n", field);
    if (-1 == msgctl(queue_id, IPC_STAT, &q_info)) {
        switch (errno) {
            case EIDRM:
            case EINVAL:
                PyErr_Format(pExistentialException,
                                                "The queue no longer exists");
            break;

            case EACCES:
                PyErr_SetString(pPermissionsException, "Permission denied");
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }

        goto error_return;
    }

    switch (field) {
        case SVIFP_MQ_LAST_SEND_TIME:
            py_value = TIME_T_TO_PY(q_info.msg_stime);
        break;

        case SVIFP_MQ_LAST_RECEIVE_TIME:
            py_value = TIME_T_TO_PY(q_info.msg_rtime);
        break;

        case SVIFP_MQ_LAST_CHANGE_TIME:
            py_value = TIME_T_TO_PY(q_info.msg_ctime);
        break;

        case SVIFP_MQ_CURRENT_MESSAGES:
            py_value = MSGQNUM_T_TO_PY(q_info.msg_qnum);
        break;

        case SVIFP_MQ_QUEUE_BYTES_MAX:
            py_value = MSGLEN_T_TO_PY(q_info.msg_qbytes);
        break;

        case SVIFP_MQ_LAST_SEND_PID:
            py_value = PID_T_TO_PY(q_info.msg_lspid);
        break;

        case SVIFP_MQ_LAST_RECEIVE_PID:
            py_value = PID_T_TO_PY(q_info.msg_lrpid);
        break;

        case SVIFP_IPC_PERM_UID:
            py_value = UID_T_TO_PY(q_info.msg_perm.uid);
        break;

        case SVIFP_IPC_PERM_GID:
            py_value = GID_T_TO_PY(q_info.msg_perm.gid);
        break;

        case SVIFP_IPC_PERM_CUID:
            py_value = UID_T_TO_PY(q_info.msg_perm.cuid);
        break;

        case SVIFP_IPC_PERM_CGID:
            py_value = GID_T_TO_PY(q_info.msg_perm.cgid);
        break;

        case SVIFP_IPC_PERM_MODE:
            py_value = MODE_T_TO_PY(q_info.msg_perm.mode);
        break;

        default:
            PyErr_Format(pInternalException,
                         "Bad field %d passed to get_a_value", field);
            goto error_return;
        break;
    }

    return py_value;

    error_return:
    return NULL;
}


int
set_a_value(int id, enum GET_SET_IDENTIFIERS field, PyObject *py_value) {
    struct msqid_ds mq_info;

#if PY_MAJOR_VERSION > 2
    if (!PyLong_Check(py_value))
#else
    if (!PyInt_Check(py_value))
#endif
    {
        PyErr_Format(PyExc_TypeError, "The attribute must be an integer");
        goto error_return;
    }

    /* Here I get the current values associated with the queue. It's
       critical to populate sem_info with current values here (rather than
       just using the struct filled with whatever garbage it acquired from
       being declared on the stack) because the call to msgctl(...IPC_SET...)
       below will copy uid, gid and mode to the kernel's data structure.
    */
    if (-1 == msgctl(id, IPC_STAT, &mq_info)) {
        switch (errno) {
            case EACCES:
            case EPERM:
                PyErr_SetString(pPermissionsException, "Permission denied");
            break;

            case EINVAL:
                PyErr_SetString(pExistentialException,
                                                "The queue no longer exists");
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }
        goto error_return;
    }

    switch (field) {
        case SVIFP_IPC_PERM_UID:
#if PY_MAJOR_VERSION > 2
            mq_info.msg_perm.uid = PyLong_AsLong(py_value);
#else
            mq_info.msg_perm.uid = PyInt_AsLong(py_value);
#endif
        break;

        case SVIFP_IPC_PERM_GID:
#if PY_MAJOR_VERSION > 2
            mq_info.msg_perm.gid = PyLong_AsLong(py_value);
#else
            mq_info.msg_perm.gid = PyInt_AsLong(py_value);
#endif
        break;

        case SVIFP_IPC_PERM_MODE:
#if PY_MAJOR_VERSION > 2
            mq_info.msg_perm.mode = PyLong_AsLong(py_value);
#else
            mq_info.msg_perm.mode = PyInt_AsLong(py_value);
#endif
        break;

        case SVIFP_MQ_QUEUE_BYTES_MAX:
            // A msglen_t is unsigned.
            // ref: http://www.opengroup.org/onlinepubs/000095399/basedefs/sys/msg.h.html
#if PY_MAJOR_VERSION > 2
            mq_info.msg_qbytes = PyLong_AsUnsignedLongMask(py_value);
#else
            mq_info.msg_qbytes = PyInt_AsUnsignedLongMask(py_value);
#endif
        break;

        default:
            PyErr_Format(pInternalException,
                         "Bad field %d passed to set_a_value", field);
            goto error_return;
        break;
    }

    if (-1 == msgctl(id, IPC_SET, &mq_info)) {
        switch (errno) {
            case EACCES:
            case EPERM:
                PyErr_SetString(pPermissionsException, "Permission denied");
            break;

            case EINVAL:
                PyErr_SetString(pExistentialException,
                                                "The queue no longer exists");
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }
        goto error_return;
    }

    return 0;

    error_return:
    return -1;
}


PyObject *
mq_get_key(MessageQueue *self) {
    return KEY_T_TO_PY(self->key);
}

PyObject *
mq_get_last_send_time(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_MQ_LAST_SEND_TIME);
}

PyObject *
mq_get_last_receive_time(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_MQ_LAST_RECEIVE_TIME);
}

PyObject *
mq_get_last_change_time(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_MQ_LAST_CHANGE_TIME);
}

PyObject *
mq_get_last_send_pid(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_MQ_LAST_SEND_PID);
}

PyObject *
mq_get_last_receive_pid(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_MQ_LAST_RECEIVE_PID);
}

PyObject *
mq_get_current_messages(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_MQ_CURRENT_MESSAGES);
}

PyObject *
mq_get_max_size(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_MQ_QUEUE_BYTES_MAX);
}

int
mq_set_max_size(MessageQueue *self, PyObject *py_value) {
    return set_a_value(self->id, SVIFP_MQ_QUEUE_BYTES_MAX, py_value);
}

PyObject *
mq_get_mode(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_IPC_PERM_MODE);
}

int
mq_set_mode(MessageQueue *self, PyObject *py_value) {
    return set_a_value(self->id, SVIFP_IPC_PERM_MODE, py_value);
}

PyObject *
mq_get_uid(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_IPC_PERM_UID);
}

int
mq_set_uid(MessageQueue *self, PyObject *py_value) {
    return set_a_value(self->id, SVIFP_IPC_PERM_UID, py_value);
}

PyObject *
mq_get_gid(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_IPC_PERM_GID);
}

int
mq_set_gid(MessageQueue *self, PyObject *py_value) {
    return set_a_value(self->id, SVIFP_IPC_PERM_GID, py_value);
}

PyObject *
mq_get_c_uid(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_IPC_PERM_CUID);
}

PyObject *
mq_get_c_gid(MessageQueue *self) {
    return get_a_value(self->id, SVIFP_IPC_PERM_CGID);
}


PyObject *
mq_remove(int queue_id) {
    struct msqid_ds mq_info;

    DPRINTF("calling msgctl(...IPC_RMID...) on id %d\n", queue_id);
    if (-1 == msgctl(queue_id, IPC_RMID, &mq_info)) {
        DPRINTF("msgctl returned -1 on id %d, errno = %d\n", queue_id, errno);
        switch (errno) {
            case EIDRM:
            case EINVAL:
                PyErr_Format(pExistentialException,
                    "The queue no longer exists");
            break;

            case EPERM:
                PyErr_SetString(pPermissionsException, "Permission denied");
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }
        goto error_return;
    }

    Py_RETURN_NONE;

    error_return:
    return NULL;
}



void
MessageQueue_dealloc(MessageQueue *self) {
    Py_TYPE(self)->tp_free((PyObject*)self);
}

PyObject *
MessageQueue_new(PyTypeObject *type, PyObject *args, PyObject *keywords) {
    MessageQueue *self;

    self = (MessageQueue *)type->tp_alloc(type, 0);

    return (PyObject *)self;
}


int
MessageQueue_init(MessageQueue *self, PyObject *args, PyObject *keywords) {
    int flags = 0;
    int mode = 0600;
    NoneableKey key;
    unsigned long max_message_size = QUEUE_MESSAGE_SIZE_MAX_DEFAULT;
    char *keyword_list[ ] = {"key", "flags", "mode", "max_message_size", NULL};

    //MessageQueue(key, [flags = 0, [mode = 0600, [max_message_size = QUEUE_MESSAGE_SIZE_MAX_DEFAULT]])

    if (!PyArg_ParseTupleAndKeywords(args, keywords, "O&|iik", keyword_list,
                                     convert_key_param, &key, &flags,
                                     &mode, &max_message_size))
        goto error_return;

    if (max_message_size > QUEUE_MESSAGE_SIZE_MAX) {
        PyErr_Format(PyExc_ValueError, "The message length must be <= %lu\n",
            (unsigned long)QUEUE_MESSAGE_SIZE_MAX);
        goto error_return;
    }

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

    self->max_message_size = max_message_size;

    // I mask the caller's flags against the two IPC_* flags to ensure that
    // nothing funky sneaks into the flags.
    flags &= (IPC_CREAT | IPC_EXCL);

    mode &= 0777;

    if (key.is_none) {
        // (key == None) ==> generate a key for the caller
        do {
            errno = 0;
            self->key = get_random_key();

            DPRINTF("Calling msgget, key=%ld, flags=0x%x\n",
                                                    (long)self->key, flags);
            self->id = msgget(self->key, mode | flags);
        } while ( (-1 == self->id) && (EEXIST == errno) );
    }
    else {
        // (key != None) ==> use key supplied by the caller
        self->key = key.value;

        DPRINTF("Calling msgget, key=%ld, flags=0x%x\n", (long)self->key, flags);
        self->id = msgget(self->key, mode | flags);
    }

    DPRINTF("id == %d\n", self->id);

    if (self->id == -1) {
        switch (errno) {
            case EACCES:
                PyErr_SetString(pPermissionsException, "Permission denied");
            break;

            case EEXIST:
                PyErr_SetString(pExistentialException,
                            "A queue with the specified key already exists");
            break;

            case ENOENT:
                PyErr_SetString(pExistentialException,
                                    "No queue exists with the specified key");
            break;

            case ENOMEM:
                PyErr_SetString(PyExc_MemoryError, "Not enough memory");
            break;

            case ENOSPC:
                PyErr_SetString(PyExc_OSError,
                    "The system limit for message queues has been reached");
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }
        goto error_return;
    }

    return 0;

    error_return:
    return -1;
}


PyObject *
MessageQueue_send(MessageQueue *self, PyObject *args, PyObject *keywords) {
    /* In Python >= 2.5, the Python argument specifier 's#' expects a
       py_ssize_t for its second parameter. A ulong is long enough to hold
       a py_ssize_t.
       It might be too big, though, on platforms where a long is larger than
       py_ssize_t. Therefore I *must* initialize it to 0 so that whatever
       Python doesn't write to is zeroed out.
   */
#if PY_MAJOR_VERSION > 2
    static char args_format[] = "s*|Oi";
    Py_buffer user_msg;
#else
    static char args_format[] = "s#|Oi";
    typedef struct {
        char *buf;
        long len;
    } MyBuffer;
    MyBuffer user_msg;
    user_msg.len = 0;
#endif
    PyObject *py_block = NULL;
    int flags = 0;
    int type = 1;
    int rc;
    struct queue_message *p_msg = NULL;
    char *keyword_list[ ] = {"message", "block", "type", NULL};

    // send(message, [block = True, [type = 1]])
    if (!PyArg_ParseTupleAndKeywords(args, keywords, args_format, keyword_list,
#if PY_MAJOR_VERSION > 2
                                     &user_msg,
#else
                                     &(user_msg.buf), &(user_msg.len),
#endif
                                     &py_block, &type))
        goto error_return;

    if (type <= 0) {
        PyErr_SetString(PyExc_ValueError, "The type must be > 0");
        goto error_return;
    }

    // self->max_message_size is a ulong while user_msg.len is a long. Casting the latter to
    // unsigned long is safe because the length will never be negative.
    if ((unsigned long)user_msg.len > self->max_message_size) {
        PyErr_Format(PyExc_ValueError,
            "The message length exceeds queue's max_message_size (%lu)",
            self->max_message_size);
        goto error_return;
    }
    // default behavior (when py_block == NULL) is to block/wait.
    if (py_block && PyObject_Not(py_block))
        flags |= IPC_NOWAIT;

    p_msg = (struct queue_message *)malloc(offsetof(struct queue_message, message) + user_msg.len);

    DPRINTF("p_msg is %p\n", p_msg);

    if (!p_msg) {
        PyErr_SetString(PyExc_MemoryError, "Out of memory");
        goto error_return;
    }

    memcpy(p_msg->message, user_msg.buf, user_msg.len);
    p_msg->type = type;

    Py_BEGIN_ALLOW_THREADS
    DPRINTF("Calling msgsnd(), id=%ld, p_msg=%p, p_msg->type=%ld, length=%lu, flags=0x%x\n",
            (long)self->id, p_msg, p_msg->type, user_msg.len, flags);
    rc = msgsnd(self->id, p_msg, (size_t)user_msg.len, flags);
    Py_END_ALLOW_THREADS

    if (-1 == rc) {
        DPRINTF("msgsnd() returned -1, id=%ld, errno=%d\n", (long)self->id,
                errno);
        switch (errno) {
            case EACCES:
                PyErr_SetString(pPermissionsException, "Permission denied");
            break;

            case EAGAIN:
                PyErr_SetString(pBusyException,
                        "The queue is full, or a system-wide limit on the number of queue messages has been reached");
            break;

            case EIDRM:
                PyErr_SetString(pExistentialException,
                                "The queue no longer exists");
            break;

            case EINTR:
                PyErr_SetString(pBaseException, "Signaled while waiting");
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }

        goto error_return;
    }


#if PY_MAJOR_VERSION > 2
    PyBuffer_Release(&user_msg);
#endif

    free(p_msg);

    Py_RETURN_NONE;

    error_return:
#if PY_MAJOR_VERSION > 2
    PyBuffer_Release(&user_msg);
#endif
    free(p_msg);
    return NULL;
}


PyObject *
MessageQueue_receive(MessageQueue *self, PyObject *args, PyObject *keywords) {
    PyObject *py_block = NULL;
    PyObject *py_return_tuple = NULL;
    int flags = 0;
    int type = 0;
    ssize_t rc;
    struct queue_message *p_msg = NULL;
    char *keyword_list[ ] = {"block", "type", NULL};

    // receive([block = True, [type = 0]])
    if (!PyArg_ParseTupleAndKeywords(args, keywords, "|Oi", keyword_list,
                                     &py_block, &type))
        goto error_return;

    // default behavior (when py_block == NULL) is to block/wait.
    if (py_block && PyObject_Not(py_block))
        flags |= IPC_NOWAIT;

    p_msg = (struct queue_message *)malloc(sizeof(struct queue_message) + self->max_message_size);

    DPRINTF("p_msg is %p, size = %lu\n",
        p_msg, sizeof(struct queue_message) + self->max_message_size);

    if (!p_msg) {
        PyErr_SetString(PyExc_MemoryError, "Out of memory");
        goto error_return;
    }

    p_msg->type = type;

    Py_BEGIN_ALLOW_THREADS;
    rc = msgrcv(self->id, p_msg, (size_t)self->max_message_size,
                type, flags);
    Py_END_ALLOW_THREADS;

    DPRINTF("after msgrcv, p_msg->type=%ld, rc (size)=%ld\n",
                p_msg->type, (long)rc);

    if ((ssize_t)-1 == rc) {
        switch (errno) {
            case EACCES:
                PyErr_SetString(pPermissionsException, "Permission denied");
            break;

            case EIDRM:
            case EINVAL:
                PyErr_SetString(pExistentialException,
                                                "The queue no longer exists");
            break;

            case EINTR:
                PyErr_SetString(pBaseException, "Signaled while waiting");
            break;

            case ENOMSG:
                PyErr_SetString(pBusyException,
                            "No available messages of the specified type");
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }

        goto error_return;
    }

    py_return_tuple = Py_BuildValue("NN",
#if PY_MAJOR_VERSION > 2
                                    PyBytes_FromStringAndSize(p_msg->message, rc),
                                    PyLong_FromLong(p_msg->type)
#else
                                    PyString_FromStringAndSize(p_msg->message, rc),
                                    PyInt_FromLong(p_msg->type)
#endif
                                   );

    free(p_msg);

    return py_return_tuple;

    error_return:
    free(p_msg);
    return NULL;
}


PyObject *
MessageQueue_remove(MessageQueue *self) {
    return mq_remove(self->id);
}

