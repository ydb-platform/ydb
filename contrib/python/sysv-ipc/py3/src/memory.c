#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "structmember.h"

#include "common.h"
#include "memory.h"


/******************    Internal use only     **********************/
PyObject *
shm_str(SharedMemory *self) {
	return PyUnicode_FromFormat("Key=%ld, id=%d", (long)self->key, self->id);
}

PyObject *
shm_repr(SharedMemory *self) {
    return PyUnicode_FromFormat("sysv_ipc.SharedMemory(%ld)", (long)self->key);
}

PyObject *
shm_attach(SharedMemory *self, void *address, int shmat_flags) {
    DPRINTF("attaching memory @ address %p with id %d using flags 0x%x\n",
             address, self->id, shmat_flags);

    self->address = shmat(self->id, address, shmat_flags);

    if ((void *)-1 == self->address) {
        self->address = NULL;
        switch (errno) {
            case EACCES:
                PyErr_SetString(pPermissionsException, "No permission to attach");
            break;

            case ENOMEM:
                PyErr_SetString(PyExc_MemoryError, "Not enough memory");
            break;

            case EINVAL:
                PyErr_SetString(PyExc_ValueError, "Invalid id, address, or flags");
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }

        goto error_return;
    }
    else {
        // memory was attached successfully
        self->read_only = (shmat_flags & SHM_RDONLY) ? 1 : 0;
        DPRINTF("set memory's internal read_only flag to %d\n", self->read_only);
    }

    Py_RETURN_NONE;

    error_return:
    return NULL;
}


PyObject *
shm_remove(int shared_memory_id) {
    struct shmid_ds shm_info;

    DPRINTF("removing shm with id %d\n", shared_memory_id);
    if (-1 == shmctl(shared_memory_id, IPC_RMID, &shm_info)) {
        switch (errno) {
            case EIDRM:
            case EINVAL:
                PyErr_Format(pExistentialException,
                             "No shared memory with id %d exists",
                             shared_memory_id);
            break;

            case EPERM:
                PyErr_SetString(pPermissionsException,
                                "You do not have permission to remove the shared memory");
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


static PyObject *
shm_get_value(int shared_memory_id, enum GET_SET_IDENTIFIERS field) {
	// Gets one of the values in GET_SET_IDENTIFIERS and returns it as a boxed Python int or long.
	// The caller assumes responsibility for the reference.
	// If an error occurs, sets the Python error and returns NULL.
    struct shmid_ds shm_info;
    PyObject *py_value = NULL;

    DPRINTF("Calling shmctl(...IPC_STAT...), field = %d\n", field);
    if (-1 == shmctl(shared_memory_id, IPC_STAT, &shm_info)) {
        switch (errno) {
            case EIDRM:
            case EINVAL:
                PyErr_Format(pExistentialException,
                             "No shared memory with id %d exists",
                             shared_memory_id);
            break;

            case EACCES:
                PyErr_SetString(pPermissionsException,
                                "You do not have permission to read the shared memory attribute");
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }

        goto error_return;
    }

    switch (field) {
        case SVIFP_SHM_SIZE:
            py_value = SIZE_T_TO_PY(shm_info.shm_segsz);
        break;

        case SVIFP_SHM_LAST_ATTACH_TIME:
            py_value = TIME_T_TO_PY(shm_info.shm_atime);
        break;

        case SVIFP_SHM_LAST_DETACH_TIME:
            py_value = TIME_T_TO_PY(shm_info.shm_dtime);
        break;

        case SVIFP_SHM_LAST_CHANGE_TIME:
            py_value = TIME_T_TO_PY(shm_info.shm_ctime);
        break;

        case SVIFP_SHM_CREATOR_PID:
            py_value = PID_T_TO_PY(shm_info.shm_cpid);
        break;

        case SVIFP_SHM_LAST_AT_DT_PID:
            py_value = PID_T_TO_PY(shm_info.shm_lpid);
        break;

        case SVIFP_SHM_NUMBER_ATTACHED:
            // shm_nattch is unsigned
            // ref: http://www.opengroup.org/onlinepubs/007908799/xsh/sysshm.h.html
            py_value = PyLong_FromUnsignedLong(shm_info.shm_nattch);
        break;

        case SVIFP_IPC_PERM_UID:
            py_value = UID_T_TO_PY(shm_info.shm_perm.uid);
        break;

        case SVIFP_IPC_PERM_GID:
            py_value = GID_T_TO_PY(shm_info.shm_perm.gid);
        break;

        case SVIFP_IPC_PERM_CUID:
            py_value = UID_T_TO_PY(shm_info.shm_perm.cuid);
        break;

        case SVIFP_IPC_PERM_CGID:
            py_value = GID_T_TO_PY(shm_info.shm_perm.cgid);
        break;

        case SVIFP_IPC_PERM_MODE:
            py_value = MODE_T_TO_PY(shm_info.shm_perm.mode);
        break;

        default:
            PyErr_Format(pInternalException, "Bad field %d passed to shm_get_value", field);
            goto error_return;
        break;
    }

    return py_value;

    error_return:
    return NULL;
}


static int
shm_set_ipc_perm_value(int id, enum GET_SET_IDENTIFIERS field, union ipc_perm_value value) {
    struct shmid_ds shm_info;

    if (-1 == shmctl(id, IPC_STAT, &shm_info)) {
        switch (errno) {
            case EIDRM:
            case EINVAL:
                PyErr_Format(pExistentialException,
                             "No shared memory with id %d exists", id);
            break;

            case EACCES:
                PyErr_SetString(pPermissionsException,
                                "You do not have permission to read the shared memory attribute");
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }
        goto error_return;
    }

    switch (field) {
        case SVIFP_IPC_PERM_UID:
            shm_info.shm_perm.uid = value.uid;
        break;

        case SVIFP_IPC_PERM_GID:
            shm_info.shm_perm.gid = value.gid;
        break;

        case SVIFP_IPC_PERM_MODE:
            shm_info.shm_perm.mode = value.mode;
        break;

        default:
            PyErr_Format(pInternalException,
                         "Bad field %d passed to shm_set_ipc_perm_value",
                         field);
            goto error_return;
        break;
    }

    if (-1 == shmctl(id, IPC_SET, &shm_info)) {
        switch (errno) {
            case EIDRM:
            case EINVAL:
                PyErr_Format(pExistentialException,
                             "No shared memory with id %d exists", id);
            break;

            case EPERM:
                PyErr_SetString(pPermissionsException,
                                "You do not have permission to change the shared memory's attributes");
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

int
shm_get_buffer(SharedMemory *self, Py_buffer *view, int flags)
// Implementation of buffer interface (getbufferproc).
// https://docs.python.org/3/c-api/typeobj.html#buffer-structs
{
	PyObject *py_size = shm_get_value(self->id, SVIFP_SHM_SIZE);
	Py_ssize_t size;

    if (!py_size) {
    	// If shm_get_value() failed, the Python error will already be set so there's no
    	// need for me to set it here.
        return -1;
    }
    else {
    	size = PyLong_AsSsize_t(py_size);
    	Py_DECREF(py_size);
	    return PyBuffer_FillInfo(view,
	    						 (PyObject *)self,
	                             self->address,
	                             size,
	                             0,
	                             flags);
	}
}


/******************    Class methods     **********************/


void
SharedMemory_dealloc(SharedMemory *self) {
    Py_TYPE(self)->tp_free((PyObject*)self);
}

PyObject *
SharedMemory_new(PyTypeObject *type, PyObject *args, PyObject *kwlist) {
    SharedMemory *self;

    self = (SharedMemory *)type->tp_alloc(type, 0);

    if (NULL != self) {
        self->key = (key_t)-1;
        self->id = 0;
        self->read_only = 0;
        self->address = NULL;
    }

    return (PyObject *)self;
}


int
SharedMemory_init(SharedMemory *self, PyObject *args, PyObject *keywords) {
    NoneableKey key;
    int mode = 0600;
    unsigned long size = 0;
    int shmget_flags = 0;
    int shmat_flags = 0;
    char init_character = ' ';
    char *keyword_list[ ] = {"key", "flags", "mode", "size", "init_character", NULL};
    PyObject *py_size = NULL;

    DPRINTF("Inside SharedMemory_init()\n");

    if (!PyArg_ParseTupleAndKeywords(args, keywords, "O&|iikc", keyword_list,
                                     &convert_key_param, &key,
                                     &shmget_flags, &mode, &size,
                                     &init_character))
        goto error_return;

    mode &= 0777;
    shmget_flags &= ~0777;

    DPRINTF("key is none = %d, key value = %ld\n", key.is_none, (long)key.value);

    if ( !(shmget_flags & IPC_CREAT) && (shmget_flags & IPC_EXCL) ) {
        PyErr_SetString(PyExc_ValueError,
                "IPC_EXCL must be combined with IPC_CREAT");
        goto error_return;
    }

    if (key.is_none && ((shmget_flags & IPC_EXCL) != IPC_EXCL)) {
        PyErr_SetString(PyExc_ValueError,
                "Key can only be None if IPC_EXCL is set");
        goto error_return;
    }

    // When creating a new segment, the default size is PAGE_SIZE.
    if (((shmget_flags & IPC_CREX) == IPC_CREX) && (!size))
        size = PAGE_SIZE;

    if (key.is_none) {
        // (key == None) ==> generate a key for the caller
        do {
            errno = 0;
            self->key = get_random_key();

            DPRINTF("Calling shmget, key=%ld, size=%lu, mode=%o, flags=0x%x\n",
                    (long)self->key, size, mode, shmget_flags);
            self->id = shmget(self->key, size, mode | shmget_flags);
        } while ( (-1 == self->id) && (EEXIST == errno) );
    }
    else {
        // (key != None) ==> use key supplied by the caller
        self->key = key.value;

        DPRINTF("Calling shmget, key=%ld, size=%lu, mode=%o, flags=0x%x\n",
                (long)self->key, size, mode, shmget_flags);
        self->id = shmget(self->key, size, mode | shmget_flags);
    }

    DPRINTF("id == %d\n", self->id);

    if (self->id == -1) {
        switch (errno) {
            case EACCES:
                PyErr_Format(pPermissionsException,
                             "Permission %o cannot be granted on the existing segment",
                             mode);
            break;

            case EEXIST:
                PyErr_Format(pExistentialException,
                    "Shared memory with the key %ld already exists",
                    (long)self->key);
            break;

            case ENOENT:
                PyErr_Format(pExistentialException,
                    "No shared memory exists with the key %ld", (long)self->key);
            break;

            case EINVAL:
                PyErr_SetString(PyExc_ValueError, "The size is invalid");
            break;

            case ENOMEM:
                PyErr_SetString(PyExc_MemoryError, "Not enough memory");
            break;

            case ENOSPC:
                PyErr_SetString(PyExc_OSError,
                    "Not enough shared memory identifiers available (ENOSPC)");
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }
        goto error_return;
    }

    // Attach the memory. If no write permissions requested, attach read-only.
    shmat_flags = (mode & 0200) ? 0 : SHM_RDONLY;
    if (NULL == shm_attach(self, NULL, shmat_flags)) {
        // Bad news, something went wrong.
        goto error_return;
    }

    if ( ((shmget_flags & IPC_CREX) == IPC_CREX) && (!(shmat_flags & SHM_RDONLY)) ) {
        // Initialize the memory.

        py_size = shm_get_value(self->id, SVIFP_SHM_SIZE);

        if (!py_size)
            goto error_return;
        else {
            size = PyLong_AsUnsignedLongMask(py_size);

            DPRINTF("memsetting address %p to %lu bytes of ASCII 0x%x (%c)\n", \
                    self->address, size, (int)init_character, init_character);
            memset(self->address, init_character, size);
        }

        Py_DECREF(py_size);
    }

    return 0;

    error_return:
    return -1;
}


PyObject *
SharedMemory_attach(SharedMemory *self, PyObject *args, PyObject *keywords) {
    PyObject *py_address = NULL;
    void *address = NULL;
    int flags = 0;
    static char *keyword_list[ ] = {"address", "flags", NULL};

    DPRINTF("Inside SharedMemory_attach()\n");

    if (!PyArg_ParseTupleAndKeywords(args, keywords, "|Oi", keyword_list,
                                     &py_address, &flags))
        goto error_return;

    if ((!py_address) || (py_address == Py_None))
        address = NULL;
    else {
        if (PyLong_Check(py_address))
            address = PyLong_AsVoidPtr(py_address);
        else {
            PyErr_SetString(PyExc_TypeError, "address must be a long");
            goto error_return;
        }
    }

    return shm_attach(self, address, flags);

    error_return:
    return NULL;
}


PyObject *
SharedMemory_detach(SharedMemory *self) {
    if (-1 == shmdt(self->address)) {
        self->address = NULL;
        switch (errno) {
            case EINVAL:
                PyErr_SetNone(pNotAttachedException);
            break;

            default:
                PyErr_SetFromErrno(PyExc_OSError);
            break;
        }
        goto error_return;
    }

    self->address = NULL;

    Py_RETURN_NONE;

    error_return:
    return NULL;
}


PyObject *
SharedMemory_read(SharedMemory *self, PyObject *args, PyObject *keywords) {
    /* Tricky business here. A memory segment's size is a size_t which is
       ulong or smaller. However, the largest string that Python can
       construct is of ssize_t which is long or smaller. Therefore, the
       size and offset variables must be ulongs while the byte_count
       must be a long (and must not exceed LONG_MAX).
       Mind your math!
    */
    long byte_count = 0;
    unsigned long offset = 0;
    unsigned long size;
    PyObject *py_size;
    char *keyword_list[ ] = {"byte_count", "offset", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, keywords, "|lk", keyword_list,
                                     &byte_count, &offset))
        goto error_return;

    if (self->address == NULL) {
        PyErr_SetString(pNotAttachedException,
                        "Read attempt on unattached memory segment");
        goto error_return;
    }

    if ( (py_size = shm_get_value(self->id, SVIFP_SHM_SIZE)) ) {
        size = PyLong_AsUnsignedLongMask(py_size);
        Py_DECREF(py_size);
    }
    else
        goto error_return;

    DPRINTF("offset = %lu, byte_count = %ld, size = %lu\n",
            offset, byte_count, size);

    if (offset >= size) {
        PyErr_SetString(PyExc_ValueError, "The offset must be less than the segment size");
        goto error_return;
    }

    if (byte_count < 0) {
        PyErr_SetString(PyExc_ValueError, "The byte_count cannot be negative");
        goto error_return;
    }

    /* If the caller didn't specify a byte count or specified one that would
       read past the end of the segment, return everything from the offset to
       the end of the segment.
       Be careful here not to express the second if condition w/addition, e.g.
            (byte_count + offset > size)
       It might be more intuitive but since byte_count is a long and offset
       is a ulong, their sum could cause an arithmetic overflow. */
    if ((!byte_count) || ((unsigned long)byte_count > size - offset)) {
        // byte_count needs to be calculated
        if (size - offset <= (unsigned long)PY_STRING_LENGTH_MAX)
            byte_count = size - offset;
        else {
            // Caller is asking for more bytes than I can stuff into
            // a Python string.
            PyErr_Format(PyExc_ValueError,
                         "The byte_count cannot exceed Python's max string length %ld",
                         (long)PY_STRING_LENGTH_MAX);
            goto error_return;
        }
    }

    return PyBytes_FromStringAndSize(self->address + offset, byte_count);

    error_return:
    return NULL;
}


PyObject *
SharedMemory_write(SharedMemory *self, PyObject *args, PyObject *kw) {
    /* See comments for read() regarding "size issues". Note that here
       Python provides the byte_count so it can't be negative.
    */
    unsigned long offset = 0;
    unsigned long size;
    PyObject *py_size;
    char *keyword_list[ ] = {"s", "offset", NULL};
    static char args_format[] = "s*|k";
    Py_buffer data;

    if (!PyArg_ParseTupleAndKeywords(args, kw, args_format, keyword_list,
                          &data,
                          &offset))
        goto error_return;

    if (self->read_only) {
        PyErr_SetString(PyExc_OSError, "Write attempt on read-only memory segment");
        goto error_return;
    }

    if (self->address == NULL) {
        PyErr_SetString(pNotAttachedException, "Write attempt on unattached memory segment");
        goto error_return;
    }

    if ( (py_size = shm_get_value(self->id, SVIFP_SHM_SIZE)) ) {
        size = PyLong_AsUnsignedLongMask(py_size);
        Py_DECREF(py_size);
    }
    else
        goto error_return;

    DPRINTF("write size check; size=%lu, offset=%lu, dat.len=%ld\n",
            size, offset, data.len);

    // Remember that offset and size are both ulongs, so size > offset, then
    // size - offset (as in the second part of the if expression) will evaluate
    // to a "negative" number which is a very large ulong.
    if ((offset > size) || ((unsigned long)data.len > size - offset)) {
        PyErr_SetString(PyExc_ValueError, "Attempt to write past end of memory segment");
        goto error_return;
    }

    memcpy((self->address + offset), data.buf, data.len);

    PyBuffer_Release(&data);

    Py_RETURN_NONE;

    error_return:
    PyBuffer_Release(&data);
    return NULL;
}

PyObject *
SharedMemory_remove(SharedMemory *self) {
    return shm_remove(self->id);
}


PyObject *
shm_get_key(SharedMemory *self) {
    return KEY_T_TO_PY(self->key);
}

PyObject *
shm_get_size(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_SHM_SIZE);
}

PyObject *
shm_get_address(SharedMemory *self) {
    return PyLong_FromVoidPtr(self->address);
}

PyObject *
shm_get_attached(SharedMemory *self) {
    if (self->address)
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}

PyObject *
shm_get_last_attach_time(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_SHM_LAST_ATTACH_TIME);
}

PyObject *
shm_get_last_detach_time(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_SHM_LAST_DETACH_TIME);
}

PyObject *
shm_get_last_change_time(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_SHM_LAST_CHANGE_TIME);
}

PyObject *
shm_get_creator_pid(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_SHM_CREATOR_PID);
}

PyObject *
shm_get_last_pid(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_SHM_LAST_AT_DT_PID);
}

PyObject *
shm_get_number_attached(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_SHM_NUMBER_ATTACHED);
}

PyObject *
shm_get_uid(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_IPC_PERM_UID);
}

PyObject *
shm_get_cuid(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_IPC_PERM_CUID);
}

PyObject *
shm_get_cgid(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_IPC_PERM_CGID);
}

PyObject *
shm_get_mode(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_IPC_PERM_MODE);
}

int
shm_set_uid(SharedMemory *self, PyObject *py_value) {
    union ipc_perm_value new_value;

    if (!PyLong_Check(py_value))
    {
        PyErr_SetString(PyExc_TypeError, "Attribute 'uid' must be an integer");
        goto error_return;
    }

    new_value.uid = PyLong_AsLong(py_value);

    if (((uid_t)-1 == new_value.uid) && PyErr_Occurred()) {
        // no idea what could have gone wrong -- punt it up to the caller
        goto error_return;
    }

    return shm_set_ipc_perm_value(self->id, SVIFP_IPC_PERM_UID, new_value);

    error_return:
    return -1;
}


PyObject *
shm_get_gid(SharedMemory *self) {
    return shm_get_value(self->id, SVIFP_IPC_PERM_GID);
}

int
shm_set_gid(SharedMemory *self, PyObject *py_value) {
    union ipc_perm_value new_value;

    if (!PyLong_Check(py_value))
    {
        PyErr_Format(PyExc_TypeError, "attribute 'gid' must be an integer");
        goto error_return;
    }

    new_value.gid = PyLong_AsLong(py_value);

    if (((gid_t)-1 == new_value.gid) && PyErr_Occurred()) {
        // no idea what could have gone wrong -- punt it up to the caller
        goto error_return;
    }

    return shm_set_ipc_perm_value(self->id, SVIFP_IPC_PERM_GID, new_value);

    error_return:
    return -1;
}

int
shm_set_mode(SharedMemory *self, PyObject *py_value) {
    union ipc_perm_value new_value;

    if (!PyLong_Check(py_value))
    {
        PyErr_Format(PyExc_TypeError, "attribute 'mode' must be an integer");
        goto error_return;
    }

    new_value.mode = PyLong_AsLong(py_value);

    if (((mode_t)-1 == new_value.mode) && PyErr_Occurred()) {
        // no idea what could have gone wrong -- punt it up to the caller
        goto error_return;
    }

    return shm_set_ipc_perm_value(self->id, SVIFP_IPC_PERM_MODE, new_value);

    error_return:
    return -1;
}
