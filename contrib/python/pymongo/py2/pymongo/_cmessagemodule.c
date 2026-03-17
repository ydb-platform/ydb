/*
 * Copyright 2009-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains C implementations of some of the functions
 * needed by the message module. If possible, these implementations
 * should be used to speed up message creation.
 */

#define PY_SSIZE_T_CLEAN
#include "Python.h"

#include "_cbsonmodule.h"
#include "buffer.h"

struct module_state {
    PyObject* _cbson;
};

/* See comments about module initialization in _cbsonmodule.c */
#if PY_MAJOR_VERSION >= 3
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
#else
#define GETSTATE(m) (&_state)
static struct module_state _state;
#endif

#define DOC_TOO_LARGE_FMT "BSON document too large (%d bytes)" \
                          " - the connected server supports" \
                          " BSON document sizes up to %ld bytes."

/* Get an error class from the pymongo.errors module.
 *
 * Returns a new ref */
static PyObject* _error(char* name) {
    PyObject* error;
    PyObject* errors = PyImport_ImportModule("pymongo.errors");
    if (!errors) {
        return NULL;
    }
    error = PyObject_GetAttrString(errors, name);
    Py_DECREF(errors);
    return error;
}

/* The same as buffer_write_bytes except that it also validates
 * "size" will fit in an int.
 * Returns 0 on failure */
static int buffer_write_bytes_ssize_t(buffer_t buffer, const char* data, Py_ssize_t size) {
    int downsize = _downcast_and_check(size, 0);
    if (size == -1) {
        return 0;
    }
    return buffer_write_bytes(buffer, data, downsize);
}

/* add a lastError message on the end of the buffer.
 * returns 0 on failure */
static int add_last_error(PyObject* self, buffer_t buffer,
                          int request_id, char* ns, Py_ssize_t nslen,
                          codec_options_t* options, PyObject* args) {
    struct module_state *state = GETSTATE(self);

    int message_start;
    int document_start;
    int message_length;
    int document_length;
    PyObject* key = NULL;
    PyObject* value = NULL;
    Py_ssize_t pos = 0;
    PyObject* one;
    char *p = strchr(ns, '.');
    /* Length of the database portion of ns. */
    nslen = p ? (int)(p - ns) : nslen;

    message_start = buffer_save_space(buffer, 4);
    if (message_start == -1) {
        return 0;
    }
    if (!buffer_write_int32(buffer, (int32_t)request_id) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"  /* responseTo */
                            "\xd4\x07\x00\x00"  /* opcode */
                            "\x00\x00\x00\x00", /* options */
                            12) ||
        !buffer_write_bytes_ssize_t(buffer, ns, nslen) || /* database */
        !buffer_write_bytes(buffer,
                            ".$cmd\x00"         /* collection name */
                            "\x00\x00\x00\x00"  /* skip */
                            "\xFF\xFF\xFF\xFF", /* limit (-1) */
                            14)) {
        return 0;
    }

    /* save space for length */
    document_start = buffer_save_space(buffer, 4);
    if (document_start == -1) {
        return 0;
    }

    /* getlasterror: 1 */
    if (!(one = PyLong_FromLong(1)))
        return 0;

    if (!write_pair(state->_cbson, buffer, "getlasterror", 12, one, 0,
                    options, 1)) {
        Py_DECREF(one);
        return 0;
    }
    Py_DECREF(one);

    /* getlasterror options */
    while (PyDict_Next(args, &pos, &key, &value)) {
        if (!decode_and_write_pair(state->_cbson, buffer, key, value, 0,
                                   options, 0)) {
            return 0;
        }
    }

    /* EOD */
    if (!buffer_write_bytes(buffer, "\x00", 1)) {
        return 0;
    }

    message_length = buffer_get_position(buffer) - message_start;
    document_length = buffer_get_position(buffer) - document_start;
    buffer_write_int32_at_position(
        buffer, message_start, (int32_t)message_length);
    buffer_write_int32_at_position(
        buffer, document_start, (int32_t)document_length);
    return 1;
}

static int init_insert_buffer(buffer_t buffer, int request_id, int options,
                              const char* coll_name, Py_ssize_t coll_name_len,
                              int compress) {
    int length_location = 0;
    if (!compress) {
        /* Save space for message length */
        int length_location = buffer_save_space(buffer, 4);
        if (length_location == -1) {
            return length_location;
        }
        if (!buffer_write_int32(buffer, (int32_t)request_id) ||
            !buffer_write_bytes(buffer,
                                "\x00\x00\x00\x00"
                                "\xd2\x07\x00\x00",
                                8)) {
            return -1;
        }
    }
    if (!buffer_write_int32(buffer, (int32_t)options) ||
        !buffer_write_bytes_ssize_t(buffer,
                                    coll_name,
                                    coll_name_len + 1)) {
        return -1;
    }
    return length_location;
}

static PyObject* _cbson_insert_message(PyObject* self, PyObject* args) {
    /* Used by the Bulk API to insert into pre-2.6 servers. Collection.insert
     * uses _cbson_do_batched_insert. */
    struct module_state *state = GETSTATE(self);

    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    char* collection_name = NULL;
    Py_ssize_t collection_name_length;
    PyObject* docs;
    PyObject* doc;
    PyObject* iterator;
    int before, cur_size, max_size = 0;
    int flags = 0;
    unsigned char check_keys;
    unsigned char safe;
    unsigned char continue_on_error;
    codec_options_t options;
    PyObject* last_error_args;
    buffer_t buffer = NULL;
    int length_location, message_length;
    PyObject* result = NULL;

    if (!PyArg_ParseTuple(args, "et#ObbObO&",
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &docs, &check_keys, &safe,
                          &last_error_args,
                          &continue_on_error,
                          convert_codec_options, &options)) {
        return NULL;
    }
    if (continue_on_error) {
        flags += 1;
    }
    buffer = buffer_new();
    if (!buffer) {
        goto fail;
    }

    length_location = init_insert_buffer(buffer,
                                         request_id,
                                         flags,
                                         collection_name,
                                         collection_name_length,
                                         0);
    if (length_location == -1) {
        goto fail;
    }

    iterator = PyObject_GetIter(docs);
    if (iterator == NULL) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "input is not iterable");
            Py_DECREF(InvalidOperation);
        }
        goto fail;
    }
    while ((doc = PyIter_Next(iterator)) != NULL) {
        before = buffer_get_position(buffer);
        if (!write_dict(state->_cbson, buffer, doc, check_keys,
                        &options, 1)) {
            Py_DECREF(doc);
            Py_DECREF(iterator);
            goto fail;
        }
        Py_DECREF(doc);
        cur_size = buffer_get_position(buffer) - before;
        max_size = (cur_size > max_size) ? cur_size : max_size;
    }
    Py_DECREF(iterator);

    if (PyErr_Occurred()) {
        goto fail;
    }

    if (!max_size) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "cannot do an empty bulk insert");
            Py_DECREF(InvalidOperation);
        }
        goto fail;
    }

    message_length = buffer_get_position(buffer) - length_location;
    buffer_write_int32_at_position(
        buffer, length_location, (int32_t)message_length);

    if (safe) {
        if (!add_last_error(self, buffer, request_id, collection_name,
                            collection_name_length, &options, last_error_args)) {
            goto fail;
        }
    }

    /* objectify buffer */
    result = Py_BuildValue("i" BYTES_FORMAT_STRING "i", request_id,
                           buffer_get_buffer(buffer),
                           (Py_ssize_t)buffer_get_position(buffer),
                           max_size);
fail:
    PyMem_Free(collection_name);
    destroy_codec_options(&options);
    if (buffer) {
        buffer_free(buffer);
    }
    return result;
}

static PyObject* _cbson_update_message(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    struct module_state *state = GETSTATE(self);

    int request_id = rand();
    char* collection_name = NULL;
    Py_ssize_t collection_name_length;
    int before, cur_size, max_size = 0;
    PyObject* doc;
    PyObject* spec;
    unsigned char multi;
    unsigned char upsert;
    unsigned char safe;
    unsigned char check_keys;
    codec_options_t options;
    PyObject* last_error_args;
    int flags;
    buffer_t buffer = NULL;
    int length_location, message_length;
    PyObject* result = NULL;

    if (!PyArg_ParseTuple(args, "et#bbOObObO&",
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &upsert, &multi, &spec, &doc, &safe,
                          &last_error_args, &check_keys,
                          convert_codec_options, &options)) {
        return NULL;
    }

    flags = 0;
    if (upsert) {
        flags += 1;
    }
    if (multi) {
        flags += 2;
    }
    buffer = buffer_new();
    if (!buffer) {
        goto fail;
    }

    // save space for message length
    length_location = buffer_save_space(buffer, 4);
    if (length_location == -1) {
        goto fail;
    }
    if (!buffer_write_int32(buffer, (int32_t)request_id) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"
                            "\xd1\x07\x00\x00"
                            "\x00\x00\x00\x00",
                            12) ||
        !buffer_write_bytes_ssize_t(buffer,
                                    collection_name,
                                    collection_name_length + 1) ||
        !buffer_write_int32(buffer, (int32_t)flags)) {
        goto fail;
    }

    before = buffer_get_position(buffer);
    if (!write_dict(state->_cbson, buffer, spec, 0, &options, 1)) {
        goto fail;
    }
    max_size = buffer_get_position(buffer) - before;

    before = buffer_get_position(buffer);
    if (!write_dict(state->_cbson, buffer, doc, check_keys,
                    &options, 1)) {
        goto fail;
    }
    cur_size = buffer_get_position(buffer) - before;
    max_size = (cur_size > max_size) ? cur_size : max_size;

    message_length = buffer_get_position(buffer) - length_location;
    buffer_write_int32_at_position(
        buffer, length_location, (int32_t)message_length);

    if (safe) {
        if (!add_last_error(self, buffer, request_id, collection_name,
                            collection_name_length, &options, last_error_args)) {
            goto fail;
        }
    }

    /* objectify buffer */
    result = Py_BuildValue("i" BYTES_FORMAT_STRING "i", request_id,
                           buffer_get_buffer(buffer),
                           (Py_ssize_t)buffer_get_position(buffer),
                           max_size);
fail:
    PyMem_Free(collection_name);
    destroy_codec_options(&options);
    if (buffer) {
        buffer_free(buffer);
    }
    return result;
}

static PyObject* _cbson_query_message(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    struct module_state *state = GETSTATE(self);

    int request_id = rand();
    PyObject* cluster_time = NULL;
    unsigned int flags;
    char* collection_name = NULL;
    Py_ssize_t collection_name_length;
    int begin, cur_size, max_size = 0;
    int num_to_skip;
    int num_to_return;
    PyObject* query;
    PyObject* field_selector;
    codec_options_t options;
    buffer_t buffer = NULL;
    int length_location, message_length;
    unsigned char check_keys = 0;
    PyObject* result = NULL;

    if (!PyArg_ParseTuple(args, "Iet#iiOOO&|b",
                          &flags,
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &num_to_skip, &num_to_return,
                          &query, &field_selector,
                          convert_codec_options, &options,
                          &check_keys)) {
        return NULL;
    }
    buffer = buffer_new();
    if (!buffer) {
        goto fail;
    }

    // save space for message length
    length_location = buffer_save_space(buffer, 4);
    if (length_location == -1) {
        goto fail;
    }

    /* Pop $clusterTime from dict and write it at the end, avoiding an error
     * from the $-prefix and check_keys.
     *
     * If "dict" is a defaultdict we don't want to call PyMapping_GetItemString
     * on it. That would **create** an _id where one didn't previously exist
     * (PYTHON-871).
     */
    if (PyDict_Check(query)) {
        cluster_time = PyDict_GetItemString(query, "$clusterTime");
        if (cluster_time) {
            /* PyDict_GetItemString returns a borrowed reference. */
            Py_INCREF(cluster_time);
            if (-1 == PyMapping_DelItemString(query, "$clusterTime")) {
                goto fail;
            }
        }
    } else if (PyMapping_HasKeyString(query, "$clusterTime")) {
        cluster_time = PyMapping_GetItemString(query, "$clusterTime");
        if (!cluster_time
                || -1 == PyMapping_DelItemString(query, "$clusterTime")) {
            goto fail;
        }
    }
    if (!buffer_write_int32(buffer, (int32_t)request_id) ||
        !buffer_write_bytes(buffer, "\x00\x00\x00\x00\xd4\x07\x00\x00", 8) ||
        !buffer_write_int32(buffer, (int32_t)flags) ||
        !buffer_write_bytes_ssize_t(buffer, collection_name,
                                    collection_name_length + 1) ||
        !buffer_write_int32(buffer, (int32_t)num_to_skip) ||
        !buffer_write_int32(buffer, (int32_t)num_to_return)) {
        goto fail;
    }

    begin = buffer_get_position(buffer);
    if (!write_dict(state->_cbson, buffer, query, check_keys, &options, 1)) {
        goto fail;
    }

    /* back up a byte and write $clusterTime */
    if (cluster_time) {
        int length;
        char zero = 0;

        buffer_update_position(buffer, buffer_get_position(buffer) - 1);
        if (!write_pair(state->_cbson, buffer, "$clusterTime", 12, cluster_time,
                        0, &options, 1)) {
            goto fail;
        }

        if (!buffer_write_bytes(buffer, &zero, 1)) {
            goto fail;
        }

        length = buffer_get_position(buffer) - begin;
        buffer_write_int32_at_position(buffer, begin, (int32_t)length);

        /* undo popping $clusterTime */
        if (-1 == PyMapping_SetItemString(
                query, "$clusterTime", cluster_time)) {
            goto fail;
        }

        Py_CLEAR(cluster_time);
    }

    max_size = buffer_get_position(buffer) - begin;

    if (field_selector != Py_None) {
        begin = buffer_get_position(buffer);
        if (!write_dict(state->_cbson, buffer, field_selector, 0,
                        &options, 1)) {
            goto fail;
        }
        cur_size = buffer_get_position(buffer) - begin;
        max_size = (cur_size > max_size) ? cur_size : max_size;
    }

    message_length = buffer_get_position(buffer) - length_location;
    buffer_write_int32_at_position(
        buffer, length_location, (int32_t)message_length);

    /* objectify buffer */
    result = Py_BuildValue("i" BYTES_FORMAT_STRING "i", request_id,
                           buffer_get_buffer(buffer),
                           (Py_ssize_t)buffer_get_position(buffer),
                           max_size);
fail:
    PyMem_Free(collection_name);
    destroy_codec_options(&options);
    if (buffer) {
        buffer_free(buffer);
    }
    Py_XDECREF(cluster_time);
    return result;
}

static PyObject* _cbson_get_more_message(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    char* collection_name = NULL;
    Py_ssize_t collection_name_length;
    int num_to_return;
    long long cursor_id;
    buffer_t buffer = NULL;
    int length_location, message_length;
    PyObject* result = NULL;

    if (!PyArg_ParseTuple(args, "et#iL",
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &num_to_return,
                          &cursor_id)) {
        return NULL;
    }
    buffer = buffer_new();
    if (!buffer) {
        goto fail;
    }

    // save space for message length
    length_location = buffer_save_space(buffer, 4);
    if (length_location == -1) {
        goto fail;
    }
    if (!buffer_write_int32(buffer, (int32_t)request_id) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"
                            "\xd5\x07\x00\x00"
                            "\x00\x00\x00\x00", 12) ||
        !buffer_write_bytes_ssize_t(buffer,
                                    collection_name,
                                    collection_name_length + 1) ||
        !buffer_write_int32(buffer, (int32_t)num_to_return) ||
        !buffer_write_int64(buffer, (int64_t)cursor_id)) {
        goto fail;
    }

    message_length = buffer_get_position(buffer) - length_location;
    buffer_write_int32_at_position(
        buffer, length_location, (int32_t)message_length);

    /* objectify buffer */
    result = Py_BuildValue("i" BYTES_FORMAT_STRING, request_id,
                           buffer_get_buffer(buffer),
                           (Py_ssize_t)buffer_get_position(buffer));
fail:
    PyMem_Free(collection_name);
    if (buffer) {
        buffer_free(buffer);
    }
    return result;
}

/*
 * NOTE this method handles multiple documents in a type one payload but
 * it does not perform batch splitting and the total message size is
 * only checked *after* generating the entire message.
 */
static PyObject* _cbson_op_msg(PyObject* self, PyObject* args) {
    struct module_state *state = GETSTATE(self);

    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    unsigned int flags;
    PyObject* command;
    char* identifier = NULL;
    Py_ssize_t identifier_length = 0;
    PyObject* docs;
    PyObject* doc;
    unsigned char check_keys = 0;
    codec_options_t options;
    buffer_t buffer = NULL;
    int length_location, message_length;
    int total_size = 0;
    int max_doc_size = 0;
    PyObject* result = NULL;
    PyObject* iterator = NULL;

    /*flags, command, identifier, docs, check_keys, opts*/
    if (!PyArg_ParseTuple(args, "IOet#ObO&",
                          &flags,
                          &command,
                          "utf-8",
                          &identifier,
                          &identifier_length,
                          &docs,
                          &check_keys,
                          convert_codec_options, &options)) {
        return NULL;
    }
    buffer = buffer_new();
    if (!buffer) {
        goto fail;
    }

    // save space for message length
    length_location = buffer_save_space(buffer, 4);
    if (length_location == -1) {
        goto fail;
    }
    if (!buffer_write_int32(buffer, (int32_t)request_id) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00" /* responseTo */
                            "\xdd\x07\x00\x00" /* 2013 */, 8)) {
        goto fail;
    }

    if (!buffer_write_int32(buffer, (int32_t)flags) ||
        !buffer_write_bytes(buffer, "\x00", 1) /* Payload type 0 */) {
        goto fail;
    }
    total_size = write_dict(state->_cbson, buffer, command, 0,
                          &options, 1);
    if (!total_size) {
        goto fail;
    }

    if (identifier_length) {
        int payload_one_length_location, payload_length;
        /* Payload type 1 */
        if (!buffer_write_bytes(buffer, "\x01", 1)) {
            goto fail;
        }
        /* save space for payload 0 length */
        payload_one_length_location = buffer_save_space(buffer, 4);
        /* C string identifier */
        if (!buffer_write_bytes_ssize_t(buffer, identifier, identifier_length + 1)) {
            goto fail;
        }
        iterator = PyObject_GetIter(docs);
        if (iterator == NULL) {
            goto fail;
        }
        while ((doc = PyIter_Next(iterator)) != NULL) {
            int encoded_doc_size = write_dict(
                      state->_cbson, buffer, doc, check_keys,
                      &options, 1);
            if (!encoded_doc_size) {
                Py_CLEAR(doc);
                goto fail;
            }
            if (encoded_doc_size > max_doc_size) {
                max_doc_size = encoded_doc_size;
            }
            Py_CLEAR(doc);
        }

        payload_length = buffer_get_position(buffer) - payload_one_length_location;
        buffer_write_int32_at_position(
            buffer, payload_one_length_location, (int32_t)payload_length);
        total_size += payload_length;
    }

    message_length = buffer_get_position(buffer) - length_location;
    buffer_write_int32_at_position(
        buffer, length_location, (int32_t)message_length);

    /* objectify buffer */
    result = Py_BuildValue("i" BYTES_FORMAT_STRING "ii", request_id,
                           buffer_get_buffer(buffer),
                           (Py_ssize_t)buffer_get_position(buffer),
                           total_size,
                           max_doc_size);
fail:
    Py_XDECREF(iterator);
    if (buffer) {
        buffer_free(buffer);
    }
    PyMem_Free(identifier);
    destroy_codec_options(&options);
    return result;
}


static void
_set_document_too_large(int size, long max) {
    PyObject* DocumentTooLarge = _error("DocumentTooLarge");
    if (DocumentTooLarge) {
#if PY_MAJOR_VERSION >= 3
        PyObject* error = PyUnicode_FromFormat(DOC_TOO_LARGE_FMT, size, max);
#else
        PyObject* error = PyString_FromFormat(DOC_TOO_LARGE_FMT, size, max);
#endif
        if (error) {
            PyErr_SetObject(DocumentTooLarge, error);
            Py_DECREF(error);
        }
        Py_DECREF(DocumentTooLarge);
    }
}

static PyObject*
_send_insert(PyObject* self, PyObject* ctx,
             PyObject* gle_args, buffer_t buffer,
             char* coll_name, Py_ssize_t coll_len, int request_id, int safe,
             codec_options_t* options, PyObject* to_publish, int compress) {

    if (safe) {
        if (!add_last_error(self, buffer, request_id,
                            coll_name, coll_len, options, gle_args)) {
            return NULL;
        }
    }

    /* The max_doc_size parameter for legacy_bulk_insert is the max size of
     * any document in buffer. We enforced max size already, pass 0 here. */
    return PyObject_CallMethod(ctx, "legacy_bulk_insert",
                               "i" BYTES_FORMAT_STRING "iNOi",
                               request_id,
                               buffer_get_buffer(buffer),
                               (Py_ssize_t)buffer_get_position(buffer),
                               0,
                               PyBool_FromLong((long)safe),
                               to_publish, compress);
}

static PyObject* _cbson_do_batched_insert(PyObject* self, PyObject* args) {
    struct module_state *state = GETSTATE(self);

    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    int send_safe, flags = 0;
    int length_location, message_length;
    Py_ssize_t collection_name_length;
    int compress;
    char* collection_name = NULL;
    PyObject* docs;
    PyObject* doc;
    PyObject* iterator;
    PyObject* ctx;
    PyObject* last_error_args;
    PyObject* result;
    PyObject* max_bson_size_obj;
    PyObject* max_message_size_obj;
    PyObject* compress_obj;
    PyObject* to_publish = NULL;
    unsigned char check_keys;
    unsigned char safe;
    unsigned char continue_on_error;
    codec_options_t options;
    unsigned char empty = 1;
    long max_bson_size;
    long max_message_size;
    buffer_t buffer;
    PyObject *exc_type = NULL, *exc_value = NULL, *exc_trace = NULL;

    if (!PyArg_ParseTuple(args, "et#ObbObO&O",
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &docs, &check_keys, &safe,
                          &last_error_args,
                          &continue_on_error,
                          convert_codec_options, &options,
                          &ctx)) {
        return NULL;
    }
    if (continue_on_error) {
        flags += 1;
    }
    /*
     * If we are doing unacknowledged writes *and* continue_on_error
     * is True it's pointless (and slower) to send GLE.
     */
    send_safe = (safe || !continue_on_error);
    max_bson_size_obj = PyObject_GetAttrString(ctx, "max_bson_size");
#if PY_MAJOR_VERSION >= 3
    max_bson_size = PyLong_AsLong(max_bson_size_obj);
#else
    max_bson_size = PyInt_AsLong(max_bson_size_obj);
#endif
    Py_XDECREF(max_bson_size_obj);
    if (max_bson_size == -1) {
        destroy_codec_options(&options);
        PyMem_Free(collection_name);
        return NULL;
    }

    max_message_size_obj = PyObject_GetAttrString(ctx, "max_message_size");
#if PY_MAJOR_VERSION >= 3
    max_message_size = PyLong_AsLong(max_message_size_obj);
#else
    max_message_size = PyInt_AsLong(max_message_size_obj);
#endif
    Py_XDECREF(max_message_size_obj);
    if (max_message_size == -1) {
        destroy_codec_options(&options);
        PyMem_Free(collection_name);
        return NULL;
    }

    compress_obj = PyObject_GetAttrString(ctx, "compress");
    compress = PyObject_IsTrue(compress_obj);
    Py_XDECREF(compress_obj);
    if (compress == -1) {
        destroy_codec_options(&options);
        PyMem_Free(collection_name);
        return NULL;
    }

    compress = compress && !(safe || send_safe);

    buffer = buffer_new();
    if (!buffer) {
        destroy_codec_options(&options);
        PyMem_Free(collection_name);
        return NULL;
    }

    length_location = init_insert_buffer(buffer,
                                         request_id,
                                         flags,
                                         collection_name,
                                         collection_name_length,
                                         compress);
    if (length_location == -1) {
        goto insertfail;
    }

    if (!(to_publish = PyList_New(0))) {
        goto insertfail;
    }

    iterator = PyObject_GetIter(docs);
    if (iterator == NULL) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "input is not iterable");
            Py_DECREF(InvalidOperation);
        }
        goto insertfail;
    }
    while ((doc = PyIter_Next(iterator)) != NULL) {
        int before = buffer_get_position(buffer);
        int cur_size;
        if (!write_dict(state->_cbson, buffer, doc, check_keys,
                        &options, 1)) {
            goto iterfail;
        }

        cur_size = buffer_get_position(buffer) - before;
        if (cur_size > max_bson_size) {
            /* If we've encoded anything send it before raising. */
            if (!empty) {
                buffer_update_position(buffer, before);
                if (!compress) {
                    message_length = buffer_get_position(buffer) - length_location;
                    buffer_write_int32_at_position(
                        buffer, length_location, (int32_t)message_length);
                }
                result = _send_insert(self, ctx, last_error_args, buffer,
                                      collection_name, collection_name_length,
                                      request_id, send_safe, &options,
                                      to_publish, compress);
                if (!result)
                    goto iterfail;
                Py_DECREF(result);
            }
            _set_document_too_large(cur_size, max_bson_size);
            goto iterfail;
        }
        empty = 0;

        /* We have enough data, send this batch. */
        if (buffer_get_position(buffer) > max_message_size) {
            int new_request_id = rand();
            int message_start;
            buffer_t new_buffer = buffer_new();
            if (!new_buffer) {
                goto iterfail;
            }
            message_start = init_insert_buffer(new_buffer,
                                               new_request_id,
                                               flags,
                                               collection_name,
                                               collection_name_length,
                                               compress);
            if (message_start == -1) {
                buffer_free(new_buffer);
                goto iterfail;
            }

            /* Copy the overflow encoded document into the new buffer. */
            if (!buffer_write_bytes(new_buffer,
                (const char*)buffer_get_buffer(buffer) + before, cur_size)) {
                buffer_free(new_buffer);
                goto iterfail;
            }

            /* Roll back to the beginning of this document. */
            buffer_update_position(buffer, before);
            if (!compress) {
                message_length = buffer_get_position(buffer) - length_location;
                buffer_write_int32_at_position(
                    buffer, length_location, (int32_t)message_length);
            }

            result = _send_insert(self, ctx, last_error_args, buffer,
                                  collection_name, collection_name_length,
                                  request_id, send_safe, &options, to_publish,
                                  compress);

            buffer_free(buffer);
            buffer = new_buffer;
            request_id = new_request_id;
            length_location = message_start;

            Py_DECREF(to_publish);
            if (!(to_publish = PyList_New(0))) {
                goto insertfail;
            }

            if (!result) {
                PyObject *etype = NULL, *evalue = NULL, *etrace = NULL;
                PyObject* OperationFailure;
                PyErr_Fetch(&etype, &evalue, &etrace);
                OperationFailure = _error("OperationFailure");
                if (OperationFailure) {
                    if (PyErr_GivenExceptionMatches(etype, OperationFailure)) {
                        if (!safe || continue_on_error) {
                            Py_DECREF(OperationFailure);
                            if (!safe) {
                                /* We're doing unacknowledged writes and
                                 * continue_on_error is False. Just return. */
                                Py_DECREF(etype);
                                Py_XDECREF(evalue);
                                Py_XDECREF(etrace);
                                Py_DECREF(to_publish);
                                Py_DECREF(iterator);
                                Py_DECREF(doc);
                                buffer_free(buffer);
                                PyMem_Free(collection_name);
                                Py_RETURN_NONE;
                            }
                            /* continue_on_error is True, store the error
                             * details to re-raise after the final batch */
                            Py_XDECREF(exc_type);
                            Py_XDECREF(exc_value);
                            Py_XDECREF(exc_trace);
                            exc_type = etype;
                            exc_value = evalue;
                            exc_trace = etrace;
                            if (PyList_Append(to_publish, doc) < 0) {
                                goto iterfail;
                            }
                            Py_CLEAR(doc);
                            continue;
                        }
                    }
                    Py_DECREF(OperationFailure);
                }
                /* This isn't OperationFailure, we couldn't
                 * import OperationFailure, or we are doing
                 * acknowledged writes. Re-raise immediately. */
                PyErr_Restore(etype, evalue, etrace);
                goto iterfail;
            } else {
                Py_DECREF(result);
            }
        }
        if (PyList_Append(to_publish, doc) < 0) {
            goto iterfail;
        }
        Py_CLEAR(doc);
    }
    Py_DECREF(iterator);

    if (PyErr_Occurred()) {
        goto insertfail;
    }

    if (empty) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "cannot do an empty bulk insert");
            Py_DECREF(InvalidOperation);
        }
        goto insertfail;
    }

    if (!compress) {
        message_length = buffer_get_position(buffer) - length_location;
        buffer_write_int32_at_position(
            buffer, length_location, (int32_t)message_length);
    }

    /* Send the last (or only) batch */
    result = _send_insert(self, ctx, last_error_args, buffer,
                          collection_name, collection_name_length,
                          request_id, safe, &options, to_publish, compress);

    Py_DECREF(to_publish);
    PyMem_Free(collection_name);
    buffer_free(buffer);

    if (!result) {
        Py_XDECREF(exc_type);
        Py_XDECREF(exc_value);
        Py_XDECREF(exc_trace);
        return NULL;
    } else {
        Py_DECREF(result);
    }

    if (exc_type) {
        /* Re-raise any previously stored exception
         * due to continue_on_error being True */
        PyErr_Restore(exc_type, exc_value, exc_trace);
        return NULL;
    }

    Py_RETURN_NONE;

iterfail:
    Py_XDECREF(doc);
    Py_DECREF(iterator);
insertfail:
    Py_XDECREF(exc_type);
    Py_XDECREF(exc_value);
    Py_XDECREF(exc_trace);
    Py_XDECREF(to_publish);
    buffer_free(buffer);
    PyMem_Free(collection_name);
    return NULL;
}

#define _INSERT 0
#define _UPDATE 1
#define _DELETE 2

/* OP_MSG ----------------------------------------------- */

static int
_batched_op_msg(
        unsigned char op, unsigned char check_keys, unsigned char ack,
        PyObject* command, PyObject* docs, PyObject* ctx,
        PyObject* to_publish, codec_options_t options,
        buffer_t buffer, struct module_state *state) {

    long max_bson_size;
    long max_write_batch_size;
    long max_message_size;
    int idx = 0;
    int size_location;
    int position;
    int length;
    PyObject* max_bson_size_obj = NULL;
    PyObject* max_write_batch_size_obj = NULL;
    PyObject* max_message_size_obj = NULL;
    PyObject* doc = NULL;
    PyObject* iterator = NULL;
    char* flags = ack ? "\x00\x00\x00\x00" : "\x02\x00\x00\x00";

    max_bson_size_obj = PyObject_GetAttrString(ctx, "max_bson_size");
#if PY_MAJOR_VERSION >= 3
    max_bson_size = PyLong_AsLong(max_bson_size_obj);
#else
    max_bson_size = PyInt_AsLong(max_bson_size_obj);
#endif
    Py_XDECREF(max_bson_size_obj);
    if (max_bson_size == -1) {
        return 0;
    }

    max_write_batch_size_obj = PyObject_GetAttrString(ctx, "max_write_batch_size");
#if PY_MAJOR_VERSION >= 3
    max_write_batch_size = PyLong_AsLong(max_write_batch_size_obj);
#else
    max_write_batch_size = PyInt_AsLong(max_write_batch_size_obj);
#endif
    Py_XDECREF(max_write_batch_size_obj);
    if (max_write_batch_size == -1) {
        return 0;
    }

    max_message_size_obj = PyObject_GetAttrString(ctx, "max_message_size");
#if PY_MAJOR_VERSION >= 3
    max_message_size = PyLong_AsLong(max_message_size_obj);
#else
    max_message_size = PyInt_AsLong(max_message_size_obj);
#endif
    Py_XDECREF(max_message_size_obj);
    if (max_message_size == -1) {
        return 0;
    }

    if (!buffer_write_bytes(buffer, flags, 4)) {
        return 0;
    }
    /* Type 0 Section */
    if (!buffer_write_bytes(buffer, "\x00", 1)) {
        return 0;
    }
    if (!write_dict(state->_cbson, buffer, command, 0,
                    &options, 0)) {
        return 0;
    }

    /* Type 1 Section */
    if (!buffer_write_bytes(buffer, "\x01", 1)) {
        return 0;
    }
    /* Save space for size */
    size_location = buffer_save_space(buffer, 4);
    if (size_location == -1) {
        return 0;
    }

    switch (op) {
    case _INSERT:
        {
            if (!buffer_write_bytes(buffer, "documents\x00", 10))
                goto fail;
            break;
        }
    case _UPDATE:
        {
            /* MongoDB does key validation for update. */
            check_keys = 0;
            if (!buffer_write_bytes(buffer, "updates\x00", 8))
                goto fail;
            break;
        }
    case _DELETE:
        {
            /* Never check keys in a delete command. */
            check_keys = 0;
            if (!buffer_write_bytes(buffer, "deletes\x00", 8))
                goto fail;
            break;
        }
    default:
        {
            PyObject* InvalidOperation = _error("InvalidOperation");
            if (InvalidOperation) {
                PyErr_SetString(InvalidOperation, "Unknown command");
                Py_DECREF(InvalidOperation);
            }
            return 0;
        }
    }

    iterator = PyObject_GetIter(docs);
    if (iterator == NULL) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "input is not iterable");
            Py_DECREF(InvalidOperation);
        }
        return 0;
    }
    while ((doc = PyIter_Next(iterator)) != NULL) {
        int cur_doc_begin = buffer_get_position(buffer);
        int cur_size;
        int doc_too_large = 0;
        int unacked_doc_too_large = 0;
        if (!write_dict(state->_cbson, buffer, doc, check_keys,
                        &options, 1)) {
            goto fail;
        }
        cur_size = buffer_get_position(buffer) - cur_doc_begin;

        /* Does the first document exceed max_message_size? */
        doc_too_large = (idx == 0 && (buffer_get_position(buffer) > max_message_size));
        /* When OP_MSG is used unacknowledged we have to check
         * document size client side or applications won't be notified.
         * Otherwise we let the server deal with documents that are too large
         * since ordered=False causes those documents to be skipped instead of
         * halting the bulk write operation.
         * */
        unacked_doc_too_large = (!ack && cur_size > max_bson_size);
        if (doc_too_large || unacked_doc_too_large) {
            if (op == _INSERT) {
                _set_document_too_large(cur_size, max_bson_size);
            } else {
                PyObject* DocumentTooLarge = _error("DocumentTooLarge");
                if (DocumentTooLarge) {
                    /*
                     * There's nothing intelligent we can say
                     * about size for update and delete.
                     */
                    PyErr_Format(
                        DocumentTooLarge,
                        "%s command document too large",
                        (op == _UPDATE) ? "update": "delete");
                    Py_DECREF(DocumentTooLarge);
                }
            }
            goto fail;
        }
        /* We have enough data, return this batch. */
        if (buffer_get_position(buffer) > max_message_size) {
            /*
             * Roll the existing buffer back to the beginning
             * of the last document encoded.
             */
            buffer_update_position(buffer, cur_doc_begin);
            Py_CLEAR(doc);
            break;
        }
        if (PyList_Append(to_publish, doc) < 0) {
            goto fail;
        }
        Py_CLEAR(doc);
        idx += 1;
        /* We have enough documents, return this batch. */
        if (idx == max_write_batch_size) {
            break;
        }
    }
    Py_CLEAR(iterator);

    if (PyErr_Occurred()) {
        goto fail;
    }

    position = buffer_get_position(buffer);
    length = position - size_location;
    buffer_write_int32_at_position(buffer, size_location, (int32_t)length);
    return 1;

fail:
    Py_XDECREF(doc);
    Py_XDECREF(iterator);
    return 0;
}

static PyObject*
_cbson_encode_batched_op_msg(PyObject* self, PyObject* args) {
    unsigned char op;
    unsigned char check_keys;
    unsigned char ack;
    PyObject* command;
    PyObject* docs;
    PyObject* ctx = NULL;
    PyObject* to_publish = NULL;
    PyObject* result = NULL;
    codec_options_t options;
    buffer_t buffer;
    struct module_state *state = GETSTATE(self);

    if (!PyArg_ParseTuple(args, "bOObbO&O",
                          &op, &command, &docs, &check_keys, &ack,
                          convert_codec_options, &options,
                          &ctx)) {
        return NULL;
    }
    if (!(buffer = buffer_new())) {
        destroy_codec_options(&options);
        return NULL;
    }
    if (!(to_publish = PyList_New(0))) {
        goto fail;
    }

    if (!_batched_op_msg(
            op,
            check_keys,
            ack,
            command,
            docs,
            ctx,
            to_publish,
            options,
            buffer,
            state)) {
        goto fail;
    }

    result = Py_BuildValue(BYTES_FORMAT_STRING "O",
                           buffer_get_buffer(buffer),
                           (Py_ssize_t)buffer_get_position(buffer),
                           to_publish);
fail:
    destroy_codec_options(&options);
    buffer_free(buffer);
    Py_XDECREF(to_publish);
    return result;
}

static PyObject*
_cbson_batched_op_msg(PyObject* self, PyObject* args) {
    unsigned char op;
    unsigned char check_keys;
    unsigned char ack;
    int request_id;
    int position;
    PyObject* command;
    PyObject* docs;
    PyObject* ctx = NULL;
    PyObject* to_publish = NULL;
    PyObject* result = NULL;
    codec_options_t options;
    buffer_t buffer;
    struct module_state *state = GETSTATE(self);

    if (!PyArg_ParseTuple(args, "bOObbO&O",
                          &op, &command, &docs, &check_keys, &ack,
                          convert_codec_options, &options,
                          &ctx)) {
        return NULL;
    }
    if (!(buffer = buffer_new())) {
        destroy_codec_options(&options);
        return NULL;
    }
    /* Save space for message length and request id */
    if ((buffer_save_space(buffer, 8)) == -1) {
        goto fail;
    }
    if (!buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"  /* responseTo */
                            "\xdd\x07\x00\x00", /* opcode */
                            8)) {
        goto fail;
    }
    if (!(to_publish = PyList_New(0))) {
        goto fail;
    }

    if (!_batched_op_msg(
            op,
            check_keys,
            ack,
            command,
            docs,
            ctx,
            to_publish,
            options,
            buffer,
            state)) {
        goto fail;
    }

    request_id = rand();
    position = buffer_get_position(buffer);
    buffer_write_int32_at_position(buffer, 0, (int32_t)position);
    buffer_write_int32_at_position(buffer, 4, (int32_t)request_id);
    result = Py_BuildValue("i" BYTES_FORMAT_STRING "O", request_id,
                           buffer_get_buffer(buffer),
                           (Py_ssize_t)buffer_get_position(buffer),
                           to_publish);
fail:
    destroy_codec_options(&options);
    buffer_free(buffer);
    Py_XDECREF(to_publish);
    return result;
}

/* End OP_MSG -------------------------------------------- */

static int
_batched_write_command(
        char* ns, Py_ssize_t ns_len, unsigned char op, int check_keys,
        PyObject* command, PyObject* docs, PyObject* ctx,
        PyObject* to_publish, codec_options_t options,
        buffer_t buffer, struct module_state *state) {

    long max_bson_size;
    long max_cmd_size;
    long max_write_batch_size;
    long max_split_size;
    int idx = 0;
    int cmd_len_loc;
    int lst_len_loc;
    int position;
    int length;
    PyObject* max_bson_size_obj = NULL;
    PyObject* max_write_batch_size_obj = NULL;
    PyObject* max_split_size_obj = NULL;
    PyObject* doc = NULL;
    PyObject* iterator = NULL;

    max_bson_size_obj = PyObject_GetAttrString(ctx, "max_bson_size");
#if PY_MAJOR_VERSION >= 3
    max_bson_size = PyLong_AsLong(max_bson_size_obj);
#else
    max_bson_size = PyInt_AsLong(max_bson_size_obj);
#endif
    Py_XDECREF(max_bson_size_obj);
    if (max_bson_size == -1) {
        return 0;
    }
    /*
     * Max BSON object size + 16k - 2 bytes for ending NUL bytes
     * XXX: This should come from the server - SERVER-10643
     */
    max_cmd_size = max_bson_size + 16382;

    max_write_batch_size_obj = PyObject_GetAttrString(ctx, "max_write_batch_size");
#if PY_MAJOR_VERSION >= 3
    max_write_batch_size = PyLong_AsLong(max_write_batch_size_obj);
#else
    max_write_batch_size = PyInt_AsLong(max_write_batch_size_obj);
#endif
    Py_XDECREF(max_write_batch_size_obj);
    if (max_write_batch_size == -1) {
        return 0;
    }

    // max_split_size is the size at which to perform a batch split.
    // Normally this this value is equal to max_bson_size (16MiB). However,
    // when auto encryption is enabled max_split_size is reduced to 2MiB.
    max_split_size_obj = PyObject_GetAttrString(ctx, "max_split_size");
#if PY_MAJOR_VERSION >= 3
    max_split_size = PyLong_AsLong(max_split_size_obj);
#else
    max_split_size = PyInt_AsLong(max_split_size_obj);
#endif
    Py_XDECREF(max_split_size_obj);
    if (max_split_size == -1) {
        return 0;
    }

    if (!buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00", /* flags */
                            4) ||
        !buffer_write_bytes_ssize_t(buffer, ns, ns_len + 1) || /* namespace */
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"  /* skip */
                            "\xFF\xFF\xFF\xFF", /* limit (-1) */
                             8)) {
        return 0;
    }

    /* Position of command document length */
    cmd_len_loc = buffer_get_position(buffer);
    if (!write_dict(state->_cbson, buffer, command, 0,
                    &options, 0)) {
        return 0;
    }

    /* Write type byte for array */
    *(buffer_get_buffer(buffer) + (buffer_get_position(buffer) - 1)) = 0x4;

    switch (op) {
    case _INSERT:
        {
            if (!buffer_write_bytes(buffer, "documents\x00", 10))
                goto fail;
            break;
        }
    case _UPDATE:
        {
            /* MongoDB does key validation for update. */
            check_keys = 0;
            if (!buffer_write_bytes(buffer, "updates\x00", 8))
                goto fail;
            break;
        }
    case _DELETE:
        {
            /* Never check keys in a delete command. */
            check_keys = 0;
            if (!buffer_write_bytes(buffer, "deletes\x00", 8))
                goto fail;
            break;
        }
    default:
        {
            PyObject* InvalidOperation = _error("InvalidOperation");
            if (InvalidOperation) {
                PyErr_SetString(InvalidOperation, "Unknown command");
                Py_DECREF(InvalidOperation);
            }
            return 0;
        }
    }

    /* Save space for list document */
    lst_len_loc = buffer_save_space(buffer, 4);
    if (lst_len_loc == -1) {
        return 0;
    }

    iterator = PyObject_GetIter(docs);
    if (iterator == NULL) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "input is not iterable");
            Py_DECREF(InvalidOperation);
        }
        return 0;
    }
    while ((doc = PyIter_Next(iterator)) != NULL) {
        int sub_doc_begin = buffer_get_position(buffer);
        int cur_doc_begin;
        int cur_size;
        int enough_data = 0;
        char key[16];
        INT2STRING(key, idx);
        if (!buffer_write_bytes(buffer, "\x03", 1) ||
            !buffer_write_bytes(buffer, key, (int)strlen(key) + 1)) {
            goto fail;
        }
        cur_doc_begin = buffer_get_position(buffer);
        if (!write_dict(state->_cbson, buffer, doc,
                        check_keys, &options, 1)) {
            goto fail;
        }

        /* We have enough data, return this batch.
         * max_cmd_size accounts for the two trailing null bytes.
         */
        cur_size = buffer_get_position(buffer) - cur_doc_begin;
        /* This single document is too large for the command. */
        if (cur_size > max_cmd_size) {
            if (op == _INSERT) {
                _set_document_too_large(cur_size, max_bson_size);
            } else {
                PyObject* DocumentTooLarge = _error("DocumentTooLarge");
                if (DocumentTooLarge) {
                    /*
                     * There's nothing intelligent we can say
                     * about size for update and delete.
                     */
                    PyErr_Format(
                        DocumentTooLarge,
                        "%s command document too large",
                        (op == _UPDATE) ? "update": "delete");
                    Py_DECREF(DocumentTooLarge);
                }
            }
            goto fail;
        }
        enough_data = (idx >= 1 &&
                       (buffer_get_position(buffer) > max_split_size));
        if (enough_data) {
            /*
             * Roll the existing buffer back to the beginning
             * of the last document encoded.
             */
            buffer_update_position(buffer, sub_doc_begin);
            Py_CLEAR(doc);
            break;
        }
        if (PyList_Append(to_publish, doc) < 0) {
            goto fail;
        }
        Py_CLEAR(doc);
        idx += 1;
        /* We have enough documents, return this batch. */
        if (idx == max_write_batch_size) {
            break;
        }
    }
    Py_CLEAR(iterator);

    if (PyErr_Occurred()) {
        goto fail;
    }

    if (!buffer_write_bytes(buffer, "\x00\x00", 2)) {
        goto fail;
    }

    position = buffer_get_position(buffer);
    length = position - lst_len_loc - 1;
    buffer_write_int32_at_position(buffer, lst_len_loc, (int32_t)length);
    length = position - cmd_len_loc;
    buffer_write_int32_at_position(buffer, cmd_len_loc, (int32_t)length);
    return 1;

fail:
    Py_XDECREF(doc);
    Py_XDECREF(iterator);
    return 0;
}

static PyObject*
_cbson_encode_batched_write_command(PyObject* self, PyObject* args) {
    char *ns = NULL;
    unsigned char op;
    unsigned char check_keys;
    Py_ssize_t ns_len;
    PyObject* command;
    PyObject* docs;
    PyObject* ctx = NULL;
    PyObject* to_publish = NULL;
    PyObject* result = NULL;
    codec_options_t options;
    buffer_t buffer;
    struct module_state *state = GETSTATE(self);

    if (!PyArg_ParseTuple(args, "et#bOObO&O", "utf-8",
                          &ns, &ns_len, &op, &command, &docs, &check_keys,
                          convert_codec_options, &options,
                          &ctx)) {
        return NULL;
    }
    if (!(buffer = buffer_new())) {
        PyMem_Free(ns);
        destroy_codec_options(&options);
        return NULL;
    }
    if (!(to_publish = PyList_New(0))) {
        goto fail;
    }

    if (!_batched_write_command(
            ns,
            ns_len,
            op,
            check_keys,
            command,
            docs,
            ctx,
            to_publish,
            options,
            buffer,
            state)) {
        goto fail;
    }

    result = Py_BuildValue(BYTES_FORMAT_STRING "O",
                           buffer_get_buffer(buffer),
                           (Py_ssize_t)buffer_get_position(buffer),
                           to_publish);
fail:
    PyMem_Free(ns);
    destroy_codec_options(&options);
    buffer_free(buffer);
    Py_XDECREF(to_publish);
    return result;
}

static PyObject*
_cbson_batched_write_command(PyObject* self, PyObject* args) {
    char *ns = NULL;
    unsigned char op;
    unsigned char check_keys;
    Py_ssize_t ns_len;
    int request_id;
    int position;
    PyObject* command;
    PyObject* docs;
    PyObject* ctx = NULL;
    PyObject* to_publish = NULL;
    PyObject* result = NULL;
    codec_options_t options;
    buffer_t buffer;
    struct module_state *state = GETSTATE(self);

    if (!PyArg_ParseTuple(args, "et#bOObO&O", "utf-8",
                          &ns, &ns_len, &op, &command, &docs, &check_keys,
                          convert_codec_options, &options,
                          &ctx)) {
        return NULL;
    }
    if (!(buffer = buffer_new())) {
        PyMem_Free(ns);
        destroy_codec_options(&options);
        return NULL;
    }
    /* Save space for message length and request id */
    if ((buffer_save_space(buffer, 8)) == -1) {
        goto fail;
    }
    if (!buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"  /* responseTo */
                            "\xd4\x07\x00\x00", /* opcode */
                            8)) {
        goto fail;
    }
    if (!(to_publish = PyList_New(0))) {
        goto fail;
    }

    if (!_batched_write_command(
            ns,
            ns_len,
            op,
            check_keys,
            command,
            docs,
            ctx,
            to_publish,
            options,
            buffer,
            state)) {
        goto fail;
    }

    request_id = rand();
    position = buffer_get_position(buffer);
    buffer_write_int32_at_position(buffer, 0, (int32_t)position);
    buffer_write_int32_at_position(buffer, 4, (int32_t)request_id);
    result = Py_BuildValue("i" BYTES_FORMAT_STRING "O", request_id,
                           buffer_get_buffer(buffer),
                           (Py_ssize_t)buffer_get_position(buffer),
                           to_publish);
fail:
    PyMem_Free(ns);
    destroy_codec_options(&options);
    buffer_free(buffer);
    Py_XDECREF(to_publish);
    return result;
}

static PyMethodDef _CMessageMethods[] = {
    {"_insert_message", _cbson_insert_message, METH_VARARGS,
     "Create an insert message to be sent to MongoDB"},
    {"_update_message", _cbson_update_message, METH_VARARGS,
     "create an update message to be sent to MongoDB"},
    {"_query_message", _cbson_query_message, METH_VARARGS,
     "create a query message to be sent to MongoDB"},
    {"_get_more_message", _cbson_get_more_message, METH_VARARGS,
     "create a get more message to be sent to MongoDB"},
    {"_op_msg", _cbson_op_msg, METH_VARARGS,
     "create an OP_MSG message to be sent to MongoDB"},
    {"_do_batched_insert", _cbson_do_batched_insert, METH_VARARGS,
     "insert a batch of documents, splitting the batch as needed"},
    {"_batched_write_command", _cbson_batched_write_command, METH_VARARGS,
     "Create the next batched insert, update, or delete command"},
    {"_encode_batched_write_command", _cbson_encode_batched_write_command, METH_VARARGS,
     "Encode the next batched insert, update, or delete command"},
    {"_batched_op_msg", _cbson_batched_op_msg, METH_VARARGS,
     "Create the next batched insert, update, or delete using OP_MSG"},
    {"_encode_batched_op_msg", _cbson_encode_batched_op_msg, METH_VARARGS,
     "Encode the next batched insert, update, or delete using OP_MSG"},
    {NULL, NULL, 0, NULL}
};

#if PY_MAJOR_VERSION >= 3
#define INITERROR return NULL
static int _cmessage_traverse(PyObject *m, visitproc visit, void *arg) {
    Py_VISIT(GETSTATE(m)->_cbson);
    return 0;
}

static int _cmessage_clear(PyObject *m) {
    Py_CLEAR(GETSTATE(m)->_cbson);
    return 0;
}

static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "_cmessage",
        NULL,
        sizeof(struct module_state),
        _CMessageMethods,
        NULL,
        _cmessage_traverse,
        _cmessage_clear,
        NULL
};

PyMODINIT_FUNC
PyInit__cmessage(void)
#else
#define INITERROR return
PyMODINIT_FUNC
init_cmessage(void)
#endif
{
    PyObject *_cbson = NULL;
    PyObject *c_api_object = NULL;
    PyObject *m = NULL;

    /* Store a reference to the _cbson module since it's needed to call some
     * of its functions
     */
    _cbson = PyImport_ImportModule("bson._cbson");
    if (_cbson == NULL) {
        goto fail;
    }

    /* Import C API of _cbson
     * The header file accesses _cbson_API to call the functions
     */
    c_api_object = PyObject_GetAttrString(_cbson, "_C_API");
    if (c_api_object == NULL) {
        goto fail;
    }
#if PY_VERSION_HEX >= 0x03010000
    _cbson_API = (void **)PyCapsule_GetPointer(c_api_object, "_cbson._C_API");
#else
    _cbson_API = (void **)PyCObject_AsVoidPtr(c_api_object);
#endif
    if (_cbson_API == NULL) {
        goto fail;
    }

#if PY_MAJOR_VERSION >= 3
    /* Returns a new reference. */
    m = PyModule_Create(&moduledef);
#else
    /* Returns a borrowed reference. */
    m = Py_InitModule("_cmessage", _CMessageMethods);
#endif
    if (m == NULL) {
        goto fail;
    }

    GETSTATE(m)->_cbson = _cbson;

    Py_DECREF(c_api_object);

#if PY_MAJOR_VERSION >= 3
    return m;
#else
    return;
#endif

fail:
#if PY_MAJOR_VERSION >= 3
    Py_XDECREF(m);
#endif
    Py_XDECREF(c_api_object);
    Py_XDECREF(_cbson);
    INITERROR;
}
