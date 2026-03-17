/**
 * Copyright 2016 Confluent Inc.
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

#include "confluent_kafka.h"

#include <stdarg.h>


/**
 * @brief KNOWN ISSUES
 *
 *  - Partitioners will cause a dead-lock with librdkafka, because:
 *     GIL + topic lock in topic_new  is different lock order than
 *     topic lock in msg_partitioner + GIL.
 *     This needs to be sorted out in librdkafka, preferably making the
 *     partitioner run without any locks taken.
 *     Until this is fixed the partitioner is ignored and librdkafka's
 *     default will be used.
 *  - KafkaError type .tp_doc allocation is lost on exit.
 *
 */


PyObject *KafkaException;


/****************************************************************************
 *
 *
 * KafkaError
 *
 *
 * FIXME: Pre-create simple instances for each error code, only instantiate
 *        a new object if a rich error string is provided.
 *
 ****************************************************************************/
typedef struct {
#ifdef PY3
        PyException_HEAD
#else
        PyObject_HEAD
        /* Standard fields of PyBaseExceptionObject which we inherit from. */
        PyObject *dict;
        PyObject *args;
        PyObject *message;
#endif

	rd_kafka_resp_err_t code;  /* Error code */
	char *str;   /* Human readable representation of error, if one
		      * was provided by librdkafka.
		      * Else falls back on err2str(). */
        int   fatal; /**< Set to true if a fatal error. */
        int   retriable; /**< Set to true if operation is retriable. */
        int   txn_requires_abort; /**< Set to true if this is an abortable
                                   *   transaction error. */
} KafkaError;


static void cfl_PyErr_Fatal (rd_kafka_resp_err_t err, const char *reason);

static PyObject *KafkaError_code (KafkaError *self, PyObject *ignore) {
	return cfl_PyInt_FromInt(self->code);
}

static PyObject *KafkaError_str (KafkaError *self, PyObject *ignore) {
	if (self->str)
		return cfl_PyUnistr(_FromString(self->str));
	else
		return cfl_PyUnistr(_FromString(rd_kafka_err2str(self->code)));
}

static PyObject *KafkaError_name (KafkaError *self, PyObject *ignore) {
	/* FIXME: Pre-create name objects */
	return cfl_PyUnistr(_FromString(rd_kafka_err2name(self->code)));
}

static PyObject *KafkaError_fatal (KafkaError *self, PyObject *ignore) {
        PyObject *ret = self->fatal ? Py_True : Py_False;
        Py_INCREF(ret);
        return ret;
}

static PyObject *KafkaError_retriable (KafkaError *self, PyObject *ignore) {
        PyObject *ret = self->retriable ? Py_True : Py_False;
        Py_INCREF(ret);
        return ret;
}

static PyObject *
KafkaError_txn_requires_abort (KafkaError *self, PyObject *ignore) {
        PyObject *ret = self->txn_requires_abort ? Py_True : Py_False;
        Py_INCREF(ret);
        return ret;
}


static PyMethodDef KafkaError_methods[] = {
	{ "code", (PyCFunction)KafkaError_code, METH_NOARGS,
	  "  Returns the error/event code for comparison to"
	  "KafkaError.<ERR_CONSTANTS>.\n"
	  "\n"
	  "  :returns: error/event code\n"
	  "  :rtype: int\n"
	  "\n"
	},
	{ "str", (PyCFunction)KafkaError_str, METH_NOARGS,
	  "  Returns the human-readable error/event string.\n"
	  "\n"
	  "  :returns: error/event message string\n"
	  "  :rtype: str\n"
	  "\n"
	},
	{ "name", (PyCFunction)KafkaError_name, METH_NOARGS,
	  "  Returns the enum name for error/event.\n"
	  "\n"
	  "  :returns: error/event enum name string\n"
	  "  :rtype: str\n"
	  "\n"
	},
        { "fatal", (PyCFunction)KafkaError_fatal, METH_NOARGS,
          "  :returns: True if this a fatal error, else False.\n"
          "  :rtype: bool\n"
          "\n"
        },
        { "retriable", (PyCFunction)KafkaError_retriable, METH_NOARGS,
          "  :returns: True if the operation that failed may be retried, "
          "else False.\n"
          "  :rtype: bool\n"
          "\n"
        },
        { "txn_requires_abort", (PyCFunction)KafkaError_txn_requires_abort,
          METH_NOARGS,
          "  :returns: True if the error is an abortable transaction error "
          "in which case application must abort the current transaction with "
          "abort_transaction() and start a new transaction with "
          "begin_transaction() if it wishes to proceed with "
          "transactional operations. "
          "This will only return true for errors from the transactional "
          "producer API.\n"
          "  :rtype: bool\n"
          "\n"
        },

	{ NULL }
};


static void KafkaError_clear (PyObject *self0) {
        KafkaError *self = (KafkaError *)self0;
        if (self->str) {
                free(self->str);
                self->str = NULL;
        }
}

static void KafkaError_dealloc (PyObject *self0) {
        KafkaError *self = (KafkaError *)self0;
        KafkaError_clear(self0);;
        PyObject_GC_UnTrack(self0);
        Py_TYPE(self)->tp_free(self0);
}



static int KafkaError_traverse (KafkaError *self,
                                visitproc visit, void *arg) {
        return 0;
}

static PyObject *KafkaError_str0 (KafkaError *self) {
	return cfl_PyUnistr(_FromFormat("KafkaError{%scode=%s,val=%d,str=\"%s\"}",
                                        self->fatal?"FATAL,":"",
					 rd_kafka_err2name(self->code),
					 self->code,
					 self->str ? self->str :
					 rd_kafka_err2str(self->code)));
}

static long KafkaError_hash (KafkaError *self) {
	return self->code;
}

static PyTypeObject KafkaErrorType;



static PyObject* KafkaError_richcompare (KafkaError *self, PyObject *o2,
					 int op) {
	int code2;
	int r;
	PyObject *result;

	if (Py_TYPE(o2) == &KafkaErrorType)
		code2 = ((KafkaError *)o2)->code;
	else
		code2 = cfl_PyInt_AsInt(o2);

	switch (op)
	{
	case Py_LT:
		r = self->code < code2;
		break;
	case Py_LE:
		r = self->code <= code2;
		break;
	case Py_EQ:
		r = self->code == code2;
		break;
	case Py_NE:
		r = self->code != code2;
		break;
	case Py_GT:
		r = self->code > code2;
		break;
	case Py_GE:
		r = self->code >= code2;
		break;
	default:
		r = 0;
		break;
	}

	result = r ? Py_True : Py_False;
	Py_INCREF(result);
	return result;
}

static PyObject *KafkaError_new (PyTypeObject *type, PyObject *args,
                                 PyObject *kwargs) {
        return type->tp_alloc(type, 0);
}

/**
 * @brief Initialize a KafkaError object.
 */
static void KafkaError_init (KafkaError *self,
                             rd_kafka_resp_err_t code, const char *str) {
        self->code = code;
        self->fatal = 0;
        self->retriable = 0;
        self->txn_requires_abort = 0;
        if (str)
                self->str = strdup(str);
        else
                self->str = NULL;
}

static int KafkaError_init0 (PyObject *selfobj, PyObject *args,
                             PyObject *kwargs) {
        KafkaError *self = (KafkaError *)selfobj;
        int code;
        int fatal = 0, retriable = 0, txn_requires_abort = 0;
        const char *reason = NULL;
        static char *kws[] = { "error", "reason", "fatal",
                               "retriable", "txn_requires_abort", NULL };

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|ziii", kws, &code,
            &reason, &fatal, &retriable, &txn_requires_abort))
                return -1;

        KafkaError_init(self, code, reason ? reason : rd_kafka_err2str(code));
        self->fatal = fatal;
        self->retriable = retriable;
        self->txn_requires_abort = txn_requires_abort;

        return 0;
}

static PyTypeObject KafkaErrorType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"cimpl.KafkaError",      /*tp_name*/
	sizeof(KafkaError),    /*tp_basicsize*/
	0,                         /*tp_itemsize*/
	(destructor)KafkaError_dealloc, /*tp_dealloc*/
	0,                         /*tp_print*/
	0,                         /*tp_getattr*/
	0,                         /*tp_setattr*/
	0,                         /*tp_compare*/
	(reprfunc)KafkaError_str0, /*tp_repr*/
	0,                         /*tp_as_number*/
	0,                         /*tp_as_sequence*/
	0,                         /*tp_as_mapping*/
	(hashfunc)KafkaError_hash, /*tp_hash */
	0,                         /*tp_call*/
	0,                         /*tp_str*/
	PyObject_GenericGetAttr,   /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
        Py_TPFLAGS_BASE_EXC_SUBCLASS | Py_TPFLAGS_HAVE_GC, /*tp_flags*/
	"Kafka error and event object\n"
	"\n"
	"  The KafkaError class serves multiple purposes\n"
	"\n"
	"  - Propagation of errors\n"
	"  - Propagation of events\n"
	"  - Exceptions\n"
        "\n"
        "Args:\n"
        "  error (KafkaError): Error code indicating the type of error.\n"
        "\n"
        "  reason (str): Alternative message to describe the error.\n"
        "\n"
        "  fatal (bool): Set to true if a fatal error.\n"
        "\n"
        "  retriable (bool): Set to true if operation is retriable.\n"
        "\n"
        "  txn_requires_abort (bool): Set to true if this is an abortable\n"
        "  transaction error.\n"
	"\n", /*tp_doc*/
        (traverseproc)KafkaError_traverse, /* tp_traverse */
	(inquiry)KafkaError_clear, /* tp_clear */
	(richcmpfunc)KafkaError_richcompare, /* tp_richcompare */
	0,		           /* tp_weaklistoffset */
	0,		           /* tp_iter */
	0,		           /* tp_iternext */
	KafkaError_methods,    /* tp_methods */
	0,                         /* tp_members */
	0,                         /* tp_getset */
	0,                         /* tp_base */
	0,                         /* tp_dict */
	0,                         /* tp_descr_get */
	0,                         /* tp_descr_set */
	0,                         /* tp_dictoffset */
        KafkaError_init0,          /* tp_init */
	0,                         /* tp_alloc */
        KafkaError_new             /* tp_new */
};

/**
 * @brief Internal factory to create KafkaError object.
 */
PyObject *KafkaError_new0 (rd_kafka_resp_err_t err, const char *fmt, ...) {

	KafkaError *self;
	va_list ap;
	char buf[512];

	self = (KafkaError *)KafkaErrorType.
		tp_alloc(&KafkaErrorType, 0);
	if (!self)
		return NULL;

	if (fmt) {
		va_start(ap, fmt);
		vsnprintf(buf, sizeof(buf), fmt, ap);
		va_end(ap);
	}

	KafkaError_init(self, err, fmt ? buf : rd_kafka_err2str(err));

	return (PyObject *)self;
}

/**
 * @brief Internal factory to create KafkaError object.
 * @returns a new KafkaError object if \p err != 0, else a None object.
 */
 PyObject *KafkaError_new_or_None (rd_kafka_resp_err_t err, const char *str) {
	if (!err)
		Py_RETURN_NONE;
        if (str)
                return KafkaError_new0(err, "%s", str);
        else
                return KafkaError_new0(err, NULL);
}


/**
 * @brief Create a KafkaError object from  an rd_kafka_error_t *
 *        and destroy the C object when done.
 */
PyObject *KafkaError_new_from_error_destroy (rd_kafka_error_t *error) {
        KafkaError *kerr;

        kerr = (KafkaError *)KafkaError_new0(rd_kafka_error_code(error),
                                             "%s",
                                             rd_kafka_error_string(error));

        kerr->fatal = rd_kafka_error_is_fatal(error);
        kerr->retriable = rd_kafka_error_is_retriable(error);
        kerr->txn_requires_abort = rd_kafka_error_txn_requires_abort(error);
        rd_kafka_error_destroy(error);

        return (PyObject *)kerr;
}


/**
 * @brief Raise exception from fatal error.
 */
static void cfl_PyErr_Fatal (rd_kafka_resp_err_t err, const char *reason) {
        PyObject *eo = KafkaError_new0(err, "%s", reason);
        ((KafkaError *)eo)->fatal = 1;
        PyErr_SetObject(KafkaException, eo);
}


/****************************************************************************
 *
 *
 * Message
 *
 *
 *
 *
 ****************************************************************************/


/**
 * @returns a Message's error object, if any, else None.
 * @remark The error object refcount is increased by this function.
 */
PyObject *Message_error (Message *self, PyObject *ignore) {
	if (self->error) {
		Py_INCREF(self->error);
		return self->error;
	} else
		Py_RETURN_NONE;
}

static PyObject *Message_value (Message *self, PyObject *ignore) {
	if (self->value) {
		Py_INCREF(self->value);
		return self->value;
	} else
		Py_RETURN_NONE;
}


static PyObject *Message_key (Message *self, PyObject *ignore) {
	if (self->key) {
		Py_INCREF(self->key);
		return self->key;
	} else
		Py_RETURN_NONE;
}

static PyObject *Message_topic (Message *self, PyObject *ignore) {
	if (self->topic) {
		Py_INCREF(self->topic);
		return self->topic;
	} else
		Py_RETURN_NONE;
}

static PyObject *Message_partition (Message *self, PyObject *ignore) {
	if (self->partition != RD_KAFKA_PARTITION_UA)
		return cfl_PyInt_FromInt(self->partition);
	else
		Py_RETURN_NONE;
}


static PyObject *Message_offset (Message *self, PyObject *ignore) {
	if (self->offset >= 0)
		return PyLong_FromLongLong(self->offset);
	else
		Py_RETURN_NONE;
}

static PyObject *Message_leader_epoch (Message *self, PyObject *ignore) {
	if (self->leader_epoch >= 0)
		return cfl_PyInt_FromInt(self->leader_epoch);
	else
		Py_RETURN_NONE;
}


static PyObject *Message_timestamp (Message *self, PyObject *ignore) {
	return Py_BuildValue("iL",
			     self->tstype,
			     self->timestamp);
}

static PyObject *Message_latency (Message *self, PyObject *ignore) {
        if (self->latency == -1)
                Py_RETURN_NONE;
	return PyFloat_FromDouble((double)self->latency / 1000000.0);
}

static PyObject *Message_headers (Message *self, PyObject *ignore) {
#ifdef RD_KAFKA_V_HEADERS
	if (self->headers) {
        Py_INCREF(self->headers);
		return self->headers;
    } else if (self->c_headers) {
        self->headers = c_headers_to_py(self->c_headers);
        rd_kafka_headers_destroy(self->c_headers);
        self->c_headers = NULL;
        Py_INCREF(self->headers);
        return self->headers;
	} else {
		Py_RETURN_NONE;
    }
#else
		Py_RETURN_NONE;
#endif
}

static PyObject *Message_set_headers (Message *self, PyObject *new_headers) {
   if (self->headers)
        Py_DECREF(self->headers);
   self->headers = new_headers;
   Py_INCREF(self->headers);

   Py_RETURN_NONE;
}

static PyObject *Message_set_value (Message *self, PyObject *new_val) {
   if (self->value)
        Py_DECREF(self->value);
   self->value = new_val;
   Py_INCREF(self->value);

   Py_RETURN_NONE;
}

static PyObject *Message_set_key (Message *self, PyObject *new_key) {
   if (self->key)
        Py_DECREF(self->key);
   self->key = new_key;
   Py_INCREF(self->key);

   Py_RETURN_NONE;
}

static PyMethodDef Message_methods[] = {
	{ "error", (PyCFunction)Message_error, METH_NOARGS,
	  "  The message object is also used to propagate errors and events, "
	  "an application must check error() to determine if the Message "
	  "is a proper message (error() returns None) or an error or event "
	  "(error() returns a KafkaError object)\n"
	  "\n"
	  "  :rtype: None or :py:class:`KafkaError`\n"
	  "\n"
	},

	{ "value", (PyCFunction)Message_value, METH_NOARGS,
	  "  :returns: message value (payload) or None if not available.\n"
	  "  :rtype: str|bytes or None\n"
	  "\n"
	},
	{ "key", (PyCFunction)Message_key, METH_NOARGS,
	  "  :returns: message key or None if not available.\n"
	  "  :rtype: str|bytes or None\n"
	  "\n"
	},
	{ "topic", (PyCFunction)Message_topic, METH_NOARGS,
	  "  :returns: topic name or None if not available.\n"
	  "  :rtype: str or None\n"
	  "\n"
	},
	{ "partition", (PyCFunction)Message_partition, METH_NOARGS,
	  "  :returns: partition number or None if not available.\n"
	  "  :rtype: int or None\n"
	  "\n"
	},
	{ "offset", (PyCFunction)Message_offset, METH_NOARGS,
	  "  :returns: message offset or None if not available.\n"
	  "  :rtype: int or None\n"
	  "\n"
	},
        { "leader_epoch", (PyCFunction)Message_leader_epoch, METH_NOARGS,
	  "  :returns: message offset leader epoch or None if not available.\n"
	  "  :rtype: int or None\n"
	  "\n"
	},
	{ "timestamp", (PyCFunction)Message_timestamp, METH_NOARGS,
          "Retrieve timestamp type and timestamp from message.\n"
          "The timestamp type is one of:\n\n"
          "  * :py:const:`TIMESTAMP_NOT_AVAILABLE` "
          "- Timestamps not supported by broker.\n"
          "  * :py:const:`TIMESTAMP_CREATE_TIME` "
          "- Message creation time (or source / producer time).\n"
          "  * :py:const:`TIMESTAMP_LOG_APPEND_TIME` "
          "- Broker receive time.\n"
          "\n"
          "  The returned timestamp should be ignored if the timestamp type is "
          ":py:const:`TIMESTAMP_NOT_AVAILABLE`.\n"
          "\n"
          "  The timestamp is the number of milliseconds since the epoch (UTC).\n"
          "\n"
          "  Timestamps require broker version 0.10.0.0 or later and "
          "``{'api.version.request': True}`` configured on the client.\n"
          "\n"
	  "  :returns: tuple of message timestamp type, and timestamp.\n"
	  "  :rtype: (int, int)\n"
	  "\n"
	},
	{ "latency", (PyCFunction)Message_latency, METH_NOARGS,
          "Retrieve the time it took to produce the message, from calling "
          "produce() to the time the acknowledgement was received from "
          "the broker.\n"
          "Must only be used with the producer for successfully produced "
          "messages.\n"
          "\n"
	  "  :returns: latency as float seconds, or None if latency "
          "information is not available (such as for errored messages).\n"
	  "  :rtype: float or None\n"
	  "\n"
	},
	{ "headers", (PyCFunction)Message_headers, METH_NOARGS,
      "  Retrieve the headers set on a message. Each header is a key value"
      "pair. Please note that header keys are ordered and can repeat.\n"
      "\n"
	  "  :returns: list of two-tuples, one (key, value) pair for each header.\n"
	  "  :rtype: [(str, bytes),...] or None.\n"
	  "\n"
	},
	{ "set_headers", (PyCFunction)Message_set_headers, METH_O,
	  "  Set the field 'Message.headers' with new value.\n"
          "\n"
	  "  :param object value: Message.headers.\n"
	  "  :returns: None.\n"
	  "  :rtype: None\n"
	  "\n"
	},
	{ "set_value", (PyCFunction)Message_set_value, METH_O,
	  "  Set the field 'Message.value' with new value.\n"
          "\n"
	  "  :param object value: Message.value.\n"
	  "  :returns: None.\n"
	  "  :rtype: None\n"
	  "\n"
	},
	{ "set_key", (PyCFunction)Message_set_key, METH_O,
	  "  Set the field 'Message.key' with new value.\n"
          "\n"
	  "  :param object value: Message.key.\n"
	  "  :returns: None.\n"
	  "  :rtype: None\n"
	  "\n"
	},
	{ NULL }
};

static int Message_clear (Message *self) {
	if (self->topic) {
		Py_DECREF(self->topic);
		self->topic = NULL;
	}
	if (self->value) {
		Py_DECREF(self->value);
		self->value = NULL;
	}
	if (self->key) {
		Py_DECREF(self->key);
		self->key = NULL;
	}
	if (self->error) {
		Py_DECREF(self->error);
		self->error = NULL;
	}
	if (self->headers) {
		Py_DECREF(self->headers);
		self->headers = NULL;
	}
#ifdef RD_KAFKA_V_HEADERS
    if (self->c_headers){
        rd_kafka_headers_destroy(self->c_headers);
        self->c_headers = NULL;
    }
#endif
	return 0;
}


static void Message_dealloc (Message *self) {
	Message_clear(self);
	PyObject_GC_UnTrack(self);
	Py_TYPE(self)->tp_free((PyObject *)self);
}


static int Message_traverse (Message *self,
			     visitproc visit, void *arg) {
	if (self->topic)
		Py_VISIT(self->topic);
	if (self->value)
		Py_VISIT(self->value);
	if (self->key)
		Py_VISIT(self->key);
	if (self->error)
		Py_VISIT(self->error);
	if (self->headers)
		Py_VISIT(self->headers);
	return 0;
}

static Py_ssize_t Message__len__ (Message *self) {
	return self->value ? PyObject_Length(self->value) : 0;
}

static PySequenceMethods Message_seq_methods = {
	(lenfunc)Message__len__ /* sq_length */
};

PyTypeObject MessageType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"cimpl.Message",         /*tp_name*/
	sizeof(Message),       /*tp_basicsize*/
	0,                         /*tp_itemsize*/
	(destructor)Message_dealloc, /*tp_dealloc*/
	0,                         /*tp_print*/
	0,                         /*tp_getattr*/
	0,                         /*tp_setattr*/
	0,                         /*tp_compare*/
	0,                         /*tp_repr*/
	0,                         /*tp_as_number*/
	&Message_seq_methods,  /*tp_as_sequence*/
	0,                         /*tp_as_mapping*/
	0,                         /*tp_hash */
	0,                         /*tp_call*/
	0,                         /*tp_str*/
	PyObject_GenericGetAttr,                         /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
	Py_TPFLAGS_HAVE_GC, /*tp_flags*/
	"The Message object represents either a single consumed or "
	"produced message, or an event (:py:func:`error()` is not None).\n"
	"\n"
	"An application must check with :py:func:`error()` to see if the "
	"object is a proper message (error() returns None) or an "
	"error/event.\n"
        "\n"
	"This class is not user-instantiable.\n"
	"\n"
	".. py:function:: len()\n"
	"\n"
	"  :returns: Message value (payload) size in bytes\n"
	"  :rtype: int\n"
	"\n", /*tp_doc*/
	(traverseproc)Message_traverse,        /* tp_traverse */
	(inquiry)Message_clear,	           /* tp_clear */
	0,		           /* tp_richcompare */
	0,		           /* tp_weaklistoffset */
	0,		           /* tp_iter */
	0,		           /* tp_iternext */
	Message_methods,           /* tp_methods */
	0,                         /* tp_members */
	0,                         /* tp_getset */
	0,                         /* tp_base */
	0,                         /* tp_dict */
	0,                         /* tp_descr_get */
	0,                         /* tp_descr_set */
	0,                         /* tp_dictoffset */
	0,                         /* tp_init */
	0                          /* tp_alloc */
};

/**
 * @brief Internal factory to create Message object from message_t
 */
PyObject *Message_new0 (const Handle *handle, const rd_kafka_message_t *rkm) {
	Message *self;

	self = (Message *)MessageType.tp_alloc(&MessageType, 0);
	if (!self)
		return NULL;

        /* Only use message error string on Consumer, for Producers
         * it will contain the original message payload. */
        self->error = KafkaError_new_or_None(
                rkm->err,
                (rkm->err && handle->type != RD_KAFKA_PRODUCER) ?
                rd_kafka_message_errstr(rkm) : NULL);

	if (rkm->rkt)
		self->topic = cfl_PyUnistr(
			_FromString(rd_kafka_topic_name(rkm->rkt)));
	if (rkm->payload)
		self->value = cfl_PyBin(_FromStringAndSize(rkm->payload,
							   rkm->len));
	if (rkm->key)
		self->key = cfl_PyBin(
			_FromStringAndSize(rkm->key, rkm->key_len));

	self->partition = rkm->partition;
	self->offset = rkm->offset;
        self->leader_epoch = rd_kafka_message_leader_epoch(rkm);

	self->timestamp = rd_kafka_message_timestamp(rkm, &self->tstype);

        if (handle->type == RD_KAFKA_PRODUCER)
                self->latency = rd_kafka_message_latency(rkm);
        else
                self->latency = -1;

	return (PyObject *)self;
}



/****************************************************************************
 *
 *
 * Uuid
 *
 *
 *
 *
 ****************************************************************************/
static PyObject *Uuid_most_significant_bits (Uuid *self, PyObject *ignore) {
        if(self->cUuid) {
                return cfl_PyLong_FromLong(rd_kafka_Uuid_most_significant_bits(self->cUuid));
        }
        Py_RETURN_NONE;
}

static PyObject *Uuid_least_significant_bits (Uuid *self, PyObject *ignore) {
        if(self->cUuid) {
                return cfl_PyLong_FromLong(rd_kafka_Uuid_least_significant_bits(self->cUuid));
        }
        Py_RETURN_NONE;
}

static PyMethodDef Uuid_methods[] = {
	{ "get_most_significant_bits", (PyCFunction)Uuid_most_significant_bits, METH_NOARGS,
	  "  :returns: Most significant 64 bits of the 128 bits Uuid\n"
	  "  :rtype: int\n"
	  "\n"
	},
	{ "get_least_significant_bits", (PyCFunction)Uuid_least_significant_bits, METH_NOARGS,
	  "  :returns: Least significant 64 bits of the 128 bits Uuid\n"
	  "  :rtype: int\n"
	  "\n"
	},
	{ NULL }
};


static PyObject *Uuid_str0 (Uuid *self) {
        if(self->cUuid) {
                const char *base64str = rd_kafka_Uuid_base64str(self->cUuid);
                if(base64str)
                        return cfl_PyUnistr(_FromString(base64str));
        }
        Py_RETURN_NONE;
}

static long Uuid_hash (Uuid *self) {
        
        return rd_kafka_Uuid_most_significant_bits(self->cUuid) ^ rd_kafka_Uuid_least_significant_bits(self->cUuid);
}


static PyObject *Uuid_new (PyTypeObject *type, PyObject *args,
                               PyObject *kwargs) {
        PyObject *self = type->tp_alloc(type, 1);
        return self;
}


static int Uuid_init (PyObject *self0, PyObject *args,
                          PyObject *kwargs) {
        Uuid *self = (Uuid *)self0;
        static char *kws[] = { "most_significant_bits",
                               "least_significant_bits",
                               NULL };
        int64_t most_significant_bits;
        int64_t least_significant_bits;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "LL", kws,
                                         &most_significant_bits, 
                                         &least_significant_bits))
                return -1;

        self->cUuid = rd_kafka_Uuid_new(most_significant_bits, least_significant_bits);

        return 0;
}

static int Uuid_clear (Uuid *self) {
	if (self->cUuid) {
		rd_kafka_Uuid_destroy(self->cUuid);
		self->cUuid = NULL;
	}
	return 0;
}

static int Uuid_traverse (Uuid *self,
                        visitproc visit, void *arg) {
        return 0;
}

static void Uuid_dealloc (Uuid *self) {
        PyObject_GC_UnTrack(self);
        Uuid_clear(self);

        Py_TYPE(self)->tp_free((PyObject *)self);
}

PyTypeObject UuidType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        "cimpl.Uuid",                           /* tp_name */
        sizeof(Uuid),                           /* tp_basicsize */
        0,                                      /* tp_itemsize */
        (destructor)Uuid_dealloc,               /* tp_dealloc */
        0,                                      /* tp_print */
        0,                                      /* tp_getattr */
        0,                                      /* tp_setattr */
        0,                                      /* tp_compare */
        (reprfunc)Uuid_str0,                    /* tp_repr */
        0,                                      /* tp_as_number */
        0,                                      /* tp_as_sequence */
        0,                                      /* tp_as_mapping */
        (hashfunc)Uuid_hash,                    /* tp_hash */
        0,                                      /* tp_call */
        0,                                      /* tp_str */
        PyObject_GenericGetAttr,                /* tp_getattro */
        0,                                      /* tp_setattro */
        0,                                      /* tp_as_buffer */
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | 
	Py_TPFLAGS_HAVE_GC,                     /* tp_flags */
        "Generic Uuid. Being used in various identifiers including topic_id.\n"
        "\n"
        ".. py:function:: Uuid(most_significant_bits, least_significant_bits)\n"
        "\n"
        "  Instantiate a Uuid object.\n"
        "\n"
        "  :param long most_significant_bits: Most significant 64 bits of the 128 bits Uuid.\n"
        "  :param long least_significant_bits: Least significant 64 bits of the 128 bits Uuid.\n"
        "  :rtype: Uuid\n"
        "\n"
"\n",                                           /* tp_doc */
        (traverseproc)Uuid_traverse,            /* tp_traverse */
        (inquiry)Uuid_clear,                    /* tp_clear */
        0,                                      /* tp_richcompare */
        0,                                      /* tp_weaklistoffset */
        0,                                      /* tp_iter */
        0,                                      /* tp_iternext */
        Uuid_methods,                           /* tp_methods */
        0,                                      /* tp_members */
        0,                                      /* tp_getset */
        0,                                      /* tp_base */
        0,                                      /* tp_dict */
        0,                                      /* tp_descr_get */
        0,                                      /* tp_descr_set */
        0,                                      /* tp_dictoffset */
        Uuid_init,                              /* tp_init */
        0,                                      /* tp_alloc */
        Uuid_new                                /* tp_new */
};



/****************************************************************************
 *
 *
 * TopicPartition
 *
 *
 *
 *
 ****************************************************************************/
static int TopicPartition_clear (TopicPartition *self) {
	if (self->topic) {
		free(self->topic);
		self->topic = NULL;
	}
	if (self->error) {
		Py_DECREF(self->error);
		self->error = NULL;
	}
	if (self->metadata) {
		free(self->metadata);
		self->metadata = NULL;
	}
	return 0;
}

static void TopicPartition_setup (TopicPartition *self, const char *topic,
				  int partition, long long offset,
                                  int32_t leader_epoch,
				  const char *metadata,
				  rd_kafka_resp_err_t err) {
	self->topic = strdup(topic);
	self->partition = partition;
	self->offset = offset;

        if (leader_epoch < 0)
                leader_epoch = -1;
        self->leader_epoch = leader_epoch;

	if (metadata != NULL) {
		self->metadata = strdup(metadata);
	} else {
		self->metadata = NULL;
	}

	self->error = KafkaError_new_or_None(err, NULL);
}


static void TopicPartition_dealloc (TopicPartition *self) {
	PyObject_GC_UnTrack(self);

	TopicPartition_clear(self);

	Py_TYPE(self)->tp_free((PyObject *)self);
}


static int TopicPartition_init (PyObject *self, PyObject *args,
				      PyObject *kwargs) {
	const char *topic;
	int partition = RD_KAFKA_PARTITION_UA;
        int32_t leader_epoch = -1;
	long long offset = RD_KAFKA_OFFSET_INVALID;
	const char *metadata = NULL;

	static char *kws[] = { "topic",
			       "partition",
			       "offset",
			       "metadata",
                               "leader_epoch",
			       NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|iLsi", kws,
					 &topic, &partition, &offset,
					 &metadata,
                                         &leader_epoch)) {
		return -1;
	}

	TopicPartition_setup((TopicPartition *)self,
			     topic, partition, offset,
                             leader_epoch, metadata, 0);
	return 0;
}


static PyObject *TopicPartition_new (PyTypeObject *type, PyObject *args,
				     PyObject *kwargs) {
	PyObject *self = type->tp_alloc(type, 1);
	return self;
}



static int TopicPartition_traverse (TopicPartition *self,
				    visitproc visit, void *arg) {
	if (self->error)
		Py_VISIT(self->error);
	return 0;
}

static PyObject *TopicPartition_get_leader_epoch (TopicPartition *tp, void *closure) {
        if (tp->leader_epoch >= 0) {
                return cfl_PyInt_FromInt(tp->leader_epoch);
        }
        Py_RETURN_NONE;
}


static PyMemberDef TopicPartition_members[] = {
        { "topic", T_STRING, offsetof(TopicPartition, topic), READONLY,
          ":attribute topic: Topic name (string)" },
        { "partition", T_INT, offsetof(TopicPartition, partition), 0,
          ":attribute partition: Partition number (int)" },
        { "offset", T_LONGLONG, offsetof(TopicPartition, offset), 0,
          ":attribute offset: Offset (long)\n"
          "\n"
          "Either an absolute offset (>=0) or a logical offset: "
          " :py:const:`OFFSET_BEGINNING`,"
          " :py:const:`OFFSET_END`,"
          " :py:const:`OFFSET_STORED`,"
          " :py:const:`OFFSET_INVALID`\n"
        },
        {"metadata", T_STRING, offsetof(TopicPartition, metadata), READONLY,
         "attribute metadata: Optional application metadata committed with the "
         "offset (string)"},
        { "error", T_OBJECT, offsetof(TopicPartition, error), READONLY,
          ":attribute error: Indicates an error (with :py:class:`KafkaError`) unless None." },
        { NULL }
};

static PyGetSetDef TopicPartition_getters_and_setters[] = {
        {
          /* name */
          "leader_epoch",
          (getter) TopicPartition_get_leader_epoch,
          NULL,
          /* doc */
          ":attribute leader_epoch: Offset leader epoch (int), or None",
          /* closure */
          NULL
        },
        { NULL }
};


static PyObject *TopicPartition_str0 (TopicPartition *self) {
        PyObject *errstr = NULL;
        PyObject *errstr8 = NULL;
        const char *c_errstr = NULL;
	PyObject *ret;
	char offset_str[40];
        char leader_epoch_str[12];

	snprintf(offset_str, sizeof(offset_str), "%"CFL_PRId64"", self->offset);
        if (self->leader_epoch >= 0)
                snprintf(leader_epoch_str, sizeof(leader_epoch_str),
                        "%"CFL_PRId32"", self->leader_epoch);
        else
                snprintf(leader_epoch_str, sizeof(leader_epoch_str),
                        "None");

        if (self->error != Py_None) {
                errstr = cfl_PyObject_Unistr(self->error);
                c_errstr = cfl_PyUnistr_AsUTF8(errstr, &errstr8);
        }

	ret = cfl_PyUnistr(
		_FromFormat("TopicPartition{topic=%s,partition=%"CFL_PRId32
			    ",offset=%s,leader_epoch=%s,error=%s}",
			    self->topic, self->partition,
			    offset_str,
                            leader_epoch_str,
			    c_errstr ? c_errstr : "None"));
        Py_XDECREF(errstr8);
        Py_XDECREF(errstr);
	return ret;
}


static PyObject *
TopicPartition_richcompare (TopicPartition *self, PyObject *o2,
			    int op) {
	TopicPartition *a = self, *b;
	int tr, pr;
	int r;
	PyObject *result;

	if (Py_TYPE(o2) != Py_TYPE(self)) {
		PyErr_SetNone(PyExc_NotImplementedError);
		return NULL;
	}

	b = (TopicPartition *)o2;

	tr = strcmp(a->topic, b->topic);
	pr = a->partition - b->partition;
	switch (op)
	{
	case Py_LT:
		r = tr < 0 || (tr == 0 && pr < 0);
		break;
	case Py_LE:
		r = tr < 0 || (tr == 0 && pr <= 0);
		break;
	case Py_EQ:
		r = (tr == 0 && pr == 0);
		break;
	case Py_NE:
		r = (tr != 0 || pr != 0);
		break;
	case Py_GT:
		r = tr > 0 || (tr == 0 && pr > 0);
		break;
	case Py_GE:
		r = tr > 0 || (tr == 0 && pr >= 0);
		break;
	default:
		r = 0;
		break;
	}

	result = r ? Py_True : Py_False;
	Py_INCREF(result);
	return result;
}


static long TopicPartition_hash (TopicPartition *self) {
	PyObject *topic = cfl_PyUnistr(_FromString(self->topic));
	long r = PyObject_Hash(topic) ^ self->partition;
	Py_DECREF(topic);
	return r;
}


PyTypeObject TopicPartitionType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"cimpl.TopicPartition",         /*tp_name*/
	sizeof(TopicPartition),       /*tp_basicsize*/
	0,                         /*tp_itemsize*/
	(destructor)TopicPartition_dealloc, /*tp_dealloc*/
	0,                         /*tp_print*/
	0,                         /*tp_getattr*/
	0,                         /*tp_setattr*/
	0,                         /*tp_compare*/
	(reprfunc)TopicPartition_str0, /*tp_repr*/
	0,                         /*tp_as_number*/
	0,                         /*tp_as_sequence*/
	0,                         /*tp_as_mapping*/
	(hashfunc)TopicPartition_hash, /*tp_hash */
	0,                         /*tp_call*/
	0,                         /*tp_str*/
	PyObject_GenericGetAttr,   /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
	Py_TPFLAGS_HAVE_GC, /*tp_flags*/
	"TopicPartition is a generic type to hold a single partition and "
	"various information about it.\n"
	"\n"
	"It is typically used to provide a list of topics or partitions for "
	"various operations, such as :py:func:`Consumer.assign()`.\n"
	"\n"
	".. py:function:: TopicPartition(topic, [partition], [offset],"
        " [metadata], [leader_epoch])\n"
	"\n"
	"  Instantiate a TopicPartition object.\n"
	"\n"
	"  :param string topic: Topic name\n"
	"  :param int partition: Partition id\n"
	"  :param int offset: Initial partition offset\n"
        "  :param string metadata: Offset metadata\n"
        "  :param int leader_epoch: Offset leader epoch\n"
	"  :rtype: TopicPartition\n"
	"\n"
	"\n", /*tp_doc*/
	(traverseproc)TopicPartition_traverse, /* tp_traverse */
	(inquiry)TopicPartition_clear,       /* tp_clear */
	(richcmpfunc)TopicPartition_richcompare, /* tp_richcompare */
	0,		                    /* tp_weaklistoffset */
	0,		                    /* tp_iter */
	0,		                    /* tp_iternext */
	0,                                  /* tp_methods */
	TopicPartition_members,             /* tp_members */
	TopicPartition_getters_and_setters, /* tp_getset */
	0,                                  /* tp_base */
	0,                                  /* tp_dict */
	0,                                  /* tp_descr_get */
	0,                                  /* tp_descr_set */
	0,                                  /* tp_dictoffset */
	TopicPartition_init,                /* tp_init */
	0,                                  /* tp_alloc */
	TopicPartition_new                  /* tp_new */
};

/**
 * @brief Internal factory to create a TopicPartition object.
 */
static PyObject *TopicPartition_new0 (const char *topic, int partition,
				      long long offset, int32_t leader_epoch,
                                      const char *metadata,
				      rd_kafka_resp_err_t err) {
	TopicPartition *self;

	self = (TopicPartition *)TopicPartitionType.tp_new(
		&TopicPartitionType, NULL, NULL);

	TopicPartition_setup(self, topic, partition,
			     offset, leader_epoch,
                             metadata, err);

	return (PyObject *)self;
}



PyObject *c_part_to_py(const rd_kafka_topic_partition_t *rktpar) {
        return TopicPartition_new0(rktpar->topic, rktpar->partition,
			           rktpar->offset,
                                   rd_kafka_topic_partition_get_leader_epoch(rktpar),
				   rktpar->metadata,
				   rktpar->err);
}

/**
 * @brief Convert C rd_kafka_topic_partition_list_t to Python list(TopicPartition).
 *
 * @returns The new Python list object.
 */
PyObject *c_parts_to_py (const rd_kafka_topic_partition_list_t *c_parts) {
	PyObject *parts;
	size_t i;

	parts = PyList_New(c_parts->cnt);

	for (i = 0 ; i < (size_t)c_parts->cnt ; i++) {
		const rd_kafka_topic_partition_t *rktpar = &c_parts->elems[i];
		PyList_SET_ITEM(parts, i, c_part_to_py(rktpar));
	}

	return parts;

}

/**
 * @brief Convert Python list(TopicPartition) to C rd_kafka_topic_partition_list_t.
 *
 * @returns The new C list on success or NULL on error.
 */
rd_kafka_topic_partition_list_t *py_to_c_parts (PyObject *plist) {
	rd_kafka_topic_partition_list_t *c_parts;
	size_t i;

	if (!PyList_Check(plist)) {
		PyErr_SetString(PyExc_TypeError,
				"requires list of TopicPartition");
		return NULL;
	}

	c_parts = rd_kafka_topic_partition_list_new((int)PyList_Size(plist));

	for (i = 0 ; i < (size_t)PyList_Size(plist) ; i++) {
		rd_kafka_topic_partition_t *rktpar;
		TopicPartition *tp = (TopicPartition *)
			PyList_GetItem(plist, i);

		if (PyObject_Type((PyObject *)tp) !=
		    (PyObject *)&TopicPartitionType) {
			PyErr_Format(PyExc_TypeError,
				     "expected %s",
				     TopicPartitionType.tp_name);
			rd_kafka_topic_partition_list_destroy(c_parts);
			return NULL;
		}

		rktpar = rd_kafka_topic_partition_list_add(c_parts,
							   tp->topic,
							   tp->partition);
		rktpar->offset = tp->offset;
                rd_kafka_topic_partition_set_leader_epoch(rktpar,
                        tp->leader_epoch);
		if (tp->metadata != NULL) {
			rktpar->metadata_size = strlen(tp->metadata) + 1;
			rktpar->metadata = strdup(tp->metadata);
		} else {
			rktpar->metadata_size = 0;
			rktpar->metadata = NULL;
		}
	}

	return c_parts;
}

/**
 * @brief Convert C rd_kafka_topic_partition_result_t to Python dict(TopicPartition, KafkaException).
 * 
 * @returns The new Python dict object.
 */
PyObject *c_topic_partition_result_to_py_dict(
    const rd_kafka_topic_partition_result_t **partition_results,
    size_t cnt) {
        PyObject *result = NULL;
        size_t i;

        result = PyDict_New();

        for (i = 0; i < cnt; i++) {
                PyObject *key;
                PyObject *value;
                const rd_kafka_topic_partition_t *c_topic_partition;
                const rd_kafka_error_t *c_error;

                c_topic_partition =
                    rd_kafka_topic_partition_result_partition(partition_results[i]);
                c_error = rd_kafka_topic_partition_result_error(partition_results[i]);

                value = KafkaError_new_or_None(rd_kafka_error_code(c_error),
                                               rd_kafka_error_string(c_error));
                key = c_part_to_py(c_topic_partition);
               
                PyDict_SetItem(result, key, value);

                Py_DECREF(key);
                Py_DECREF(value);
        }

        return result;
}

#ifdef RD_KAFKA_V_HEADERS


/**
 * @brief Translate Python \p key and \p value to C types and set on
 *        provided \p rd_headers object.
 *
 * @returns 1 on success or 0 if an exception was raised.
 */
static int py_header_to_c (rd_kafka_headers_t *rd_headers,
                           PyObject *key, PyObject *value) {
        PyObject *ks, *ks8, *vo8 = NULL;
        const char *k;
        const void *v = NULL;
        Py_ssize_t vsize = 0;
        rd_kafka_resp_err_t err;

        if (!(ks = cfl_PyObject_Unistr(key))) {
                PyErr_SetString(PyExc_TypeError,
                                "expected header key to be unicode "
                                "string");
                return 0;
        }

        k = cfl_PyUnistr_AsUTF8(ks, &ks8);

        if (value != Py_None) {
                if (cfl_PyBin(_Check(value))) {
                        /* Proper binary */
                        if (cfl_PyBin(_AsStringAndSize(value, (char **)&v,
                                                       &vsize)) == -1) {
                                Py_DECREF(ks);
                                Py_XDECREF(ks8);
                                return 0;
                        }
                } else if (cfl_PyUnistr(_Check(value))) {
                        /* Unicode string, translate to utf-8. */
                        v = cfl_PyUnistr_AsUTF8(value, &vo8);
                        if (!v) {
                                Py_DECREF(ks);
                                Py_XDECREF(ks8);
                                return 0;
                        }
                        vsize = (Py_ssize_t)strlen(v);
                } else {
                        PyErr_Format(PyExc_TypeError,
                                     "expected header value to be "
                                     "None, binary, or unicode string, not %s",
                                     ((PyTypeObject *)PyObject_Type(value))->
                                     tp_name);
                        Py_DECREF(ks);
                        Py_XDECREF(ks8);
                        return 0;
                }
        }

        if ((err = rd_kafka_header_add(rd_headers, k, -1, v, vsize))) {
                cfl_PyErr_Format(err,
                                 "Unable to add message header \"%s\": "
                                 "%s",
                                 k, rd_kafka_err2str(err));
                Py_DECREF(ks);
                Py_XDECREF(ks8);
                Py_XDECREF(vo8);
                return 0;
        }

        Py_DECREF(ks);
        Py_XDECREF(ks8);
        Py_XDECREF(vo8);

        return 1;
}

/**
 * @brief Convert Python list of tuples to rd_kafka_headers_t
 *
 * Header names must be unicode strong.
 * Header values may be None, binary or unicode string, the latter is
 * automatically encoded as utf-8.
 */
static rd_kafka_headers_t *py_headers_list_to_c (PyObject *hdrs) {
        int i, len;
        rd_kafka_headers_t *rd_headers = NULL;

        len = (int)PyList_Size(hdrs);
        rd_headers = rd_kafka_headers_new(len);

        for (i = 0; i < len; i++) {
                PyObject *tuple = PyList_GET_ITEM(hdrs, i);

                if (!PyTuple_Check(tuple) || PyTuple_Size(tuple) != 2) {
                        rd_kafka_headers_destroy(rd_headers);
                        PyErr_SetString(PyExc_TypeError,
                                        "Headers are expected to be a "
                                        "list of (key, value) tuples");
                        return NULL;
                }

                if (!py_header_to_c(rd_headers,
                                    PyTuple_GET_ITEM(tuple, 0),
                                    PyTuple_GET_ITEM(tuple, 1))) {
                        rd_kafka_headers_destroy(rd_headers);
                        return NULL;
                }
        }
        return rd_headers;
}


/**
 * @brief Convert Python dict to rd_kafka_headers_t
 */
static rd_kafka_headers_t *py_headers_dict_to_c (PyObject *hdrs) {
        int len;
        Py_ssize_t pos = 0;
        rd_kafka_headers_t *rd_headers = NULL;
        PyObject *ko, *vo;

        len = (int)PyDict_Size(hdrs);
        rd_headers = rd_kafka_headers_new(len);

        while (PyDict_Next(hdrs, &pos, &ko, &vo)) {

                if (!py_header_to_c(rd_headers, ko, vo)) {
                        rd_kafka_headers_destroy(rd_headers);
                        return NULL;
                }
        }

        return rd_headers;
}


/**
 * @brief Convert Python list[(header_key, header_value),...]) to C rd_kafka_topic_partition_list_t.
 *
 * @returns The new Python list[(header_key, header_value),...] object.
 */
rd_kafka_headers_t *py_headers_to_c (PyObject *hdrs) {

        if (PyList_Check(hdrs)) {
                return py_headers_list_to_c(hdrs);
        } else if (PyDict_Check(hdrs)) {
                return py_headers_dict_to_c(hdrs);
        } else {
                PyErr_Format(PyExc_TypeError,
                             "expected headers to be "
                             "dict or list of (key, value) tuples, not %s",
                             ((PyTypeObject *)PyObject_Type(hdrs))->tp_name);
                return NULL;
        }
}


/**
 * @brief Convert rd_kafka_headers_t to Python list[(header_key, header_value),...])
 *
 * @returns The new C headers on success or NULL on error.
 */
PyObject *c_headers_to_py (rd_kafka_headers_t *headers) {
    size_t idx = 0;
    size_t header_size = 0;
    const char *header_key;
    const void *header_value;
    size_t header_value_size;
    PyObject *header_list;

    header_size = rd_kafka_header_cnt(headers);
    header_list = PyList_New(header_size);

    while (!rd_kafka_header_get_all(headers, idx++,
                                     &header_key, &header_value, &header_value_size)) {
            /* Create one (key, value) tuple for each header */
            PyObject *header_tuple = PyTuple_New(2);
            PyTuple_SetItem(header_tuple, 0,
                cfl_PyUnistr(_FromString(header_key))
            );

            if (header_value) {
                    PyTuple_SetItem(header_tuple, 1,
                        cfl_PyBin(_FromStringAndSize(header_value, header_value_size))
                    );
            } else {
                Py_INCREF(Py_None);
                PyTuple_SetItem(header_tuple, 1, Py_None);
            }
        PyList_SET_ITEM(header_list, idx-1, header_tuple);
    }

    return header_list;
}
#endif


/**
 * @brief Convert C rd_kafka_consumer_group_metadata_t to Python binary string
 *
 * @returns The new Python object, or NULL and raises an exception failure.
 */
PyObject *c_cgmd_to_py (const rd_kafka_consumer_group_metadata_t *cgmd) {
	PyObject *obj;
        void *buffer;
        size_t size;
        rd_kafka_error_t *error;

        error = rd_kafka_consumer_group_metadata_write(cgmd, &buffer, &size);
        if (error) {
                cfl_PyErr_from_error_destroy(error);
                return NULL;
        }

        obj = cfl_PyBin(_FromStringAndSize(buffer, (Py_ssize_t)size));
        rd_kafka_mem_free(NULL, buffer);

        return obj;
}

/**
 * @brief Convert Python bytes object to C rd_kafka_consumer_group_metadata_t.
 *
 * @returns The new C object, or NULL and raises an exception on failure.
 */
rd_kafka_consumer_group_metadata_t *py_to_c_cgmd (PyObject *obj) {
        rd_kafka_consumer_group_metadata_t *cgmd;
        rd_kafka_error_t *error;
        char *buffer;
        Py_ssize_t size;

        if (cfl_PyBin(_AsStringAndSize(obj, &buffer, &size)) == -1)
                return NULL;

        error = rd_kafka_consumer_group_metadata_read(&cgmd,
                                                      (const void *)buffer,
                                                      (size_t)size);
        if (error) {
                cfl_PyErr_from_error_destroy(error);
                return NULL;
        }

        return cgmd;
}

PyObject *c_Node_to_py(const rd_kafka_Node_t *c_node) {
        PyObject *node = NULL;
        PyObject *Node_type = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        const char *rack = NULL;

        if(!c_node)
                Py_RETURN_NONE;

        Node_type = cfl_PyObject_lookup("confluent_kafka",
                                        "Node");
        if (!Node_type) {
                goto err;
        }

        kwargs = PyDict_New();

        cfl_PyDict_SetInt(kwargs, "id", rd_kafka_Node_id(c_node));
        cfl_PyDict_SetInt(kwargs, "port", rd_kafka_Node_port(c_node));
        if (rd_kafka_Node_host(c_node))
                cfl_PyDict_SetString(kwargs, "host", rd_kafka_Node_host(c_node));
        else
                PyDict_SetItemString(kwargs, "host", Py_None);
        if((rack = rd_kafka_Node_rack(c_node)))
                cfl_PyDict_SetString(kwargs, "rack", rack);

        args = PyTuple_New(0);

        node = PyObject_Call(Node_type, args, kwargs);

        Py_DECREF(Node_type);
        Py_DECREF(args);
        Py_DECREF(kwargs);
        return node;

err:
        Py_XDECREF(Node_type);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(node);
        return NULL;
}

PyObject *c_Uuid_to_py(const rd_kafka_Uuid_t *c_uuid) {
        PyObject *uuid = NULL;
        PyObject *Uuid_type = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        
        if(!c_uuid)
                Py_RETURN_NONE;

        Uuid_type = cfl_PyObject_lookup("confluent_kafka",
                                        "Uuid");
        if (!Uuid_type) {
                goto err;
        }

        
        kwargs = PyDict_New();

        cfl_PyDict_SetLong(kwargs, "most_significant_bits", rd_kafka_Uuid_most_significant_bits(c_uuid));
        cfl_PyDict_SetLong(kwargs, "least_significant_bits", rd_kafka_Uuid_least_significant_bits(c_uuid));

        args = PyTuple_New(0);

        uuid = PyObject_Call(Uuid_type, args, kwargs);

        Py_DECREF(Uuid_type);
        Py_DECREF(args);
        Py_DECREF(kwargs);

        return uuid;

err:
        Py_XDECREF(Uuid_type);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(uuid);

        return NULL;

}


/****************************************************************************
 *
 *
 * Common callbacks
 *
 *
 *
 *
 ****************************************************************************/
static void error_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
	Handle *h = opaque;
	PyObject *eo, *result;
	CallState *cs;

	cs = CallState_get(h);

        /* If the client raised a fatal error we'll raise an exception
         * rather than calling the error callback. */
        if (err == RD_KAFKA_RESP_ERR__FATAL) {
                char errstr[512];
                err = rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
                cfl_PyErr_Fatal(err, errstr);
                goto crash;
        }

	if (!h->error_cb) {
		/* No callback defined */
		goto done;
	}

	eo = KafkaError_new0(err, "%s", reason);
	result = PyObject_CallFunctionObjArgs(h->error_cb, eo, NULL);
	Py_DECREF(eo);

	if (result)
		Py_DECREF(result);
	else {
        crash:
		CallState_crash(cs);
		rd_kafka_yield(h->rk);
	}

 done:
	CallState_resume(cs);
}

/**
 * @brief librdkafka throttle callback triggered by poll() or flush(), triggers the
 *        corresponding Python throttle_cb
 */
static void throttle_cb (rd_kafka_t *rk, const char *broker_name, int32_t broker_id,
                         int throttle_time_ms, void *opaque) {
        Handle *h = opaque;
        PyObject *ThrottleEvent_type, *throttle_event;
        PyObject *result, *args;
        CallState *cs;

        cs = CallState_get(h);
        if (!h->throttle_cb) {
                /* No callback defined */
                goto done;
        }

        ThrottleEvent_type = cfl_PyObject_lookup("confluent_kafka",
                                                 "ThrottleEvent");

        if (!ThrottleEvent_type) {
                /* ThrottleEvent class not found */
                goto err;
        }

        args = Py_BuildValue("(sid)", broker_name, broker_id, (double)throttle_time_ms/1000);
        throttle_event = PyObject_Call(ThrottleEvent_type, args, NULL);

        Py_DECREF(args);
        Py_DECREF(ThrottleEvent_type);

        if (!throttle_event) {
                /* Failed to instantiate ThrottleEvent object */
                goto err;
        }

        result = PyObject_CallFunctionObjArgs(h->throttle_cb, throttle_event, NULL);

        Py_DECREF(throttle_event);

        if (result) {
                /* throttle_cb executed successfully */
                Py_DECREF(result);
                goto done;
        }

        /**
        * Stop callback dispatcher, return err to application
        * fall-through to unlock GIL
        */
 err:
        CallState_crash(cs);
        rd_kafka_yield(h->rk);
 done:
        CallState_resume(cs);
}

static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
	Handle *h = opaque;
	PyObject *eo = NULL, *result = NULL;
	CallState *cs = NULL;

	cs = CallState_get(h);
	if (json_len == 0) {
		/* No data returned*/
		goto done;
	}

	eo = Py_BuildValue("s", json);
	result = PyObject_CallFunctionObjArgs(h->stats_cb, eo, NULL);
	Py_DECREF(eo);

	if (result)
		Py_DECREF(result);
	else {
		CallState_crash(cs);
		rd_kafka_yield(h->rk);
	}

 done:
	CallState_resume(cs);
	return 0;
}

static void log_cb (const rd_kafka_t *rk, int level,
                    const char *fac, const char *buf) {
        Handle *h = rd_kafka_opaque(rk);
        PyObject *result;
        CallState *cs;
        static const int level_map[8] = {
                /* Map syslog levels to python logging levels */
                50, /* LOG_EMERG   -> logging.CRITICAL */
                50, /* LOG_ALERT   -> logging.CRITICAL */
                50, /* LOG_CRIT    -> logging.CRITICAL */
                40, /* LOG_ERR     -> logging.ERROR */
                30, /* LOG_WARNING -> logging.WARNING */
                20, /* LOG_NOTICE  -> logging.INFO */
                20, /* LOG_INFO    -> logging.INFO */
                10, /* LOG_DEBUG   -> logging.DEBUG */
        };

        cs = CallState_get(h);
        result = PyObject_CallMethod(h->logger, "log", "issss",
                                     level_map[level],
                                     "%s [%s] %s",
                                     fac, rd_kafka_name(rk), buf);

        if (result)
                Py_DECREF(result);
        else {
                CallState_crash(cs);
                rd_kafka_yield(h->rk);
        }

        CallState_resume(cs);
}

/**
 * @brief Translate Python \p key and \p value to C types and set on
 *        provided \p extensions char* array at the provided index.
 *
 * @returns 1 on success or 0 if an exception was raised.
 */
static int py_extensions_to_c (char **extensions, Py_ssize_t idx,
                               PyObject *key, PyObject *value) {
        PyObject *ks, *ks8, *vo8 = NULL;
        const char *k;
        const char *v;
        Py_ssize_t ksize = 0;
        Py_ssize_t vsize = 0;

        if (!(ks = cfl_PyObject_Unistr(key))) {
                PyErr_SetString(PyExc_TypeError,
                                "expected extension key to be unicode "
                                "string");
                return 0;
        }

        k = cfl_PyUnistr_AsUTF8(ks, &ks8);
        ksize = (Py_ssize_t)strlen(k);

        if (cfl_PyUnistr(_Check(value))) {
                /* Unicode string, translate to utf-8. */
                v = cfl_PyUnistr_AsUTF8(value, &vo8);
                if (!v) {
                        Py_DECREF(ks);
                        Py_XDECREF(ks8);
                        return 0;
                }
                vsize = (Py_ssize_t)strlen(v);
        } else {
                PyErr_Format(PyExc_TypeError,
                             "expected extension value to be "
                             "unicode string, not %s",
                             ((PyTypeObject *)PyObject_Type(value))->
                             tp_name);
                Py_DECREF(ks);
                Py_XDECREF(ks8);
                return 0;
        }

        extensions[idx] = (char*)malloc(ksize + 1);
        snprintf(extensions[idx], ksize + 1, "%s", k);
        extensions[idx + 1] = (char*)malloc(vsize + 1);
        snprintf(extensions[idx + 1], vsize + 1, "%s", v);

        Py_DECREF(ks);
        Py_XDECREF(ks8);
        Py_XDECREF(vo8);

        return 1;
}

static void oauth_cb (rd_kafka_t *rk, const char *oauthbearer_config,
                      void *opaque) {
        Handle *h = opaque;
        PyObject *eo, *result;
        CallState *cs;
        const char *token;
        double expiry;
        const char *principal = "";
        PyObject *extensions = NULL;
        char **rd_extensions = NULL;
        Py_ssize_t rd_extensions_size = 0;
        char err_msg[2048];
        rd_kafka_resp_err_t err_code;

        cs = CallState_get(h);

        eo = Py_BuildValue("s", oauthbearer_config);
        result = PyObject_CallFunctionObjArgs(h->oauth_cb, eo, NULL);
        Py_DECREF(eo);

        if (!result) {
                goto fail;
        }
        if (!PyArg_ParseTuple(result, "sd|sO!", &token, &expiry, &principal, &PyDict_Type, &extensions)) {
                Py_DECREF(result);
                PyErr_SetString(PyExc_TypeError,
                             "expect returned value from oauth_cb "
                             "to be (token_str, expiry_time[, principal, extensions]) tuple");
                goto err;
        }

        if (extensions) {
                int len = (int)PyDict_Size(extensions);
                rd_extensions = (char **)malloc(2 * len * sizeof(char *));
                Py_ssize_t pos = 0;
                PyObject *ko, *vo;
                while (PyDict_Next(extensions, &pos, &ko, &vo)) {
                        if (!py_extensions_to_c(rd_extensions, rd_extensions_size, ko, vo)) {
                                Py_DECREF(result);
                                free(rd_extensions);
                                goto err;
                        }
                        rd_extensions_size = rd_extensions_size + 2;
                }
        }

        err_code = rd_kafka_oauthbearer_set_token(h->rk, token,
                                                  (int64_t)(expiry * 1000),
                                                  principal, (const char **)rd_extensions, rd_extensions_size, err_msg,
                                                  sizeof(err_msg));
        Py_DECREF(result);
        if (rd_extensions) {
                int i;
                for(i = 0; i < rd_extensions_size; i++) {
                        free(rd_extensions[i]);
                }
                free(rd_extensions);
        }

        if (err_code != RD_KAFKA_RESP_ERR_NO_ERROR) {
                PyErr_Format(PyExc_ValueError, "%s", err_msg);
                goto fail;
        }
        goto done;

fail:
        err_code = rd_kafka_oauthbearer_set_token_failure(h->rk, "OAuth callback raised exception");
        if (err_code != RD_KAFKA_RESP_ERR_NO_ERROR) {
                PyErr_SetString(PyExc_ValueError, "Failed to set token failure");
                goto err;
        }
        PyErr_Clear();
        goto done;
 err:
        CallState_crash(cs);
        rd_kafka_yield(h->rk);
 done:
        CallState_resume(cs);
}

/****************************************************************************
 *
 *
 * Common helpers
 *
 *
 *
 *
 ****************************************************************************/



/**
 * Clear Python object references in Handle
 */
void Handle_clear (Handle *h) {
        if (h->error_cb) {
                Py_DECREF(h->error_cb);
                h->error_cb = NULL;
        }

        if (h->throttle_cb) {
                Py_DECREF(h->throttle_cb);
                h->throttle_cb = NULL;
        }

        if (h->stats_cb) {
                Py_DECREF(h->stats_cb);
                h->stats_cb = NULL;
        }

        if (h->logger) {
                Py_DECREF(h->logger);
                h->logger = NULL;
        }

        if (h->initiated) {
#ifdef WITH_PY_TSS
                PyThread_tss_delete(&h->tlskey);
#else
                PyThread_delete_key(h->tlskey);
#endif
        }
}

/**
 * GC traversal for Python object references
 */
int Handle_traverse (Handle *h, visitproc visit, void *arg) {
	if (h->error_cb)
		Py_VISIT(h->error_cb);

        if (h->throttle_cb)
                Py_VISIT(h->throttle_cb);

	if (h->stats_cb)
		Py_VISIT(h->stats_cb);

	return 0;
}

/**
 * @brief Set single special producer config value.
 *
 * @returns 1 if handled, 0 if unknown, or -1 on failure (exception raised).
 */
static int producer_conf_set_special (Handle *self, rd_kafka_conf_t *conf,
				      const char *name, PyObject *valobj) {

	if (!strcmp(name, "on_delivery")) {
		if (!PyCallable_Check(valobj)) {
			cfl_PyErr_Format(
				RD_KAFKA_RESP_ERR__INVALID_ARG,
				"%s requires a callable "
				"object", name);
			return -1;
		}

		self->u.Producer.default_dr_cb = valobj;
		Py_INCREF(self->u.Producer.default_dr_cb);

		return 1;

        } else if (!strcmp(name, "delivery.report.only.error")) {
                /* Since we allocate msgstate for each produced message
                 * with a callback we can't use delivery.report.only.error
                 * as-is, as we wouldn't be able to ever free those msgstates.
                 * Instead we shortcut this setting in the Python client,
                 * providing the same functionality from dr_msg_cb trampoline.
                 */

                if (!cfl_PyBool_get(valobj, name,
                                    &self->u.Producer.dr_only_error))
                        return -1;

                return 1;
        }

	return 0; /* Not handled */
}


/**
 * @brief Set single special consumer config value.
 *
 * @returns 1 if handled, 0 if unknown, or -1 on failure (exception raised).
 */
static int consumer_conf_set_special (Handle *self, rd_kafka_conf_t *conf,
				      const char *name, PyObject *valobj) {

	if (!strcmp(name, "on_commit")) {
		if (!PyCallable_Check(valobj)) {
			cfl_PyErr_Format(
				RD_KAFKA_RESP_ERR__INVALID_ARG,
				"%s requires a callable "
				"object", name);
			return -1;
		}

		self->u.Consumer.on_commit = valobj;
		Py_INCREF(self->u.Consumer.on_commit);

		return 1;
	}

	return 0;
}

/**
 * @brief Call out to __init__.py _resolve_plugins() to see if any
 *        of the specified `plugin.library.paths` are found in the
 *        wheel's embedded library directory, and if so change the
 *        path to use these libraries.
 *
 * @returns a possibly updated plugin.library.paths string object which
 *          must be DECREF:ed, or NULL if an exception was raised.
 */
static PyObject *resolve_plugins (PyObject *plugins) {
        PyObject *resolved;
        PyObject *module, *function;

        module = PyImport_ImportModule("confluent_kafka");
        if (!module)
                return NULL;

        function = PyObject_GetAttrString(module, "_resolve_plugins");
        if (!function) {
                PyErr_SetString(PyExc_RuntimeError,
                                "confluent_kafka._resolve_plugins() not found");
                Py_DECREF(module);
                return NULL;
        }

        resolved = PyObject_CallFunctionObjArgs(function, plugins, NULL);

        Py_DECREF(function);
        Py_DECREF(module);

        if (!resolved) {
                PyErr_SetString(PyExc_RuntimeError,
                                "confluent_kafka._resolve_plugins() failed");
                return NULL;
        }

        return resolved;
}

/**
 * @brief Remove property from confidct and set rd_kafka_conf with its value
 *
 * @param vo The property value object
 *
 * @returns 1 on success or 0 on failure (exception raised).
 */
static int common_conf_set_special(PyObject *confdict, rd_kafka_conf_t *conf,
                                   const char *name, PyObject *vo) {
        const char *v;
        char errstr[256];
        PyObject *vs;
        PyObject *vs8 = NULL;

        if (!(vs = cfl_PyObject_Unistr(vo))) {
                PyErr_Format(PyExc_TypeError, "expected configuration property %s "
                             "as type unicode string", name);
                return 0;
        }

        v = cfl_PyUnistr_AsUTF8(vs, &vs8);
        if (rd_kafka_conf_set(conf, name, v, errstr, sizeof(errstr))
            != RD_KAFKA_CONF_OK) {
                cfl_PyErr_Format(RD_KAFKA_RESP_ERR__INVALID_ARG,
                                 "%s", errstr);
                Py_DECREF(vs);
                Py_XDECREF(vs8);
                return 0;
        }

        Py_DECREF(vs);
        Py_XDECREF(vs8);
        PyDict_DelItemString(confdict, name);
        return 1;
}


/**
 * @brief KIP-511: Set client.software.name and .version which is reported
 *          to the broker.
 *          Errors are ignored here since this is best-effort.
 */
static void common_conf_set_software (rd_kafka_conf_t *conf) {
        char version[128];

        rd_kafka_conf_set(conf, "client.software.name",
                          "confluent-kafka-python", NULL, 0);

        snprintf(version, sizeof(version), "%s-rdkafka-%s",
                 CFL_VERSION_STR, rd_kafka_version_str());
        rd_kafka_conf_set(conf, "client.software.version", version, NULL, 0);
}


/**
 * Common config setup for Kafka client handles.
 *
 * Returns a conf object on success or NULL on failure in which case
 * an exception has been raised.
 */
rd_kafka_conf_t *common_conf_setup (rd_kafka_type_t ktype,
				    Handle *h,
				    PyObject *args,
				    PyObject *kwargs) {
	rd_kafka_conf_t *conf;
	Py_ssize_t pos = 0;
	PyObject *ko, *vo;
        PyObject *confdict = NULL;

        if (rd_kafka_version() < MIN_RD_KAFKA_VERSION) {
                PyErr_Format(PyExc_RuntimeError,
                             "%s: librdkafka version %s (0x%x) detected",
                             MIN_VER_ERRSTR, rd_kafka_version_str(),
                             rd_kafka_version());
                return NULL;
        }

        /* Supported parameter constellations:
         *  - kwargs (conf={..}, logger=..)
         *  - args and kwargs ({..}, logger=..)
         *  - args ({..})
         * When both args and kwargs are present the kwargs take
         * precedence in case of duplicate keys.
         * All keys map to configuration properties.
         *
         * Copy configuration dict to avoid manipulating application config.
         */
        if (args && PyTuple_Size(args)) {
                if (!PyTuple_Check(args) ||
                    PyTuple_Size(args) > 1) {
                        PyErr_SetString(PyExc_TypeError,
                                        "expected tuple containing single dict");
                        return NULL;
                } else if (PyTuple_Size(args) == 1 &&
                           !PyDict_Check((confdict = PyTuple_GetItem(args, 0)))) {
                                PyErr_SetString(PyExc_TypeError,
                                                "expected configuration dict");
                                return NULL;
                }
                confdict = PyDict_Copy(confdict);
        }

        if (!confdict) {
                if (!kwargs) {
                        PyErr_SetString(PyExc_TypeError,
                                        "expected configuration dict");
                        return NULL;
                }

                confdict = PyDict_Copy(kwargs);

        } else if (kwargs) {
                /* Update confdict with kwargs */
                PyDict_Update(confdict, kwargs);
        }

        if (ktype == RD_KAFKA_CONSUMER &&
                !PyDict_GetItemString(confdict, "group.id")) {

                PyErr_SetString(PyExc_ValueError,
                                "Failed to create consumer: group.id must be set");
                Py_DECREF(confdict);
                return NULL;
        }

	conf = rd_kafka_conf_new();

        /* Set software name and verison prior to applying the confdict to
         * allow even higher-level clients to override it. */
        common_conf_set_software(conf);

        /*
         * Set debug contexts first to capture all events including plugin loading
         */
         if ((vo = PyDict_GetItemString(confdict, "debug")) &&
              !common_conf_set_special(confdict, conf, "debug", vo))
                        goto outer_err;

        /*
         * Plugins must be configured prior to handling any of their
         * configuration properties.
         * Dicts are unordered so we explicitly check for, set, and delete the
         * plugin paths here.
         * This ensures plugin configuration properties are handled in the
         * correct order.
         */
        if ((vo = PyDict_GetItemString(confdict, "plugin.library.paths"))) {
                /* Resolve plugin paths */
                PyObject *resolved;

                resolved = resolve_plugins(vo);
                if (!resolved)
                        goto outer_err;

                if (!common_conf_set_special(confdict, conf,
                                             "plugin.library.paths",
                                             resolved)) {
                        Py_DECREF(resolved);
                        goto outer_err;
                }
                Py_DECREF(resolved);
        }

        if ((vo = PyDict_GetItemString(confdict, "default.topic.config"))) {
        /* TODO: uncomment for 1.0 release
                PyErr_Warn(PyExc_DeprecationWarning,
                             "default.topic.config has being deprecated, "
                             "set default topic configuration values in the global dict");
        */
                if (PyDict_Update(confdict, vo) == -1) {
                        goto outer_err;
                }
                PyDict_DelItemString(confdict, "default.topic.config");
        }

	/* Convert config dict to config key-value pairs. */
	while (PyDict_Next(confdict, &pos, &ko, &vo)) {
		PyObject *ks;
		PyObject *ks8 = NULL;
		PyObject *vs = NULL, *vs8 = NULL;
		const char *k;
		const char *v;
		char errstr[256];
                int r = 0;

		if (!(ks = cfl_PyObject_Unistr(ko))) {
                        PyErr_SetString(PyExc_TypeError,
                                        "expected configuration property name "
                                        "as type unicode string");
                        goto inner_err;
		}

		k = cfl_PyUnistr_AsUTF8(ks, &ks8);
		if (!strcmp(k, "error_cb")) {
			if (!PyCallable_Check(vo)) {
				PyErr_SetString(PyExc_TypeError,
						"expected error_cb property "
						"as a callable function");
                                goto inner_err;
                        }
			if (h->error_cb) {
				Py_DECREF(h->error_cb);
				h->error_cb = NULL;
			}
			if (vo != Py_None) {
				h->error_cb = vo;
				Py_INCREF(h->error_cb);
			}
                        Py_XDECREF(ks8);
			Py_DECREF(ks);
			continue;
                } else if (!strcmp(k, "throttle_cb")) {
                        if (!PyCallable_Check(vo)) {
                                PyErr_SetString(PyExc_ValueError,
                                        "expected throttle_cb property "
                                        "as a callable function");
                                goto inner_err;
                        }
                        if (h->throttle_cb) {
                                Py_DECREF(h->throttle_cb);
                                h->throttle_cb = NULL;
                        }
                        if (vo != Py_None) {
                                h->throttle_cb = vo;
                                Py_INCREF(h->throttle_cb);
                        }
                        Py_XDECREF(ks8);
                        Py_DECREF(ks);
                        continue;
		} else if (!strcmp(k, "stats_cb")) {
			if (!PyCallable_Check(vo)) {
				PyErr_SetString(PyExc_TypeError,
						"expected stats_cb property "
						"as a callable function");
                                goto inner_err;
                        }

			if (h->stats_cb) {
				Py_DECREF(h->stats_cb);
				h->stats_cb = NULL;
			}
			if (vo != Py_None) {
				h->stats_cb = vo;
				Py_INCREF(h->stats_cb);
			}
                        Py_XDECREF(ks8);
			Py_DECREF(ks);
			continue;
                } else if (!strcmp(k, "logger")) {
                        if (h->logger) {
                                Py_DECREF(h->logger);
                                h->logger = NULL;
                        }

                        if (vo != Py_None) {
                                h->logger = vo;
                                Py_INCREF(h->logger);
                        }
                        Py_XDECREF(ks8);
                        Py_DECREF(ks);
                        continue;
                } else if (!strcmp(k, "oauth_cb")) {
                        if (!PyCallable_Check(vo)) {
                                PyErr_SetString(PyExc_TypeError,
                                                "expected oauth_cb property "
                                                "as a callable function");
                                goto inner_err;
                        }
                        if (h->oauth_cb) {
                                Py_DECREF(h->oauth_cb);
                                h->oauth_cb = NULL;
                        }

                        if (vo != Py_None) {
                                h->oauth_cb = vo;
                                Py_INCREF(h->oauth_cb);
                        }
                        Py_XDECREF(ks8);
                        Py_DECREF(ks);
                        continue;
                }

		/* Special handling for certain config keys. */
		if (ktype == RD_KAFKA_PRODUCER)
			r = producer_conf_set_special(h, conf, k, vo);
		else if (ktype == RD_KAFKA_CONSUMER)
			r = consumer_conf_set_special(h, conf, k, vo);
		if (r == -1) {
			/* Error */
                        goto inner_err;
		} else if (r == 1) {
			/* Handled */
			continue;
		}


		/*
		 * Pass configuration property through to librdkafka.
		 */
                if (vo == Py_None) {
                        v = NULL;
                } else {
                        if (!(vs = cfl_PyObject_Unistr(vo))) {
                                PyErr_SetString(PyExc_TypeError,
                                                "expected configuration "
                                                "property value as type "
                                                "unicode string");
                                goto inner_err;
                        }
                        v = cfl_PyUnistr_AsUTF8(vs, &vs8);
                }

		if (rd_kafka_conf_set(conf, k, v, errstr, sizeof(errstr)) !=
		    RD_KAFKA_CONF_OK) {
			cfl_PyErr_Format(RD_KAFKA_RESP_ERR__INVALID_ARG,
					  "%s", errstr);
                        goto inner_err;
		}

                Py_XDECREF(vs8);
                Py_XDECREF(vs);
                Py_XDECREF(ks8);
		Py_DECREF(ks);
                continue;

inner_err:
                Py_XDECREF(vs8);
                Py_XDECREF(vs);
                Py_XDECREF(ks8);
                Py_XDECREF(ks);
                goto outer_err;
        }

        Py_DECREF(confdict);

        rd_kafka_conf_set_error_cb(conf, error_cb);

        if (h->throttle_cb)
                rd_kafka_conf_set_throttle_cb(conf, throttle_cb);

	if (h->stats_cb)
		rd_kafka_conf_set_stats_cb(conf, stats_cb);

        if (h->logger) {
                /* Write logs to log queue (which is forwarded
                 * to the polled queue in the Producer/Consumer constructors) */
                rd_kafka_conf_set(conf, "log.queue", "true", NULL, 0);
                rd_kafka_conf_set_log_cb(conf, log_cb);
        }

        if (h->oauth_cb)
                rd_kafka_conf_set_oauthbearer_token_refresh_cb(conf, oauth_cb);

	rd_kafka_conf_set_opaque(conf, h);

#ifdef WITH_PY_TSS
        if (PyThread_tss_create(&h->tlskey)) {
                PyErr_SetString(PyExc_RuntimeError,
                                "Failed to initialize thread local storage");
                rd_kafka_conf_destroy(conf);
                return NULL;
        }
#else
        h->tlskey = PyThread_create_key();
#endif

        h->initiated = 1;

	return conf;

outer_err:
        Py_DECREF(confdict);
        rd_kafka_conf_destroy(conf);

        return NULL;
}




/**
 * @brief Initialiase a CallState and unlock the GIL prior to a
 *        possibly blocking external call.
 */
void CallState_begin (Handle *h, CallState *cs) {
	cs->thread_state = PyEval_SaveThread();
	assert(cs->thread_state != NULL);
	cs->crashed = 0;
#ifdef WITH_PY_TSS
        PyThread_tss_set(&h->tlskey, cs);
#else
        PyThread_set_key_value(h->tlskey, cs);
#endif
}

/**
 * @brief Relock the GIL after external call is done.
 * @returns 0 if a Python signal was raised or a callback crashed, else 1.
 */
int CallState_end (Handle *h, CallState *cs) {
#ifdef WITH_PY_TSS
        PyThread_tss_set(&h->tlskey, NULL);
#else
        PyThread_delete_key_value(h->tlskey);
#endif

	PyEval_RestoreThread(cs->thread_state);

	if (PyErr_CheckSignals() == -1 || cs->crashed)
		return 0;

	return 1;
}


/**
 * @brief Get the current thread's CallState and re-locks the GIL.
 */
CallState *CallState_get (Handle *h) {
        CallState *cs;
#ifdef WITH_PY_TSS
        cs = PyThread_tss_get(&h->tlskey);
#else
        cs = PyThread_get_key_value(h->tlskey);
#endif
	assert(cs != NULL);
	assert(cs->thread_state != NULL);
	PyEval_RestoreThread(cs->thread_state);
	cs->thread_state = NULL;
	return cs;
}

/**
 * @brief Un-locks the GIL to resume blocking external call.
 */
void CallState_resume (CallState *cs) {
	assert(cs->thread_state == NULL);
	cs->thread_state = PyEval_SaveThread();
}

/**
 * @brief Indicate that call crashed.
 */
void CallState_crash (CallState *cs) {
	cs->crashed++;
}



/**
 * @brief Find class/type/object \p typename in \p modulename
 *
 * @returns a new reference to the object.
 *
 * @raises a TypeError exception if the type is not found.
 */

PyObject *cfl_PyObject_lookup (const char *modulename, const char *typename) {
        PyObject *module = PyImport_ImportModule(modulename);
        PyObject *obj;

        if (!module) {
                PyErr_Format(PyExc_ImportError,
                             "Module not found when looking up %s.%s",
                             modulename, typename);
                return NULL;
        }

        obj = PyObject_GetAttrString(module, typename);
        if (!obj) {
                Py_DECREF(module);
                PyErr_Format(PyExc_TypeError,
                             "No such class/type/object: %s.%s",
                             modulename, typename);
                return NULL;
        }

        return obj;
}


void cfl_PyDict_SetString (PyObject *dict, const char *name, const char *val) {
        PyObject *vo = cfl_PyUnistr(_FromString(val));
        PyDict_SetItemString(dict, name, vo);
        Py_DECREF(vo);
}

void cfl_PyDict_SetInt (PyObject *dict, const char *name, int val) {
        PyObject *vo = cfl_PyInt_FromInt(val);
        PyDict_SetItemString(dict, name, vo);
        Py_DECREF(vo);
}

void cfl_PyDict_SetLong (PyObject *dict, const char *name, long val) {
        PyObject *vo = cfl_PyLong_FromLong(val);
        PyDict_SetItemString(dict, name, vo);
        Py_DECREF(vo);
}

int cfl_PyObject_SetString (PyObject *o, const char *name, const char *val) {
        PyObject *vo = cfl_PyUnistr(_FromString(val));
        int r = PyObject_SetAttrString(o, name, vo);
        Py_DECREF(vo);
        return r;
}

int cfl_PyObject_SetInt (PyObject *o, const char *name, int val) {
        PyObject *vo = cfl_PyInt_FromInt(val);
        int r = PyObject_SetAttrString(o, name, vo);
        Py_DECREF(vo);
        return r;
}


/**
 * @brief Get attribute \p attr_name from \p object and verify it is
 *        of type \p py_type.
 *
 * @param py_type the value type of \p attr_name must match \p py_type, unless
 *                \p py_type is NULL.
 *
 * @returns 1 if \p valp was updated with the object (new reference) or NULL
 *          if not matched and not required, or
 *          0 if an exception was raised.
 */
int cfl_PyObject_GetAttr (PyObject *object, const char *attr_name,
                          PyObject **valp, const PyTypeObject *py_type,
                          int required, int allow_None) {
        PyObject *o;

        o = PyObject_GetAttrString(object, attr_name);
        if (!o) {
                if (!required) {
                        *valp = NULL;
                        return 1;
                }

                PyErr_Format(PyExc_TypeError,
                             "Required attribute .%s missing", attr_name);
                return 0;
        }

        if (!(allow_None && o == Py_None) && py_type && Py_TYPE(o) != py_type) {
                Py_DECREF(o);
                PyErr_Format(PyExc_TypeError,
                             "Expected .%s to be %s type, not %s",
                             attr_name, py_type->tp_name,
                             ((PyTypeObject *)PyObject_Type(o))->tp_name);
                return 0;
        }

        *valp = o;

        return 1;
}

/**
 * @brief Get attribute \p attr_name from \p object and make sure it is
 *        an integer type.
 *
 * @returns 1 if \p valp was updated with either the object value, or \p defval.
 *          0 if an exception was raised.
 */
int cfl_PyObject_GetInt (PyObject *object, const char *attr_name, int *valp,
                         int defval, int required) {
        PyObject *o;

        if (!cfl_PyObject_GetAttr(object, attr_name, &o,
#ifdef PY3
                                  &PyLong_Type,
#else
                                  &PyInt_Type,
#endif
                                  required, 0))
                return 0;

        if (!o) {
                *valp = defval;
                return 1;
        }

        *valp = cfl_PyInt_AsInt(o);
        Py_DECREF(o);

        return 1;
}


/**
 * @brief Checks that \p object is a bool (or boolable) and sets
 *        \p *valp according to the object.
 *
 * @returns 1 if \p valp was set, or 0 if \p object is not a boolable object.
 *          An exception is raised in the error case.
 */
int cfl_PyBool_get (PyObject *object, const char *name, int *valp) {
        if (!PyBool_Check(object)) {
                PyErr_Format(PyExc_TypeError,
                             "Expected %s to be bool type, not %s",
                             name,
                             ((PyTypeObject *)PyObject_Type(object))->tp_name);
                return 0;
        }

        *valp = object == Py_True;

        return 1;
}

/**
 * @brief Get attribute \p attr_name from \p object and make sure it is
 *        a string type or None if \p allow_None is 1
 *
 * @returns 1 if \p valp was updated with a newly allocated copy of either the
 *          object value (UTF8), or \p defval or NULL if the attr is None
 *          0 if an exception was raised.
 */
int cfl_PyObject_GetString (PyObject *object, const char *attr_name,
                            char **valp, const char *defval, int required,
                            int allow_None) {
        PyObject *o, *uo, *uop;

        if (!cfl_PyObject_GetAttr(object, attr_name, &o,
#ifdef PY3
                                  &PyUnicode_Type,
#else
                                  /* Python 2: support both str and unicode
                                   *           let PyObject_Unistr() do the
                                   *           proper conversion below. */
                                  NULL,
#endif
                                  required, allow_None))
                return 0;

        if (!o) {
                *valp = defval ? strdup(defval) : NULL;
                return 1;
        }

        if (o == Py_None) {
                Py_DECREF(o);
                *valp = NULL;
                return 1;
        }

        if (!(uo = cfl_PyObject_Unistr(o))) {
                Py_DECREF(o);
                PyErr_Format(PyExc_TypeError,
                             "Expected .%s to be a unicode string type, not %s",
                             attr_name,
                             ((PyTypeObject *)PyObject_Type(o))->tp_name);
                return 0;
        }
        Py_DECREF(o);

        *valp = (char *)cfl_PyUnistr_AsUTF8(uo, &uop);
        if (!*valp) {
                Py_DECREF(uo);
                Py_XDECREF(uop);
                return 0; /* exception raised by AsUTF8 */
        }

        *valp = strdup(*valp);
        Py_DECREF(uo);
        Py_XDECREF(uop);

        return 1;
}



/**
 * @returns a Python list of longs based on the input int32_t array
 */
PyObject *cfl_int32_array_to_py_list (const int32_t *arr, size_t cnt) {
        PyObject *list;
        size_t i;

        list = PyList_New((Py_ssize_t)cnt);
        if (!list)
                return NULL;

        for (i = 0 ; i < cnt ; i++)
                PyList_SET_ITEM(list, (Py_ssize_t)i,
                                cfl_PyInt_FromInt(arr[i]));

        return list;
}


/****************************************************************************
 *
 *
 * Methods common across all types of clients.
 *
 *
 *
 *
 ****************************************************************************/

const char set_sasl_credentials_doc[] = PyDoc_STR(
        ".. py:function:: set_sasl_credentials(username, password)\n"
        "\n"
        "  Sets the SASL credentials used for this client.\n"
        "  These credentials will overwrite the old ones, and will be used the next time the client needs to authenticate.\n"
        "  This method will not disconnect existing broker connections that have been established with the old credentials.\n"
        "  This method is applicable only to SASL PLAIN and SCRAM mechanisms.\n");


PyObject *set_sasl_credentials(Handle *self, PyObject *args, PyObject *kwargs) {
        const char *username = NULL;
        const char *password = NULL;
        rd_kafka_error_t* error;
        CallState cs;
        static char *kws[] = {"username", "password", NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss", kws,
                                         &username, &password)) {
                return NULL;
        }

        CallState_begin(self, &cs);
        error = rd_kafka_sasl_set_credentials(self->rk, username, password);

        if (!CallState_end(self, &cs)) {
                if (error) /* Ignore error in favour of callstate exception */
                        rd_kafka_error_destroy(error);
                return NULL;
        }

        if (error) {
                cfl_PyErr_from_error_destroy(error);
                return NULL;
        }

        Py_RETURN_NONE;
}


/****************************************************************************
 *
 *
 * Base
 *
 *
 *
 *
 ****************************************************************************/


static PyObject *libversion (PyObject *self, PyObject *args) {
	return Py_BuildValue("si",
			     rd_kafka_version_str(),
			     rd_kafka_version());
}

/**
 * @brief confluent-kafka-python version.
 */
static PyObject *version (PyObject *self, PyObject *args) {
	return Py_BuildValue("si", CFL_VERSION_STR, CFL_VERSION);
}

static PyMethodDef cimpl_methods[] = {
	{"libversion", libversion, METH_NOARGS,
	 "  Retrieve librdkafka version string and integer\n"
	 "\n"
	 "  :returns: (version_string, version_int) tuple\n"
	 "  :rtype: tuple(str,int)\n"
	 "\n"
	},
	{"version", version, METH_NOARGS,
	 "  Retrieve module version string and integer\n"
	 "\n"
	 "  :returns: (version_string, version_int) tuple\n"
	 "  :rtype: tuple(str,int)\n"
	 "\n"
	},
	{ NULL }
};


/**
 * @brief Add librdkafka error enums to KafkaError's type dict.
 * @returns an updated doc string containing all error constants.
 */
static char *KafkaError_add_errs (PyObject *dict, const char *origdoc) {
	const struct rd_kafka_err_desc *descs;
	size_t cnt;
	size_t i;
	char *doc;
	size_t dof = 0, dsize;
	/* RST grid table column widths */
#define _COL1_W 50
#define _COL2_W 100 /* Must be larger than COL1 */
	char dash[_COL2_W], eq[_COL2_W];

	rd_kafka_get_err_descs(&descs, &cnt);

	memset(dash, '-', sizeof(dash));
	memset(eq, '=', sizeof(eq));

	/* Setup output doc buffer. */
	dof = strlen(origdoc);
	dsize = dof + 500 + (cnt * 200);
	doc = malloc(dsize);
	memcpy(doc, origdoc, dof+1);

#define _PRINT(...) do {						\
		char tmpdoc[512];					\
		size_t _len;						\
		_len = snprintf(tmpdoc, sizeof(tmpdoc), __VA_ARGS__);	\
		if (_len > sizeof(tmpdoc)) _len = sizeof(tmpdoc)-1;	\
		if (dof + _len >= dsize) {				\
			dsize += 2;					\
			doc = realloc(doc, dsize);			\
		}							\
		memcpy(doc+dof, tmpdoc, _len+1);			\
		dof += _len;						\
	} while (0)

	/* Error constant table header (RST grid table) */
	_PRINT("Error and event constants:\n\n"
	       "+-%.*s-+-%.*s-+\n"
	       "| %-*.*s | %-*.*s |\n"
	       "+=%.*s=+=%.*s=+\n",
	       _COL1_W, dash, _COL2_W, dash,
	       _COL1_W, _COL1_W, "Constant", _COL2_W, _COL2_W, "Description",
	       _COL1_W, eq, _COL2_W, eq);

	for (i = 0 ; i < cnt ; i++) {
		PyObject *code;

		if (!descs[i].desc)
			continue;

		code = cfl_PyInt_FromInt(descs[i].code);

		PyDict_SetItemString(dict, descs[i].name, code);

		Py_DECREF(code);

		_PRINT("| %-*.*s | %-*.*s |\n"
		       "+-%.*s-+-%.*s-+\n",
		       _COL1_W, _COL1_W, descs[i].name,
		       _COL2_W, _COL2_W, descs[i].desc,
		       _COL1_W, dash, _COL2_W, dash);
	}

	_PRINT("\n");

	return doc; /* FIXME: leak */
}


#ifdef PY3
static struct PyModuleDef cimpl_moduledef = {
	PyModuleDef_HEAD_INIT,
	"cimpl",                                  /* m_name */
	"Confluent's Python client for Apache Kafka (C implementation)", /* m_doc */
	-1,                                       /* m_size */
	cimpl_methods,                            /* m_methods */
};
#endif


static PyObject *_init_cimpl (void) {
	PyObject *m;

/* PyEval_InitThreads became deprecated in Python 3.9 and will be removed in Python 3.11.
 * Prior to Python 3.7, this call was required to initialize the GIL. */
#if PY_VERSION_HEX < 0x03090000
        PyEval_InitThreads();
#endif

	if (PyType_Ready(&KafkaErrorType) < 0)
		return NULL;
	if (PyType_Ready(&MessageType) < 0)
		return NULL;
	if (PyType_Ready(&UuidType) < 0)
		return NULL;
	if (PyType_Ready(&TopicPartitionType) < 0)
		return NULL;
	if (PyType_Ready(&ProducerType) < 0)
		return NULL;
	if (PyType_Ready(&ConsumerType) < 0)
		return NULL;
        if (PyType_Ready(&AdminType) < 0)
                return NULL;
        if (AdminTypes_Ready() < 0)
                return NULL;

#ifdef PY3
	m = PyModule_Create(&cimpl_moduledef);
#else
	m = Py_InitModule3("cimpl", cimpl_methods,
			   "Confluent's Python client for Apache Kafka (C implementation)");
#endif
	if (!m)
		return NULL;

	Py_INCREF(&KafkaErrorType);
	KafkaErrorType.tp_doc =
		KafkaError_add_errs(KafkaErrorType.tp_dict,
				    KafkaErrorType.tp_doc);
	PyModule_AddObject(m, "KafkaError", (PyObject *)&KafkaErrorType);

	Py_INCREF(&MessageType);
	PyModule_AddObject(m, "Message", (PyObject *)&MessageType);

        Py_INCREF(&UuidType);
        PyModule_AddObject(m, "Uuid", (PyObject *)&UuidType);

	Py_INCREF(&TopicPartitionType);
	PyModule_AddObject(m, "TopicPartition",
			   (PyObject *)&TopicPartitionType);

	Py_INCREF(&ProducerType);
	PyModule_AddObject(m, "Producer", (PyObject *)&ProducerType);

	Py_INCREF(&ConsumerType);
	PyModule_AddObject(m, "Consumer", (PyObject *)&ConsumerType);

        Py_INCREF(&AdminType);
        PyModule_AddObject(m, "_AdminClientImpl", (PyObject *)&AdminType);

        AdminTypes_AddObjects(m);

#if PY_VERSION_HEX >= 0x02070000
	KafkaException = PyErr_NewExceptionWithDoc(
		"cimpl.KafkaException",
		"Kafka exception that wraps the :py:class:`KafkaError` "
		"class.\n"
		"\n"
		"Use ``exception.args[0]`` to extract the "
		":py:class:`KafkaError` object\n"
		"\n",
		NULL, NULL);
#else
        KafkaException = PyErr_NewException("cimpl.KafkaException", NULL, NULL);
#endif
	Py_INCREF(KafkaException);
	PyModule_AddObject(m, "KafkaException", KafkaException);

	PyModule_AddIntConstant(m, "TIMESTAMP_NOT_AVAILABLE", RD_KAFKA_TIMESTAMP_NOT_AVAILABLE);
	PyModule_AddIntConstant(m, "TIMESTAMP_CREATE_TIME", RD_KAFKA_TIMESTAMP_CREATE_TIME);
	PyModule_AddIntConstant(m, "TIMESTAMP_LOG_APPEND_TIME", RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME);

        PyModule_AddIntConstant(m, "OFFSET_BEGINNING", RD_KAFKA_OFFSET_BEGINNING);
        PyModule_AddIntConstant(m, "OFFSET_END", RD_KAFKA_OFFSET_END);
        PyModule_AddIntConstant(m, "OFFSET_STORED", RD_KAFKA_OFFSET_STORED);
        PyModule_AddIntConstant(m, "OFFSET_INVALID", RD_KAFKA_OFFSET_INVALID);

	return m;
}


#ifdef PY3
PyMODINIT_FUNC PyInit_cimpl (void) {
	return _init_cimpl();
}
#else
PyMODINIT_FUNC initcimpl (void) {
	_init_cimpl();
}
#endif
