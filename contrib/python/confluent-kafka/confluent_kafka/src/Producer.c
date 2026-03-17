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
 *
 */



/****************************************************************************
 *
 *
 * Producer
 *
 *
 *
 *
 ****************************************************************************/

/**
 * Per-message state.
 */
struct Producer_msgstate {
	Handle   *self;
	PyObject *dr_cb;
};


/**
 * Create a new per-message state.
 * Returns NULL if neither dr_cb or partitioner_cb is set.
 */
static __inline struct Producer_msgstate *
Producer_msgstate_new (Handle *self,
		       PyObject *dr_cb) {
	struct Producer_msgstate *msgstate;

	msgstate = calloc(1, sizeof(*msgstate));
	msgstate->self = self;

	if (dr_cb) {
		msgstate->dr_cb = dr_cb;
		Py_INCREF(dr_cb);
	}
	return msgstate;
}

static __inline void
Producer_msgstate_destroy (struct Producer_msgstate *msgstate) {
	if (msgstate->dr_cb)
		Py_DECREF(msgstate->dr_cb);
	free(msgstate);
}


static void Producer_clear0 (Handle *self) {
        if (self->u.Producer.default_dr_cb) {
                Py_DECREF(self->u.Producer.default_dr_cb);
                self->u.Producer.default_dr_cb = NULL;
        }
}

static int Producer_clear (Handle *self) {
        Producer_clear0(self);
        Handle_clear(self);
        return 0;
}

static void Producer_dealloc (Handle *self) {
	PyObject_GC_UnTrack(self);

        Producer_clear0(self);

        if (self->rk) {
                CallState cs;
                CallState_begin(self, &cs);

                rd_kafka_destroy(self->rk);

                CallState_end(self, &cs);
        }

        Handle_clear(self);

	Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Producer_traverse (Handle *self,
			      visitproc visit, void *arg) {
	if (self->u.Producer.default_dr_cb)
		Py_VISIT(self->u.Producer.default_dr_cb);

	Handle_traverse(self, visit, arg);

	return 0;
}


static void dr_msg_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkm,
			   void *opaque) {
	struct Producer_msgstate *msgstate = rkm->_private;
	Handle *self = opaque;
	CallState *cs;
	PyObject *args;
	PyObject *result;
	PyObject *msgobj;

	if (!msgstate)
		return;

	cs = CallState_get(self);

	if (!msgstate->dr_cb) {
		/* No callback defined */
		goto done;
	}

        /* Skip callback if delivery.report.only.error=true */
        if (self->u.Producer.dr_only_error && !rkm->err)
                goto done;

	msgobj = Message_new0(self, rkm);

        args = Py_BuildValue("(OO)", ((Message *)msgobj)->error, msgobj);

	Py_DECREF(msgobj);

	if (!args) {
		cfl_PyErr_Format(RD_KAFKA_RESP_ERR__FAIL,
				 "Unable to build callback args");
		CallState_crash(cs);
		goto done;
	}

	result = PyObject_CallObject(msgstate->dr_cb, args);
	Py_DECREF(args);

	if (result)
		Py_DECREF(result);
	else {
		CallState_crash(cs);
		rd_kafka_yield(rk);
	}

 done:
	Producer_msgstate_destroy(msgstate);
	CallState_resume(cs);
}


#if HAVE_PRODUCEV
static rd_kafka_resp_err_t
Producer_producev (Handle *self,
                   const char *topic, int32_t partition,
                   const void *value, size_t value_len,
                   const void *key, size_t key_len,
                   void *opaque, int64_t timestamp
#ifdef RD_KAFKA_V_HEADERS
                   ,rd_kafka_headers_t *headers
#endif
                   ) {

        return rd_kafka_producev(self->rk,
                                 RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                 RD_KAFKA_V_TOPIC(topic),
                                 RD_KAFKA_V_PARTITION(partition),
                                 RD_KAFKA_V_KEY(key, (size_t)key_len),
                                 RD_KAFKA_V_VALUE((void *)value,
                                                  (size_t)value_len),
                                 RD_KAFKA_V_TIMESTAMP(timestamp),
#ifdef RD_KAFKA_V_HEADERS
                                 RD_KAFKA_V_HEADERS(headers),
#endif
                                 RD_KAFKA_V_OPAQUE(opaque),
                                 RD_KAFKA_V_END);
}
#else

static rd_kafka_resp_err_t
Producer_produce0 (Handle *self,
                   const char *topic, int32_t partition,
                   const void *value, size_t value_len,
                   const void *key, size_t key_len,
                   void *opaque) {
        rd_kafka_topic_t *rkt;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;

        if (!(rkt = rd_kafka_topic_new(self->rk, topic, NULL)))
                return RD_KAFKA_RESP_ERR__INVALID_ARG;

	if (rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
			     (void *)value, value_len,
			     (void *)key, key_len, opaque) == -1)
                err = rd_kafka_last_error();

        rd_kafka_topic_destroy(rkt);

        return err;
}
#endif


static PyObject *Producer_produce (Handle *self, PyObject *args,
				       PyObject *kwargs) {
	const char *topic, *value = NULL, *key = NULL;
        Py_ssize_t value_len = 0, key_len = 0;
	int partition = RD_KAFKA_PARTITION_UA;
	PyObject *headers = NULL, *dr_cb = NULL, *dr_cb2 = NULL;
        long long timestamp = 0;
        rd_kafka_resp_err_t err;
	struct Producer_msgstate *msgstate;
#ifdef RD_KAFKA_V_HEADERS
    rd_kafka_headers_t *rd_headers = NULL;
#endif

	static char *kws[] = { "topic",
			       "value",
			       "key",
			       "partition",
			       "callback",
			       "on_delivery", /* Alias */
                   "timestamp",
                   "headers",
			       NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs,
					 "s|z#z#iOOLO"
                                         , kws,
					 &topic, &value, &value_len,
					 &key, &key_len, &partition,
					 &dr_cb, &dr_cb2,
                     &timestamp, &headers))
		return NULL;

#if !HAVE_PRODUCEV
        if (timestamp) {
                PyErr_Format(PyExc_NotImplementedError,
                             "Producer timestamps require "
                             "confluent-kafka-python built for librdkafka "
                             "version >=v0.9.4 (librdkafka runtime 0x%x, "
                             "buildtime 0x%x)",
                             rd_kafka_version(), RD_KAFKA_VERSION);
                return NULL;
        }
#endif

#ifndef RD_KAFKA_V_HEADERS
    if (headers) {
            PyErr_Format(PyExc_NotImplementedError,
                         "Producer message headers requires "
                         "confluent-kafka-python built for librdkafka "
                         "version >=v0.11.4 (librdkafka runtime 0x%x, "
                         "buildtime 0x%x)",
                         rd_kafka_version(), RD_KAFKA_VERSION);
            return NULL;
    }
#else
    if (headers && headers != Py_None) {
        if(!(rd_headers = py_headers_to_c(headers)))
            return NULL;
    }
#endif


	if (dr_cb2 && !dr_cb) /* Alias */
		dr_cb = dr_cb2;

	if (!dr_cb || dr_cb == Py_None)
		dr_cb = self->u.Producer.default_dr_cb;

	/* Create msgstate if necessary, may return NULL if no callbacks
	 * are wanted. */
	msgstate = Producer_msgstate_new(self, dr_cb);

        /* Produce message */
#if HAVE_PRODUCEV
        err = Producer_producev(self, topic, partition,
                                value, value_len,
                                key, key_len,
                                msgstate, timestamp
#ifdef RD_KAFKA_V_HEADERS
                                ,rd_headers
#endif
                                );
#else
        err = Producer_produce0(self, topic, partition,
                                value, value_len,
                                key, key_len,
                                msgstate);
#endif

        if (err) {
		if (msgstate)
			Producer_msgstate_destroy(msgstate);

		if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
			PyErr_Format(PyExc_BufferError,
				     "%s", rd_kafka_err2str(err));
                else
			cfl_PyErr_Format(err,
					 "Unable to produce message: %s",
					 rd_kafka_err2str(err));

		return NULL;
	}

	Py_RETURN_NONE;
}


/**
 * @brief Call rd_kafka_poll() and keep track of crashing callbacks.
 * @returns -1 if callback crashed (or poll() failed), else the number
 * of events served.
 */
static int Producer_poll0 (Handle *self, int tmout) {
	int r;
	CallState cs;

	CallState_begin(self, &cs);

	r = rd_kafka_poll(self->rk, tmout);

	if (!CallState_end(self, &cs)) {
		return -1;
	}

	return r;
}


static PyObject *Producer_poll (Handle *self, PyObject *args,
				    PyObject *kwargs) {
        double tmout = -1.0;
	int r;
	static char *kws[] = { "timeout", NULL };

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|d", kws, &tmout))
                return NULL;

	r = Producer_poll0(self, cfl_timeout_ms(tmout));
	if (r == -1)
		return NULL;

	return cfl_PyInt_FromInt(r);
}


static PyObject *Producer_flush (Handle *self, PyObject *args,
                                 PyObject *kwargs) {
        double tmout = -1;
        int qlen = 0;
        static char *kws[] = { "timeout", NULL };
        rd_kafka_resp_err_t err;
        CallState cs;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|d", kws, &tmout))
                return NULL;

        CallState_begin(self, &cs);
        err = rd_kafka_flush(self->rk, cfl_timeout_ms(tmout));
        if (!CallState_end(self, &cs))
                return NULL;

        if (err) /* Get the queue length on error (timeout) */
                qlen = rd_kafka_outq_len(self->rk);

        return cfl_PyInt_FromInt(qlen);
}

static PyObject *Producer_init_transactions (Handle *self, PyObject *args) {
        CallState cs;
        rd_kafka_error_t *error;
        double tmout = -1.0;

        if (!PyArg_ParseTuple(args, "|d", &tmout))
                return NULL;

        CallState_begin(self, &cs);

        error = rd_kafka_init_transactions(self->rk, cfl_timeout_ms(tmout));

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

static PyObject *Producer_begin_transaction (Handle *self) {
        rd_kafka_error_t *error;

        error = rd_kafka_begin_transaction(self->rk);

        if (error) {
                cfl_PyErr_from_error_destroy(error);
                return NULL;
        }

        Py_RETURN_NONE;
}

static PyObject *Producer_send_offsets_to_transaction(Handle *self,
                                                      PyObject *args) {
        CallState cs;
        rd_kafka_error_t *error;
        PyObject *metadata = NULL, *offsets = NULL;
        rd_kafka_topic_partition_list_t *c_offsets;
        rd_kafka_consumer_group_metadata_t *cgmd;
        double tmout = -1.0;

        if (!PyArg_ParseTuple(args, "OO|d", &offsets, &metadata, &tmout))
                return NULL;

        if (!(c_offsets = py_to_c_parts(offsets)))
                return NULL;

        if (!(cgmd = py_to_c_cgmd(metadata))) {
                rd_kafka_topic_partition_list_destroy(c_offsets);
                return NULL;
        }

        CallState_begin(self, &cs);

        error = rd_kafka_send_offsets_to_transaction(self->rk, c_offsets,
                                                     cgmd,
                                                     cfl_timeout_ms(tmout));

        rd_kafka_consumer_group_metadata_destroy(cgmd);
        rd_kafka_topic_partition_list_destroy(c_offsets);

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

static PyObject *Producer_commit_transaction(Handle *self, PyObject *args) {
        CallState cs;
        rd_kafka_error_t *error;
        double tmout = -1.0;

        if (!PyArg_ParseTuple(args, "|d", &tmout))
                return NULL;

        CallState_begin(self, &cs);

        error = rd_kafka_commit_transaction(self->rk, cfl_timeout_ms(tmout));

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

static PyObject *Producer_abort_transaction(Handle *self, PyObject *args) {
        CallState cs;
        rd_kafka_error_t *error;
        double tmout = -1.0;

        if (!PyArg_ParseTuple(args, "|d", &tmout))
                return NULL;

        CallState_begin(self, &cs);

        error = rd_kafka_abort_transaction(self->rk, cfl_timeout_ms(tmout));

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

static void *Producer_purge (Handle *self, PyObject *args,
                                 PyObject *kwargs) {
        int in_queue = 1;
        int in_flight = 1;
        int blocking = 1;
        int purge_strategy = 0;

        rd_kafka_resp_err_t err;
        static char *kws[] = { "in_queue", "in_flight", "blocking", NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|bbb", kws,
                                         &in_queue, &in_flight, &blocking))
                return NULL;
        if (in_queue)
                purge_strategy = RD_KAFKA_PURGE_F_QUEUE;
        if (in_flight)
                purge_strategy |= RD_KAFKA_PURGE_F_INFLIGHT;
        if (blocking)
                purge_strategy |= RD_KAFKA_PURGE_F_NON_BLOCKING;

        err = rd_kafka_purge(self->rk, purge_strategy);

        if (err) {
                cfl_PyErr_Format(err, "Purge failed: %s",
                                 rd_kafka_err2str(err));
                return NULL;
        }

        Py_RETURN_NONE;
}


static PyMethodDef Producer_methods[] = {
	{ "produce", (PyCFunction)Producer_produce,
	  METH_VARARGS|METH_KEYWORDS,
	  ".. py:function:: produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])\n"
	  "\n"
	  "  Produce message to topic.\n"
	  "  This is an asynchronous operation, an application may use the "
	  "``callback`` (alias ``on_delivery``) argument to pass a function "
	  "(or lambda) that will be called from :py:func:`poll()` when the "
	  "message has been successfully delivered or permanently fails delivery.\n"
      "\n"
      "  Currently message headers are not supported on the message returned to the "
      "callback. The ``msg.headers()`` will return None even if the original message "
      "had headers set.\n"
	  "\n"
	  "  :param str topic: Topic to produce message to\n"
	  "  :param str|bytes value: Message payload\n"
	  "  :param str|bytes key: Message key\n"
	  "  :param int partition: Partition to produce to, else uses the "
	  "configured built-in partitioner.\n"
	  "  :param func on_delivery(err,msg): Delivery report callback to call "
	  "(from :py:func:`poll()` or :py:func:`flush()`) on successful or "
	  "failed delivery\n"
          "  :param int timestamp: Message timestamp (CreateTime) in milliseconds since epoch UTC (requires librdkafka >= v0.9.4, api.version.request=true, and broker >= 0.10.0.0). Default value is current time.\n"
	  "\n"
          "  :param dict|list headers: Message headers to set on the message. The header key must be a string while the value must be binary, unicode or None. Accepts a list of (key,value) or a dict. (Requires librdkafka >= v0.11.4 and broker version >= 0.11.0.0)\n"
	  "  :rtype: None\n"
	  "  :raises BufferError: if the internal producer message queue is "
	  "full (``queue.buffering.max.messages`` exceeded)\n"
	  "  :raises KafkaException: for other errors, see exception code\n"
          "  :raises NotImplementedError: if timestamp is specified without underlying library support.\n"
	  "\n"
	},

	{ "poll", (PyCFunction)Producer_poll, METH_VARARGS|METH_KEYWORDS,
	  ".. py:function:: poll([timeout])\n"
	  "\n"
	  "  Polls the producer for events and calls the corresponding "
	  "callbacks (if registered).\n"
	  "\n"
	  "  Callbacks:\n"
	  "\n"
	  "  - ``on_delivery`` callbacks from :py:func:`produce()`\n"
	  "  - ...\n"
	  "\n"
	  "  :param float timeout: Maximum time to block waiting for events. (Seconds)\n"
	  "  :returns: Number of events processed (callbacks served)\n"
	  "  :rtype: int\n"
	  "\n"
	},

	{ "flush", (PyCFunction)Producer_flush, METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: flush([timeout])\n"
          "\n"
	  "   Wait for all messages in the Producer queue to be delivered.\n"
	  "   This is a convenience method that calls :py:func:`poll()` until "
	  ":py:func:`len()` is zero or the optional timeout elapses.\n"
	  "\n"
          "  :param: float timeout: Maximum time to block (requires librdkafka >= v0.9.4). (Seconds)\n"
          "  :returns: Number of messages still in queue.\n"
          "\n"
	  ".. note:: See :py:func:`poll()` for a description on what "
	  "callbacks may be triggered.\n"
	  "\n"
	},
	{ "purge", (PyCFunction)Producer_purge, METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: purge([in_queue=True], [in_flight=True], [blocking=True])\n"
          "\n"
	  "   Purge messages currently handled by the producer instance.\n"
	  "   The application will need to call poll() or flush() "
	  "afterwards to serve the delivery report callbacks of the purged messages.\n"
	  "\n"
	  "  :param: bool in_queue: Purge messages from internal queues. By default, true.\n"
	  "  :param: bool in_flight: Purge messages in flight to or from the broker. By default, true.\n"
	  "  :param: bool blocking: If set to False, will not wait on background thread queue "
	  "purging to finish. By default, true.\n"
          "\n"
	},
        { "list_topics", (PyCFunction)list_topics, METH_VARARGS|METH_KEYWORDS,
          list_topics_doc
        },
        { "init_transactions", (PyCFunction)Producer_init_transactions,
          METH_VARARGS,
          ".. py:function: init_transactions([timeout])\n"
          "\n"
          "  Initializes transactions for the producer instance.\n"
          "\n"
          "  This function ensures any transactions initiated by previous\n"
          "  instances of the producer with the same `transactional.id` are\n"
          "  completed.\n"
          "  If the previous instance failed with a transaction in progress\n"
          "  the previous transaction will be aborted.\n"
          "  This function needs to be called before any other transactional\n"
          "  or produce functions are called when the `transactional.id` is\n"
          "  configured.\n"
          "\n"
          "  If the last transaction had begun completion (following\n"
          "  transaction commit) but not yet finished, this function will\n"
          "  await the previous transaction's completion.\n"
          "\n"
          "  When any previous transactions have been fenced this function\n"
          "  will acquire the internal producer id and epoch, used in all\n"
          "  future transactional messages issued by this producer instance.\n"
          "\n"
          "  Upon successful return from this function the application has to\n"
          "  perform at least one of the following operations within \n"
          "  `transaction.timeout.ms` to avoid timing out the transaction\n"
          "  on the broker:\n"
          "  * produce() (et.al)\n"
          "  * send_offsets_to_transaction()\n"
          "  * commit_transaction()\n"
          "  * abort_transaction()\n"
          "\n"
          "  :param float timeout: Maximum time to block in seconds.\n"
          "\n"
          "  :raises: KafkaError: Use exc.args[0].retriable() to check if the\n"
          "                       operation may be retried, else treat the\n"
          "                       error as a fatal error.\n"
        },
        { "begin_transaction", (PyCFunction)Producer_begin_transaction,
          METH_NOARGS,
          ".. py:function:: begin_transaction()\n"
          "\n"
          "  Begin a new transaction.\n"
          "\n"
          "  init_transactions() must have been called successfully (once)\n"
          "  before this function is called.\n"
          "\n"
          "  Any messages produced or offsets sent to a transaction, after\n"
          "  the successful return of this function will be part of the\n"
          "  transaction and committed or aborted atomically.\n"
          "\n"
          "  Complete the transaction by calling commit_transaction() or\n"
          "  Abort the transaction by calling abort_transaction().\n"
          "\n"
          "  :raises: KafkaError: Use exc.args[0].retriable() to check if the\n"
          "                       operation may be retried, else treat the\n"
          "                       error as a fatal error.\n"
        },
        { "send_offsets_to_transaction",
          (PyCFunction)Producer_send_offsets_to_transaction,
           METH_VARARGS,
          ".. py:function:: send_offsets_to_transaction(positions,"
          " group_metadata, [timeout])\n"
          "\n"
          "  Sends a list of topic partition offsets to the consumer group\n"
          "  coordinator for group_metadata and marks the offsets as part\n"
          "  of the current transaction.\n"
          "  These offsets will be considered committed only if the\n"
          "  transaction is committed successfully.\n"
          "\n"
          "  The offsets should be the next message your application will\n"
          "  consume, i.e., the last processed message's offset + 1 for each\n"
          "  partition.\n"
          "  Either track the offsets manually during processing or use\n"
          "  consumer.position() (on the consumer) to get the current offsets\n"
          "  for the partitions assigned to the consumer.\n"
          "\n"
          "  Use this method at the end of a consume-transform-produce loop\n"
          "  prior to committing the transaction with commit_transaction().\n"
          "\n"
          "  Note: The consumer must disable auto commits\n"
          "        (set `enable.auto.commit` to false on the consumer).\n"
          "\n"
          "  Note: Logical and invalid offsets (e.g., OFFSET_INVALID) in\n"
          "  offsets will be ignored. If there are no valid offsets in\n"
          "  offsets the function will return successfully and no action\n"
          "  will be taken.\n"
          "\n"
          "  :param list(TopicPartition) offsets: current consumer/processing\n"
          "                                       position(offsets) for the\n"
          "                                       list of partitions.\n"
          "  :param object group_metadata: consumer group metadata retrieved\n"
          "                                from the input consumer's\n"
          "                                get_consumer_group_metadata().\n"
          "  :param float timeout: Amount of time to block in seconds.\n"
          "\n"
          "  :raises: KafkaError: Use exc.args[0].retriable() to check if the\n"
          "           operation may be retried, or\n"
          "           exc.args[0].txn_requires_abort() if the current\n"
          "           transaction has failed and must be aborted by calling\n"
          "           abort_transaction() and then start a new transaction\n"
          "           with begin_transaction().\n"
          "           Treat any other error as a fatal error.\n"
          "\n"
        },
        { "commit_transaction", (PyCFunction)Producer_commit_transaction,
          METH_VARARGS,
          ".. py:function:: commit_transaction([timeout])\n"
          "\n"
          "  Commmit the current transaction.\n"
          "  Any outstanding messages will be flushed (delivered) before\n"
          "  actually committing the transaction.\n"
          "\n"
          "  If any of the outstanding messages fail permanently the current\n"
          "  transaction will enter the abortable error state and this\n"
          "  function will return an abortable error, in this case the\n"
          "  application must call abort_transaction() before attempting\n"
          "  a new transaction with begin_transaction().\n"
          "\n"
          "  Note: This function will block until all outstanding messages\n"
          "  are delivered and the transaction commit request has been\n"
          "  successfully handled by the transaction coordinator, or until\n"
          "  the timeout expires, which ever comes first. On timeout the\n"
          "  application may call the function again.\n"
          "\n"
          "  Note: Will automatically call flush() to ensure all queued\n"
          "  messages are delivered before attempting to commit the\n"
          "  transaction. Delivery reports and other callbacks may thus be\n"
          "  triggered from this method.\n"
          "\n"
          "  :param float timeout: The amount of time to block in seconds.\n"
          "\n"
          "  :raises: KafkaError: Use exc.args[0].retriable() to check if the\n"
          "           operation may be retried, or\n"
          "           exc.args[0].txn_requires_abort() if the current\n"
          "           transaction has failed and must be aborted by calling\n"
          "           abort_transaction() and then start a new transaction\n"
          "           with begin_transaction().\n"
          "           Treat any other error as a fatal error.\n"
          "\n"
        },
        { "abort_transaction", (PyCFunction)Producer_abort_transaction,
          METH_VARARGS,
          ".. py:function:: abort_transaction([timeout])\n"
          "\n"
          "  Aborts the current transaction.\n"
          "  This function should also be used to recover from non-fatal\n"
          "  abortable transaction errors when KafkaError.txn_requires_abort()\n"
          "  is True.\n"
          " \n"
          "  Any outstanding messages will be purged and fail with\n"
          "  _PURGE_INFLIGHT or _PURGE_QUEUE.\n"
          " \n"
          "  Note: This function will block until all outstanding messages\n"
          "  are purged and the transaction abort request has been\n"
          "  successfully handled by the transaction coordinator, or until\n"
          "  the timeout expires, which ever comes first. On timeout the\n"
          "  application may call the function again.\n"
          " \n"
          "  Note: Will automatically call purge() and flush()  to ensure\n"
          "  all queued and in-flight messages are purged before attempting\n"
          "  to abort the transaction.\n"
          "\n"
          "  :param float timeout: The maximum amount of time to block\n"
          "       waiting for transaction to abort in seconds.\n"
          "\n"
          "  :raises: KafkaError: Use exc.args[0].retriable() to check if the\n"
          "           operation may be retried.\n"
          "           Treat any other error as a fatal error.\n"
          "\n"
        },
        { "set_sasl_credentials", (PyCFunction)set_sasl_credentials, METH_VARARGS|METH_KEYWORDS,
           set_sasl_credentials_doc
        },
        { NULL }
};


static Py_ssize_t Producer__len__ (Handle *self) {
	return rd_kafka_outq_len(self->rk);
}


static PySequenceMethods Producer_seq_methods = {
	(lenfunc)Producer__len__ /* sq_length */
};

static int Producer__bool__ (Handle *self) {
        return 1;
}

static PyNumberMethods Producer_num_methods = {
     0, // nb_add
     0, // nb_subtract
     0, // nb_multiply
     0, // nb_remainder
     0, // nb_divmod
     0, // nb_power
     0, // nb_negative
     0, // nb_positive
     0, // nb_absolute
     (inquiry)Producer__bool__ // nb_bool
};


static int Producer_init (PyObject *selfobj, PyObject *args, PyObject *kwargs) {
        Handle *self = (Handle *)selfobj;
        char errstr[256];
        rd_kafka_conf_t *conf;

        if (self->rk) {
                PyErr_SetString(PyExc_RuntimeError,
                                "Producer already __init__:ialized");
                return -1;
        }

        self->type = RD_KAFKA_PRODUCER;

        if (!(conf = common_conf_setup(RD_KAFKA_PRODUCER, self,
                                       args, kwargs)))
                return -1;

        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

        self->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                                errstr, sizeof(errstr));
        if (!self->rk) {
                cfl_PyErr_Format(rd_kafka_last_error(),
                                 "Failed to create producer: %s", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* Forward log messages to poll queue */
        if (self->logger)
                rd_kafka_set_log_queue(self->rk, NULL);

        return 0;
}


static PyObject *Producer_new (PyTypeObject *type, PyObject *args,
                               PyObject *kwargs) {
        return type->tp_alloc(type, 0);
}



PyTypeObject ProducerType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"cimpl.Producer",        /*tp_name*/
	sizeof(Handle),      /*tp_basicsize*/
	0,                         /*tp_itemsize*/
	(destructor)Producer_dealloc, /*tp_dealloc*/
	0,                         /*tp_print*/
	0,                         /*tp_getattr*/
	0,                         /*tp_setattr*/
	0,                         /*tp_compare*/
	0,                         /*tp_repr*/
	&Producer_num_methods,     /*tp_as_number*/
	&Producer_seq_methods,     /*tp_as_sequence*/
	0,                         /*tp_as_mapping*/
	0,                         /*tp_hash */
	0,                         /*tp_call*/
	0,                         /*tp_str*/
	0,                         /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
	Py_TPFLAGS_HAVE_GC, /*tp_flags*/
        "Asynchronous Kafka Producer\n"
        "\n"
        ".. py:function:: Producer(config)\n"
        "\n"
        "  :param dict config: Configuration properties. At a minimum ``bootstrap.servers`` **should** be set\n"
        "\n"
        "  Create a new Producer instance using the provided configuration dict.\n"
        "\n"
        "\n"
        ".. py:function:: __len__(self)\n"
        "\n"
	"  Producer implements __len__ that can be used as len(producer) to obtain number of messages waiting.\n"
        "  :returns: Number of messages and Kafka protocol requests waiting to be delivered to broker.\n"
        "  :rtype: int\n"
        "\n", /*tp_doc*/
	(traverseproc)Producer_traverse, /* tp_traverse */
	(inquiry)Producer_clear, /* tp_clear */
	0,		           /* tp_richcompare */
	0,		           /* tp_weaklistoffset */
	0,		           /* tp_iter */
	0,		           /* tp_iternext */
	Producer_methods,      /* tp_methods */
	0,                         /* tp_members */
	0,                         /* tp_getset */
	0,                         /* tp_base */
	0,                         /* tp_dict */
	0,                         /* tp_descr_get */
	0,                         /* tp_descr_set */
	0,                         /* tp_dictoffset */
        Producer_init,             /* tp_init */
	0,                         /* tp_alloc */
	Producer_new           /* tp_new */
};
