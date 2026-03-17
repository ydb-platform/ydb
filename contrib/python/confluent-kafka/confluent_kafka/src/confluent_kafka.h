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

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include <pythread.h>

#include <stdint.h>
#include <librdkafka/rdkafka.h>

#ifdef _MSC_VER
/* Windows */
#define CFL_PRId64 "I64d"
#define CFL_PRId32 "I32d"

#else
/* C99 */
#include <inttypes.h>
#define CFL_PRId64 PRId64
#define CFL_PRId32 PRId32
#endif


/**
 * @brief confluent-kafka-python version, must match that of pyproject.toml.
 *
 * Hex version representation:
 *  0xMMmmRRPP
 *  MM=major, mm=minor, RR=revision, PP=patchlevel (not used)
 */
#define CFL_VERSION     0x02060100
#define CFL_VERSION_STR "2.6.1"

/**
 * Minimum required librdkafka version. This is checked both during
 * build-time (just below) and runtime (see confluent_kafka.c).
 * Make sure to keep the MIN_RD_KAFKA_VERSION, MIN_VER_ERRSTR and #error
 * defines and strings in sync.
 */
#define MIN_RD_KAFKA_VERSION 0x020601ff

#ifdef __APPLE__
#define MIN_VER_ERRSTR "confluent-kafka-python requires librdkafka v2.6.1 or later. Install the latest version of librdkafka from Homebrew by running `brew install librdkafka` or `brew upgrade librdkafka`"
#else
#define MIN_VER_ERRSTR "confluent-kafka-python requires librdkafka v2.6.1 or later. Install the latest version of librdkafka from the Confluent repositories, see http://docs.confluent.io/current/installation.html"
#endif

#if RD_KAFKA_VERSION < MIN_RD_KAFKA_VERSION
#ifdef __APPLE__
#error "confluent-kafka-python requires librdkafka v2.6.1 or later. Install the latest version of librdkafka from Homebrew by running `brew install librdkafka` or `brew upgrade librdkafka`"
#else
#error "confluent-kafka-python requires librdkafka v2.6.1 or later. Install the latest version of librdkafka from the Confluent repositories, see http://docs.confluent.io/current/installation.html"
#endif
#endif


#if PY_MAJOR_VERSION >= 3
#define PY3
#include <bytesobject.h>

 #if PY_MINOR_VERSION >= 7
  #define WITH_PY_TSS
 #endif
#endif

/**
 * Avoid unused function warnings
 */
#if _WIN32
#define CFL_UNUSED
#define CFL_INLINE __inline
#else
#define CFL_UNUSED __attribute__((unused))
#define CFL_INLINE __inline
#endif

/**
 * librdkafka feature detection
 */
#ifdef RD_KAFKA_V_TIMESTAMP
#define HAVE_PRODUCEV  1 /* rd_kafka_producev() */
#endif



/****************************************************************************
 *
 *
 * Python 2 & 3 portability
 *
 * Binary data (we call it cfl_PyBin):
 *   Python 2: string
 *   Python 3: bytes
 *
 * Unicode Strings (we call it cfl_PyUnistr):
 *   Python 2: unicode
 *   Python 3: strings
 *
 ****************************************************************************/

#ifdef PY3 /* Python 3 */
/**
 * @brief Binary type, use as cfl_PyBin(_X(A,B)) where _X() is the type-less
 *        suffix of a PyBytes/Str_X() function
*/
#define cfl_PyBin(X)    PyBytes ## X

/**
 * @brief Unicode type, same usage as PyBin()
 */
#define cfl_PyUnistr(X) PyUnicode ## X

/**
 * @returns Unicode Python object as char * in UTF-8 encoding
 * @param uobjp might be set to NULL or a new object reference (depending
 *              on Python version) which needs to be cleaned up with
 *              Py_XDECREF() after finished use of the returned string.
 */
static __inline const char *
cfl_PyUnistr_AsUTF8 (PyObject *o, PyObject **uobjp) {
        *uobjp = NULL; /* No intermediary object needed in Py3 */
        return PyUnicode_AsUTF8(o);
}

/**
 * @returns Unicode Python string object
 */
#define cfl_PyObject_Unistr(X)  PyObject_Str(X)

#else /* Python 2 */

/* See comments above */
#define cfl_PyBin(X)    PyString ## X
#define cfl_PyUnistr(X) PyUnicode ## X

/**
 * @returns NULL if object \p can't be represented as UTF8, else a temporary
 *          char string with a lifetime equal of \p o and \p uobjp
 */
static __inline const char *
cfl_PyUnistr_AsUTF8 (PyObject *o, PyObject **uobjp) {
        if (!PyUnicode_Check(o)) {
                PyObject *uo;
                if (!(uo = PyUnicode_FromObject(o))) {
                        *uobjp = NULL;
                        return NULL;
                }
                /*UTF8 intermediary object on Py2*/
                *uobjp = PyUnicode_AsUTF8String(o);
                Py_DECREF(uo);
        } else {
                /*UTF8 intermediary object on Py2*/
                *uobjp = PyUnicode_AsUTF8String(o);
        }
        if (!*uobjp)
                return NULL;
        return PyBytes_AsString(*uobjp);
}
#define cfl_PyObject_Unistr(X) PyObject_Unicode(X)
#endif


/****************************************************************************
 *
 *
 * KafkaError
 *
 *
 *
 *
 ****************************************************************************/
extern PyObject *KafkaException;

PyObject *KafkaError_new0 (rd_kafka_resp_err_t err, const char *fmt, ...);
PyObject *KafkaError_new_or_None (rd_kafka_resp_err_t err, const char *str);
PyObject *KafkaError_new_from_error_destroy (rd_kafka_error_t *error);

/**
 * @brief Raise an exception using KafkaError.
 * \p err and and \p ... (string representation of error) is set on the returned
 * KafkaError object.
 */
#define cfl_PyErr_Format(err,...) do {					\
		PyObject *_eo = KafkaError_new0(err, __VA_ARGS__);	\
		PyErr_SetObject(KafkaException, _eo);			\
	} while (0)

/**
 * @brief Create a Python exception from an rd_kafka_error_t *
 *        and destroy it the C object when done.
 */
#define cfl_PyErr_from_error_destroy(error) do {                        \
		PyObject *_eo = KafkaError_new_from_error_destroy(error); \
		PyErr_SetObject(KafkaException, _eo);			\
	} while (0)


/****************************************************************************
 *
 *
 * Common instance handle for both Producer and Consumer
 *
 *
 *
 *
 ****************************************************************************/
typedef struct {
	PyObject_HEAD
	rd_kafka_t *rk;
	PyObject *error_cb;
	PyObject *throttle_cb;
	PyObject *stats_cb;
        int initiated;

        /* Thread-Local-Storage key */
#ifdef WITH_PY_TSS
        Py_tss_t tlskey;
#else
        int tlskey;
#endif

        rd_kafka_type_t type; /* Producer or consumer */

        PyObject *logger;
        PyObject *oauth_cb;

	union {
		/**
		 * Producer
		 */
		struct {
			PyObject *default_dr_cb;
                        int dr_only_error; /**< delivery.report.only.error */
		} Producer;

		/**
		 * Consumer
		 */
		struct {
			int rebalance_assigned;  /* Rebalance: Callback performed assign() call.*/
			int rebalance_incremental_assigned; /* Rebalance: Callback performed incremental_assign() call.*/
			int rebalance_incremental_unassigned; /* Rebalance: Callback performed incremental_unassign() call.*/
			PyObject *on_assign;     /* Rebalance: on_assign callback */
			PyObject *on_revoke;     /* Rebalance: on_revoke callback */
			PyObject *on_lost;     /* Rebalance: on_lost callback */
			PyObject *on_commit;     /* Commit callback */
			rd_kafka_queue_t *rkqu;  /* Consumer queue */

		} Consumer;
	} u;
} Handle;


void Handle_clear (Handle *h);
int  Handle_traverse (Handle *h, visitproc visit, void *arg);


/**
 * @brief Current thread's state for "blocking" calls to librdkafka.
 */
typedef struct {
	PyThreadState *thread_state;
	int crashed;   /* Callback crashed */
} CallState;

/**
 * @brief Initialiase a CallState and unlock the GIL prior to a
 *        possibly blocking external call.
 */
void CallState_begin (Handle *h, CallState *cs);
/**
 * @brief Relock the GIL after external call is done, remove TLS state.
 * @returns 0 if a Python signal was raised or a callback crashed, else 1.
 */
int CallState_end (Handle *h, CallState *cs);

/**
 * @brief Get the current thread's CallState and re-locks the GIL.
 */
CallState *CallState_get (Handle *h);
/**
 * @brief Un-locks the GIL to resume blocking external call.
 */
void CallState_resume (CallState *cs);

/**
 * @brief Indicate that call crashed.
 */
void CallState_crash (CallState *cs);


/**
 * @brief Python 3 renamed the internal PyInt type to PyLong, but the
 *        type is still exposed as 'int' in Python.
 *        We use the (cfl_)PyInt name for both Python 2 and 3 to mean an int,
 *        assuming it will be at least 31 bits+signed on all platforms.
 */
#ifdef PY3
#define cfl_PyInt_Check(o) PyLong_Check(o)
#define cfl_PyInt_AsInt(o) (int)PyLong_AsLong(o)
#define cfl_PyInt_FromInt(v) PyLong_FromLong(v)
#else
#define cfl_PyInt_Check(o) PyInt_Check(o)
#define cfl_PyInt_AsInt(o) (int)PyInt_AsLong(o)
#define cfl_PyInt_FromInt(v) PyInt_FromLong(v)
#endif

#define cfl_PyLong_Check(o) PyLong_Check(o)
#define cfl_PyLong_AsLong(o) (int)PyLong_AsLong(o)
#define cfl_PyLong_FromLong(v) PyLong_FromLong(v)

PyObject *cfl_PyObject_lookup (const char *modulename, const char *typename);

void cfl_PyDict_SetString (PyObject *dict, const char *name, const char *val);
void cfl_PyDict_SetInt (PyObject *dict, const char *name, int val);
void cfl_PyDict_SetLong (PyObject *dict, const char *name, long val);
int cfl_PyObject_SetString (PyObject *o, const char *name, const char *val);
int cfl_PyObject_SetInt (PyObject *o, const char *name, int val);
int cfl_PyObject_GetAttr (PyObject *object, const char *attr_name,
                          PyObject **valp, const PyTypeObject *py_type,
                          int required, int allow_None);
int cfl_PyObject_GetInt (PyObject *object, const char *attr_name, int *valp,
                         int defval, int required);
int cfl_PyObject_GetString (PyObject *object, const char *attr_name,
                            char **valp, const char *defval, int required,
                            int allow_None);
int cfl_PyBool_get (PyObject *object, const char *name, int *valp);

PyObject *cfl_int32_array_to_py_list (const int32_t *arr, size_t cnt);

/****************************************************************************
 *
 *
 * TopicPartition
 *
 *
 *
 *
 ****************************************************************************/
typedef struct {
	PyObject_HEAD
	char *topic;
	int   partition;
	int64_t offset;
	int32_t leader_epoch;
	char *metadata;
	PyObject *error;
} TopicPartition;

extern PyTypeObject TopicPartitionType;


/****************************************************************************
 *
 *
 * Common
 *
 *
 *
 *
 ****************************************************************************/
#define PY_RD_KAFKA_ADMIN  100 /* There is no Admin client type in librdkafka,
                                * so we use the producer type for now,
                                * but we need to differentiate between a
                                * proper producer and an admin client in the
                                * python code in some places. */
rd_kafka_conf_t *common_conf_setup (rd_kafka_type_t ktype,
				    Handle *h,
				    PyObject *args,
				    PyObject *kwargs);
PyObject *c_part_to_py(const rd_kafka_topic_partition_t *c_part);
PyObject *c_parts_to_py (const rd_kafka_topic_partition_list_t *c_parts);
PyObject *c_Node_to_py(const rd_kafka_Node_t *c_node);
PyObject *c_Uuid_to_py(const rd_kafka_Uuid_t *c_uuid);
rd_kafka_topic_partition_list_t *py_to_c_parts (PyObject *plist);
PyObject *c_topic_partition_result_to_py_dict(
    const rd_kafka_topic_partition_result_t **partition_results,
    size_t cnt);
PyObject *list_topics (Handle *self, PyObject *args, PyObject *kwargs);
PyObject *list_groups (Handle *self, PyObject *args, PyObject *kwargs);
PyObject *set_sasl_credentials(Handle *self, PyObject *args, PyObject *kwargs);


extern const char list_topics_doc[];
extern const char list_groups_doc[];
extern const char set_sasl_credentials_doc[];


#ifdef RD_KAFKA_V_HEADERS
rd_kafka_headers_t *py_headers_to_c (PyObject *hdrs);
PyObject *c_headers_to_py (rd_kafka_headers_t *headers);
#endif

PyObject *c_cgmd_to_py (const rd_kafka_consumer_group_metadata_t *cgmd);
rd_kafka_consumer_group_metadata_t *py_to_c_cgmd (PyObject *obj);


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
 * @brief confluent_kafka.Message object
 */
typedef struct {
	PyObject_HEAD
	PyObject *topic;
	PyObject *value;
	PyObject *key;
	PyObject *headers;
#ifdef RD_KAFKA_V_HEADERS
	rd_kafka_headers_t *c_headers;
#endif
	PyObject *error;
	int32_t partition;
	int64_t offset;
	int32_t leader_epoch;
	int64_t timestamp;
	rd_kafka_timestamp_type_t tstype;
        int64_t latency;  /**< Producer: time it took to produce message */
} Message;

extern PyTypeObject MessageType;

PyObject *Message_new0 (const Handle *handle, const rd_kafka_message_t *rkm);
PyObject *Message_error (Message *self, PyObject *ignore);


/****************************************************************************
 *
 *
 * Producer
 *
 *
 *
 *
 ****************************************************************************/

extern PyTypeObject ProducerType;

static CFL_UNUSED CFL_INLINE int cfl_timeout_ms(double tmout) {
        if (tmout < 0)
                return -1;
        return (int)(tmout * 1000);
}
/****************************************************************************
 *
 *
 * Consumer
 *
 *
 *
 *
 ****************************************************************************/

extern PyTypeObject ConsumerType;


/****************************************************************************
 *
 *
 * AdminClient types
 *
 *
 *
 *
 ****************************************************************************/

typedef struct {
        PyObject_HEAD
        char *topic;
        int   num_partitions;
        int   replication_factor;
        PyObject *replica_assignment;  /**< list<int> */
        PyObject *config;              /**< dict<str,str> */
} NewTopic;

extern PyTypeObject NewTopicType;


typedef struct {
        PyObject_HEAD
		rd_kafka_Uuid_t *cUuid;
} Uuid;

extern PyTypeObject UuidType;


typedef struct {
        PyObject_HEAD
        char *topic;
        int   new_total_count;
        PyObject *replica_assignment;
} NewPartitions;

extern PyTypeObject NewPartitionsType;

int AdminTypes_Ready (void);
void AdminTypes_AddObjects (PyObject *m);

/****************************************************************************
 *
 *
 * AdminClient
 *
 *
 *
 *
 ****************************************************************************/

extern PyTypeObject AdminType;
