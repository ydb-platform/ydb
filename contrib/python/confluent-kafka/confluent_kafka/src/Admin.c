/**
 * Copyright 2018 Confluent Inc.
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


/****************************************************************************
 *
 *
 * Admin Client API
 *
 *
 ****************************************************************************/




static int Admin_clear (Handle *self) {

        Handle_clear(self);

        return 0;
}

static void Admin_dealloc (Handle *self) {
        PyObject_GC_UnTrack(self);

        if (self->rk) {
                CallState cs;
                CallState_begin(self, &cs);

                rd_kafka_destroy(self->rk);

                CallState_end(self, &cs);
        }

        Admin_clear(self);

        Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Admin_traverse (Handle *self,
                           visitproc visit, void *arg) {
        Handle_traverse(self, visit, arg);

        return 0;
}


/**
 * @name AdminOptions
 *
 *
 */
#define Admin_options_def_int   (-12345)
#define Admin_options_def_float ((float)Admin_options_def_int)
#define Admin_options_def_ptr   (NULL)
#define Admin_options_def_cnt   (0)

struct Admin_options {
        int   validate_only;                            /* needs special bool parsing */
        float request_timeout;                          /* parser: f */
        float operation_timeout;                        /* parser: f */
        int   broker;                                   /* parser: i */
        int require_stable_offsets;                     /* needs special bool parsing */
        int include_authorized_operations;              /* needs special bool parsing */
        rd_kafka_IsolationLevel_t isolation_level;
        rd_kafka_consumer_group_state_t* states;
        int states_cnt;
        rd_kafka_consumer_group_type_t* types;
        int types_cnt;
};

/**@brief "unset" value initializers for Admin_options
 * Make sure this is kept up to date with Admin_options above. */
#define Admin_options_INITIALIZER {              \
                Admin_options_def_int,           \
                Admin_options_def_float,         \
                Admin_options_def_float,         \
                Admin_options_def_int,           \
                Admin_options_def_int,           \
                Admin_options_def_int,           \
                Admin_options_def_int,           \
                Admin_options_def_ptr,           \
                Admin_options_def_cnt,           \
                Admin_options_def_ptr,           \
                Admin_options_def_cnt,           \
        }

#define Admin_options_is_set_int(v) ((v) != Admin_options_def_int)
#define Admin_options_is_set_float(v) Admin_options_is_set_int((int)(v))
#define Admin_options_is_set_ptr(v) ((v) != NULL)


/**
 * @brief Convert Admin_options to rd_kafka_AdminOptions_t.
 *
 * @param forApi is the librdkafka name of the admin API that these options
 *               will be used for, e.g., "CreateTopics".
 * @param future is set as the options opaque.
 *
 * @returns a new C admin options object on success, or NULL on failure in
 *          which case an exception is raised.
 */
static rd_kafka_AdminOptions_t *
Admin_options_to_c (Handle *self, rd_kafka_admin_op_t for_api,
                    const struct Admin_options *options,
                    PyObject *future) {
        rd_kafka_AdminOptions_t *c_options;
        rd_kafka_resp_err_t err;
        rd_kafka_error_t *err_obj = NULL;
        char errstr[512];

        c_options = rd_kafka_AdminOptions_new(self->rk, for_api);
        if (!c_options) {
                PyErr_Format(PyExc_RuntimeError,
                             "This Admin API method "
                             "is unsupported by librdkafka %s",
                             rd_kafka_version_str());
                return NULL;
        }

        rd_kafka_AdminOptions_set_opaque(c_options, (void *)future);

        if (Admin_options_is_set_int(options->validate_only) &&
            (err = rd_kafka_AdminOptions_set_validate_only(
                    c_options, options->validate_only,
                    errstr, sizeof(errstr))))
                goto err;

        if (Admin_options_is_set_float(options->request_timeout) &&
            (err = rd_kafka_AdminOptions_set_request_timeout(
                    c_options, (int)(options->request_timeout * 1000.0f),
                    errstr, sizeof(errstr))))
                goto err;

        if (Admin_options_is_set_float(options->operation_timeout) &&
            (err = rd_kafka_AdminOptions_set_operation_timeout(
                    c_options, (int)(options->operation_timeout * 1000.0f),
                    errstr, sizeof(errstr))))
                goto err;

        if (Admin_options_is_set_int(options->broker) &&
            (err = rd_kafka_AdminOptions_set_broker(
                    c_options, (int32_t)options->broker,
                    errstr, sizeof(errstr))))
                goto err;

        if (Admin_options_is_set_int(options->require_stable_offsets) &&
            (err_obj = rd_kafka_AdminOptions_set_require_stable_offsets(
                    c_options, options->require_stable_offsets))) {
                snprintf(errstr, sizeof(errstr), "%s", rd_kafka_error_string(err_obj));
                goto err;
        }

        if (Admin_options_is_set_int(options->include_authorized_operations) &&
            (err_obj = rd_kafka_AdminOptions_set_include_authorized_operations(
                    c_options, options->include_authorized_operations))) {
                snprintf(errstr, sizeof(errstr), "%s", rd_kafka_error_string(err_obj));
                goto err;
        }

        if (Admin_options_is_set_int((int)options->isolation_level) &&
             (err_obj = rd_kafka_AdminOptions_set_isolation_level(
                     c_options,options->isolation_level))) {
                snprintf(errstr, sizeof(errstr), "%s", rd_kafka_error_string(err_obj));
                goto err;
        }

        if (Admin_options_is_set_ptr(options->states) &&
            (err_obj = rd_kafka_AdminOptions_set_match_consumer_group_states(
                c_options, options->states, options->states_cnt))) {
                snprintf(errstr, sizeof(errstr), "%s", rd_kafka_error_string(err_obj));
                goto err;
        }

        if (Admin_options_is_set_ptr(options->types) &&
            (err_obj = rd_kafka_AdminOptions_set_match_consumer_group_types(
                c_options, options->types, options->types_cnt))) {
                snprintf(errstr, sizeof(errstr), "%s", rd_kafka_error_string(err_obj));
                goto err;
        }

        return c_options;

 err:
        if (c_options) rd_kafka_AdminOptions_destroy(c_options);
        PyErr_Format(PyExc_ValueError, "%s", errstr);
        if(err_obj) {
                rd_kafka_error_destroy(err_obj);
        }
        return NULL;
}


/**
 * @brief Convert py AclBinding to C
 */
static rd_kafka_AclBinding_t *
Admin_py_to_c_AclBinding (const PyObject *py_obj_arg,
                        char *errstr,
                        size_t errstr_size) {
        int restype, resource_pattern_type, operation, permission_type;
        char *resname = NULL, *principal = NULL, *host = NULL;
        rd_kafka_AclBinding_t *ret = NULL;

        PyObject *py_obj = (PyObject *) py_obj_arg;
        if(cfl_PyObject_GetInt(py_obj, "restype_int", &restype, 0, 1)
            && cfl_PyObject_GetString(py_obj, "name", &resname, NULL, 1, 0)
            && cfl_PyObject_GetInt(py_obj, "resource_pattern_type_int", &resource_pattern_type, 0, 1)
            && cfl_PyObject_GetString(py_obj, "principal", &principal, NULL, 1, 0)
            && cfl_PyObject_GetString(py_obj, "host", &host, NULL, 1, 0)
            && cfl_PyObject_GetInt(py_obj, "operation_int", &operation, 0, 1)
            && cfl_PyObject_GetInt(py_obj, "permission_type_int", &permission_type, 0, 1)) {
                    ret = rd_kafka_AclBinding_new(restype, resname, \
                        resource_pattern_type, principal, host, \
                        operation, permission_type, errstr, errstr_size);
        }
        if (resname) free(resname);
        if (principal) free(principal);
        if (host) free(host);
        return ret;
}

/**
 * @brief Convert py AclBindingFilter to C
 */
static rd_kafka_AclBindingFilter_t*
Admin_py_to_c_AclBindingFilter (const PyObject *py_obj_arg,
                        char *errstr,
                        size_t errstr_size) {
        int restype, resource_pattern_type, operation, permission_type;
        char *resname = NULL, *principal = NULL, *host = NULL;
        PyObject *py_obj = (PyObject *) py_obj_arg;
        rd_kafka_AclBindingFilter_t* ret = NULL;

        if(cfl_PyObject_GetInt(py_obj, "restype_int", &restype, 0, 1)
            && cfl_PyObject_GetString(py_obj, "name", &resname, NULL, 1, 1)
            && cfl_PyObject_GetInt(py_obj, "resource_pattern_type_int", &resource_pattern_type, 0, 1)
            && cfl_PyObject_GetString(py_obj, "principal", &principal, NULL, 1, 1)
            && cfl_PyObject_GetString(py_obj, "host", &host, NULL, 1, 1)
            && cfl_PyObject_GetInt(py_obj, "operation_int", &operation, 0, 1)
            && cfl_PyObject_GetInt(py_obj, "permission_type_int", &permission_type, 0, 1)) {
                    ret = rd_kafka_AclBindingFilter_new(restype, resname, \
                        resource_pattern_type, principal, host, \
                        operation, permission_type, errstr, errstr_size);
        }
        if (resname) free(resname);
        if (principal) free(principal);
        if (host) free(host);
        return ret;
}

/**
 * @brief Translate Python list(list(int)) replica assignments and set
 *        on the specified generic C object using a setter based on
 *        forApi.
 *
 * @returns 1 on success or 0 on error in which case an exception is raised.
 */
static int Admin_set_replica_assignment (const char *forApi, void *c_obj,
                                         PyObject *ra, int
                                         min_count, int max_count,
                                         const char *err_count_desc) {
        int pi;

        if (!PyList_Check(ra) ||
            (int)PyList_Size(ra) < min_count ||
            (int)PyList_Size(ra) > max_count) {
                PyErr_Format(PyExc_ValueError,
                             "replica_assignment must be "
                             "a list of int lists with an "
                             "outer size of %s", err_count_desc);
                return 0;
        }

        for (pi = 0 ; pi < (int)PyList_Size(ra) ; pi++) {
                size_t ri;
                PyObject *replicas = PyList_GET_ITEM(ra, pi);
                rd_kafka_resp_err_t err;
                int32_t *c_replicas;
                size_t replica_cnt;
                char errstr[512];

                if (!PyList_Check(replicas) ||
                    (replica_cnt = (size_t)PyList_Size(replicas)) < 1) {
                        PyErr_Format(
                                PyExc_ValueError,
                                "replica_assignment must be "
                                "a list of int lists with an "
                                "outer size of %s", err_count_desc);
                        return 0;
                }

                c_replicas = malloc(sizeof(*c_replicas) *
                                    replica_cnt);

                for (ri = 0 ; ri < replica_cnt ; ri++) {
                        PyObject *replica =
                                PyList_GET_ITEM(replicas, ri);

                        if (!cfl_PyInt_Check(replica)) {
                                PyErr_Format(
                                        PyExc_ValueError,
                                        "replica_assignment must be "
                                        "a list of int lists with an "
                                        "outer size of %s", err_count_desc);
                                free(c_replicas);
                                return 0;
                        }

                        c_replicas[ri] = (int32_t)cfl_PyInt_AsInt(replica);

                }


                if (!strcmp(forApi, "CreateTopics"))
                        err = rd_kafka_NewTopic_set_replica_assignment(
                                (rd_kafka_NewTopic_t *)c_obj, (int32_t)pi,
                                c_replicas, replica_cnt,
                                errstr, sizeof(errstr));
                else if (!strcmp(forApi, "CreatePartitions"))
                        err = rd_kafka_NewPartitions_set_replica_assignment(
                                (rd_kafka_NewPartitions_t *)c_obj, (int32_t)pi,
                                c_replicas, replica_cnt,
                                errstr, sizeof(errstr));
                else {
                        /* Should never be reached */
                        err = RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE;
                        snprintf(errstr, sizeof(errstr),
                                 "Unsupported forApi %s", forApi);
                }

                free(c_replicas);

                if (err) {
                        PyErr_SetString(
                                PyExc_ValueError, errstr);
                        return 0;
                }
        }

        return 1;
}


static int
Admin_incremental_config_to_c(PyObject *incremental_configs,
                              rd_kafka_ConfigResource_t *c_obj,
                              PyObject *ConfigEntry_type){
        int config_entry_count = 0;
        Py_ssize_t i = 0;
        char *name = NULL;
        char *value = NULL;
        PyObject *incremental_operation = NULL;

        if (!PyList_Check(incremental_configs)) {
                PyErr_Format(PyExc_TypeError,
                             "expected list of ConfigEntry "
                             "in incremental_configs field");
                goto err;
        }

        if ((config_entry_count = (int)PyList_Size(incremental_configs)) < 1) {
                PyErr_Format(PyExc_ValueError,
                             "expected non-empty list of ConfigEntry "
                             "to alter incrementally "
                             "in incremental_configs field");
                goto err;
        }

        for (i = 0; i < config_entry_count; i++) {
                PyObject *config_entry;
                int incremental_operation_value, r;
                rd_kafka_error_t *error;

                config_entry = PyList_GET_ITEM(incremental_configs, i);

                r = PyObject_IsInstance(config_entry, ConfigEntry_type);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_Format(PyExc_TypeError,
                                     "expected ConfigEntry type "
                                     "in incremental_configs field, "
                                     "index %zd", i);
                        goto err;
                }

                if (!cfl_PyObject_GetAttr(config_entry, "incremental_operation",
                                          &incremental_operation, NULL, 1, 0))
                        goto err;

                if (!cfl_PyObject_GetInt(incremental_operation, "value",
                    &incremental_operation_value, -1, 1))
                        goto err;

                if (!cfl_PyObject_GetString(config_entry, "name", &name, NULL, 1, 0))
                        goto err;

                if (incremental_operation_value != RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE &&
                    !cfl_PyObject_GetString(config_entry, "value", &value, NULL, 1, 0))
                        goto err;

                error = rd_kafka_ConfigResource_add_incremental_config(
                        c_obj,
                        name,
                        (rd_kafka_AlterConfigOpType_t) incremental_operation_value,
                        value);
                if (error) {
                        PyErr_Format(PyExc_ValueError,
                                "setting config entry \"%s\", "
                                "index %zd, failed: %s",
                                name, i, rd_kafka_error_string(error));
                        rd_kafka_error_destroy(error);
                        goto err;
                }

                Py_DECREF(incremental_operation);
                free(name);
                if (value)
                        free(value);
                name = NULL;
                value = NULL;
                incremental_operation = NULL;
        }
        return 1;
err:
        Py_XDECREF(incremental_operation);
        if (name)
                free(name);
        if (value)
                free(value);
        return 0;
}

/**
 * @brief Translate a dict to ConfigResource set_config() calls,
 *        or to NewTopic_add_config() calls.
 *
 *
 * @returns 1 on success or 0 if an exception was raised.
 */
static int
Admin_config_dict_to_c (void *c_obj, PyObject *dict, const char *op_name) {
        Py_ssize_t pos = 0;
        PyObject *ko, *vo;

        while (PyDict_Next(dict, &pos, &ko, &vo)) {
                PyObject *ks, *ks8;
                PyObject *vs = NULL, *vs8 = NULL;
                const char *k;
                const char *v;
                rd_kafka_resp_err_t err;

                if (!(ks = cfl_PyObject_Unistr(ko))) {
                        PyErr_Format(PyExc_ValueError,
                                     "expected %s config name to be unicode "
                                     "string", op_name);
                        return 0;
                }

                k = cfl_PyUnistr_AsUTF8(ks, &ks8);

                if (!(vs = cfl_PyObject_Unistr(vo)) ||
                    !(v = cfl_PyUnistr_AsUTF8(vs, &vs8))) {
                        PyErr_Format(PyExc_ValueError,
                                     "expect %s config value for %s "
                                     "to be unicode string",
                                     op_name, k);
                        Py_XDECREF(vs);
                        Py_XDECREF(vs8);
                        Py_DECREF(ks);
                        Py_XDECREF(ks8);
                        return 0;
                }

                if (!strcmp(op_name, "set_config"))
                        err = rd_kafka_ConfigResource_set_config(
                                (rd_kafka_ConfigResource_t *)c_obj,
                                k, v);
                else if (!strcmp(op_name, "newtopic_set_config"))
                        err = rd_kafka_NewTopic_set_config(
                                (rd_kafka_NewTopic_t *)c_obj, k, v);
                else
                        err = RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;

                if (err) {
                        PyErr_Format(PyExc_ValueError,
                                     "%s config %s failed: %s",
                                     op_name, k, rd_kafka_err2str(err));
                        Py_XDECREF(vs);
                        Py_XDECREF(vs8);
                        Py_DECREF(ks);
                        Py_XDECREF(ks8);
                        return 0;
                }

                Py_XDECREF(vs);
                Py_XDECREF(vs8);
                Py_DECREF(ks);
                Py_XDECREF(ks8);
        }

        return 1;
}


/**
 * @brief create_topics
 */
static PyObject *Admin_create_topics (Handle *self, PyObject *args,
                                      PyObject *kwargs) {
        PyObject *topics = NULL, *future, *validate_only_obj = NULL;
        static char *kws[] = { "topics",
                               "future",
                               /* options */
                               "validate_only",
                               "request_timeout",
                               "operation_timeout",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        int tcnt;
        int i;
        int topic_partition_count;
        rd_kafka_NewTopic_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* topics is a list of NewTopic objects. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Off", kws,
                                         &topics, &future,
                                         &validate_only_obj,
                                         &options.request_timeout,
                                         &options.operation_timeout))
                return NULL;

        if (!PyList_Check(topics) || (tcnt = (int)PyList_Size(topics)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of NewTopic objects");
                return NULL;
        }

        if (validate_only_obj &&
            !cfl_PyBool_get(validate_only_obj, "validate_only",
                            &options.validate_only))
                return NULL;

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_CREATETOPICS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of NewTopics and convert to corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * tcnt);

        for (i = 0 ; i < tcnt ; i++) {
                NewTopic *newt = (NewTopic *)PyList_GET_ITEM(topics, i);
                char errstr[512];
                int r;

                r = PyObject_IsInstance((PyObject *)newt,
                                        (PyObject *)&NewTopicType);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of NewTopic objects");
                        goto err;
                }

                c_objs[i] = rd_kafka_NewTopic_new(newt->topic,
                                                   newt->num_partitions,
                                                   newt->replication_factor,
                                                   errstr, sizeof(errstr));
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_ValueError,
                                     "Invalid NewTopic(%s): %s",
                                     newt->topic, errstr);
                        goto err;
                }

                if (newt->replica_assignment) {
                        if (newt->replication_factor != -1) {
                                PyErr_SetString(PyExc_ValueError,
                                                "replication_factor and "
                                                "replica_assignment are "
                                                "mutually exclusive");
                                i++;
                                goto err;
                        }

                        if (newt->num_partitions == -1) {
                                topic_partition_count = PyList_Size(newt->replica_assignment);
                        } else {
                                topic_partition_count = newt->num_partitions;
                        }
                        if (!Admin_set_replica_assignment(
                                    "CreateTopics", (void *)c_objs[i],
                                    newt->replica_assignment,
                                    topic_partition_count,
                                    topic_partition_count,
                                    "num_partitions")) {
                                i++;
                                goto err;
                        }
                }

                if (newt->config) {
                        if (!Admin_config_dict_to_c((void *)c_objs[i],
                                                    newt->config,
                                                    "newtopic_set_config")) {
                                i++;
                                goto err;
                        }
                }
        }


        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call CreateTopics.
         *
         * We need to set up a CallState and release GIL here since
         * the background_event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_CreateTopics(self->rk, c_objs, tcnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_NewTopic_destroy_array(c_objs, tcnt);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        Py_RETURN_NONE;

 err:
        rd_kafka_NewTopic_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}


/**
 * @brief delete_topics
 */
static PyObject *Admin_delete_topics (Handle *self, PyObject *args,
                                      PyObject *kwargs) {
        PyObject *topics = NULL, *future;
        static char *kws[] = { "topics",
                               "future",
                               /* options */
                               "request_timeout",
                               "operation_timeout",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        int tcnt;
        int i;
        rd_kafka_DeleteTopic_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* topics is a list of strings. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!O|ff", kws,
                                         (PyObject *)&PyList_Type, &topics,
                                         &future,
                                         &options.request_timeout,
                                         &options.operation_timeout))
                return NULL;

        if (!PyList_Check(topics) || (tcnt = (int)PyList_Size(topics)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of topic strings");
                return NULL;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DELETETOPICS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* options_to_c() sets opaque to the future object, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of strings and convert to corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * tcnt);

        for (i = 0 ; i < tcnt ; i++) {
                PyObject *topic = PyList_GET_ITEM(topics, i);
                PyObject *utopic;
                PyObject *uotopic = NULL;

                if (topic == Py_None ||
                    !(utopic = cfl_PyObject_Unistr(topic))) {
                        PyErr_Format(PyExc_ValueError,
                                     "Expected list of topic strings, "
                                     "not %s",
                                     ((PyTypeObject *)PyObject_Type(topic))->
                                     tp_name);
                        goto err;
                }

                c_objs[i] = rd_kafka_DeleteTopic_new(
                        cfl_PyUnistr_AsUTF8(utopic, &uotopic));

                Py_XDECREF(utopic);
                Py_XDECREF(uotopic);
        }


        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DeleteTopics.
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DeleteTopics(self->rk, c_objs, tcnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_DeleteTopic_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        Py_RETURN_NONE;

 err:
        rd_kafka_DeleteTopic_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}


/**
 * @brief create_partitions
 */
static PyObject *Admin_create_partitions (Handle *self, PyObject *args,
                                          PyObject *kwargs) {
        PyObject *topics = NULL, *future, *validate_only_obj = NULL;
        static char *kws[] = { "topics",
                               "future",
                               /* options */
                               "validate_only",
                               "request_timeout",
                               "operation_timeout",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        int tcnt;
        int i;
        rd_kafka_NewPartitions_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* topics is a list of NewPartitions_t objects. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Off", kws,
                                         &topics, &future,
                                         &validate_only_obj,
                                         &options.request_timeout,
                                         &options.operation_timeout))
                return NULL;

        if (!PyList_Check(topics) || (tcnt = (int)PyList_Size(topics)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of "
                                "NewPartitions objects");
                return NULL;
        }

        if (validate_only_obj &&
            !cfl_PyBool_get(validate_only_obj, "validate_only",
                            &options.validate_only))
                return NULL;

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_CREATEPARTITIONS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of NewPartitions and convert to corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * tcnt);

        for (i = 0 ; i < tcnt ; i++) {
                NewPartitions *newp = (NewPartitions *)PyList_GET_ITEM(topics,
                                                                       i);
                char errstr[512];
                int r;

                r = PyObject_IsInstance((PyObject *)newp,
                                        (PyObject *)&NewPartitionsType);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of "
                                        "NewPartitions objects");
                        goto err;
                }

                c_objs[i] = rd_kafka_NewPartitions_new(newp->topic,
                                                       newp->new_total_count,
                                                       errstr, sizeof(errstr));
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_ValueError,
                                     "Invalid NewPartitions(%s): %s",
                                     newp->topic, errstr);
                        goto err;
                }

                if (newp->replica_assignment &&
                    !Admin_set_replica_assignment(
                            "CreatePartitions", (void *)c_objs[i],
                            newp->replica_assignment,
                            1, newp->new_total_count,
                            "new_total_count - "
                            "existing partition count")) {
                        i++;
                        goto err; /* Exception raised by set_..() */
                }
        }


        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call CreatePartitions
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_CreatePartitions(self->rk, c_objs, tcnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_NewPartitions_destroy_array(c_objs, tcnt);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        Py_RETURN_NONE;

 err:
        rd_kafka_NewPartitions_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}


/**
 * @brief describe_configs
 */
static PyObject *Admin_describe_configs (Handle *self, PyObject *args,
                                         PyObject *kwargs) {
        PyObject *resources, *future;
        static char *kws[] = { "resources",
                               "future",
                               /* options */
                               "request_timeout",
                               "broker",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        PyObject *ConfigResource_type;
        int cnt, i;
        rd_kafka_ConfigResource_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* resources is a list of ConfigResource objects. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|fi", kws,
                                         &resources, &future,
                                         &options.request_timeout,
                                         &options.broker))
                return NULL;

        if (!PyList_Check(resources) ||
            (cnt = (int)PyList_Size(resources)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of ConfigResource "
                                "objects");
                return NULL;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* Look up the ConfigResource class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        ConfigResource_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "ConfigResource");
        if (!ConfigResource_type) {
                rd_kafka_AdminOptions_destroy(c_options);
                return NULL; /* Exception raised by lookup() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of ConfigResources and convert to
         * corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * cnt);

        for (i = 0 ; i < cnt ; i++) {
                PyObject *res = PyList_GET_ITEM(resources, i);
                int r;
                int restype;
                char *resname;

                r = PyObject_IsInstance(res, ConfigResource_type);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of "
                                        "ConfigResource objects");
                        goto err;
                }

                if (!cfl_PyObject_GetInt(res, "restype_int", &restype, 0, 0))
                        goto err;

                if (!cfl_PyObject_GetString(res, "name", &resname, NULL, 0, 0))
                        goto err;

                c_objs[i] = rd_kafka_ConfigResource_new(
                        (rd_kafka_ResourceType_t)restype, resname);
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_ValueError,
                                     "Invalid ConfigResource(%d,%s)",
                                     restype, resname);
                        free(resname);
                        goto err;
                }
                free(resname);
        }


        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DescribeConfigs
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DescribeConfigs(self->rk, c_objs, cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_ConfigResource_destroy_array(c_objs, cnt);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        Py_DECREF(ConfigResource_type); /* from lookup() */

        Py_RETURN_NONE;

 err:
        rd_kafka_ConfigResource_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        Py_DECREF(ConfigResource_type); /* from lookup() */
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}

static PyObject *Admin_incremental_alter_configs(Handle *self,PyObject *args,PyObject *kwargs) {
        PyObject *resources, *future;
        PyObject *validate_only_obj = NULL;
        static char *kws[] = { "resources",
                               "future",
                               /* options */
                               "validate_only",
                               "request_timeout",
                               "broker",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        PyObject *ConfigResource_type, *ConfigEntry_type;
        int cnt, i;
        rd_kafka_ConfigResource_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* resources is a list of ConfigResource objects. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Ofi", kws,
                                         &resources, &future,
                                         &validate_only_obj,
                                         &options.request_timeout,
                                         &options.broker))
                return NULL;

        if (!PyList_Check(resources) ||
            (cnt = (int)PyList_Size(resources)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of ConfigResource "
                                "objects");
                return NULL;
        }

        if (validate_only_obj &&
            !cfl_PyBool_get(validate_only_obj, "validate_only",
                            &options.validate_only))
                return NULL;
        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_INCREMENTALALTERCONFIGS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* Look up the ConfigResource class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        ConfigResource_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "ConfigResource");
        if (!ConfigResource_type) {
                rd_kafka_AdminOptions_destroy(c_options);
                return NULL; /* Exception raised by find() */
        }

        ConfigEntry_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "ConfigEntry");
        if (!ConfigEntry_type) {
                Py_DECREF(ConfigResource_type);
                rd_kafka_AdminOptions_destroy(c_options);
                return NULL; /* Exception raised by find() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of ConfigResources and convert to
         * corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * cnt);

        for (i = 0 ; i < cnt ; i++) {
                PyObject *res = PyList_GET_ITEM(resources, i);
                int r;
                int restype;
                char *resname;
                PyObject *incremental_configs;

                r = PyObject_IsInstance(res, ConfigResource_type);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of "
                                        "ConfigResource objects");
                        goto err;
                }

                if (!cfl_PyObject_GetInt(res, "restype_int", &restype, 0, 0))
                        goto err;

                if (!cfl_PyObject_GetString(res, "name", &resname, NULL, 0, 0))
                        goto err;

                c_objs[i] = rd_kafka_ConfigResource_new(
                        (rd_kafka_ResourceType_t)restype, resname);
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_ValueError,
                                     "Invalid ConfigResource(%d,%s)",
                                     restype, resname);
                        free(resname);
                        goto err;
                }
                free(resname);
                /*
                 * Translate and apply config entries in the various dicts.
                 */
                if (!cfl_PyObject_GetAttr(res, "incremental_configs",
                                          &incremental_configs,
                                          &PyList_Type, 1, 0)) {
                        i++;
                        goto err;
                }
                if (!Admin_incremental_config_to_c(incremental_configs,
                                                   c_objs[i],
                                                   ConfigEntry_type)) {
                        Py_DECREF(incremental_configs);
                        i++;
                        goto err;
                }
                Py_DECREF(incremental_configs);
        }


        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call AlterConfigs
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_IncrementalAlterConfigs(self->rk, c_objs, cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_ConfigResource_destroy_array(c_objs, cnt);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        Py_DECREF(ConfigResource_type); /* from lookup() */
        Py_DECREF(ConfigEntry_type); /* from lookup() */

        Py_RETURN_NONE;

 err:
        rd_kafka_ConfigResource_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        Py_DECREF(ConfigResource_type); /* from lookup() */
        Py_DECREF(ConfigEntry_type); /* from lookup() */
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}


/**
 * @brief alter_configs
 */
static PyObject *Admin_alter_configs (Handle *self, PyObject *args,
                                         PyObject *kwargs) {
        PyObject *resources, *future;
        PyObject *validate_only_obj = NULL;
        static char *kws[] = { "resources",
                               "future",
                               /* options */
                               "validate_only",
                               "request_timeout",
                               "broker",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        PyObject *ConfigResource_type;
        int cnt, i;
        rd_kafka_ConfigResource_t **c_objs;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* resources is a list of ConfigResource objects. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Ofi", kws,
                                         &resources, &future,
                                         &validate_only_obj,
                                         &options.request_timeout,
                                         &options.broker))
                return NULL;

        if (!PyList_Check(resources) ||
            (cnt = (int)PyList_Size(resources)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of ConfigResource "
                                "objects");
                return NULL;
        }

        if (validate_only_obj &&
            !cfl_PyBool_get(validate_only_obj, "validate_only",
                            &options.validate_only))
                return NULL;

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_ALTERCONFIGS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */

        /* Look up the ConfigResource class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        ConfigResource_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "ConfigResource");
        if (!ConfigResource_type) {
                rd_kafka_AdminOptions_destroy(c_options);
                return NULL; /* Exception raised by find() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of ConfigResources and convert to
         * corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * cnt);

        for (i = 0 ; i < cnt ; i++) {
                PyObject *res = PyList_GET_ITEM(resources, i);
                int r;
                int restype;
                char *resname;
                PyObject *dict;

                r = PyObject_IsInstance(res, ConfigResource_type);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of "
                                        "ConfigResource objects");
                        goto err;
                }

                if (!cfl_PyObject_GetInt(res, "restype_int", &restype, 0, 0))
                        goto err;

                if (!cfl_PyObject_GetString(res, "name", &resname, NULL, 0, 0))
                        goto err;

                c_objs[i] = rd_kafka_ConfigResource_new(
                        (rd_kafka_ResourceType_t)restype, resname);
                if (!c_objs[i]) {
                        PyErr_Format(PyExc_ValueError,
                                     "Invalid ConfigResource(%d,%s)",
                                     restype, resname);
                        free(resname);
                        goto err;
                }
                free(resname);

                /*
                 * Translate and apply config entries in the various dicts.
                 */
                if (!cfl_PyObject_GetAttr(res, "set_config_dict", &dict,
                                          &PyDict_Type, 1, 0)) {
                        i++;
                        goto err;
                }
                if (!Admin_config_dict_to_c(c_objs[i], dict, "set_config")) {
                        Py_DECREF(dict);
                        i++;
                        goto err;
                }
                Py_DECREF(dict);
        }


        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call AlterConfigs
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_AlterConfigs(self->rk, c_objs, cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_ConfigResource_destroy_array(c_objs, cnt);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        Py_DECREF(ConfigResource_type); /* from lookup() */

        Py_RETURN_NONE;

 err:
        rd_kafka_ConfigResource_destroy_array(c_objs, i);
        rd_kafka_AdminOptions_destroy(c_options);
        free(c_objs);
        Py_DECREF(ConfigResource_type); /* from lookup() */
        Py_DECREF(future); /* from options_to_c() */

        return NULL;
}


/**
 * @brief create_acls
 */
static PyObject *Admin_create_acls (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *acls_list, *future;
        int cnt, i = 0;
        struct Admin_options options = Admin_options_INITIALIZER;
        PyObject *AclBinding_type = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_AclBinding_t **c_objs = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        char errstr[512];

        static char *kws[] = {"acls",
                             "future",
                             /* options */
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &acls_list,
                                         &future,
                                         &options.request_timeout))
                goto err;

        if (!PyList_Check(acls_list) ||
            (cnt = (int)PyList_Size(acls_list)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                        "Expected non-empty list of AclBinding "
                        "objects");
                goto err;
        }


        /* Look up the AclBinding class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        AclBinding_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "AclBinding");
        if (!AclBinding_type) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_CREATEACLS,
                                       &options, future);
        if (!c_options)
                goto err; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of AclBinding and convert to
         * corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * cnt);

        for (i = 0 ; i < cnt ; i++) {
                int r;
                PyObject *res = PyList_GET_ITEM(acls_list, i);

                r = PyObject_IsInstance(res, AclBinding_type);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of "
                                        "AclBinding objects");
                        goto err;
                }


                c_objs[i] = Admin_py_to_c_AclBinding(res, errstr, sizeof(errstr));
                if (!c_objs[i]) {
                        PyErr_SetString(PyExc_ValueError, errstr);
                        goto err;
                }
        }

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call CreateAcls
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_CreateAcls(self->rk, c_objs, cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AclBinding_destroy_array(c_objs, cnt);
        free(c_objs);
        Py_DECREF(AclBinding_type); /* from lookup() */
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if (c_objs) {
                rd_kafka_AclBinding_destroy_array(c_objs, i);
                free(c_objs);
        }
        if (AclBinding_type) Py_DECREF(AclBinding_type);
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


static const char Admin_create_acls_doc[] = PyDoc_STR(
        ".. py:function:: create_acls(acl_bindings, future, [request_timeout])\n"
        "\n"
        "  Create a list of ACL bindings.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.create_acls()\n"
);


/**
 * @brief describe_acls
 */
static PyObject *Admin_describe_acls (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *acl_binding_filter, *future;
        int r;
        struct Admin_options options = Admin_options_INITIALIZER;
        PyObject *AclBindingFilter_type = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_AclBindingFilter_t *c_obj = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        char errstr[512];

        static char *kws[] = {"acl_binding_filter",
                             "future",
                             /* options */
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &acl_binding_filter,
                                         &future,
                                         &options.request_timeout))
                goto err;


        /* Look up the AclBindingFilter class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        AclBindingFilter_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "AclBindingFilter");
        if (!AclBindingFilter_type) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_CREATEACLS,
                                       &options, future);
        if (!c_options)
                goto err; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * convert the AclBindingFilter to the
         * corresponding C type.
         */
        r = PyObject_IsInstance(acl_binding_filter, AclBindingFilter_type);
        if (r == -1)
                goto err; /* Exception raised by IsInstance() */
        else if (r == 0) {
                PyErr_SetString(PyExc_TypeError,
                                "Expected an "
                                "AclBindingFilter object");
                goto err;
        }

        c_obj = Admin_py_to_c_AclBindingFilter(acl_binding_filter, errstr, sizeof(errstr));
        if (!c_obj) {
                PyErr_SetString(PyExc_ValueError, errstr);
                goto err;
        }

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DeleteAcls
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DescribeAcls(self->rk, c_obj, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AclBinding_destroy(c_obj);
        Py_DECREF(AclBindingFilter_type); /* from lookup() */
        rd_kafka_AdminOptions_destroy(c_options);
        Py_RETURN_NONE;
err:
        if(AclBindingFilter_type) Py_DECREF(AclBindingFilter_type);
        if(c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


static const char Admin_describe_acls_doc[] = PyDoc_STR(
        ".. py:function:: describe_acls(acl_binding_filter, future, [request_timeout])\n"
        "\n"
        "  Get a list of ACL bindings matching an ACL binding filter.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.describe_acls()\n"
);

/**
 * @brief delete_acls
 */
static PyObject *Admin_delete_acls (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *acls_list, *future;
        int cnt, i = 0;
        struct Admin_options options = Admin_options_INITIALIZER;
        PyObject *AclBindingFilter_type = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_AclBindingFilter_t **c_objs = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        char errstr[512];

        static char *kws[] = {"acls",
                             "future",
                             /* options */
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &acls_list,
                                         &future,
                                         &options.request_timeout))
                goto err;

        if (!PyList_Check(acls_list) ||
            (cnt = (int)PyList_Size(acls_list)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                        "Expected non-empty list of AclBindingFilter "
                        "objects");
                goto err;
        }


        /* Look up the AclBindingFilter class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        AclBindingFilter_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "AclBindingFilter");
        if (!AclBindingFilter_type) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DELETEACLS,
                                       &options, future);
        if (!c_options)
                goto err; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /*
         * Parse the list of AclBindingFilter and convert to
         * corresponding C types.
         */
        c_objs = malloc(sizeof(*c_objs) * cnt);

        for (i = 0 ; i < cnt ; i++) {
                int r;
                PyObject *res = PyList_GET_ITEM(acls_list, i);

                r = PyObject_IsInstance(res, AclBindingFilter_type);
                if (r == -1)
                        goto err; /* Exception raised by IsInstance() */
                else if (r == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected list of "
                                        "AclBindingFilter objects");
                        goto err;
                }


                c_objs[i] = Admin_py_to_c_AclBindingFilter(res, errstr, sizeof(errstr));
                if (!c_objs[i]) {
                        PyErr_SetString(PyExc_ValueError, errstr);
                        goto err;
                }
        }

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DeleteAcls
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DeleteAcls(self->rk, c_objs, cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AclBinding_destroy_array(c_objs, cnt);
        free(c_objs);
        Py_DECREF(AclBindingFilter_type); /* from lookup() */
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if (c_objs) {
                rd_kafka_AclBinding_destroy_array(c_objs, i);
                free(c_objs);
        }
        if(AclBindingFilter_type) Py_DECREF(AclBindingFilter_type);
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


static const char Admin_delete_acls_doc[] = PyDoc_STR(
        ".. py:function:: delete_acls(acl_binding_filters, future, [request_timeout])\n"
        "\n"
        "  Deletes ACL bindings matching one or more ACL binding filter.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.delete_acls()\n"
);

/**
 * @brief List consumer groups
 */
PyObject *Admin_list_consumer_groups (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *future, *states_int = NULL, *types_int = NULL;
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        rd_kafka_consumer_group_state_t *c_states = NULL;
        rd_kafka_consumer_group_type_t *c_types = NULL;
        int states_cnt = 0;
        int types_cnt = 0;
        int i = 0;

        static char *kws[] = {"future",
                             /* options */
                             "states_int",
                             "types_int",
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOf", kws,
                                         &future,
                                         &states_int,
                                         &types_int,
                                         &options.request_timeout)) {
                goto err;
        }

        if(states_int != NULL && states_int != Py_None) {
                if(!PyList_Check(states_int)) {
                        PyErr_SetString(PyExc_ValueError,
                                "states must of type list");
                        goto err;
                }

                states_cnt = (int)PyList_Size(states_int);

                if(states_cnt > 0) {
                        c_states = (rd_kafka_consumer_group_state_t *)
                                        malloc(states_cnt*sizeof(rd_kafka_consumer_group_state_t));
                        for(i = 0 ; i < states_cnt ; i++) {
                                PyObject *state = PyList_GET_ITEM(states_int, i);
                                if(!cfl_PyInt_Check(state)) {
                                        PyErr_SetString(PyExc_ValueError,
                                                "Element of states must be valid states");
                                        goto err;
                                }
                                c_states[i] = (rd_kafka_consumer_group_state_t) cfl_PyInt_AsInt(state);
                        }
                        options.states = c_states;
                        options.states_cnt = states_cnt;
                }
        }

        if(types_int != NULL && types_int != Py_None) {
                if(!PyList_Check(types_int)) {
                        PyErr_SetString(PyExc_ValueError,
                                "types must of type list");
                        goto err;
                }

                types_cnt = (int)PyList_Size(types_int);

                if(types_cnt > 0) {
                        c_types = (rd_kafka_consumer_group_type_t *)
                                        malloc(types_cnt *
                                               sizeof(rd_kafka_consumer_group_type_t));
                        for(i = 0 ; i < types_cnt ; i++) {
                                PyObject *type = PyList_GET_ITEM(types_int, i);
                                if(!cfl_PyInt_Check(type)) {
                                        PyErr_SetString(PyExc_ValueError,
                                                "Element of types must be valid group types");
                                        goto err;
                                }
                                c_types[i] = (rd_kafka_consumer_group_type_t) cfl_PyInt_AsInt(type);
                        }
                        options.types = c_types;
                        options.types_cnt = types_cnt;
                }
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call ListConsumerGroupOffsets
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_ListConsumerGroups(self->rk, c_options, rkqu);
        CallState_end(self, &cs);

        if(c_states) {
                free(c_states);
        }
        if(c_types) {
                free(c_types);
        }
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AdminOptions_destroy(c_options);
        Py_RETURN_NONE;
err:
        if(c_states) {
                free(c_states);
        }
        if(c_types) {
                free(c_types);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}
const char Admin_list_consumer_groups_doc[] = PyDoc_STR(
        ".. py:function:: list_consumer_groups(future, [states_int], [types_int], [request_timeout])\n"
        "\n"
        "  List all the consumer groups.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.list_consumer_groups()\n");


/**
 * @brief DescribeUserScramCredentials
*/
static PyObject *Admin_describe_user_scram_credentials(Handle *self, PyObject *args,
                                                       PyObject *kwargs){
        PyObject *users, *future;
        static char *kws[] = { "users",
                               "future",
                               /* options */
                               "request_timeout",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        int user_cnt = 0, i;
        const char **c_users = NULL;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        /* users is a list of strings. */
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &users, &future,
                                         &options.request_timeout))
                return NULL;

        if (users != Py_None && !PyList_Check(users)) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of string "
                                "objects in 'users' parameter");
                return NULL;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DESCRIBEUSERSCRAMCREDENTIALS,
                                       &options, future);
        if (!c_options)
                return NULL; /* Exception raised by options_to_c() */
        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        if (users != Py_None) {
                user_cnt = (int)PyList_Size(users);
                if (user_cnt > 0)
                        c_users = malloc(sizeof(char *) * user_cnt);

                for (i = 0 ; i < user_cnt ; i++) {
                        PyObject *user = PyList_GET_ITEM(users, i);
                        PyObject *u_user;
                        PyObject *uo_user = NULL;

                        if (user == Py_None) {
                                PyErr_Format(PyExc_TypeError,
                                        "User %d in 'users' parameters must not "
                                        "be  None", i);
                                goto err;
                        }

                        if (!(u_user = cfl_PyObject_Unistr(user))) {
                                PyErr_Format(PyExc_ValueError,
                                        "User %d in 'users' parameters must "
                                        " be convertible to str", i);
                                goto err;
                        }

                        c_users[i] = cfl_PyUnistr_AsUTF8(u_user, &uo_user);
                        Py_XDECREF(u_user);
                        Py_XDECREF(uo_user);
                }
        }
        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DescribeUserScramCredentials
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DescribeUserScramCredentials(self->rk, c_users, user_cnt, c_options, rkqu);
        CallState_end(self, &cs);

        if(c_users)
                free(c_users);
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AdminOptions_destroy(c_options);
        Py_RETURN_NONE;
err:
        if(c_users)
                free(c_users);
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}

const char describe_user_scram_credentials_doc[] = PyDoc_STR(
        ".. py:function:: describe_user_scram_credentials(users, future, [request_timeout])\n"
        "\n"
        "  Describe all the credentials for a user.\n"
        "  \n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.describe_user_scram_credentials()\n");

static PyObject *Admin_alter_user_scram_credentials(Handle *self, PyObject *args,
                                                       PyObject *kwargs){
        PyObject *alterations, *future;
        static char *kws[] = { "alterations",
                               "future",
                               /* options */
                               "request_timeout",
                               NULL };
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        int c_alteration_cnt = 0, i;
        rd_kafka_UserScramCredentialAlteration_t **c_alterations = NULL;
        PyObject *UserScramCredentialAlteration_type = NULL;
        PyObject *UserScramCredentialUpsertion_type = NULL;
        PyObject *UserScramCredentialDeletion_type = NULL;
        PyObject *ScramCredentialInfo_type = NULL;
        PyObject *ScramMechanism_type = NULL;
        rd_kafka_queue_t *rkqu;
        CallState cs;

        PyObject *user = NULL;
        const char *c_user;
        PyObject *u_user = NULL;
        PyObject *uo_user = NULL;

        PyObject *salt = NULL;
        const unsigned char *c_salt = NULL;
        Py_ssize_t c_salt_size = 0;

        PyObject *password = NULL;
        const unsigned char *c_password;
        Py_ssize_t c_password_size;

        PyObject *scram_credential_info = NULL;
        PyObject *mechanism = NULL;
        int32_t iterations;
        int c_mechanism;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &alterations, &future,
                                         &options.request_timeout))
                return NULL;

        if (!PyList_Check(alterations)) {
                PyErr_SetString(PyExc_TypeError,
                                "Expected non-empty list of Alteration "
                                "objects");
                return NULL;
        }
        UserScramCredentialAlteration_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "UserScramCredentialAlteration");


        if (!UserScramCredentialAlteration_type) {
                        PyErr_SetString(PyExc_ImportError,
                                "Not able to load UserScramCredentialAlteration type");
                        goto err;
        }

        UserScramCredentialUpsertion_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "UserScramCredentialUpsertion");
        if (!UserScramCredentialUpsertion_type) {
                        PyErr_SetString(PyExc_ImportError,
                                "Not able to load UserScramCredentialUpsertion type");
                        goto err;
        }
        UserScramCredentialDeletion_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "UserScramCredentialDeletion");
        if (!UserScramCredentialDeletion_type) {
                        PyErr_SetString(PyExc_ImportError,
                                "Not able to load UserScramCredentialDeletion type");
                        goto err;
        }

        ScramCredentialInfo_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "ScramCredentialInfo");
        if (!ScramCredentialInfo_type) {
                        PyErr_SetString(PyExc_ImportError,
                                "Not able to load ScramCredentialInfo type");
                        goto err;
        }

        ScramMechanism_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "ScramMechanism");
        if (!ScramMechanism_type) {
                        PyErr_SetString(PyExc_ImportError,
                                "Not able to load ScramMechanism type");
                        goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_ALTERUSERSCRAMCREDENTIALS,
                                       &options, future);
        if (!c_options)
                goto err; /* Exception raised by options_to_c() */

        /* options_to_c() sets future as the opaque, which is used in the
         * event_cb to set the results on the future as the admin operation
         * is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        c_alteration_cnt = (int)PyList_Size(alterations);
        c_alterations = malloc(sizeof(rd_kafka_UserScramCredentialAlteration_t *) * c_alteration_cnt);

        for (i = 0 ; i < c_alteration_cnt ; i++) {
                PyObject *alteration = PyList_GET_ITEM(alterations, i);
                if(!PyObject_IsInstance(alteration, UserScramCredentialAlteration_type)) {
                        PyErr_Format(PyExc_TypeError,
                                     "Alteration %d: should be a UserScramCredentialAlteration"
                                     ", got %s", i,
                                     ((PyTypeObject *)PyObject_Type(alteration))->
                                     tp_name);
                        goto err;
                }

                cfl_PyObject_GetAttr(alteration, "user", &user,NULL,1,1);
                if (user == Py_None ||
                    !(u_user = cfl_PyObject_Unistr(user))) {
                        PyErr_Format(PyExc_TypeError,
                                     "Alteration %d: user field should be a string, got %s",
                                     i,
                                     ((PyTypeObject *)PyObject_Type(user))->
                                     tp_name);
                        goto err;
                }

                Py_DECREF(user);
                c_user = cfl_PyUnistr_AsUTF8(u_user, &uo_user);

                if(PyObject_IsInstance(alteration,UserScramCredentialUpsertion_type)){
                        /* Upsertion Type*/
                        cfl_PyObject_GetAttr(alteration,"scram_credential_info",&scram_credential_info,NULL,0,0);
                        if (!PyObject_IsInstance(scram_credential_info, ScramCredentialInfo_type)) {
                                PyErr_Format(PyExc_TypeError,
                                     "Alteration %d: field \"scram_credential_info\" "
                                     "should be a ScramCredentialInfo"
                                     ", got %s", i,
                                     ((PyTypeObject *)PyObject_Type(scram_credential_info))->
                                     tp_name);
                                goto err;
                        }

                        cfl_PyObject_GetInt(scram_credential_info,"iterations",&iterations,0,1);
                        cfl_PyObject_GetAttr(scram_credential_info,"mechanism", &mechanism, NULL, 0, 0);
                        if (!PyObject_IsInstance(mechanism, ScramMechanism_type)) {
                                PyErr_Format(PyExc_TypeError,
                                     "Alteration %d: field \"scram_credential_info."
                                     "mechanism\" should be a ScramMechanism"
                                     ", got %s", i,
                                     ((PyTypeObject *)PyObject_Type(mechanism))->
                                     tp_name);
                                goto err;
                        }
                        cfl_PyObject_GetInt(mechanism,"value", &c_mechanism,0,1);

                        cfl_PyObject_GetAttr(alteration,"password",&password,NULL,0,0);
                        if (Py_TYPE(password) != &PyBytes_Type) {
                                PyErr_Format(PyExc_TypeError,
                                        "Alteration %d: password field should be bytes, got %s",
                                        i,
                                        ((PyTypeObject *)PyObject_Type(password))->
                                        tp_name);
                                goto err;
                        }
                        PyBytes_AsStringAndSize(password, (char **) &c_password, &c_password_size);

                        cfl_PyObject_GetAttr(alteration,"salt",&salt,NULL,0,0);
                        if (salt != Py_None && Py_TYPE(salt) != &PyBytes_Type) {
                                PyErr_Format(PyExc_TypeError,
                                        "Alteration %d: salt field should be bytes, got %s",
                                        i,
                                        ((PyTypeObject *)PyObject_Type(salt))->
                                        tp_name);
                                goto err;
                        }
                        if (salt != Py_None) {
                                PyBytes_AsStringAndSize(salt, (char **) &c_salt, &c_salt_size);
                        }

                        c_alterations[i] = rd_kafka_UserScramCredentialUpsertion_new(c_user,
                                (rd_kafka_ScramMechanism_t) c_mechanism, iterations,
                                c_password, c_password_size,
                                c_salt, c_salt_size);

                        Py_DECREF(salt);
                        Py_DECREF(password);
                        Py_DECREF(scram_credential_info);
                        Py_DECREF(mechanism);
                        salt = NULL,
                        password = NULL,
                        scram_credential_info = NULL;
                        mechanism = NULL;

                } else if(PyObject_IsInstance(alteration,UserScramCredentialDeletion_type)){
                        /* Deletion Type*/
                        cfl_PyObject_GetAttr(alteration,"mechanism",&mechanism,NULL,0,0);
                        if (!PyObject_IsInstance(mechanism, ScramMechanism_type)) {
                                PyErr_Format(PyExc_TypeError,
                                     "Alteration %d: field \"mechanism\" "
                                     "should be a ScramMechanism"
                                     ", got %s", i,
                                     ((PyTypeObject *)PyObject_Type(mechanism))->
                                     tp_name);
                                goto err;
                        }
                        cfl_PyObject_GetInt(mechanism,"value",&c_mechanism,0,1);

                        c_alterations[i] = rd_kafka_UserScramCredentialDeletion_new(c_user,(rd_kafka_ScramMechanism_t)c_mechanism);
                        Py_DECREF(mechanism);
                        mechanism = NULL;
                }

                Py_DECREF(u_user);
                Py_XDECREF(uo_user);
        }
        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call AlterConfigs
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_AlterUserScramCredentials(self->rk, c_alterations, c_alteration_cnt, c_options, rkqu);
        CallState_end(self, &cs);

        if(c_alterations) {
                rd_kafka_UserScramCredentialAlteration_destroy_array(c_alterations, c_alteration_cnt);
                free(c_alterations);
        }
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AdminOptions_destroy(c_options);
        Py_DECREF(UserScramCredentialAlteration_type); /* from lookup() */
        Py_DECREF(UserScramCredentialUpsertion_type); /* from lookup() */
        Py_DECREF(UserScramCredentialDeletion_type); /* from lookup() */
        Py_DECREF(ScramCredentialInfo_type); /* from lookup() */
        Py_DECREF(ScramMechanism_type); /* from lookup() */
        Py_RETURN_NONE;
err:

        Py_XDECREF(u_user);
        Py_XDECREF(uo_user);
        Py_XDECREF(salt);
        Py_XDECREF(password);
        Py_XDECREF(scram_credential_info);
        Py_XDECREF(mechanism);

        Py_XDECREF(UserScramCredentialAlteration_type); /* from lookup() */
        Py_XDECREF(UserScramCredentialUpsertion_type); /* from lookup() */
        Py_XDECREF(UserScramCredentialDeletion_type); /* from lookup() */
        Py_XDECREF(ScramCredentialInfo_type); /* from lookup() */
        Py_XDECREF(ScramMechanism_type); /* from lookup() */

        if(c_alterations) {
                rd_kafka_UserScramCredentialAlteration_destroy_array(c_alterations, i);
                free(c_alterations);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


const char alter_user_scram_credentials_doc[] = PyDoc_STR(
        ".. py:function:: alter_user_scram_credentials(alterations, future, [request_timeout])\n"
        "\n"
        "  Alters the credentials for a user.\n"
        "  Supported : Upsertion , Deletion.\n"
        "  \n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.alter_user_scram_credentials()\n");

/**
 * @brief Describe consumer groups
 */
PyObject *Admin_describe_consumer_groups (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *future, *group_ids, *include_authorized_operations = NULL;
        struct Admin_options options = Admin_options_INITIALIZER;
        const char **c_groups = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        int groups_cnt = 0;
        int i = 0;

        static char *kws[] = {"future",
                             "group_ids",
                             /* options */
                             "request_timeout",
                             "include_authorized_operations",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|fO", kws,
                                         &group_ids,
                                         &future,
                                         &options.request_timeout,
                                         &include_authorized_operations
                                         )) {
                goto err;
        }


        if (include_authorized_operations &&
            !cfl_PyBool_get(include_authorized_operations, "include_authorized_operations",
                            &options.include_authorized_operations))
                goto err;

        if (!PyList_Check(group_ids) || (groups_cnt = (int)PyList_Size(group_ids)) < 1) {
                PyErr_SetString(PyExc_ValueError,
                                "Expected non-empty list of group_ids");
                goto err;
        }

        c_groups = malloc(sizeof(char *) * groups_cnt);

        for (i = 0 ; i < groups_cnt ; i++) {
                PyObject *group = PyList_GET_ITEM(group_ids, i);
                PyObject *ugroup;
                PyObject *uogroup = NULL;

                if (group == Py_None ||
                    !(ugroup = cfl_PyObject_Unistr(group))) {
                        PyErr_Format(PyExc_ValueError,
                                     "Expected list of group strings, "
                                     "not %s",
                                     ((PyTypeObject *)PyObject_Type(group))->
                                     tp_name);
                        goto err;
                }

                c_groups[i] = cfl_PyUnistr_AsUTF8(ugroup, &uogroup);

                Py_XDECREF(ugroup);
                Py_XDECREF(uogroup);
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DescribeConsumerGroups
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DescribeConsumerGroups(self->rk, c_groups, groups_cnt, c_options, rkqu);
        CallState_end(self, &cs);

        if(c_groups) {
                free(c_groups);
        }
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if(c_groups) {
                free(c_groups);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


const char Admin_describe_consumer_groups_doc[] = PyDoc_STR(
        ".. py:function:: describe_consumer_groups(future, group_ids, [request_timeout], [include_authorized_operations])\n"
        "\n"
        "  Describes the provided consumer groups.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.describe_consumer_groups()\n");

/**
 * @brief Describe topics
 */
PyObject *Admin_describe_topics (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *future, *topics, *include_authorized_operations = NULL;
        struct Admin_options options = Admin_options_INITIALIZER;
        const char **c_topics = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        rd_kafka_TopicCollection_t *c_topic_collection = NULL;
        int topics_cnt = 0;
        int i = 0;

        static char *kws[] = {"future",
                             "topic_names",
                             /* options */
                             "request_timeout",
                             "include_authorized_operations",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|fO", kws,
                                         &topics,
                                         &future,
                                         &options.request_timeout,
                                         &include_authorized_operations
                                         )) {
                goto err;
        }


        if (include_authorized_operations &&
            !cfl_PyBool_get(include_authorized_operations, "include_authorized_operations",
                            &options.include_authorized_operations))
                goto err;

        if (!PyList_Check(topics)) {
                PyErr_SetString(PyExc_TypeError,
                                "Expected a list of topics");
                goto err;
        }

        topics_cnt = PyList_Size(topics);

        if (topics_cnt) {
                c_topics = malloc(sizeof(char *) * topics_cnt);
                for (i = 0 ; i < topics_cnt ; i++) {
                        PyObject *topic = PyList_GET_ITEM(topics, i);
                        PyObject *uotopic = NULL;

                        if (topic == Py_None ||
                            !PyUnicode_Check(topic)) {
                                PyErr_Format(PyExc_TypeError,
                                        "Expected list of topics strings, "
                                        "not %s",
                                        ((PyTypeObject *)PyObject_Type(topic))->
                                        tp_name);
                                goto err;
                        }

                        c_topics[i] = cfl_PyUnistr_AsUTF8(topic, &uotopic);
                        Py_XDECREF(uotopic);

                        if (!c_topics[i][0]) {
                                PyErr_Format(PyExc_ValueError,
                                        "Empty topic name at index %d isn't "
                                        "allowed", i);
                                goto err;
                        }
                }
        }

        c_topic_collection = rd_kafka_TopicCollection_of_topic_names(c_topics, topics_cnt);
        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DESCRIBETOPICS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DescribeTopics
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DescribeTopics(self->rk, c_topic_collection, c_options, rkqu);
        CallState_end(self, &cs);

        if(c_topics) {
                free(c_topics);
        }
        if(c_topic_collection) {
                rd_kafka_TopicCollection_destroy(c_topic_collection);
        }
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if(c_topics) {
                free(c_topics);
        }
        if(c_topic_collection) {
                rd_kafka_TopicCollection_destroy(c_topic_collection);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


const char Admin_describe_topics_doc[] = PyDoc_STR(
        ".. py:function:: describe_topics(future, topic_names, [request_timeout], [include_authorized_operations])\n"
        "\n"
        "  Describes the provided topics.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.describe_topics()\n");

/**
 * @brief Describe cluster
 */
PyObject *Admin_describe_cluster (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *future, *include_authorized_operations = NULL;
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;

        static char *kws[] = {"future",
                             /* options */
                             "request_timeout",
                             "include_authorized_operations",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|fO", kws,
                                         &future,
                                         &options.request_timeout,
                                         &include_authorized_operations
                                         )) {
                goto err;
        }


        if (include_authorized_operations &&
            !cfl_PyBool_get(include_authorized_operations, "include_authorized_operations",
                            &options.include_authorized_operations))
                goto err;

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DescribeCluster
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DescribeCluster(self->rk, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


const char Admin_describe_cluster_doc[] = PyDoc_STR(
        ".. py:function:: describe_cluster(future, [request_timeout], [include_authorized_operations])\n"
        "\n"
        "  Describes the cluster.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.describe_cluster()\n");

/**
 * @brief Delete consumer groups offsets
 */
PyObject *Admin_delete_consumer_groups (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *group_ids, *future;
        PyObject *group_id;
        int group_ids_cnt;
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_DeleteGroup_t **c_delete_group_ids = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        int i;

        static char *kws[] = {"group_ids",
                             "future",
                             /* options */
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &group_ids,
                                         &future,
                                         &options.request_timeout)) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DELETEGROUPS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        if (!PyList_Check(group_ids)) {
                PyErr_SetString(PyExc_ValueError, "Expected 'group_ids' to be a list");
                goto err;
        }

        group_ids_cnt = (int)PyList_Size(group_ids);

        c_delete_group_ids = malloc(sizeof(rd_kafka_DeleteGroup_t *) * group_ids_cnt);
        for(i = 0 ; i < group_ids_cnt ; i++) {
                group_id = PyList_GET_ITEM(group_ids, i);

                PyObject *ks, *ks8;
                const char *group_id_string;
                if (!(ks = cfl_PyObject_Unistr(group_id))) {
                        PyErr_SetString(PyExc_TypeError,
                                        "Expected element of 'group_ids' "
                                        "to be unicode string");
                        goto err;
                }

                group_id_string = cfl_PyUnistr_AsUTF8(ks, &ks8);

                Py_DECREF(ks);
                Py_XDECREF(ks8);

                c_delete_group_ids[i] = rd_kafka_DeleteGroup_new(group_id_string);
        }

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DeleteGroups
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DeleteGroups(self->rk, c_delete_group_ids, group_ids_cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_DeleteGroup_destroy_array(c_delete_group_ids, group_ids_cnt);
        free(c_delete_group_ids);
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if (c_delete_group_ids) {
                rd_kafka_DeleteGroup_destroy_array(c_delete_group_ids, i);
                free(c_delete_group_ids);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}


const char Admin_delete_consumer_groups_doc[] = PyDoc_STR(
        ".. py:function:: delete_consumer_groups(request, future, [request_timeout])\n"
        "\n"
        "  Deletes consumer groups provided in the request.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.delete_consumer_groups()\n");


/**
 * @brief List consumer groups offsets
 */
PyObject *Admin_list_consumer_group_offsets (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *request, *future, *require_stable_obj = NULL;
        int requests_cnt;
        struct Admin_options options = Admin_options_INITIALIZER;
        PyObject *ConsumerGroupTopicPartitions_type = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_ListConsumerGroupOffsets_t **c_obj = NULL;
        rd_kafka_topic_partition_list_t *c_topic_partitions = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        PyObject *topic_partitions = NULL;
        char *group_id = NULL;

        static char *kws[] = {"request",
                             "future",
                             /* options */
                             "require_stable",
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Of", kws,
                                         &request,
                                         &future,
                                         &require_stable_obj,
                                         &options.request_timeout)) {
                goto err;
        }

        if (require_stable_obj &&
            !cfl_PyBool_get(require_stable_obj, "require_stable",
                            &options.require_stable_offsets))
                return NULL;

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        if (PyList_Check(request) &&
            (requests_cnt = (int)PyList_Size(request)) != 1) {
                PyErr_SetString(PyExc_ValueError,
                        "Currently we support listing only 1 consumer groups offset information");
                goto err;
        }

        PyObject *single_request = PyList_GET_ITEM(request, 0);

        /* Look up the ConsumerGroupTopicPartition class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        ConsumerGroupTopicPartitions_type = cfl_PyObject_lookup("confluent_kafka",
                                                  "ConsumerGroupTopicPartitions");
        if (!ConsumerGroupTopicPartitions_type) {
                PyErr_SetString(PyExc_ImportError,
                        "Not able to load ConsumerGroupTopicPartitions type");
                goto err;
        }

        if(!PyObject_IsInstance(single_request, ConsumerGroupTopicPartitions_type)) {
                PyErr_SetString(PyExc_ImportError,
                        "Each request should be of ConsumerGroupTopicPartitions type");
                goto err;
        }

        cfl_PyObject_GetString(single_request, "group_id", &group_id, NULL, 1, 0);

        if(group_id == NULL) {
                PyErr_SetString(PyExc_ValueError,
                        "Group name is mandatory for list consumer offset operation");
                goto err;
        }

        cfl_PyObject_GetAttr(single_request, "topic_partitions", &topic_partitions, &PyList_Type, 0, 1);

        if(topic_partitions != Py_None) {
                c_topic_partitions = py_to_c_parts(topic_partitions);
        }

        c_obj = malloc(sizeof(rd_kafka_ListConsumerGroupOffsets_t *) * requests_cnt);
        c_obj[0] = rd_kafka_ListConsumerGroupOffsets_new(group_id, c_topic_partitions);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call ListConsumerGroupOffsets
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_ListConsumerGroupOffsets(self->rk, c_obj, requests_cnt, c_options, rkqu);
        CallState_end(self, &cs);

        if (c_topic_partitions) {
                rd_kafka_topic_partition_list_destroy(c_topic_partitions);
        }
        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_ListConsumerGroupOffsets_destroy_array(c_obj, requests_cnt);
        free(c_obj);
        free(group_id);
        Py_DECREF(ConsumerGroupTopicPartitions_type); /* from lookup() */
        Py_XDECREF(topic_partitions);
        rd_kafka_AdminOptions_destroy(c_options);

        Py_RETURN_NONE;
err:
        if (c_topic_partitions) {
                rd_kafka_topic_partition_list_destroy(c_topic_partitions);
        }
        if (c_obj) {
                rd_kafka_ListConsumerGroupOffsets_destroy_array(c_obj, requests_cnt);
                free(c_obj);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        if(group_id) {
                free(group_id);
        }
        Py_XDECREF(topic_partitions);
        Py_XDECREF(ConsumerGroupTopicPartitions_type);
        return NULL;
}


const char Admin_list_consumer_group_offsets_doc[] = PyDoc_STR(
        ".. py:function:: list_consumer_group_offsets(request, future, [require_stable], [request_timeout])\n"
        "\n"
        "  List offset information for the consumer group and (optional) topic partition provided in the request.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.list_consumer_group_offsets()\n");


/**
 * @brief Alter consumer groups offsets
 */
PyObject *Admin_alter_consumer_group_offsets (Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *request, *future;
        int requests_cnt;
        struct Admin_options options = Admin_options_INITIALIZER;
        PyObject *ConsumerGroupTopicPartitions_type = NULL;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_AlterConsumerGroupOffsets_t **c_obj = NULL;
        rd_kafka_topic_partition_list_t *c_topic_partitions = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;
        PyObject *topic_partitions = NULL;
        char *group_id = NULL;

        static char *kws[] = {"request",
                             "future",
                             /* options */
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|f", kws,
                                         &request,
                                         &future,
                                         &options.request_timeout)) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        if (PyList_Check(request) &&
            (requests_cnt = (int)PyList_Size(request)) != 1) {
                PyErr_SetString(PyExc_ValueError,
                        "Currently we support alter consumer groups offset request for 1 group only");
                goto err;
        }

        PyObject *single_request = PyList_GET_ITEM(request, 0);

        /* Look up the ConsumerGroupTopicPartition class so we can check if the provided
         * topics are of correct type.
         * Since this is not in the fast path we treat ourselves
         * to the luxury of looking up this for each call. */
        ConsumerGroupTopicPartitions_type = cfl_PyObject_lookup("confluent_kafka",
                                                  "ConsumerGroupTopicPartitions");
        if (!ConsumerGroupTopicPartitions_type) {
                PyErr_SetString(PyExc_ImportError,
                        "Not able to load ConsumerGroupTopicPartitions type");
                goto err;
        }

        if(!PyObject_IsInstance(single_request, ConsumerGroupTopicPartitions_type)) {
                PyErr_SetString(PyExc_ImportError,
                        "Each request should be of ConsumerGroupTopicPartitions type");
                goto err;
        }

        cfl_PyObject_GetString(single_request, "group_id", &group_id, NULL, 1, 0);

        if(group_id == NULL) {
                PyErr_SetString(PyExc_ValueError,
                        "Group name is mandatory for alter consumer offset operation");
                goto err;
        }

        cfl_PyObject_GetAttr(single_request, "topic_partitions", &topic_partitions, &PyList_Type, 0, 1);

        if(topic_partitions != Py_None) {
                c_topic_partitions = py_to_c_parts(topic_partitions);
        }

        c_obj = malloc(sizeof(rd_kafka_AlterConsumerGroupOffsets_t *) * requests_cnt);
        c_obj[0] = rd_kafka_AlterConsumerGroupOffsets_new(group_id, c_topic_partitions);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call AlterConsumerGroupOffsets
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_AlterConsumerGroupOffsets(self->rk, c_obj, requests_cnt, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */
        rd_kafka_AlterConsumerGroupOffsets_destroy_array(c_obj, requests_cnt);
        free(c_obj);
        free(group_id);
        Py_DECREF(ConsumerGroupTopicPartitions_type); /* from lookup() */
        Py_XDECREF(topic_partitions);
        rd_kafka_AdminOptions_destroy(c_options);
        rd_kafka_topic_partition_list_destroy(c_topic_partitions);

        Py_RETURN_NONE;
err:
        if (c_obj) {
                rd_kafka_AlterConsumerGroupOffsets_destroy_array(c_obj, requests_cnt);
                free(c_obj);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        if(c_topic_partitions) {
                rd_kafka_topic_partition_list_destroy(c_topic_partitions);
        }
        if(group_id) {
                free(group_id);
        }
        Py_XDECREF(topic_partitions);
        Py_XDECREF(ConsumerGroupTopicPartitions_type);
        return NULL;
}


const char Admin_alter_consumer_group_offsets_doc[] = PyDoc_STR(
        ".. py:function:: alter_consumer_group_offsets(request, future, [request_timeout])\n"
        "\n"
        "  Alter offset for the consumer group and topic partition provided in the request.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.alter_consumer_group_offsets()\n");


PyObject *Admin_list_offsets (Handle *self,PyObject *args, PyObject *kwargs) {
        PyObject *topic_partitions, *future;
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_topic_partition_list_t *c_topic_partitions = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;

        static char *kws[] = {"topic_partitions",
                             "future",
                             /* options */
                             "isolation_level_value",
                             "request_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|if", kws,
                                         &topic_partitions,
                                         &future,
                                         &options.isolation_level,
                                         &options.request_timeout)) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_LISTOFFSETS,
                                       &options, future);
        if (!c_options)  {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        if (!PyList_Check(topic_partitions)) {
                PyErr_SetString(PyExc_ValueError,
                        "topic_partitions must be a list");
                goto err;
        }
        c_topic_partitions = py_to_c_parts(topic_partitions);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call ListOffsets
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_ListOffsets(self->rk, c_topic_partitions, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        rd_kafka_AdminOptions_destroy(c_options);
        rd_kafka_topic_partition_list_destroy(c_topic_partitions);

        Py_RETURN_NONE;
err:
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}

const char Admin_list_offsets_doc[] = PyDoc_STR(
        ".. py:function:: list_offsets(topic_partitions, future, [isolation_level_value], [request_timeout])\n"
        "\n"
        "  List offset for the topic partition provided in the request.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.list_offsets()\n");


/**
 * @brief Delete records 
 */
PyObject* Admin_delete_records (Handle *self,PyObject *args,PyObject *kwargs){
        PyObject *topic_partition_offsets = NULL, *future;
        int del_record_cnt = 1;
        rd_kafka_DeleteRecords_t **c_obj = NULL;
        struct Admin_options options = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_topic_partition_list_t *c_topic_partition_offsets = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;

        static char *kws[] = {"topic_partition_offsets",
                             "future",
                             /* options */
                             "request_timeout",
                             "operation_timeout",
                             NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|ff", kws,
                                         &topic_partition_offsets,
                                         &future,
                                         &options.request_timeout,
                                         &options.operation_timeout)) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_DELETERECORDS,
                                       &options, future);
        if (!c_options) {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        c_topic_partition_offsets = py_to_c_parts(topic_partition_offsets);

        if(!c_topic_partition_offsets) {
                goto err; /* Exception raised by py_to_c_parts() */
        }
        
        c_obj = malloc(sizeof(rd_kafka_DeleteRecords_t *) * del_record_cnt);
        c_obj[0] = rd_kafka_DeleteRecords_new(c_topic_partition_offsets);

        /* Use librdkafka's background thread queue to automatically dispatch
        * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /*
         * Call DeleteRecords
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         */
        CallState_begin(self, &cs);
        rd_kafka_DeleteRecords(self->rk, c_obj, del_record_cnt, c_options, rkqu);
        CallState_end(self,&cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        rd_kafka_AdminOptions_destroy(c_options);
        rd_kafka_DeleteRecords_destroy_array(c_obj, del_record_cnt);
        free(c_obj);

        rd_kafka_topic_partition_list_destroy(c_topic_partition_offsets);     
        Py_XDECREF(topic_partition_offsets);
        
        Py_RETURN_NONE;
err: 
        if (c_obj) {
                rd_kafka_DeleteRecords_destroy_array(c_obj, del_record_cnt);
                free(c_obj);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        if(c_topic_partition_offsets) {
                rd_kafka_topic_partition_list_destroy(c_topic_partition_offsets);
        }
        Py_XDECREF(topic_partition_offsets);
        return NULL;

}

const char Admin_delete_records_doc[] = PyDoc_STR(
        ".. py:function:: delete_records(topic_partition_offsets, future, [request_timeout, operation_timeout])\n"
        "\n"
        "  Delete all the records for the particular topic partition before the specified offset provided in the request.\n"
        "\n"
        "  This method should not be used directly, use confluent_kafka.AdminClient.delete_records()\n");

/**
 * @brief Elect leaders
 */
PyObject *Admin_elect_leaders(Handle *self, PyObject *args, PyObject *kwargs) {
        PyObject *election_type = NULL, *partitions = NULL, *future;
        rd_kafka_ElectLeaders_t *c_elect_leaders = NULL;
        rd_kafka_ElectionType_t c_election_type;
        struct Admin_options options       = Admin_options_INITIALIZER;
        rd_kafka_AdminOptions_t *c_options = NULL;
        rd_kafka_topic_partition_list_t *c_partitions = NULL;
        CallState cs;
        rd_kafka_queue_t *rkqu;

        static char *kws[] = {"election_type",
                              "partitions"
                              "future",
                              /* options */
                              "request_timeout", "operation_timeout", NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OOO|ff", kws,
                                         &election_type, &partitions, &future,
                                         &options.request_timeout,
                                         &options.operation_timeout)) {
                goto err;
        }

        c_options = Admin_options_to_c(self, RD_KAFKA_ADMIN_OP_ELECTLEADERS,
                                       &options, future);
        if (!c_options) {
                goto err; /* Exception raised by options_to_c() */
        }

        /* options_to_c() sets future as the opaque, which is used in the
         * background_event_cb to set the results on the future as the
         * admin operation is finished, so we need to keep our own refcount. */
        Py_INCREF(future);

        c_election_type = (rd_kafka_ElectionType_t)cfl_PyInt_AsInt(election_type);

        if (partitions != Py_None && !PyList_Check(partitions)) {
                PyErr_SetString(PyExc_ValueError, "partitions must be None or a list");
                goto err;
        }

        if (partitions != Py_None) {
                c_partitions = py_to_c_parts(partitions);
        }

        c_elect_leaders = rd_kafka_ElectLeaders_new(c_election_type, c_partitions);
        
        if(c_partitions) {
                rd_kafka_topic_partition_list_destroy(c_partitions);
        }

        /* Use librdkafka's background thread queue to automatically dispatch
         * Admin_background_event_cb() when the admin operation is finished. */
        rkqu = rd_kafka_queue_get_background(self->rk);

        /**
         *
         * Call ElectLeaders
         *
         * We need to set up a CallState and release GIL here since
         * the event_cb may be triggered immediately.
         *
         */
        CallState_begin(self, &cs);
        rd_kafka_ElectLeaders(self->rk, c_elect_leaders, c_options, rkqu);
        CallState_end(self, &cs);

        rd_kafka_queue_destroy(rkqu); /* drop reference from get_background */

        rd_kafka_AdminOptions_destroy(c_options);
        rd_kafka_ElectLeaders_destroy(c_elect_leaders);

        Py_RETURN_NONE;

err:
        if (c_elect_leaders) {
                rd_kafka_ElectLeaders_destroy(c_elect_leaders);
        }
        if (c_options) {
                rd_kafka_AdminOptions_destroy(c_options);
                Py_DECREF(future);
        }
        return NULL;
}

const char Admin_elect_leaders_doc[] = PyDoc_STR(
    ".. py:function:: elect_leaders(election_type, partitions, "
    "future, [request_timeout, operation_timeout])\n"
    "\n"
    "  Perform Preferred or Unclean election for the specified "
    "partion or all partition in the cluster.\n"
    "\n"
    "  This method should not be used directly, use "
    "confluent_kafka.AdminClient.elect_leaders()\n");

/**
 * @brief Call rd_kafka_poll() and keep track of crashing callbacks.
 * @returns -1 if callback crashed (or poll() failed), else the number
 * of events served.
 */
static int Admin_poll0 (Handle *self, int tmout) {
        int r;
        CallState cs;

        CallState_begin(self, &cs);

        r = rd_kafka_poll(self->rk, tmout);

        if (!CallState_end(self, &cs)) {
                return -1;
        }

        return r;
}


static PyObject *Admin_poll (Handle *self, PyObject *args,
                             PyObject *kwargs) {
        double tmout;
        int r;
        static char *kws[] = { "timeout", NULL };

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "d", kws, &tmout))
                return NULL;

        r = Admin_poll0(self, (int)(tmout * 1000));
        if (r == -1)
                return NULL;

        return cfl_PyInt_FromInt(r);
}



static PyMethodDef Admin_methods[] = {
        { "create_topics", (PyCFunction)Admin_create_topics,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: create_topics(topics, future, [validate_only, request_timeout, operation_timeout])\n"
          "\n"
          "  Create new topics.\n"
          "\n"
          "  This method should not be used directly, use confluent_kafka.AdminClient.create_topics()\n"
        },

        { "delete_topics", (PyCFunction)Admin_delete_topics,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: delete_topics(topics, future, [request_timeout, operation_timeout])\n"
          "\n"
          "  This method should not be used directly, use confluent_kafka.AdminClient.delete_topics()\n"
        },

        { "create_partitions", (PyCFunction)Admin_create_partitions,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: create_partitions(topics, future, [validate_only, request_timeout, operation_timeout])\n"
          "\n"
          "  This method should not be used directly, use confluent_kafka.AdminClient.create_partitions()\n"
        },

        { "describe_configs", (PyCFunction)Admin_describe_configs,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: describe_configs(resources, future, [request_timeout, broker])\n"
          "\n"
          "  This method should not be used directly, use confluent_kafka.AdminClient.describe_configs()\n"
        },

        {"incremental_alter_configs", (PyCFunction)Admin_incremental_alter_configs,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: incremental_alter_configs(resources, future, [request_timeout, validate_only, broker])\n"
          "\n"
          "  This method should not be used directly, use confluent_kafka.AdminClient.incremental_alter_configs()\n"
        },

        { "alter_configs", (PyCFunction)Admin_alter_configs,
          METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: alter_configs(resources, future, [request_timeout, broker])\n"
          "\n"
          "  This method should not be used directly, use confluent_kafka.AdminClient.alter_configs()\n"
        },

        { "poll", (PyCFunction)Admin_poll, METH_VARARGS|METH_KEYWORDS,
          ".. py:function:: poll([timeout])\n"
          "\n"
          "  Polls the Admin client for event callbacks, such as error_cb, "
          "stats_cb, etc, if registered.\n"
          "\n"
          "  There is no need to call poll() if no callbacks have been registered.\n"
          "\n"
          "  :param float timeout: Maximum time to block waiting for events. (Seconds)\n"
          "  :returns: Number of events processed (callbacks served)\n"
          "  :rtype: int\n"
          "\n"
        },

        { "list_topics", (PyCFunction)list_topics, METH_VARARGS|METH_KEYWORDS,
          list_topics_doc
        },

        { "list_groups", (PyCFunction)list_groups, METH_VARARGS|METH_KEYWORDS,
          list_groups_doc
        },

        { "list_consumer_groups", (PyCFunction)Admin_list_consumer_groups, METH_VARARGS|METH_KEYWORDS,
          Admin_list_consumer_groups_doc
        },

        { "describe_consumer_groups", (PyCFunction)Admin_describe_consumer_groups, METH_VARARGS|METH_KEYWORDS,
          Admin_describe_consumer_groups_doc
        },

        { "describe_topics", (PyCFunction)Admin_describe_topics, METH_VARARGS|METH_KEYWORDS,
          Admin_describe_topics_doc
        },

        { "describe_cluster", (PyCFunction)Admin_describe_cluster, METH_VARARGS|METH_KEYWORDS,
          Admin_describe_cluster_doc
        },

        { "delete_consumer_groups", (PyCFunction)Admin_delete_consumer_groups, METH_VARARGS|METH_KEYWORDS,
          Admin_delete_consumer_groups_doc
        },

        { "list_consumer_group_offsets", (PyCFunction)Admin_list_consumer_group_offsets, METH_VARARGS|METH_KEYWORDS,
          Admin_list_consumer_group_offsets_doc
        },

        { "alter_consumer_group_offsets", (PyCFunction)Admin_alter_consumer_group_offsets, METH_VARARGS|METH_KEYWORDS,
          Admin_alter_consumer_group_offsets_doc
        },

        { "create_acls", (PyCFunction)Admin_create_acls, METH_VARARGS|METH_KEYWORDS,
           Admin_create_acls_doc
        },

        { "describe_acls", (PyCFunction)Admin_describe_acls, METH_VARARGS|METH_KEYWORDS,
           Admin_describe_acls_doc
        },

        { "delete_acls", (PyCFunction)Admin_delete_acls, METH_VARARGS|METH_KEYWORDS,
           Admin_delete_acls_doc
        },

        { "set_sasl_credentials", (PyCFunction)set_sasl_credentials, METH_VARARGS|METH_KEYWORDS,
           set_sasl_credentials_doc
        },

        { "alter_user_scram_credentials", (PyCFunction)Admin_alter_user_scram_credentials, METH_VARARGS|METH_KEYWORDS,
           alter_user_scram_credentials_doc
        },

        { "describe_user_scram_credentials", (PyCFunction)Admin_describe_user_scram_credentials, METH_VARARGS|METH_KEYWORDS,
           describe_user_scram_credentials_doc
        },

        { "list_offsets", (PyCFunction)Admin_list_offsets, METH_VARARGS|METH_KEYWORDS,
           Admin_list_offsets_doc
        },
        
        { "delete_records", (PyCFunction)Admin_delete_records, METH_VARARGS|METH_KEYWORDS,
           Admin_delete_records_doc
        },

        { "elect_leaders", (PyCFunction)Admin_elect_leaders, METH_VARARGS | METH_KEYWORDS, 
           Admin_elect_leaders_doc
        },

        { NULL }
};


static Py_ssize_t Admin__len__ (Handle *self) {
        return rd_kafka_outq_len(self->rk);
}


static PySequenceMethods Admin_seq_methods = {
        (lenfunc)Admin__len__ /* sq_length */
};


/**
 * @brief Convert C topic_result_t array to topic-indexed dict.
 */
static PyObject *
Admin_c_topic_result_to_py (const rd_kafka_topic_result_t **c_result,
                            size_t cnt) {
        PyObject *result;
        size_t i;

        result = PyDict_New();

        for (i = 0 ; i < cnt ; i++) {
                PyObject *error;

                error = KafkaError_new_or_None(
                        rd_kafka_topic_result_error(c_result[i]),
                        rd_kafka_topic_result_error_string(c_result[i]));

                PyDict_SetItemString(
                        result,
                        rd_kafka_topic_result_name(c_result[i]),
                        error);

                Py_DECREF(error);
        }

        return result;
}



/**
 * @brief Convert C ConfigEntry array to dict of py ConfigEntry objects.
 */
static PyObject *
Admin_c_ConfigEntries_to_py (PyObject *ConfigEntry_type,
                             const rd_kafka_ConfigEntry_t **c_configs,
                             size_t config_cnt) {
        PyObject *dict;
        size_t ci;

        dict = PyDict_New();

        for (ci = 0 ; ci < config_cnt ; ci++) {
                PyObject *kwargs, *args;
                const rd_kafka_ConfigEntry_t *ent = c_configs[ci];
                const rd_kafka_ConfigEntry_t **c_synonyms;
                PyObject *entry, *synonyms;
                size_t synonym_cnt;
                const char *val;

                kwargs = PyDict_New();

                cfl_PyDict_SetString(kwargs, "name",
                                     rd_kafka_ConfigEntry_name(ent));
                val = rd_kafka_ConfigEntry_value(ent);
                if (val)
                        cfl_PyDict_SetString(kwargs, "value", val);
                else
                        PyDict_SetItemString(kwargs, "value", Py_None);
                cfl_PyDict_SetInt(kwargs, "source",
                                  (int)rd_kafka_ConfigEntry_source(ent));
                cfl_PyDict_SetInt(kwargs, "is_read_only",
                                  rd_kafka_ConfigEntry_is_read_only(ent));
                cfl_PyDict_SetInt(kwargs, "is_default",
                                  rd_kafka_ConfigEntry_is_default(ent));
                cfl_PyDict_SetInt(kwargs, "is_sensitive",
                                  rd_kafka_ConfigEntry_is_sensitive(ent));
                cfl_PyDict_SetInt(kwargs, "is_synonym",
                                  rd_kafka_ConfigEntry_is_synonym(ent));

                c_synonyms = rd_kafka_ConfigEntry_synonyms(ent,
                                                           &synonym_cnt);
                synonyms = Admin_c_ConfigEntries_to_py(ConfigEntry_type,
                                                       c_synonyms,
                                                       synonym_cnt);
                if (!synonyms) {
                        Py_DECREF(kwargs);
                        Py_DECREF(dict);
                        return NULL;
                }
                PyDict_SetItemString(kwargs, "synonyms", synonyms);
                Py_DECREF(synonyms);

                args = PyTuple_New(0);
                entry = PyObject_Call(ConfigEntry_type, args, kwargs);
                Py_DECREF(args);
                Py_DECREF(kwargs);
                if (!entry) {
                        Py_DECREF(dict);
                        return NULL;
                }

                PyDict_SetItemString(dict, rd_kafka_ConfigEntry_name(ent),
                                     entry);
                Py_DECREF(entry);
        }


        return dict;
}


/**
 * @brief Convert C ConfigResource array to dict indexed by ConfigResource
 *        with the value of dict(ConfigEntry).
 *
 * @param ret_configs If true, return configs rather than None.
 */
static PyObject *
Admin_c_ConfigResource_result_to_py (const rd_kafka_ConfigResource_t **c_resources,
                                     size_t cnt,
                                     int ret_configs) {
        PyObject *result;
        PyObject *ConfigResource_type;
        PyObject *ConfigEntry_type;
        size_t ri;

        ConfigResource_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "ConfigResource");
        if (!ConfigResource_type)
                return NULL;

        ConfigEntry_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                               "ConfigEntry");
        if (!ConfigEntry_type) {
                Py_DECREF(ConfigResource_type);
                return NULL;
        }

        result = PyDict_New();

        for (ri = 0 ; ri < cnt ; ri++) {
                const rd_kafka_ConfigResource_t *c_res = c_resources[ri];
                const rd_kafka_ConfigEntry_t **c_configs;
                PyObject *kwargs, *wrap;
                PyObject *key;
                PyObject *configs, *error;
                size_t config_cnt;

                c_configs = rd_kafka_ConfigResource_configs(c_res, &config_cnt);
                configs = Admin_c_ConfigEntries_to_py(ConfigEntry_type,
                                                      c_configs, config_cnt);
                if (!configs)
                        goto err;

                error = KafkaError_new_or_None(
                        rd_kafka_ConfigResource_error(c_res),
                        rd_kafka_ConfigResource_error_string(c_res));

                kwargs = PyDict_New();
                cfl_PyDict_SetInt(kwargs, "restype",
                                  (int)rd_kafka_ConfigResource_type(c_res));
                cfl_PyDict_SetString(kwargs, "name",
                                     rd_kafka_ConfigResource_name(c_res));
                PyDict_SetItemString(kwargs, "described_configs", configs);
                PyDict_SetItemString(kwargs, "error", error);
                Py_DECREF(error);

                /* Instantiate ConfigResource */
                wrap = PyTuple_New(0);
                key = PyObject_Call(ConfigResource_type, wrap, kwargs);
                Py_DECREF(wrap);
                Py_DECREF(kwargs);
                if (!key) {
                        Py_DECREF(configs);
                        goto err;
                }

                /* Set result to dict[ConfigResource(..)] = configs | None
                 * depending on ret_configs */
                if (ret_configs)
                        PyDict_SetItem(result, key, configs);
                else
                        PyDict_SetItem(result, key, Py_None);

                Py_DECREF(configs);
                Py_DECREF(key);
        }
        return result;

 err:
        Py_DECREF(ConfigResource_type);
        Py_DECREF(ConfigEntry_type);
        Py_DECREF(result);
        return NULL;
}

/**
 * @brief Convert C AclBinding to py
 */
static PyObject *
Admin_c_AclBinding_to_py (const rd_kafka_AclBinding_t *c_acl_binding) {

        PyObject *args, *kwargs, *AclBinding_type, *acl_binding;

        AclBinding_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                        "AclBinding");
        if (!AclBinding_type) {
                return NULL;
        }

        kwargs = PyDict_New();

        cfl_PyDict_SetInt(kwargs, "restype",
                                     rd_kafka_AclBinding_restype(c_acl_binding));
        cfl_PyDict_SetString(kwargs, "name",
                                     rd_kafka_AclBinding_name(c_acl_binding));
        cfl_PyDict_SetInt(kwargs, "resource_pattern_type",
                                rd_kafka_AclBinding_resource_pattern_type(c_acl_binding));
        cfl_PyDict_SetString(kwargs, "principal",
                                     rd_kafka_AclBinding_principal(c_acl_binding));
        cfl_PyDict_SetString(kwargs, "host",
                                     rd_kafka_AclBinding_host(c_acl_binding));
        cfl_PyDict_SetInt(kwargs, "operation",
                                     rd_kafka_AclBinding_operation(c_acl_binding));
        cfl_PyDict_SetInt(kwargs, "permission_type",
                                     rd_kafka_AclBinding_permission_type(c_acl_binding));

        args = PyTuple_New(0);
        acl_binding = PyObject_Call(AclBinding_type, args, kwargs);

        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(AclBinding_type);
        return acl_binding;
}

/**
 * @brief Convert C AclBinding array to py list.
 */
static PyObject *
Admin_c_AclBindings_to_py (const rd_kafka_AclBinding_t **c_acls,
                                          size_t c_acls_cnt) {
        size_t i;
        PyObject *result;
        PyObject *acl_binding;

        result = PyList_New(c_acls_cnt);

        for (i = 0 ; i < c_acls_cnt ; i++) {
                acl_binding = Admin_c_AclBinding_to_py(c_acls[i]);
                if (!acl_binding) {
                        Py_DECREF(result);
                        return NULL;
                }
                PyList_SET_ITEM(result, i, acl_binding);
        }

        return result;
}


/**
 * @brief Convert C acl_result_t array to py list.
 */
static PyObject *
Admin_c_acl_result_to_py (const rd_kafka_acl_result_t **c_result,
                            size_t cnt) {
        PyObject *result;
        size_t i;

        result = PyList_New(cnt);

        for (i = 0 ; i < cnt ; i++) {
                PyObject *error;
                const rd_kafka_error_t *c_error = rd_kafka_acl_result_error(c_result[i]);

                error = KafkaError_new_or_None(
                        rd_kafka_error_code(c_error),
                        rd_kafka_error_string(c_error));

                PyList_SET_ITEM(result, i, error);
        }

        return result;
}

/**
 * @brief Convert C DeleteAcls result response array to py list.
 */
static PyObject *
Admin_c_DeleteAcls_result_responses_to_py (const rd_kafka_DeleteAcls_result_response_t **c_result_responses,
                            size_t cnt) {
        const rd_kafka_AclBinding_t **c_matching_acls;
        size_t c_matching_acls_cnt;
        PyObject *result;
        PyObject *acl_bindings;
        size_t i;

        result = PyList_New(cnt);

        for (i = 0 ; i < cnt ; i++) {
                PyObject *error;
                const rd_kafka_error_t *c_error = rd_kafka_DeleteAcls_result_response_error(c_result_responses[i]);

                if (c_error) {
                        error = KafkaError_new_or_None(
                                rd_kafka_error_code(c_error),
                                rd_kafka_error_string(c_error));
                        PyList_SET_ITEM(result, i, error);
                } else {
                        c_matching_acls = rd_kafka_DeleteAcls_result_response_matching_acls(
                                                                        c_result_responses[i],
                                                                        &c_matching_acls_cnt);
                        acl_bindings = Admin_c_AclBindings_to_py(c_matching_acls,c_matching_acls_cnt);
                        if (!acl_bindings) {
                                Py_DECREF(result);
                                return NULL;
                        }
                        PyList_SET_ITEM(result, i, acl_bindings);
                }
        }

        return result;
}


/**
 * @brief
 *
 */
static PyObject *Admin_c_ListConsumerGroupsResults_to_py(
                        const rd_kafka_ConsumerGroupListing_t **c_valid_responses,
                        size_t valid_cnt,
                        const rd_kafka_error_t **c_errors_responses,
                        size_t errors_cnt) {

        PyObject *result = NULL;
        PyObject *ListConsumerGroupsResult_type = NULL;
        PyObject *ConsumerGroupListing_type = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *valid_result = NULL;
        PyObject *valid_results = NULL;
        PyObject *error_result = NULL;
        PyObject *error_results = NULL;
        PyObject *py_is_simple_consumer_group = NULL;
        size_t i = 0;
        valid_results = PyList_New(valid_cnt);
        error_results = PyList_New(errors_cnt);
        if(valid_cnt > 0) {
                ConsumerGroupListing_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                                "ConsumerGroupListing");
                if (!ConsumerGroupListing_type) {
                        goto err;
                }
                for(i = 0; i < valid_cnt; i++) {

                        kwargs = PyDict_New();

                        cfl_PyDict_SetString(kwargs,
                                             "group_id",
                                             rd_kafka_ConsumerGroupListing_group_id(c_valid_responses[i]));


                        py_is_simple_consumer_group = PyBool_FromLong(
                                rd_kafka_ConsumerGroupListing_is_simple_consumer_group(c_valid_responses[i]));
                        if(PyDict_SetItemString(kwargs,
                                                "is_simple_consumer_group",
                                                py_is_simple_consumer_group) == -1) {
                                PyErr_Format(PyExc_RuntimeError,
                                             "Not able to set 'is_simple_consumer_group' in ConsumerGroupLising");
                                Py_DECREF(py_is_simple_consumer_group);
                                goto err;
                        }
                        Py_DECREF(py_is_simple_consumer_group);

                        cfl_PyDict_SetInt(kwargs, "state", rd_kafka_ConsumerGroupListing_state(c_valid_responses[i]));

                        cfl_PyDict_SetInt(kwargs, "type", rd_kafka_ConsumerGroupListing_type(c_valid_responses[i]));

                        args = PyTuple_New(0);

                        valid_result = PyObject_Call(ConsumerGroupListing_type, args, kwargs);
                        PyList_SET_ITEM(valid_results, i, valid_result);

                        Py_DECREF(args);
                        Py_DECREF(kwargs);
                }
                Py_DECREF(ConsumerGroupListing_type);
        }

        if(errors_cnt > 0) {
                for(i = 0; i < errors_cnt; i++) {

                        error_result = KafkaError_new_or_None(
                                rd_kafka_error_code(c_errors_responses[i]),
                                rd_kafka_error_string(c_errors_responses[i]));
                        PyList_SET_ITEM(error_results, i, error_result);

                }
        }

        ListConsumerGroupsResult_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                              "ListConsumerGroupsResult");
        if (!ListConsumerGroupsResult_type) {
                return NULL;
        }
        kwargs = PyDict_New();
        PyDict_SetItemString(kwargs, "valid", valid_results);
        PyDict_SetItemString(kwargs, "errors", error_results);
        args = PyTuple_New(0);
        result = PyObject_Call(ListConsumerGroupsResult_type, args, kwargs);

        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(valid_results);
        Py_DECREF(error_results);
        Py_DECREF(ListConsumerGroupsResult_type);

        return result;
err:
        Py_XDECREF(ListConsumerGroupsResult_type);
        Py_XDECREF(ConsumerGroupListing_type);
        Py_XDECREF(result);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);

        return NULL;
}

static PyObject *Admin_c_MemberAssignment_to_py(const rd_kafka_MemberAssignment_t *c_assignment) {
        PyObject *MemberAssignment_type = NULL;
        PyObject *assignment = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *topic_partitions = NULL;
        const rd_kafka_topic_partition_list_t *c_topic_partitions = NULL;

        MemberAssignment_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                     "MemberAssignment");
        if (!MemberAssignment_type) {
                goto err;
        }
        c_topic_partitions = rd_kafka_MemberAssignment_partitions(c_assignment);

        topic_partitions = c_parts_to_py(c_topic_partitions);

        kwargs = PyDict_New();

        PyDict_SetItemString(kwargs, "topic_partitions", topic_partitions);

        args = PyTuple_New(0);

        assignment = PyObject_Call(MemberAssignment_type, args, kwargs);

        Py_DECREF(MemberAssignment_type);
        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(topic_partitions);
        return assignment;

err:
        Py_XDECREF(MemberAssignment_type);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(topic_partitions);
        Py_XDECREF(assignment);
        return NULL;

}

static PyObject *Admin_c_MemberDescription_to_py(const rd_kafka_MemberDescription_t *c_member) {
        PyObject *member = NULL;
        PyObject *MemberDescription_type = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *assignment = NULL;
        const rd_kafka_MemberAssignment_t *c_assignment;

        MemberDescription_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                     "MemberDescription");
        if (!MemberDescription_type) {
                goto err;
        }

        kwargs = PyDict_New();

        cfl_PyDict_SetString(kwargs,
                             "member_id",
                             rd_kafka_MemberDescription_consumer_id(c_member));

        cfl_PyDict_SetString(kwargs,
                             "client_id",
                             rd_kafka_MemberDescription_client_id(c_member));

        cfl_PyDict_SetString(kwargs,
                             "host",
                             rd_kafka_MemberDescription_host(c_member));

        const char * c_group_instance_id = rd_kafka_MemberDescription_group_instance_id(c_member);
        if(c_group_instance_id) {
                cfl_PyDict_SetString(kwargs, "group_instance_id", c_group_instance_id);
        }

        c_assignment = rd_kafka_MemberDescription_assignment(c_member);
        assignment = Admin_c_MemberAssignment_to_py(c_assignment);
        if (!assignment) {
                goto err;
        }

        PyDict_SetItemString(kwargs, "assignment", assignment);

        args = PyTuple_New(0);

        member = PyObject_Call(MemberDescription_type, args, kwargs);

        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(MemberDescription_type);
        Py_DECREF(assignment);
        return member;

err:

        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(MemberDescription_type);
        Py_XDECREF(assignment);
        Py_XDECREF(member);
        return NULL;
}

static PyObject *Admin_c_MemberDescriptions_to_py_from_ConsumerGroupDescription(
    const rd_kafka_ConsumerGroupDescription_t *c_consumer_group_description) {
        PyObject *member_description = NULL;
        PyObject *members = NULL;
        size_t c_members_cnt;
        const rd_kafka_MemberDescription_t *c_member;
        size_t i = 0;

        c_members_cnt = rd_kafka_ConsumerGroupDescription_member_count(c_consumer_group_description);
        members = PyList_New(c_members_cnt);
        if(c_members_cnt > 0) {
                for(i = 0; i < c_members_cnt; i++) {

                        c_member = rd_kafka_ConsumerGroupDescription_member(c_consumer_group_description, i);
                        member_description = Admin_c_MemberDescription_to_py(c_member);
                        if(!member_description) {
                                goto err;
                        }
                        PyList_SET_ITEM(members, i, member_description);
                }
        }
        return members;
err:
        Py_XDECREF(members);
        return NULL;
}


static PyObject *Admin_c_ConsumerGroupDescription_to_py(
    const rd_kafka_ConsumerGroupDescription_t *c_consumer_group_description) {
        PyObject *consumer_group_description = NULL;
        PyObject *ConsumerGroupDescription_type = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *py_is_simple_consumer_group = NULL;
        PyObject *coordinator = NULL;
        PyObject *members = NULL;
        const rd_kafka_Node_t *c_coordinator = NULL;
        size_t c_authorized_operations_cnt = 0;
        size_t i = 0;
        const rd_kafka_AclOperation_t *c_authorized_operations = NULL;

        ConsumerGroupDescription_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                            "ConsumerGroupDescription");
        if (!ConsumerGroupDescription_type) {
                PyErr_Format(PyExc_TypeError, "Not able to load ConsumerGroupDescrition type");
                goto err;
        }

        kwargs = PyDict_New();

        cfl_PyDict_SetString(kwargs,
                             "group_id",
                             rd_kafka_ConsumerGroupDescription_group_id(c_consumer_group_description));

        cfl_PyDict_SetString(kwargs,
                             "partition_assignor",
                             rd_kafka_ConsumerGroupDescription_partition_assignor(c_consumer_group_description));

        members = Admin_c_MemberDescriptions_to_py_from_ConsumerGroupDescription(c_consumer_group_description);
        if(!members) {
                goto err;
        }
        PyDict_SetItemString(kwargs, "members", members);

        c_authorized_operations = rd_kafka_ConsumerGroupDescription_authorized_operations(c_consumer_group_description, &c_authorized_operations_cnt);
        if(c_authorized_operations) {
                PyObject *authorized_operations = PyList_New(c_authorized_operations_cnt);
                for(i = 0; i<c_authorized_operations_cnt; i++){
                        PyObject *acl_op = PyLong_FromLong(c_authorized_operations[i]);
                        PyList_SET_ITEM(authorized_operations, i, acl_op);
                }
                PyDict_SetItemString(kwargs, "authorized_operations", authorized_operations);
                Py_DECREF(authorized_operations);
        }

        c_coordinator = rd_kafka_ConsumerGroupDescription_coordinator(c_consumer_group_description);
        coordinator = c_Node_to_py(c_coordinator);
        if(!coordinator) {
                goto err;
        }
        PyDict_SetItemString(kwargs, "coordinator", coordinator);

        py_is_simple_consumer_group = PyBool_FromLong(
                rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(c_consumer_group_description));
        if(PyDict_SetItemString(kwargs, "is_simple_consumer_group", py_is_simple_consumer_group) == -1) {
                goto err;
        }

        cfl_PyDict_SetInt(kwargs, "state", rd_kafka_ConsumerGroupDescription_state(c_consumer_group_description));

        args = PyTuple_New(0);

        consumer_group_description = PyObject_Call(ConsumerGroupDescription_type, args, kwargs);

        Py_XDECREF(py_is_simple_consumer_group);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(ConsumerGroupDescription_type);
        Py_XDECREF(coordinator);
        Py_XDECREF(members);
        return consumer_group_description;

err:
        Py_XDECREF(py_is_simple_consumer_group);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(coordinator);
        Py_XDECREF(ConsumerGroupDescription_type);
        Py_XDECREF(members);
        return NULL;

}

static PyObject *Admin_c_DescribeConsumerGroupsResults_to_py(
    const rd_kafka_ConsumerGroupDescription_t **c_result_responses,
    size_t cnt) {
        PyObject *consumer_group_description = NULL;
        PyObject *results = NULL;
        size_t i = 0;
        results = PyList_New(cnt);
        if(cnt > 0) {
                for(i = 0; i < cnt; i++) {
                        PyObject *error;
                        const rd_kafka_error_t *c_error =
                            rd_kafka_ConsumerGroupDescription_error(c_result_responses[i]);

                        if (c_error) {
                                error = KafkaError_new_or_None(
                                        rd_kafka_error_code(c_error),
                                        rd_kafka_error_string(c_error));
                                PyList_SET_ITEM(results, i, error);
                        } else {
                                consumer_group_description =
                                    Admin_c_ConsumerGroupDescription_to_py(c_result_responses[i]);

                                if(!consumer_group_description) {
                                        goto err;
                                }

                                PyList_SET_ITEM(results, i, consumer_group_description);
                        }
                }
        }
        return results;
err:
        Py_XDECREF(results);
        return NULL;
}

static PyObject *Admin_c_TopicPartitionInfo_to_py(
          const rd_kafka_TopicPartitionInfo_t *c_topic_partition_info){
        PyObject *partition = NULL;
        PyObject *TopicPartitionInfo_type = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *replicas = NULL;
        PyObject *isrs = NULL;
        PyObject *leader = NULL;
        size_t c_isrs_cnt, c_replicas_cnt, i=0;
        const rd_kafka_Node_t *c_leader = NULL;
        const rd_kafka_Node_t **c_replicas = NULL;
        const rd_kafka_Node_t **c_isrs = NULL;

        TopicPartitionInfo_type = cfl_PyObject_lookup("confluent_kafka",
                                                        "TopicPartitionInfo");
        if (!TopicPartitionInfo_type) {
                goto err;
        }

        kwargs = PyDict_New();

        cfl_PyDict_SetInt(kwargs,
                "id",
                rd_kafka_TopicPartitionInfo_partition(c_topic_partition_info));

        c_leader = rd_kafka_TopicPartitionInfo_leader(c_topic_partition_info);
        leader = c_Node_to_py(c_leader);
        if(!leader) {
                goto err;
        }
        PyDict_SetItemString(kwargs, "leader", leader);

        c_replicas = rd_kafka_TopicPartitionInfo_replicas(c_topic_partition_info, &c_replicas_cnt);
        replicas = PyList_New(c_replicas_cnt);
        for(i=0;i<c_replicas_cnt;i++){
                PyObject *replica = c_Node_to_py(c_replicas[i]);
                PyList_SET_ITEM(replicas, i, replica);
        }
        PyDict_SetItemString(kwargs, "replicas", replicas);

        c_isrs = rd_kafka_TopicPartitionInfo_isr(c_topic_partition_info, &c_isrs_cnt);
        isrs = PyList_New(c_isrs_cnt);
        for(i=0;i<c_isrs_cnt;i++){
                PyObject *isr = c_Node_to_py(c_isrs[i]);
                PyList_SET_ITEM(isrs, i, isr);
        }
        PyDict_SetItemString(kwargs, "isr", isrs);

        args = PyTuple_New(0);

        partition = PyObject_Call(TopicPartitionInfo_type, args, kwargs);

        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(TopicPartitionInfo_type);
        Py_XDECREF(leader);
        Py_XDECREF(replicas);
        Py_XDECREF(isrs);
        return partition;
err:
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(TopicPartitionInfo_type);
        Py_XDECREF(leader);
        Py_XDECREF(replicas);
        Py_XDECREF(isrs);
        Py_XDECREF(partition);
        return NULL;
}

static PyObject *Admin_c_TopicPartitionInfos_to_py_from_TopicDescription(
    const rd_kafka_TopicDescription_t *c_topic_description) {
        PyObject *partitions = NULL;
        size_t c_partitions_cnt;
        size_t i = 0;
        const rd_kafka_TopicPartitionInfo_t **c_partitions = NULL;

        c_partitions = rd_kafka_TopicDescription_partitions(c_topic_description, &c_partitions_cnt);
        partitions = PyList_New(c_partitions_cnt);
        if(c_partitions_cnt > 0) {
                for(i = 0; i < c_partitions_cnt; i++) {
                        PyObject *topic_partition_info = Admin_c_TopicPartitionInfo_to_py(
                                        c_partitions[i]);
                        if(!topic_partition_info) {
                                goto err;
                        }
                        PyList_SET_ITEM(partitions, i, topic_partition_info);
                }
        }
        return partitions;
err:
        Py_XDECREF(partitions);
        return NULL;
}
static PyObject *Admin_c_TopicDescription_to_py(
        const rd_kafka_TopicDescription_t *c_topic_description){
        PyObject *topic_description = NULL;
        PyObject *TopicDescription_type = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *partitions = NULL;
        PyObject *is_internal = NULL;
        size_t c_authorized_operations_cnt = 0;
        size_t i = 0;
        const rd_kafka_AclOperation_t *c_authorized_operations = NULL;
        const rd_kafka_Uuid_t *c_topic_id = NULL;

        TopicDescription_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                    "TopicDescription");
        if (!TopicDescription_type) {
                PyErr_Format(PyExc_TypeError, "Not able to load TopicDescription type");
                goto err;
        }

        kwargs = PyDict_New();

        cfl_PyDict_SetString(kwargs,
                             "name",
                             rd_kafka_TopicDescription_name(c_topic_description));

        c_topic_id = rd_kafka_TopicDescription_topic_id(c_topic_description);
        PyDict_SetItemString(kwargs,
                             "topic_id",
                             c_Uuid_to_py(c_topic_id));

        is_internal = PyBool_FromLong(rd_kafka_TopicDescription_is_internal(c_topic_description));
        if(PyDict_SetItemString(kwargs, "is_internal", is_internal) == -1) {
                goto err;
        }

        partitions = Admin_c_TopicPartitionInfos_to_py_from_TopicDescription(c_topic_description);
        if(!partitions)
                goto err;
        PyDict_SetItemString(kwargs, "partitions", partitions);

        c_authorized_operations = rd_kafka_TopicDescription_authorized_operations(c_topic_description, &c_authorized_operations_cnt);
        if(c_authorized_operations) {
                PyObject *authorized_operations = PyList_New(c_authorized_operations_cnt);
                for(i = 0; i<c_authorized_operations_cnt; i++){
                        PyObject *acl_op = PyLong_FromLong(c_authorized_operations[i]);
                        PyList_SET_ITEM(authorized_operations, i, acl_op);
                }
                PyDict_SetItemString(kwargs, "authorized_operations", authorized_operations);
                Py_DECREF(authorized_operations);
        }

        args = PyTuple_New(0);

        topic_description = PyObject_Call(TopicDescription_type, args, kwargs);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(is_internal);
        Py_XDECREF(partitions);
        Py_XDECREF(TopicDescription_type);
        return topic_description;
err:
        Py_XDECREF(topic_description);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(is_internal);
        Py_XDECREF(partitions);
        Py_XDECREF(TopicDescription_type);
        return NULL;
}

static PyObject *Admin_c_DescribeTopicsResults_to_py(
    const rd_kafka_TopicDescription_t **c_result_responses,
    size_t cnt) {
        PyObject *results = NULL;
        size_t i = 0;
        results = PyList_New(cnt);
        if(cnt > 0) {
                for(i = 0; i < cnt; i++) {
                        const rd_kafka_error_t *c_error =
                            rd_kafka_TopicDescription_error(c_result_responses[i]);

                        if (rd_kafka_error_code(c_error)) {
                                PyObject *error;
                                error = KafkaError_new_or_None(
                                        rd_kafka_error_code(c_error),
                                        rd_kafka_error_string(c_error));
                                PyList_SET_ITEM(results, i, error);
                        } else {
                                PyObject *topic_description =
                                    Admin_c_TopicDescription_to_py(c_result_responses[i]);
                                if(!topic_description) {
                                        goto err;
                                }
                                PyList_SET_ITEM(results, i, topic_description);
                        }
                }
        }
        return results;
err:
        Py_XDECREF(results);
        return NULL;
}

static PyObject *Admin_c_ScramMechanism_to_py(rd_kafka_ScramMechanism_t mechanism){
        PyObject *result = NULL;
        PyObject *args = NULL, *kwargs = NULL;
        PyObject *ScramMechanism_type;
        kwargs = PyDict_New();
        cfl_PyDict_SetInt(kwargs, "value",(int) mechanism);
        args = PyTuple_New(0);
        ScramMechanism_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                  "ScramMechanism");
        result = PyObject_Call(ScramMechanism_type, args, kwargs);
        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(ScramMechanism_type);
        return result;
}

static PyObject *Admin_c_ScramCredentialInfo_to_py(const rd_kafka_ScramCredentialInfo_t *scram_credential_info){
        PyObject *result = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *ScramCredentialInfo_type = NULL;
        PyObject *scram_mechanism = NULL;
        rd_kafka_ScramMechanism_t c_mechanism;
        int32_t iterations;

        kwargs = PyDict_New();
        c_mechanism = rd_kafka_ScramCredentialInfo_mechanism(scram_credential_info);
        scram_mechanism = Admin_c_ScramMechanism_to_py(c_mechanism);
        PyDict_SetItemString(kwargs,"mechanism", scram_mechanism);
        Py_DECREF(scram_mechanism);

        iterations = rd_kafka_ScramCredentialInfo_iterations(scram_credential_info);
        cfl_PyDict_SetInt(kwargs,"iterations", iterations);
        args = PyTuple_New(0);
        ScramCredentialInfo_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                       "ScramCredentialInfo");
        result = PyObject_Call(ScramCredentialInfo_type, args, kwargs);
        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(ScramCredentialInfo_type);
        return result;
}

static PyObject *Admin_c_UserScramCredentialsDescription_to_py(const rd_kafka_UserScramCredentialsDescription_t *description){
        PyObject *result = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *scram_credential_infos = NULL;
        PyObject *UserScramCredentialsDescription_type = NULL;
        int scram_credential_info_cnt;
        int i;
        kwargs = PyDict_New();
        cfl_PyDict_SetString(kwargs, "user", rd_kafka_UserScramCredentialsDescription_user(description));

        scram_credential_info_cnt = rd_kafka_UserScramCredentialsDescription_scramcredentialinfo_count(description);
        scram_credential_infos = PyList_New(scram_credential_info_cnt);
        for(i=0; i < scram_credential_info_cnt; i++){
                const rd_kafka_ScramCredentialInfo_t *c_scram_credential_info =
                        rd_kafka_UserScramCredentialsDescription_scramcredentialinfo(description,i);
                PyList_SET_ITEM(scram_credential_infos, i,
                        Admin_c_ScramCredentialInfo_to_py(c_scram_credential_info));
        }

        PyDict_SetItemString(kwargs,"scram_credential_infos", scram_credential_infos);
        args = PyTuple_New(0);
        UserScramCredentialsDescription_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                                   "UserScramCredentialsDescription");
        result = PyObject_Call(UserScramCredentialsDescription_type, args, kwargs);
        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(scram_credential_infos);
        Py_DECREF(UserScramCredentialsDescription_type);
        return result;
}

static PyObject *Admin_c_UserScramCredentialsDescriptions_to_py(
        const rd_kafka_UserScramCredentialsDescription_t **c_descriptions,
        size_t c_description_cnt) {
        PyObject *result = NULL;
        size_t i;

        result = PyDict_New();
        for(i=0; i < c_description_cnt; i++){
                const char *c_username;
                const rd_kafka_error_t *c_error;
                rd_kafka_resp_err_t err;
                PyObject *error, *user_scram_credentials_description;
                const rd_kafka_UserScramCredentialsDescription_t *c_description = c_descriptions[i];
                c_username = rd_kafka_UserScramCredentialsDescription_user(c_description);
                c_error = rd_kafka_UserScramCredentialsDescription_error(c_description);
                err = rd_kafka_error_code(c_error);
                if (err) {
                        error = KafkaError_new_or_None(err,
                                        rd_kafka_error_string(c_error));
                        PyDict_SetItemString(result, c_username, error);
                        Py_DECREF(error);
                } else {
                        user_scram_credentials_description =
                                Admin_c_UserScramCredentialsDescription_to_py(c_description);
                        PyDict_SetItemString(result, c_username, user_scram_credentials_description);
                        Py_DECREF(user_scram_credentials_description);
                }
        }
        return result;
}

static PyObject *Admin_c_AlterUserScramCredentialsResultResponses_to_py(
        const rd_kafka_AlterUserScramCredentials_result_response_t **c_responses,
        size_t c_response_cnt) {
        PyObject *result = NULL;
        PyObject* error = NULL;
        size_t i;
        result = PyDict_New();
        for(i=0; i<c_response_cnt; i++){
                const rd_kafka_AlterUserScramCredentials_result_response_t *c_response = c_responses[i];
                const rd_kafka_error_t *c_error = rd_kafka_AlterUserScramCredentials_result_response_error(c_response);
                const char *c_username = rd_kafka_AlterUserScramCredentials_result_response_user(c_response);
                error = KafkaError_new_or_None(
                                rd_kafka_error_code(c_error),
                                rd_kafka_error_string(c_error));
                PyDict_SetItemString(result, c_username, error);
                Py_DECREF(error);
        }
        return result;
}

static PyObject *Admin_c_DescribeClusterResult_to_py(
    const rd_kafka_DescribeCluster_result_t *c_describe_cluster_result) {
        PyObject *cluster_description = NULL;
        PyObject *DescribeClusterResult_type = NULL;
        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *nodes = NULL;
        PyObject *controller = NULL;
        size_t c_authorized_operations_cnt = 0, c_nodes_cnt = 0;
        size_t i = 0;
        const rd_kafka_Node_t **c_nodes = NULL;
        const rd_kafka_Node_t *c_controller = NULL;
        const rd_kafka_AclOperation_t *c_authorized_operations = NULL;
        const char *c_cluster_id = NULL;

        DescribeClusterResult_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                            "DescribeClusterResult");
        if (!DescribeClusterResult_type) {
                PyErr_Format(PyExc_TypeError, "Not able to load DescribeClusterResult type");
                goto err;
        }

        kwargs = PyDict_New();

        c_cluster_id = rd_kafka_DescribeCluster_result_cluster_id(c_describe_cluster_result);
        if(c_cluster_id)
                cfl_PyDict_SetString(kwargs, "cluster_id", c_cluster_id);

        c_controller = rd_kafka_DescribeCluster_result_controller(c_describe_cluster_result);
        controller = c_Node_to_py(c_controller);
        if(!controller) {
                goto err;
        }
        PyDict_SetItemString(kwargs, "controller", controller);

        c_nodes = rd_kafka_DescribeCluster_result_nodes(c_describe_cluster_result, &c_nodes_cnt);
        nodes = PyList_New(c_nodes_cnt);
        for(i=0;i<c_nodes_cnt;i++){
                PyObject* node = c_Node_to_py(c_nodes[i]);
                if(!node) {
                        goto err;
                }
                PyList_SET_ITEM(nodes, i, node);
        }
        PyDict_SetItemString(kwargs, "nodes", nodes);

        c_authorized_operations = rd_kafka_DescribeCluster_result_authorized_operations(
                c_describe_cluster_result,
                &c_authorized_operations_cnt);
        if (c_authorized_operations) {
                PyObject *authorized_operations = PyList_New(c_authorized_operations_cnt);
                for(i = 0; i<c_authorized_operations_cnt; i++){
                        PyObject *acl_op = PyLong_FromLong(c_authorized_operations[i]);
                        PyList_SET_ITEM(authorized_operations, i, acl_op);
                }
                PyDict_SetItemString(kwargs, "authorized_operations", authorized_operations);
                Py_DECREF(authorized_operations);
        }

        args = PyTuple_New(0);

        cluster_description = PyObject_Call(DescribeClusterResult_type, args, kwargs);

        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(controller);
        Py_XDECREF(nodes);
        Py_XDECREF(DescribeClusterResult_type);
        return cluster_description;
err:
        Py_XDECREF(cluster_description);
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        Py_XDECREF(controller);
        Py_XDECREF(nodes);
        Py_XDECREF(DescribeClusterResult_type);
        return NULL;
}

/**
 *
 * @brief Convert C delete groups result response to pyobject.
 *
 */
static PyObject *
Admin_c_DeleteGroupResults_to_py (const rd_kafka_group_result_t **c_result_responses,
                                  size_t cnt) {

        PyObject *delete_groups_result = NULL;
        size_t i;

        delete_groups_result = PyList_New(cnt);

        for (i = 0; i < cnt; i++) {
                PyObject *error;
                const rd_kafka_error_t *c_error = rd_kafka_group_result_error(c_result_responses[i]);
                error = KafkaError_new_or_None(
                        rd_kafka_error_code(c_error),
                        rd_kafka_error_string(c_error));
                PyList_SET_ITEM(delete_groups_result, i, error);
        }

        return delete_groups_result;
}


static PyObject * Admin_c_SingleGroupResult_to_py(const rd_kafka_group_result_t *c_group_result_response) {

        PyObject *args = NULL;
        PyObject *kwargs = NULL;
        PyObject *GroupResult_type = NULL;
        PyObject *group_result = NULL;
        const rd_kafka_topic_partition_list_t *c_topic_partition_offset_list;
        PyObject *topic_partition_offset_list = NULL;

        GroupResult_type = cfl_PyObject_lookup("confluent_kafka",
                                               "ConsumerGroupTopicPartitions");
        if (!GroupResult_type) {
                return NULL;
        }

        kwargs = PyDict_New();

        cfl_PyDict_SetString(kwargs, "group_id", rd_kafka_group_result_name(c_group_result_response));

        c_topic_partition_offset_list = rd_kafka_group_result_partitions(c_group_result_response);
        if(c_topic_partition_offset_list) {
                topic_partition_offset_list = c_parts_to_py(c_topic_partition_offset_list);
                PyDict_SetItemString(kwargs, "topic_partitions", topic_partition_offset_list);
        }

        args = PyTuple_New(0);
        group_result = PyObject_Call(GroupResult_type, args, kwargs);

        Py_DECREF(args);
        Py_DECREF(kwargs);
        Py_DECREF(GroupResult_type);
        Py_XDECREF(topic_partition_offset_list);

        return group_result;
}


/**
 *
 * @brief Convert C group result response to pyobject.
 *
 */
static PyObject *
Admin_c_GroupResults_to_py (const rd_kafka_group_result_t **c_result_responses,
                            size_t cnt) {

        size_t i;
        PyObject *all_groups_result = NULL;
        PyObject *single_group_result = NULL;

        all_groups_result = PyList_New(cnt);

        for (i = 0; i < cnt; i++) {
                PyObject *error;
                const rd_kafka_error_t *c_error = rd_kafka_group_result_error(c_result_responses[i]);

                if (c_error) {
                        error = KafkaError_new_or_None(
                                rd_kafka_error_code(c_error),
                                rd_kafka_error_string(c_error));
                        PyList_SET_ITEM(all_groups_result, i, error);
                } else {
                        single_group_result =
                                Admin_c_SingleGroupResult_to_py(c_result_responses[i]);
                        if (!single_group_result) {
                                Py_XDECREF(all_groups_result);
                                return NULL;
                        }
                        PyList_SET_ITEM(all_groups_result, i, single_group_result);
                }
        }

        return all_groups_result;
}

/**
 *
 * @brief Convert C ListOffsetsResultInfo array to dict[TopicPartition, ListOffsetsResultInfo].
 *
 */
static PyObject *Admin_c_ListOffsetsResultInfos_to_py (const rd_kafka_ListOffsetsResultInfo_t **c_result_infos, size_t c_result_info_cnt) {
        PyObject *result = NULL;
        PyObject *ListOffsetsResultInfo_type = NULL;
        size_t i;

        ListOffsetsResultInfo_type = cfl_PyObject_lookup("confluent_kafka.admin",
                                                         "ListOffsetsResultInfo");
        if(!ListOffsetsResultInfo_type){
                return NULL;
        }

        result = PyDict_New();
        for(i=0; i<c_result_info_cnt; i++){
                PyObject *value = NULL;
                PyObject *key = NULL;
                const rd_kafka_topic_partition_t *c_topic_partition = rd_kafka_ListOffsetsResultInfo_topic_partition(c_result_infos[i]);

                int64_t c_timestamp = rd_kafka_ListOffsetsResultInfo_timestamp(c_result_infos[i]);

                if (c_topic_partition->err) {
                        value = KafkaError_new_or_None(c_topic_partition->err,rd_kafka_err2str(c_topic_partition->err));
                } else {
                        PyObject *args = NULL;
                        PyObject *kwargs = NULL;
                        kwargs = PyDict_New();
                        cfl_PyDict_SetLong(kwargs,"offset", c_topic_partition->offset);
                        cfl_PyDict_SetLong(kwargs,"timestamp", c_timestamp);
                        cfl_PyDict_SetInt(kwargs,"leader_epoch",
                                rd_kafka_topic_partition_get_leader_epoch(c_topic_partition));
                        args = PyTuple_New(0);
                        value = PyObject_Call(ListOffsetsResultInfo_type, args, kwargs);
                        Py_DECREF(args);
                        Py_DECREF(kwargs);
                        if (value == NULL)
                                goto raise;
                }
                key = c_part_to_py(c_topic_partition);
                PyDict_SetItem(result, key, value);
                Py_DECREF(key);
                Py_DECREF(value);
        }

        Py_DECREF(ListOffsetsResultInfo_type);
        return result;
raise:
        Py_DECREF(result);
        Py_DECREF(ListOffsetsResultInfo_type);
        return NULL;
}

static PyObject *Admin_c_DeletedRecords_to_py (const rd_kafka_topic_partition_list_t *c_topic_partitions) {
        PyObject *result = NULL;
        PyObject *DeletedRecords_type = NULL;

        int i;

        DeletedRecords_type = cfl_PyObject_lookup("confluent_kafka.admin", 
                                                  "DeletedRecords");
        if(!DeletedRecords_type)
                goto raise;  /* Exception raised by lookup() */

        result = PyDict_New();
        for(i=0; i<c_topic_partitions->cnt; i++){
                PyObject *key = NULL;
                PyObject *value = NULL;
        
                rd_kafka_topic_partition_t *c_topic_partition = &c_topic_partitions->elems[i];
                key = c_part_to_py(c_topic_partition);

                if (c_topic_partition->err) {
                        value = KafkaError_new_or_None(c_topic_partition->err, rd_kafka_err2str(c_topic_partition->err));
                } else {
                        PyObject *args = NULL;
                        PyObject *kwargs = NULL;
                        kwargs = PyDict_New();
                        cfl_PyDict_SetLong(kwargs, "low_watermark", c_topic_partition->offset);
                        args = PyTuple_New(0);
                        value = PyObject_Call(DeletedRecords_type, args, kwargs);
                        Py_DECREF(args);
                        Py_DECREF(kwargs);

                        if (!value){
                                Py_DECREF(key);
                                goto raise;
                        }
                }
                
                PyDict_SetItem(result, key, value);
                Py_DECREF(key);
                Py_DECREF(value);
        }

        Py_DECREF(DeletedRecords_type);
        return result;

raise:
        Py_XDECREF(result);
        Py_XDECREF(DeletedRecords_type);
        return NULL;
}

/**
 * @brief Event callback triggered from librdkafka's background thread
 *        when Admin API results are ready.
 *
 *        The rkev opaque (not \p opaque) is the future PyObject
 *        which we'll set the result on.
 *
 * @locality background rdkafka thread
 */
static void Admin_background_event_cb (rd_kafka_t *rk, rd_kafka_event_t *rkev,
                                       void *opaque) {
        PyObject *future = (PyObject *)rd_kafka_event_opaque(rkev);
        const rd_kafka_topic_result_t **c_topic_res;
        size_t c_topic_res_cnt;
        PyGILState_STATE gstate;
        PyObject *error, *method, *ret;
        PyObject *result = NULL;
        PyObject *exctype = NULL, *exc = NULL, *excargs = NULL;

        /* Acquire GIL */
        gstate = PyGILState_Ensure();

        /* Generic request-level error handling. */
        error = KafkaError_new_or_None(rd_kafka_event_error(rkev),
                                       rd_kafka_event_error_string(rkev));
        if (error != Py_None)
                goto raise;

        switch (rd_kafka_event_type(rkev))
        {
        case RD_KAFKA_EVENT_CREATETOPICS_RESULT:
        {
                const rd_kafka_CreateTopics_result_t *c_res;

                c_res = rd_kafka_event_CreateTopics_result(rkev);

                c_topic_res = rd_kafka_CreateTopics_result_topics(
                        c_res, &c_topic_res_cnt);

                result = Admin_c_topic_result_to_py(c_topic_res,
                                                    c_topic_res_cnt);
                break;
        }

        case RD_KAFKA_EVENT_DELETETOPICS_RESULT:
        {
                const rd_kafka_DeleteTopics_result_t *c_res;

                c_res = rd_kafka_event_DeleteTopics_result(rkev);

                c_topic_res = rd_kafka_DeleteTopics_result_topics(
                        c_res, &c_topic_res_cnt);

                result = Admin_c_topic_result_to_py(c_topic_res,
                                                    c_topic_res_cnt);
                break;
        }

        case RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT:
        {
                const rd_kafka_CreatePartitions_result_t *c_res;

                c_res = rd_kafka_event_CreatePartitions_result(rkev);

                c_topic_res = rd_kafka_CreatePartitions_result_topics(
                        c_res, &c_topic_res_cnt);

                result = Admin_c_topic_result_to_py(c_topic_res,
                                                    c_topic_res_cnt);
                break;
        }

        case RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT:
        {
                const rd_kafka_ConfigResource_t **c_resources;
                size_t resource_cnt;

                c_resources = rd_kafka_DescribeConfigs_result_resources(
                        rd_kafka_event_DescribeConfigs_result(rkev),
                        &resource_cnt);
                result = Admin_c_ConfigResource_result_to_py(
                        c_resources,
                        resource_cnt,
                        1/* return configs */);
                break;
        }

        case RD_KAFKA_EVENT_ALTERCONFIGS_RESULT:
        {
                const rd_kafka_ConfigResource_t **c_resources;
                size_t resource_cnt;

                c_resources = rd_kafka_AlterConfigs_result_resources(
                        rd_kafka_event_AlterConfigs_result(rkev),
                        &resource_cnt);
                result = Admin_c_ConfigResource_result_to_py(
                        c_resources,
                        resource_cnt,
                        0/* return None instead of (the empty) configs */);
                break;
        }

        case RD_KAFKA_EVENT_INCREMENTALALTERCONFIGS_RESULT:
        {
                const rd_kafka_ConfigResource_t **c_resources;
                size_t resource_cnt;

                c_resources = rd_kafka_IncrementalAlterConfigs_result_resources(
                        rd_kafka_event_IncrementalAlterConfigs_result(rkev),
                        &resource_cnt);
                result = Admin_c_ConfigResource_result_to_py(
                        c_resources,
                        resource_cnt,
                        0/* return None instead of (the empty) configs */);
                break;
        }

        case RD_KAFKA_EVENT_CREATEACLS_RESULT:
        {
                const rd_kafka_acl_result_t **c_acl_results;
                size_t c_acl_results_cnt;

                c_acl_results = rd_kafka_CreateAcls_result_acls(
                        rd_kafka_event_CreateAcls_result(rkev),
                        &c_acl_results_cnt
                );
                result = Admin_c_acl_result_to_py(
                        c_acl_results,
                        c_acl_results_cnt);
                break;
        }

        case RD_KAFKA_EVENT_DESCRIBEACLS_RESULT:
        {
                const rd_kafka_DescribeAcls_result_t *c_acl_result;
                const rd_kafka_AclBinding_t **c_acls;
                size_t c_acl_cnt;

                c_acl_result = rd_kafka_event_DescribeAcls_result(rkev);

                c_acls = rd_kafka_DescribeAcls_result_acls(
                        c_acl_result,
                        &c_acl_cnt
                );

                result = Admin_c_AclBindings_to_py(c_acls,
                                                   c_acl_cnt);

                break;
        }


        case RD_KAFKA_EVENT_DELETEACLS_RESULT:
        {
                const rd_kafka_DeleteAcls_result_t *c_acl_result;
                const rd_kafka_DeleteAcls_result_response_t **c_acl_result_responses;
                size_t c_acl_results_cnt;

                c_acl_result = rd_kafka_event_DeleteAcls_result(rkev);

                c_acl_result_responses = rd_kafka_DeleteAcls_result_responses(
                        c_acl_result,
                        &c_acl_results_cnt
                );

                result = Admin_c_DeleteAcls_result_responses_to_py(c_acl_result_responses,
                                                        c_acl_results_cnt);

                break;
        }

        case RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT:
        {
                const  rd_kafka_ListConsumerGroups_result_t *c_list_consumer_groups_res;
                const rd_kafka_ConsumerGroupListing_t **c_list_consumer_groups_valid_responses;
                size_t c_list_consumer_groups_valid_cnt;
                const rd_kafka_error_t **c_list_consumer_groups_errors_responses;
                size_t c_list_consumer_groups_errors_cnt;

                c_list_consumer_groups_res = rd_kafka_event_ListConsumerGroups_result(rkev);

                c_list_consumer_groups_valid_responses =
                        rd_kafka_ListConsumerGroups_result_valid(c_list_consumer_groups_res,
                                                                 &c_list_consumer_groups_valid_cnt);
                c_list_consumer_groups_errors_responses =
                        rd_kafka_ListConsumerGroups_result_errors(c_list_consumer_groups_res,
                                                                  &c_list_consumer_groups_errors_cnt);

                result = Admin_c_ListConsumerGroupsResults_to_py(c_list_consumer_groups_valid_responses,
                                                                 c_list_consumer_groups_valid_cnt,
                                                                 c_list_consumer_groups_errors_responses,
                                                                 c_list_consumer_groups_errors_cnt);

                break;
        }

        case RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT:
        {
                const rd_kafka_DescribeConsumerGroups_result_t *c_describe_consumer_groups_res;
                const rd_kafka_ConsumerGroupDescription_t **c_describe_consumer_groups_res_responses;
                size_t c_describe_consumer_groups_res_cnt;

                c_describe_consumer_groups_res = rd_kafka_event_DescribeConsumerGroups_result(rkev);

                c_describe_consumer_groups_res_responses = rd_kafka_DescribeConsumerGroups_result_groups
                                                           (c_describe_consumer_groups_res,
                                                           &c_describe_consumer_groups_res_cnt);

                result = Admin_c_DescribeConsumerGroupsResults_to_py(c_describe_consumer_groups_res_responses,
                                                                     c_describe_consumer_groups_res_cnt);

                break;
        }
        case RD_KAFKA_EVENT_DESCRIBEUSERSCRAMCREDENTIALS_RESULT:
        {
                const rd_kafka_DescribeUserScramCredentials_result_t *c_describe_user_scram_credentials_result;
                const rd_kafka_UserScramCredentialsDescription_t **c_describe_user_scram_credentials_result_descriptions;
                size_t c_describe_user_scram_credentials_result_descriptions_cnt;

                c_describe_user_scram_credentials_result = rd_kafka_event_DescribeUserScramCredentials_result(rkev);

                c_describe_user_scram_credentials_result_descriptions = rd_kafka_DescribeUserScramCredentials_result_descriptions(
                        c_describe_user_scram_credentials_result,
                        &c_describe_user_scram_credentials_result_descriptions_cnt);

                result = Admin_c_UserScramCredentialsDescriptions_to_py(c_describe_user_scram_credentials_result_descriptions,
                        c_describe_user_scram_credentials_result_descriptions_cnt);

                break;
        }
        case RD_KAFKA_EVENT_ALTERUSERSCRAMCREDENTIALS_RESULT:
        {
                const rd_kafka_AlterUserScramCredentials_result_t *c_alter_user_scram_credentials_result;
                const rd_kafka_AlterUserScramCredentials_result_response_t **c_alter_user_scram_credentials_result_responses;
                size_t c_alter_user_scram_credentials_result_response_cnt;

                c_alter_user_scram_credentials_result = rd_kafka_event_AlterUserScramCredentials_result(rkev);

                c_alter_user_scram_credentials_result_responses = rd_kafka_AlterUserScramCredentials_result_responses(
                        c_alter_user_scram_credentials_result,
                        &c_alter_user_scram_credentials_result_response_cnt);

                result = Admin_c_AlterUserScramCredentialsResultResponses_to_py(
                        c_alter_user_scram_credentials_result_responses,
                        c_alter_user_scram_credentials_result_response_cnt);
                break;
        }
        case RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT:
        {
                const rd_kafka_DescribeTopics_result_t *c_describe_topics_res;
                const rd_kafka_TopicDescription_t **c_describe_topics_res_responses;
                size_t c_describe_topics_res_cnt;

                c_describe_topics_res = rd_kafka_event_DescribeTopics_result(rkev);

                c_describe_topics_res_responses = rd_kafka_DescribeTopics_result_topics
                                                           (c_describe_topics_res,
                                                           &c_describe_topics_res_cnt);

                result = Admin_c_DescribeTopicsResults_to_py(c_describe_topics_res_responses,
                                                                     c_describe_topics_res_cnt);

                break;
        }

        case RD_KAFKA_EVENT_DESCRIBECLUSTER_RESULT:
        {
                const rd_kafka_DescribeCluster_result_t *c_describe_cluster_res;
                c_describe_cluster_res = rd_kafka_event_DescribeCluster_result(rkev);

                result = Admin_c_DescribeClusterResult_to_py(c_describe_cluster_res);

                break;
        }

        case RD_KAFKA_EVENT_DELETEGROUPS_RESULT:
        {

                const  rd_kafka_DeleteGroups_result_t *c_delete_groups_res;
                const rd_kafka_group_result_t **c_delete_groups_res_responses;
                size_t c_delete_groups_res_cnt;

                c_delete_groups_res = rd_kafka_event_DeleteGroups_result(rkev);

                c_delete_groups_res_responses =
                        rd_kafka_DeleteConsumerGroupOffsets_result_groups(
                            c_delete_groups_res,
                            &c_delete_groups_res_cnt);

                result = Admin_c_DeleteGroupResults_to_py(c_delete_groups_res_responses,
                                                          c_delete_groups_res_cnt);

                break;
        }

        case RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT:
        {
                const  rd_kafka_ListConsumerGroupOffsets_result_t *c_list_group_offset_res;
                const rd_kafka_group_result_t **c_list_group_offset_res_responses;
                size_t c_list_group_offset_res_cnt;

                c_list_group_offset_res = rd_kafka_event_ListConsumerGroupOffsets_result(rkev);

                c_list_group_offset_res_responses =
                        rd_kafka_ListConsumerGroupOffsets_result_groups(
                                c_list_group_offset_res,
                                &c_list_group_offset_res_cnt);

                result = Admin_c_GroupResults_to_py(c_list_group_offset_res_responses,
                                                    c_list_group_offset_res_cnt);

                break;
        }

        case RD_KAFKA_EVENT_ALTERCONSUMERGROUPOFFSETS_RESULT:
        {
                const  rd_kafka_AlterConsumerGroupOffsets_result_t *c_alter_group_offset_res;
                const rd_kafka_group_result_t **c_alter_group_offset_res_responses;
                size_t c_alter_group_offset_res_cnt;

                c_alter_group_offset_res = rd_kafka_event_AlterConsumerGroupOffsets_result(rkev);

                c_alter_group_offset_res_responses =
                        rd_kafka_AlterConsumerGroupOffsets_result_groups(c_alter_group_offset_res,
                                                                         &c_alter_group_offset_res_cnt);

                result = Admin_c_GroupResults_to_py(c_alter_group_offset_res_responses,
                                                    c_alter_group_offset_res_cnt);

                break;
        }

        case RD_KAFKA_EVENT_LISTOFFSETS_RESULT:
        {
                size_t c_result_info_cnt;
                const rd_kafka_ListOffsets_result_t *c_list_offsets_result = rd_kafka_event_ListOffsets_result(rkev);
                const rd_kafka_ListOffsetsResultInfo_t **c_result_infos = rd_kafka_ListOffsets_result_infos(
                        c_list_offsets_result, &c_result_info_cnt);

                result = Admin_c_ListOffsetsResultInfos_to_py(c_result_infos, c_result_info_cnt);
                break;
        }

        case RD_KAFKA_EVENT_DELETERECORDS_RESULT:
        {
                const rd_kafka_DeleteRecords_result_t *c_delete_records_res = rd_kafka_event_DeleteRecords_result(rkev);
                const rd_kafka_topic_partition_list_t *c_delete_records_res_list = rd_kafka_DeleteRecords_result_offsets(c_delete_records_res);
                
                result = Admin_c_DeletedRecords_to_py(c_delete_records_res_list);
                break;
        }

        case RD_KAFKA_EVENT_ELECTLEADERS_RESULT: 
        {
                size_t c_result_cnt;

                const rd_kafka_ElectLeaders_result_t
                    *c_elect_leaders_res_event =
                        rd_kafka_event_ElectLeaders_result(rkev);

                const rd_kafka_topic_partition_result_t **partition_results =
                        rd_kafka_ElectLeaders_result_partitions(
                            c_elect_leaders_res_event, &c_result_cnt);

                result = c_topic_partition_result_to_py_dict(partition_results, c_result_cnt);

                break;
        }

        default:
                Py_DECREF(error); /* Py_None */
                error = KafkaError_new0(RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE,
                                        "Unsupported event type %s",
                                        rd_kafka_event_name(rkev));
                goto raise;
        }

        if (!result) {
                Py_DECREF(error); /* Py_None */
                if (!PyErr_Occurred()) {
                        error = KafkaError_new0(RD_KAFKA_RESP_ERR__INVALID_ARG,
                                                "BUG: Event %s handling failed "
                                                "but no exception raised",
                                                rd_kafka_event_name(rkev));
                } else {
                        /* Extract the exception type and message
                         * and pass it as an error to raise and subsequently
                         * the future.
                         * We loose the backtrace here unfortunately, so
                         * these errors are a bit cryptic. */
                        PyObject *trace = NULL;

                        /* Fetch (and clear) currently raised exception */
                        PyErr_Fetch(&exctype, &error, &trace);
                        Py_XDECREF(trace);
                }

                goto raise;
        }

        /*
         * Call future.set_result()
         */
        method = cfl_PyUnistr(_FromString("set_result"));

        ret = PyObject_CallMethodObjArgs(future, method, result, NULL);

        Py_XDECREF(ret);
        Py_XDECREF(result);
        Py_DECREF(future);
        Py_DECREF(method);

        /* Release GIL */
        PyGILState_Release(gstate);

        rd_kafka_event_destroy(rkev);

        return;

 raise:
        /*
         * Pass an exception to future.set_exception().
         */

        if (!exctype) {
                /* No previous exception raised, use KafkaException */
                exctype = KafkaException;
                Py_INCREF(exctype);
        }

        /* Create a new exception based on exception type and error. */
        excargs = PyTuple_New(1);
        Py_INCREF(error); /* tuple's reference */
        PyTuple_SET_ITEM(excargs, 0, error);
        exc = ((PyTypeObject *)exctype)->tp_new(
                (PyTypeObject *)exctype, NULL, NULL);
        exc->ob_type->tp_init(exc, excargs, NULL);
        Py_DECREF(excargs);
        Py_XDECREF(exctype);
        Py_XDECREF(error); /* from error source above */

        /*
         * Call future.set_exception(exc)
         */
        method = cfl_PyUnistr(_FromString("set_exception"));
        ret = PyObject_CallMethodObjArgs(future, method, exc, NULL);
        Py_XDECREF(ret);
        Py_DECREF(exc);
        Py_DECREF(future);
        Py_DECREF(method);

        /* Release GIL */
        PyGILState_Release(gstate);

        rd_kafka_event_destroy(rkev);
}


static int Admin_init (PyObject *selfobj, PyObject *args, PyObject *kwargs) {
        Handle *self = (Handle *)selfobj;
        char errstr[512];
        rd_kafka_conf_t *conf;

        if (self->rk) {
                PyErr_SetString(PyExc_RuntimeError,
                                "Admin already __init__:ialized");
                return -1;
        }

        self->type = PY_RD_KAFKA_ADMIN;

        if (!(conf = common_conf_setup(PY_RD_KAFKA_ADMIN, self,
                                       args, kwargs)))
                return -1;

        rd_kafka_conf_set_background_event_cb(conf, Admin_background_event_cb);

        /* There is no dedicated ADMIN client type in librdkafka, the Admin
         * API can use either PRODUCER or CONSUMER.
         * We choose PRODUCER since it is more lightweight than a
         * CONSUMER instance. */
        self->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                                errstr, sizeof(errstr));
        if (!self->rk) {
                cfl_PyErr_Format(rd_kafka_last_error(),
                                 "Failed to create admin client: %s", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* Forward log messages to poll queue */
        if (self->logger)
                rd_kafka_set_log_queue(self->rk, NULL);

        return 0;
}


static PyObject *Admin_new (PyTypeObject *type, PyObject *args,
                            PyObject *kwargs) {
        return type->tp_alloc(type, 0);
}



PyTypeObject AdminType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        "cimpl._AdminClientImpl",   /*tp_name*/
        sizeof(Handle),            /*tp_basicsize*/
        0,                         /*tp_itemsize*/
        (destructor)Admin_dealloc, /*tp_dealloc*/
        0,                         /*tp_print*/
        0,                         /*tp_getattr*/
        0,                         /*tp_setattr*/
        0,                         /*tp_compare*/
        0,                         /*tp_repr*/
        0,                         /*tp_as_number*/
        &Admin_seq_methods,        /*tp_as_sequence*/
        0,                         /*tp_as_mapping*/
        0,                         /*tp_hash */
        0,                         /*tp_call*/
        0,                         /*tp_str*/
        0,                         /*tp_getattro*/
        0,                         /*tp_setattro*/
        0,                         /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
        Py_TPFLAGS_HAVE_GC, /*tp_flags*/
        "Kafka Admin Client\n"
        "\n"
        ".. py:function:: Admin(**kwargs)\n"
        "\n"
        "  Create a new AdminClient instance using the provided configuration dict.\n"
        "\n"
        "This class should not be used directly, use confluent_kafka.AdminClient\n."
        "\n"
        ".. py:function:: len()\n"
        "\n"
        "  :returns: Number Kafka protocol requests waiting to be delivered to, or returned from, broker.\n"
        "  :rtype: int\n"
        "\n", /*tp_doc*/
        (traverseproc)Admin_traverse, /* tp_traverse */
        (inquiry)Admin_clear,      /* tp_clear */
        0,                         /* tp_richcompare */
        0,                         /* tp_weaklistoffset */
        0,                         /* tp_iter */
        0,                         /* tp_iternext */
        Admin_methods,             /* tp_methods */
        0,                         /* tp_members */
        0,                         /* tp_getset */
        0,                         /* tp_base */
        0,                         /* tp_dict */
        0,                         /* tp_descr_get */
        0,                         /* tp_descr_set */
        0,                         /* tp_dictoffset */
        Admin_init,                /* tp_init */
        0,                         /* tp_alloc */
        Admin_new                  /* tp_new */
};
