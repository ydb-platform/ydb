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
 * Admin Client types
 *
 *
 ****************************************************************************/



/****************************************************************************
 *
 *
 * NewTopic
 *
 *
 *
 *
 ****************************************************************************/
static int NewTopic_clear (NewTopic *self) {
        if (self->topic) {
                free(self->topic);
                self->topic = NULL;
        }
        if (self->replica_assignment) {
                Py_DECREF(self->replica_assignment);
                self->replica_assignment = NULL;
        }
        if (self->config) {
                Py_DECREF(self->config);
                self->config = NULL;
        }
        return 0;
}

static void NewTopic_dealloc (NewTopic *self) {
        PyObject_GC_UnTrack(self);

        NewTopic_clear(self);

        Py_TYPE(self)->tp_free((PyObject *)self);
}


static int NewTopic_init (PyObject *self0, PyObject *args,
                          PyObject *kwargs) {
        NewTopic *self = (NewTopic *)self0;
        const char *topic;
        static char *kws[] = { "topic",
                               "num_partitions",
                               "replication_factor",
                               "replica_assignment",
                               "config",
                               NULL };

        self->num_partitions = -1;
        self->replication_factor = -1;
        self->replica_assignment = NULL;
        self->config = NULL;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|iiOO", kws,
                                         &topic, &self->num_partitions,
                                         &self->replication_factor,
                                         &self->replica_assignment,
                                         &self->config))
                return -1;



        if (self->config) {
                if (!PyDict_Check(self->config)) {
                        PyErr_SetString(PyExc_TypeError,
                                        "config must be a dict of strings");
                        return -1;
                }
                Py_INCREF(self->config);
        }

        Py_XINCREF(self->replica_assignment);

        self->topic = strdup(topic);

        return 0;
}


static PyObject *NewTopic_new (PyTypeObject *type, PyObject *args,
                               PyObject *kwargs) {
        PyObject *self = type->tp_alloc(type, 1);
        return self;
}



static int NewTopic_traverse (NewTopic *self,
                              visitproc visit, void *arg) {
        if (self->replica_assignment)
                Py_VISIT(self->replica_assignment);
        if (self->config)
                Py_VISIT(self->config);
        return 0;
}


static PyMemberDef NewTopic_members[] = {
        { "topic", T_STRING, offsetof(NewTopic, topic), READONLY,
          ":py:attribute:topic - Topic name (string)" },
        { "num_partitions", T_INT, offsetof(NewTopic, num_partitions), 0,
          ":py:attribute: Number of partitions (int).\n"
          "Or -1 if a replica_assignment is specified" },
        { "replication_factor", T_INT, offsetof(NewTopic, replication_factor),
          0,
          " :py:attribute: Replication factor (int).\n"
          "Must be set to -1 if a replica_assignment is specified.\n" },
        { "replica_assignment", T_OBJECT, offsetof(NewTopic, replica_assignment),
          0,
          ":py:attribute: Replication assignment (list of lists).\n"
          "The outer list index represents the partition index, the inner "
          "list is the replica assignment (broker ids) for that partition.\n"
          "replication_factor and replica_assignment are mutually exclusive.\n"
        },
        { "config", T_OBJECT, offsetof(NewTopic, config),
          0,
          ":py:attribute: Optional topic configuration.\n"
          "See http://kafka.apache.org/documentation.html#topicconfigs.\n"
        },
        { NULL }
};


static PyObject *NewTopic_str0 (NewTopic *self) {
        if (self->num_partitions == -1) {
                return cfl_PyUnistr(
                _FromFormat("NewTopic(topic=%s)",
                            self->topic));
        }
        return cfl_PyUnistr(
                _FromFormat("NewTopic(topic=%s,num_partitions=%d)",
                            self->topic, self->num_partitions));
}


static PyObject *
NewTopic_richcompare (NewTopic *self, PyObject *o2, int op) {
        NewTopic *a = self, *b;
        int tr, pr;
        int r;
        PyObject *result;

        if (Py_TYPE(o2) != Py_TYPE(self)) {
                PyErr_SetNone(PyExc_NotImplementedError);
                return NULL;
        }

        b = (NewTopic *)o2;

        tr = strcmp(a->topic, b->topic);
        pr = a->num_partitions - b->num_partitions;
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


static long NewTopic_hash (NewTopic *self) {
        PyObject *topic = cfl_PyUnistr(_FromString(self->topic));
        long r;
        if (self->num_partitions == -1) {
                r = PyObject_Hash(topic);
        } else {
                r = PyObject_Hash(topic) ^ self->num_partitions;
        }
        Py_DECREF(topic);
        return r;
}


PyTypeObject NewTopicType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        "cimpl.NewTopic",         /*tp_name*/
        sizeof(NewTopic),       /*tp_basicsize*/
        0,                         /*tp_itemsize*/
        (destructor)NewTopic_dealloc, /*tp_dealloc*/
        0,                         /*tp_print*/
        0,                         /*tp_getattr*/
        0,                         /*tp_setattr*/
        0,                         /*tp_compare*/
        (reprfunc)NewTopic_str0, /*tp_repr*/
        0,                         /*tp_as_number*/
        0,                         /*tp_as_sequence*/
        0,                         /*tp_as_mapping*/
        (hashfunc)NewTopic_hash, /*tp_hash */
        0,                         /*tp_call*/
        0,                         /*tp_str*/
        PyObject_GenericGetAttr,   /*tp_getattro*/
        0,                         /*tp_setattro*/
        0,                         /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
        Py_TPFLAGS_HAVE_GC, /*tp_flags*/
        "NewTopic specifies per-topic settings for passing to "
        "AdminClient.create_topics().\n"
        "\n"
        ".. py:function:: NewTopic(topic, [num_partitions], [replication_factor], [replica_assignment], [config])\n"
        "\n"
        "  Instantiate a NewTopic object.\n"
        "\n"
        "  :param string topic: Topic name\n"
        "  :param int num_partitions: Number of partitions to create, or -1 if replica_assignment is used.\n"
        "  :param int replication_factor: Replication factor of partitions, or -1 if replica_assignment is used.\n"
        "  :param list replica_assignment: List of lists with the replication assignment for each new partition.\n"
        "  :param dict config: Dict (str:str) of topic configuration. See http://kafka.apache.org/documentation.html#topicconfigs\n"
        "  :rtype: NewTopic\n"
        "\n"
        "\n", /*tp_doc*/
        (traverseproc)NewTopic_traverse, /* tp_traverse */
        (inquiry)NewTopic_clear,       /* tp_clear */
        (richcmpfunc)NewTopic_richcompare, /* tp_richcompare */
        0,                         /* tp_weaklistoffset */
        0,                         /* tp_iter */
        0,                         /* tp_iternext */
        0,                         /* tp_methods */
        NewTopic_members,/* tp_members */
        0,                         /* tp_getset */
        0,                         /* tp_base */
        0,                         /* tp_dict */
        0,                         /* tp_descr_get */
        0,                         /* tp_descr_set */
        0,                         /* tp_dictoffset */
        NewTopic_init,       /* tp_init */
        0,                         /* tp_alloc */
        NewTopic_new         /* tp_new */
};



/****************************************************************************
 *
 *
 * NewPartitions
 *
 *
 *
 *
 ****************************************************************************/
static int NewPartitions_clear (NewPartitions *self) {
        if (self->topic) {
                free(self->topic);
                self->topic = NULL;
        }
        if (self->replica_assignment) {
                Py_DECREF(self->replica_assignment);
                self->replica_assignment = NULL;
        }
        return 0;
}

static void NewPartitions_dealloc (NewPartitions *self) {
        PyObject_GC_UnTrack(self);

        NewPartitions_clear(self);

        Py_TYPE(self)->tp_free((PyObject *)self);
}


static int NewPartitions_init (PyObject *self0, PyObject *args,
                          PyObject *kwargs) {
        NewPartitions *self = (NewPartitions *)self0;
        const char *topic;
        static char *kws[] = { "topic",
                               "new_total_count",
                               "replica_assignment",
                               NULL };

        self->replica_assignment = NULL;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "si|O", kws,
                                         &topic, &self->new_total_count,
                                         &self->replica_assignment))
                return -1;

        self->topic = strdup(topic);
        Py_XINCREF(self->replica_assignment);

        return 0;
}


static PyObject *NewPartitions_new (PyTypeObject *type, PyObject *args,
                               PyObject *kwargs) {
        PyObject *self = type->tp_alloc(type, 1);
        return self;
}



static int NewPartitions_traverse (NewPartitions *self,
                              visitproc visit, void *arg) {
        if (self->replica_assignment)
                Py_VISIT(self->replica_assignment);
        return 0;
}


static PyMemberDef NewPartitions_members[] = {
        { "topic", T_STRING, offsetof(NewPartitions, topic), READONLY,
          ":py:attribute:topic - Topic name (string)" },
        { "new_total_count", T_INT, offsetof(NewPartitions, new_total_count), 0,
          ":py:attribute: Total number of partitions (int)" },
        { "replica_assignment", T_OBJECT, offsetof(NewPartitions, replica_assignment),
          0,
          ":py:attribute: Replication assignment (list of lists).\n"
          "The outer list index represents the partition index, the inner "
          "list is the replica assignment (broker ids) for that partition.\n"
        },
        { NULL }
};


static PyObject *NewPartitions_str0 (NewPartitions *self) {
        return cfl_PyUnistr(
                _FromFormat("NewPartitions(topic=%s,new_total_count=%d)",
                            self->topic, self->new_total_count));
}


static PyObject *
NewPartitions_richcompare (NewPartitions *self, PyObject *o2, int op) {
        NewPartitions *a = self, *b;
        int tr, pr;
        int r;
        PyObject *result;

        if (Py_TYPE(o2) != Py_TYPE(self)) {
                PyErr_SetNone(PyExc_NotImplementedError);
                return NULL;
        }

        b = (NewPartitions *)o2;

        tr = strcmp(a->topic, b->topic);
        pr = a->new_total_count - b->new_total_count;
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


static long NewPartitions_hash (NewPartitions *self) {
        PyObject *topic = cfl_PyUnistr(_FromString(self->topic));
        long r = PyObject_Hash(topic) ^ self->new_total_count;
        Py_DECREF(topic);
        return r;
}


PyTypeObject NewPartitionsType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        "cimpl.NewPartitions",         /*tp_name*/
        sizeof(NewPartitions),       /*tp_basicsize*/
        0,                         /*tp_itemsize*/
        (destructor)NewPartitions_dealloc, /*tp_dealloc*/
        0,                         /*tp_print*/
        0,                         /*tp_getattr*/
        0,                         /*tp_setattr*/
        0,                         /*tp_compare*/
        (reprfunc)NewPartitions_str0, /*tp_repr*/
        0,                         /*tp_as_number*/
        0,                         /*tp_as_sequence*/
        0,                         /*tp_as_mapping*/
        (hashfunc)NewPartitions_hash, /*tp_hash */
        0,                         /*tp_call*/
        0,                         /*tp_str*/
        PyObject_GenericGetAttr,   /*tp_getattro*/
        0,                         /*tp_setattro*/
        0,                         /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
        Py_TPFLAGS_HAVE_GC, /*tp_flags*/
        "NewPartitions specifies per-topic settings for passing to "
        "passed to AdminClient.create_partitions().\n"
        "\n"
        ".. py:function:: NewPartitions(topic, new_total_count, [replica_assignment])\n"
        "\n"
        "  Instantiate a NewPartitions object.\n"
        "\n"
        "  :param string topic: Topic name\n"
        "  :param int new_total_count: Increase the topic's partition count to this value.\n"
        "  :param list replica_assignment: List of lists with the replication assignment for each new partition.\n"
        "  :rtype: NewPartitions\n"
        "\n"
        "\n", /*tp_doc*/
        (traverseproc)NewPartitions_traverse, /* tp_traverse */
        (inquiry)NewPartitions_clear,       /* tp_clear */
        (richcmpfunc)NewPartitions_richcompare, /* tp_richcompare */
        0,                         /* tp_weaklistoffset */
        0,                         /* tp_iter */
        0,                         /* tp_iternext */
        0,                         /* tp_methods */
        NewPartitions_members,/* tp_members */
        0,                         /* tp_getset */
        0,                         /* tp_base */
        0,                         /* tp_dict */
        0,                         /* tp_descr_get */
        0,                         /* tp_descr_set */
        0,                         /* tp_dictoffset */
        NewPartitions_init,       /* tp_init */
        0,                         /* tp_alloc */
        NewPartitions_new         /* tp_new */
};






/**
 * @brief Finalize type objects
 */
int AdminTypes_Ready (void) {
        int r;

        r = PyType_Ready(&NewTopicType);
        if (r < 0)
                return r;
        r = PyType_Ready(&NewPartitionsType);
        if (r < 0)
                return r;
        return r;
}


static void AdminTypes_AddObjectsConfigSource (PyObject *m) {
        /* rd_kafka_ConfigSource_t */
        PyModule_AddIntConstant(m, "CONFIG_SOURCE_UNKNOWN_CONFIG",
                                RD_KAFKA_CONFIG_SOURCE_UNKNOWN_CONFIG);
        PyModule_AddIntConstant(m, "CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG",
                                RD_KAFKA_CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG);
        PyModule_AddIntConstant(m, "CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG",
                                RD_KAFKA_CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG);
        PyModule_AddIntConstant(m, "CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG",
                                RD_KAFKA_CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG);
        PyModule_AddIntConstant(m, "CONFIG_SOURCE_STATIC_BROKER_CONFIG",
                                RD_KAFKA_CONFIG_SOURCE_STATIC_BROKER_CONFIG);
        PyModule_AddIntConstant(m, "CONFIG_SOURCE_DEFAULT_CONFIG",
                                RD_KAFKA_CONFIG_SOURCE_DEFAULT_CONFIG);
}


static void AdminTypes_AddObjectsResourceType (PyObject *m) {
        /* rd_kafka_ResourceType_t */
        PyModule_AddIntConstant(m, "RESOURCE_UNKNOWN", RD_KAFKA_RESOURCE_UNKNOWN);
        PyModule_AddIntConstant(m, "RESOURCE_ANY", RD_KAFKA_RESOURCE_ANY);
        PyModule_AddIntConstant(m, "RESOURCE_TOPIC", RD_KAFKA_RESOURCE_TOPIC);
        PyModule_AddIntConstant(m, "RESOURCE_GROUP", RD_KAFKA_RESOURCE_GROUP);
        PyModule_AddIntConstant(m, "RESOURCE_BROKER", RD_KAFKA_RESOURCE_BROKER);
        PyModule_AddIntConstant(m, "RESOURCE_TRANSACTIONAL_ID", RD_KAFKA_RESOURCE_TRANSACTIONAL_ID);
}

static void AdminTypes_AddObjectsResourcePatternType (PyObject *m) {
        /* rd_kafka_ResourcePatternType_t */
        PyModule_AddIntConstant(m, "RESOURCE_PATTERN_UNKNOWN", RD_KAFKA_RESOURCE_PATTERN_UNKNOWN);
        PyModule_AddIntConstant(m, "RESOURCE_PATTERN_ANY", RD_KAFKA_RESOURCE_PATTERN_ANY);
        PyModule_AddIntConstant(m, "RESOURCE_PATTERN_MATCH", RD_KAFKA_RESOURCE_PATTERN_MATCH);
        PyModule_AddIntConstant(m, "RESOURCE_PATTERN_LITERAL", RD_KAFKA_RESOURCE_PATTERN_LITERAL);
        PyModule_AddIntConstant(m, "RESOURCE_PATTERN_PREFIXED", RD_KAFKA_RESOURCE_PATTERN_PREFIXED);
}

static void AdminTypes_AddObjectsAclOperation (PyObject *m) {
        /* rd_kafka_AclOperation_t */
        PyModule_AddIntConstant(m, "ACL_OPERATION_UNKNOWN", RD_KAFKA_ACL_OPERATION_UNKNOWN);
        PyModule_AddIntConstant(m, "ACL_OPERATION_ANY", RD_KAFKA_ACL_OPERATION_ANY);
        PyModule_AddIntConstant(m, "ACL_OPERATION_ALL", RD_KAFKA_ACL_OPERATION_ALL);
        PyModule_AddIntConstant(m, "ACL_OPERATION_READ", RD_KAFKA_ACL_OPERATION_READ);
        PyModule_AddIntConstant(m, "ACL_OPERATION_WRITE", RD_KAFKA_ACL_OPERATION_WRITE);
        PyModule_AddIntConstant(m, "ACL_OPERATION_CREATE", RD_KAFKA_ACL_OPERATION_CREATE);
        PyModule_AddIntConstant(m, "ACL_OPERATION_DELETE", RD_KAFKA_ACL_OPERATION_DELETE);
        PyModule_AddIntConstant(m, "ACL_OPERATION_ALTER", RD_KAFKA_ACL_OPERATION_ALTER);
        PyModule_AddIntConstant(m, "ACL_OPERATION_DESCRIBE", RD_KAFKA_ACL_OPERATION_DESCRIBE);
        PyModule_AddIntConstant(m, "ACL_OPERATION_CLUSTER_ACTION", RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION);
        PyModule_AddIntConstant(m, "ACL_OPERATION_DESCRIBE_CONFIGS", RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS);
        PyModule_AddIntConstant(m, "ACL_OPERATION_ALTER_CONFIGS", RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS);
        PyModule_AddIntConstant(m, "ACL_OPERATION_IDEMPOTENT_WRITE", RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE);
}

static void AdminTypes_AddObjectsAclPermissionType (PyObject *m) {
        /* rd_kafka_AclPermissionType_t */
        PyModule_AddIntConstant(m, "ACL_PERMISSION_TYPE_UNKNOWN", RD_KAFKA_ACL_PERMISSION_TYPE_UNKNOWN);
        PyModule_AddIntConstant(m, "ACL_PERMISSION_TYPE_ANY", RD_KAFKA_ACL_PERMISSION_TYPE_ANY);
        PyModule_AddIntConstant(m, "ACL_PERMISSION_TYPE_DENY", RD_KAFKA_ACL_PERMISSION_TYPE_DENY);
        PyModule_AddIntConstant(m, "ACL_PERMISSION_TYPE_ALLOW", RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW);
}

static void AdminTypes_AddObjectsConsumerGroupStates (PyObject *m) {
        /* rd_kafka_consumer_group_state_t */
        PyModule_AddIntConstant(m, "CONSUMER_GROUP_STATE_UNKNOWN", RD_KAFKA_CONSUMER_GROUP_STATE_UNKNOWN);
        PyModule_AddIntConstant(m, "CONSUMER_GROUP_STATE_PREPARING_REBALANCE", RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE);
        PyModule_AddIntConstant(m, "CONSUMER_GROUP_STATE_COMPLETING_REBALANCE", RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE);
        PyModule_AddIntConstant(m, "CONSUMER_GROUP_STATE_STABLE", RD_KAFKA_CONSUMER_GROUP_STATE_STABLE);
        PyModule_AddIntConstant(m, "CONSUMER_GROUP_STATE_DEAD", RD_KAFKA_CONSUMER_GROUP_STATE_DEAD);
        PyModule_AddIntConstant(m, "CONSUMER_GROUP_STATE_EMPTY", RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY);
}

static void AdminTypes_AddObjectsConsumerGroupTypes (PyObject *m) {
        /* rd_kafka_consumer_group_type_t */
        PyModule_AddIntConstant(m, "CONSUMER_GROUP_TYPE_UNKNOWN", RD_KAFKA_CONSUMER_GROUP_TYPE_UNKNOWN);
        PyModule_AddIntConstant(m, "CONSUMER_GROUP_TYPE_CONSUMER", RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER);
        PyModule_AddIntConstant(m, "CONSUMER_GROUP_TYPE_CLASSIC", RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC);
}

static void AdminTypes_AddObjectsAlterConfigOpType (PyObject *m) {
        PyModule_AddIntConstant(m, "ALTER_CONFIG_OP_TYPE_SET", RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET);
        PyModule_AddIntConstant(m, "ALTER_CONFIG_OP_TYPE_DELETE", RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE);
        PyModule_AddIntConstant(m, "ALTER_CONFIG_OP_TYPE_APPEND", RD_KAFKA_ALTER_CONFIG_OP_TYPE_APPEND);
        PyModule_AddIntConstant(m, "ALTER_CONFIG_OP_TYPE_SUBTRACT", RD_KAFKA_ALTER_CONFIG_OP_TYPE_SUBTRACT);
}

static void AdminTypes_AddObjectsScramMechanismType (PyObject *m) {
        PyModule_AddIntConstant(m, "SCRAM_MECHANISM_UNKNOWN", RD_KAFKA_SCRAM_MECHANISM_UNKNOWN);
        PyModule_AddIntConstant(m, "SCRAM_MECHANISM_SHA_256", RD_KAFKA_SCRAM_MECHANISM_SHA_256);
        PyModule_AddIntConstant(m, "SCRAM_MECHANISM_SHA_512", RD_KAFKA_SCRAM_MECHANISM_SHA_512);
}

static void AdminTypes_AddObjectsIsolationLevel (PyObject *m) {
        /* rd_kafka_IsolationLevel_t */
        PyModule_AddIntConstant(m,"ISOLATION_LEVEL_READ_COMMITTED", RD_KAFKA_ISOLATION_LEVEL_READ_COMMITTED);
        PyModule_AddIntConstant(m,"ISOLATION_LEVEL_READ_UNCOMMITTED", RD_KAFKA_ISOLATION_LEVEL_READ_UNCOMMITTED);
}

static void AdminTypes_AddObjectsOffsetSpec (PyObject *m) {
        /* rd_kafka_OffsetSpec_t */
        PyModule_AddIntConstant(m,"OFFSET_SPEC_MAX_TIMESTAMP", RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP);
        PyModule_AddIntConstant(m,"OFFSET_SPEC_EARLIEST", RD_KAFKA_OFFSET_SPEC_EARLIEST);
        PyModule_AddIntConstant(m,"OFFSET_SPEC_LATEST", RD_KAFKA_OFFSET_SPEC_LATEST);
}

static void AdminTypes_AddObjectsElectionType(PyObject *m) {
        /* rd_kafka_ElectionType_t */
        PyModule_AddIntConstant(m, "ELECTION_TYPE_PREFERRED",
                                RD_KAFKA_ELECTION_TYPE_PREFERRED);
        PyModule_AddIntConstant(m, "ELECTION_TYPE_UNCLEAN",
                                RD_KAFKA_ELECTION_TYPE_UNCLEAN);
}

/**
 * @brief Add Admin types to module
 */
void AdminTypes_AddObjects (PyObject *m) {
        Py_INCREF(&NewTopicType);
        PyModule_AddObject(m, "NewTopic", (PyObject *)&NewTopicType);
        Py_INCREF(&NewPartitionsType);
        PyModule_AddObject(m, "NewPartitions", (PyObject *)&NewPartitionsType);

        AdminTypes_AddObjectsConfigSource(m);
        AdminTypes_AddObjectsResourceType(m);
        AdminTypes_AddObjectsResourcePatternType(m);
        AdminTypes_AddObjectsAclOperation(m);
        AdminTypes_AddObjectsAclPermissionType(m);
        AdminTypes_AddObjectsConsumerGroupStates(m);
        AdminTypes_AddObjectsConsumerGroupTypes(m);
        AdminTypes_AddObjectsAlterConfigOpType(m);
        AdminTypes_AddObjectsScramMechanismType(m);
        AdminTypes_AddObjectsIsolationLevel(m);
        AdminTypes_AddObjectsOffsetSpec(m);
        AdminTypes_AddObjectsElectionType(m);
}
