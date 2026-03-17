# Copyright 2022 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Kafka admin client: create, view, alter, and delete topics and resources.
"""
import warnings
import concurrent.futures

# Unused imports are keeped to be accessible using this public module
from ._config import (ConfigSource,  # noqa: F401
                      ConfigEntry,
                      ConfigResource,
                      AlterConfigOpType)
from ._resource import (ResourceType,  # noqa: F401
                        ResourcePatternType)
from ._acl import (AclOperation,  # noqa: F401
                   AclPermissionType,
                   AclBinding,
                   AclBindingFilter)
from ._metadata import (BrokerMetadata,  # noqa: F401
                        ClusterMetadata,
                        GroupMember,
                        GroupMetadata,
                        PartitionMetadata,
                        TopicMetadata)
from ._group import (ConsumerGroupListing,  # noqa: F401
                     ListConsumerGroupsResult,
                     ConsumerGroupDescription,
                     MemberAssignment,
                     MemberDescription)
from ._scram import (UserScramCredentialAlteration,  # noqa: F401
                     UserScramCredentialUpsertion,
                     UserScramCredentialDeletion,
                     ScramCredentialInfo,
                     ScramMechanism,
                     UserScramCredentialsDescription)

from ._topic import (TopicDescription)  # noqa: F401

from ._cluster import (DescribeClusterResult)  # noqa: F401

from ._listoffsets import (OffsetSpec,  # noqa: F401
                           ListOffsetsResultInfo)

from ._records import DeletedRecords  # noqa: F401

from .._model import (TopicCollection as _TopicCollection,
                      ConsumerGroupType as _ConsumerGroupType,
                      ElectionType as _ElectionType)

from ..cimpl import (KafkaException,  # noqa: F401
                     KafkaError,
                     _AdminClientImpl,
                     NewTopic,
                     NewPartitions,
                     TopicPartition as _TopicPartition,
                     CONFIG_SOURCE_UNKNOWN_CONFIG,
                     CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG,
                     CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG,
                     CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG,
                     CONFIG_SOURCE_STATIC_BROKER_CONFIG,
                     CONFIG_SOURCE_DEFAULT_CONFIG,
                     RESOURCE_UNKNOWN,
                     RESOURCE_ANY,
                     RESOURCE_TOPIC,
                     RESOURCE_GROUP,
                     RESOURCE_BROKER,
                     RESOURCE_TRANSACTIONAL_ID,
                     OFFSET_INVALID)

from confluent_kafka import \
    ConsumerGroupTopicPartitions as _ConsumerGroupTopicPartitions, \
    ConsumerGroupState as _ConsumerGroupState, \
    IsolationLevel as _IsolationLevel


try:
    string_type = basestring
except NameError:
    string_type = str


class AdminClient (_AdminClientImpl):
    """
    AdminClient provides admin operations for Kafka brokers, topics, groups,
    and other resource types supported by the broker.

    The Admin API methods are asynchronous and return a dict of
    concurrent.futures.Future objects keyed by the entity.
    The entity is a topic name for create_topics(), delete_topics(), create_partitions(),
    and a ConfigResource for alter_configs() and describe_configs().

    All the futures for a single API call will currently finish/fail at
    the same time (backed by the same protocol request), but this might
    change in future versions of the client.

    See examples/adminapi.py for example usage.

    For more information see the `Java Admin API documentation
    <https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/clients/admin/package-frame.html>`_.

    Requires broker version v0.11.0.0 or later.
    """

    def __init__(self, conf, **kwargs):
        """
        Create a new AdminClient using the provided configuration dictionary.

        The AdminClient is a standard Kafka protocol client, supporting
        the standard librdkafka configuration properties as specified at
        https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

        :param dict conf: Configuration properties. At a minimum ``bootstrap.servers`` **should** be set\n"
        :param Logger logger: Optional Logger instance to use as a custom log messages handler.
        """
        super(AdminClient, self).__init__(conf, **kwargs)

    @staticmethod
    def _make_topics_result(f, futmap):
        """
        Map per-topic results to per-topic futures in futmap.
        The result value of each (successful) future is None.
        """
        try:
            result = f.result()
            for topic, error in result.items():
                fut = futmap.get(topic, None)
                if fut is None:
                    raise RuntimeError("Topic {} not found in future-map: {}".format(topic, futmap))

                if error is not None:
                    # Topic-level exception
                    fut.set_exception(KafkaException(error))
                else:
                    # Topic-level success
                    fut.set_result(None)
        except Exception as e:
            # Request-level exception, raise the same for all topics
            for topic, fut in futmap.items():
                fut.set_exception(e)

    @staticmethod
    def _make_resource_result(f, futmap):
        """
        Map per-resource results to per-resource futures in futmap.
        The result value of each (successful) future is a ConfigResource.
        """
        try:
            result = f.result()
            for resource, configs in result.items():
                fut = futmap.get(resource, None)
                if fut is None:
                    raise RuntimeError("Resource {} not found in future-map: {}".format(resource, futmap))
                if resource.error is not None:
                    # Resource-level exception
                    fut.set_exception(KafkaException(resource.error))
                else:
                    # Resource-level success
                    # configs will be a dict for describe_configs()
                    # and None for alter_configs()
                    fut.set_result(configs)
        except Exception as e:
            # Request-level exception, raise the same for all resources
            for resource, fut in futmap.items():
                fut.set_exception(e)

    @staticmethod
    def _make_list_consumer_groups_result(f, futmap):
        pass

    @staticmethod
    def _make_consumer_groups_result(f, futmap):
        """
        Map per-group results to per-group futures in futmap.
        """
        try:

            results = f.result()
            futmap_values = list(futmap.values())
            len_results = len(results)
            len_futures = len(futmap_values)
            if len_results != len_futures:
                raise RuntimeError(
                    "Results length {} is different from future-map length {}".format(len_results, len_futures))
            for i, result in enumerate(results):
                fut = futmap_values[i]
                if isinstance(result, KafkaError):
                    fut.set_exception(KafkaException(result))
                else:
                    fut.set_result(result)
        except Exception as e:
            # Request-level exception, raise the same for all groups
            for _, fut in futmap.items():
                fut.set_exception(e)

    @staticmethod
    def _make_consumer_group_offsets_result(f, futmap):
        """
        Map per-group results to per-group futures in futmap.
        The result value of each (successful) future is ConsumerGroupTopicPartitions.
        """
        try:

            results = f.result()
            futmap_values = list(futmap.values())
            len_results = len(results)
            len_futures = len(futmap_values)
            if len_results != len_futures:
                raise RuntimeError(
                    "Results length {} is different from future-map length {}".format(len_results, len_futures))
            for i, result in enumerate(results):
                fut = futmap_values[i]
                if isinstance(result, KafkaError):
                    fut.set_exception(KafkaException(result))
                else:
                    fut.set_result(result)
        except Exception as e:
            # Request-level exception, raise the same for all groups
            for _, fut in futmap.items():
                fut.set_exception(e)

    @staticmethod
    def _make_acls_result(f, futmap):
        """
        Map create ACL binding results to corresponding futures in futmap.
        For create_acls the result value of each (successful) future is None.
        For delete_acls the result value of each (successful) future is the list of deleted AclBindings.
        """
        try:
            results = f.result()
            futmap_values = list(futmap.values())
            len_results = len(results)
            len_futures = len(futmap_values)
            if len_results != len_futures:
                raise RuntimeError(
                    "Results length {} is different from future-map length {}".format(len_results, len_futures))
            for i, result in enumerate(results):
                fut = futmap_values[i]
                if isinstance(result, KafkaError):
                    fut.set_exception(KafkaException(result))
                else:
                    fut.set_result(result)
        except Exception as e:
            # Request-level exception, raise the same for all the AclBindings or AclBindingFilters
            for resource, fut in futmap.items():
                fut.set_exception(e)

    @staticmethod
    def _make_futmap_result_from_list(f, futmap):
        try:

            results = f.result()
            futmap_values = list(futmap.values())
            len_results = len(results)
            len_futures = len(futmap_values)
            if len_results != len_futures:
                raise RuntimeError(
                    "Results length {} is different from future-map length {}".format(len_results, len_futures))
            for i, result in enumerate(results):
                fut = futmap_values[i]
                if isinstance(result, KafkaError):
                    fut.set_exception(KafkaException(result))
                else:
                    fut.set_result(result)
        except Exception as e:
            # Request-level exception, raise the same for all topics
            for _, fut in futmap.items():
                fut.set_exception(e)

    @staticmethod
    def _make_futmap_result(f, futmap):
        try:
            results = f.result()
            len_results = len(results)
            len_futures = len(futmap)
            if len(results) != len_futures:
                raise RuntimeError(
                    f"Results length {len_results} is different from future-map length {len_futures}")
            for key, value in results.items():
                fut = futmap.get(key, None)
                if fut is None:
                    raise RuntimeError(
                        f"Key {key} not found in future-map: {futmap}")
                if isinstance(value, KafkaError):
                    fut.set_exception(KafkaException(value))
                else:
                    fut.set_result(value)
        except Exception as e:
            for _, fut in futmap.items():
                fut.set_exception(e)

    @staticmethod
    def _create_future():
        f = concurrent.futures.Future()
        if not f.set_running_or_notify_cancel():
            raise RuntimeError("Future was cancelled prematurely")
        return f

    @staticmethod
    def _make_futures(futmap_keys, class_check, make_result_fn):
        """
        Create futures and a futuremap for the keys in futmap_keys,
        and create a request-level future to be bassed to the C API.

        FIXME: use _make_futures_v2 with TypeError in next major release.
        """
        futmap = {}
        for key in futmap_keys:
            if class_check is not None and not isinstance(key, class_check):
                raise ValueError("Expected list of {}".format(repr(class_check)))
            futmap[key] = AdminClient._create_future()

        # Create an internal future for the entire request,
        # this future will trigger _make_..._result() and set result/exception
        # per topic,future in futmap.
        f = AdminClient._create_future()
        f.add_done_callback(lambda f: make_result_fn(f, futmap))

        return f, futmap

    @staticmethod
    def _make_futures_v2(futmap_keys, class_check, make_result_fn):
        """
        Create futures and a futuremap for the keys in futmap_keys,
        and create a request-level future to be bassed to the C API.
        """
        futmap = {}
        for key in futmap_keys:
            if class_check is not None and not isinstance(key, class_check):
                raise TypeError("Expected list of {}".format(repr(class_check)))
            futmap[key] = AdminClient._create_future()

        # Create an internal future for the entire request,
        # this future will trigger _make_..._result() and set result/exception
        # per topic,future in futmap.
        f = AdminClient._create_future()
        f.add_done_callback(lambda f: make_result_fn(f, futmap))

        return f, futmap

    @staticmethod
    def _make_single_future_pair():
        """
        Create an pair of futures, one for internal usage and one
        to use externally, the external one throws a KafkaException if
        any of the values in the map returned by the first future is
        a KafkaError.
        """
        def single_future_result(internal_f, f):
            try:
                results = internal_f.result()
                for _, value in results.items():
                    if isinstance(value, KafkaError):
                        f.set_exception(KafkaException(value))
                        return
                f.set_result(results)
            except Exception as e:
                f.set_exception(e)

        f = AdminClient._create_future()
        internal_f = AdminClient._create_future()
        internal_f.add_done_callback(lambda internal_f: single_future_result(internal_f, f))
        return internal_f, f

    @staticmethod
    def _has_duplicates(items):
        return len(set(items)) != len(items)

    @staticmethod
    def _check_list_consumer_group_offsets_request(request):
        if request is None:
            raise TypeError("request cannot be None")
        if not isinstance(request, list):
            raise TypeError("request must be a list")
        if len(request) != 1:
            raise ValueError("Currently we support listing offsets for a single consumer group only")
        for req in request:
            if not isinstance(req, _ConsumerGroupTopicPartitions):
                raise TypeError("Expected list of 'ConsumerGroupTopicPartitions'")

            if req.group_id is None:
                raise TypeError("'group_id' cannot be None")
            if not isinstance(req.group_id, string_type):
                raise TypeError("'group_id' must be a string")
            if not req.group_id:
                raise ValueError("'group_id' cannot be empty")

            if req.topic_partitions is not None:
                if not isinstance(req.topic_partitions, list):
                    raise TypeError("'topic_partitions' must be a list or None")
                if len(req.topic_partitions) == 0:
                    raise ValueError("'topic_partitions' cannot be empty")
                for topic_partition in req.topic_partitions:
                    if topic_partition is None:
                        raise ValueError("Element of 'topic_partitions' cannot be None")
                    if not isinstance(topic_partition, _TopicPartition):
                        raise TypeError("Element of 'topic_partitions' must be of type TopicPartition")
                    if topic_partition.topic is None:
                        raise TypeError("Element of 'topic_partitions' must not have 'topic' attribute as None")
                    if not topic_partition.topic:
                        raise ValueError("Element of 'topic_partitions' must not have 'topic' attribute as Empty")
                    if topic_partition.partition < 0:
                        raise ValueError("Element of 'topic_partitions' must not have negative 'partition' value")
                    if topic_partition.offset != OFFSET_INVALID:
                        raise ValueError("Element of 'topic_partitions' must not have 'offset' value")

    @staticmethod
    def _check_alter_consumer_group_offsets_request(request):
        if request is None:
            raise TypeError("request cannot be None")
        if not isinstance(request, list):
            raise TypeError("request must be a list")
        if len(request) != 1:
            raise ValueError("Currently we support altering offsets for a single consumer group only")
        for req in request:
            if not isinstance(req, _ConsumerGroupTopicPartitions):
                raise TypeError("Expected list of 'ConsumerGroupTopicPartitions'")
            if req.group_id is None:
                raise TypeError("'group_id' cannot be None")
            if not isinstance(req.group_id, string_type):
                raise TypeError("'group_id' must be a string")
            if not req.group_id:
                raise ValueError("'group_id' cannot be empty")
            if req.topic_partitions is None:
                raise ValueError("'topic_partitions' cannot be null")
            if not isinstance(req.topic_partitions, list):
                raise TypeError("'topic_partitions' must be a list")
            if len(req.topic_partitions) == 0:
                raise ValueError("'topic_partitions' cannot be empty")
            for topic_partition in req.topic_partitions:
                if topic_partition is None:
                    raise ValueError("Element of 'topic_partitions' cannot be None")
                if not isinstance(topic_partition, _TopicPartition):
                    raise TypeError("Element of 'topic_partitions' must be of type TopicPartition")
                if topic_partition.topic is None:
                    raise TypeError("Element of 'topic_partitions' must not have 'topic' attribute as None")
                if not topic_partition.topic:
                    raise ValueError("Element of 'topic_partitions' must not have 'topic' attribute as Empty")
                if topic_partition.partition < 0:
                    raise ValueError(
                        "Element of 'topic_partitions' must not have negative value for 'partition' field")
                if topic_partition.offset < 0:
                    raise ValueError(
                        "Element of 'topic_partitions' must not have negative value for 'offset' field")

    @staticmethod
    def _check_describe_user_scram_credentials_request(users):
        if users is None:
            return
        if not isinstance(users, list):
            raise TypeError("Expected input to be list of String")
        for user in users:
            if user is None:
                raise TypeError("'user' cannot be None")
            if not isinstance(user, string_type):
                raise TypeError("Each value should be a string")
            if not user:
                raise ValueError("'user' cannot be empty")

    @staticmethod
    def _check_alter_user_scram_credentials_request(alterations):
        if not isinstance(alterations, list):
            raise TypeError("Expected input to be list")
        if len(alterations) == 0:
            raise ValueError("Expected at least one alteration")
        for alteration in alterations:
            if not isinstance(alteration, UserScramCredentialAlteration):
                raise TypeError("Expected each element of list to be subclass of UserScramCredentialAlteration")
            if alteration.user is None:
                raise TypeError("'user' cannot be None")
            if not isinstance(alteration.user, string_type):
                raise TypeError("'user' must be a string")
            if not alteration.user:
                raise ValueError("'user' cannot be empty")

            if isinstance(alteration, UserScramCredentialUpsertion):
                if alteration.password is None:
                    raise TypeError("'password' cannot be None")
                if not isinstance(alteration.password, bytes):
                    raise TypeError("'password' must be bytes")
                if not alteration.password:
                    raise ValueError("'password' cannot be empty")

                if alteration.salt is not None and not alteration.salt:
                    raise ValueError("'salt' can be None but cannot be empty")
                if alteration.salt and not isinstance(alteration.salt, bytes):
                    raise TypeError("'salt' must be bytes")

                if not isinstance(alteration.scram_credential_info, ScramCredentialInfo):
                    raise TypeError("Expected credential_info to be ScramCredentialInfo Type")
                if alteration.scram_credential_info.iterations < 1:
                    raise ValueError("Iterations should be positive")
                if not isinstance(alteration.scram_credential_info.mechanism, ScramMechanism):
                    raise TypeError("Expected the mechanism to be ScramMechanism Type")
            elif isinstance(alteration, UserScramCredentialDeletion):
                if not isinstance(alteration.mechanism, ScramMechanism):
                    raise TypeError("Expected the mechanism to be ScramMechanism Type")
            else:
                raise TypeError("Expected each element of list 'alterations' " +
                                "to be either a UserScramCredentialUpsertion or a " +
                                "UserScramCredentialDeletion")

    @staticmethod
    def _check_list_offsets_request(topic_partition_offsets, kwargs):
        if not isinstance(topic_partition_offsets, dict):
            raise TypeError("Expected topic_partition_offsets to be " +
                            "dict of [TopicPartitions,OffsetSpec] for list offsets request")

        for topic_partition, offset_spec in topic_partition_offsets.items():
            if topic_partition is None:
                raise TypeError("partition cannot be None")
            if not isinstance(topic_partition, _TopicPartition):
                raise TypeError("partition must be a TopicPartition")
            if topic_partition.topic is None:
                raise TypeError("partition topic name cannot be None")
            if not isinstance(topic_partition.topic, string_type):
                raise TypeError("partition topic name must be string")
            if not topic_partition.topic:
                raise ValueError("partition topic name cannot be empty")
            if topic_partition.partition < 0:
                raise ValueError("partition index must be non-negative")
            if offset_spec is None:
                raise TypeError("OffsetSpec cannot be None")
            if not isinstance(offset_spec, OffsetSpec):
                raise TypeError("Value must be a OffsetSpec")

        if 'isolation_level' in kwargs:
            if not isinstance(kwargs['isolation_level'], _IsolationLevel):
                raise TypeError("isolation_level argument should be an IsolationLevel")

    @staticmethod
    def _check_delete_records(request):
        if not isinstance(request, list):
            raise TypeError(f"Expected Request to be a list, got '{type(request).__name__}' ")
        for req in request:
            if not isinstance(req, _TopicPartition):
                raise TypeError("Element of the request list must be of type 'TopicPartition'" +
                                f" got '{type(req).__name__}' ")
            if req.partition < 0:
                raise ValueError("'partition' cannot be negative")

    @staticmethod
    def _check_elect_leaders(election_type, partitions):
        if not isinstance(election_type, _ElectionType):
            raise TypeError("Expected 'election_type' to be of type 'ElectionType'")
        if partitions is not None:
            if not isinstance(partitions, list):
                raise TypeError("Expected 'partitions' to be a list, got " +
                                f"'{type(partitions).__name__}'")
            for partition in partitions:
                if not isinstance(partition, _TopicPartition):
                    raise TypeError("Element of the 'partitions' list must be of type 'TopicPartition'" +
                                    f" got '{type(partition).__name__}' ")
                if partition.partition < 0:
                    raise ValueError("Elements of the 'partitions' list must not have negative value" +
                                     " for 'partition' field")

    def create_topics(self, new_topics, **kwargs):
        """
        Create one or more new topics.

        :param list(NewTopic) new_topics: A list of specifictions (NewTopic) for
                  the topics that should be created.
        :param float operation_timeout: The operation timeout in seconds,
                  controlling how long the CreateTopics request will block
                  on the broker waiting for the topic creation to propagate
                  in the cluster. A value of 0 returns immediately.
                  Default: `socket.timeout.ms/1000.0`
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`
        :param bool validate_only: If true, the request is only validated
                  without creating the topic. Default: False

        :returns: A dict of futures for each topic, keyed by the topic name.
                  The future result() method returns None.

        :rtype: dict(<topic_name, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f, futmap = AdminClient._make_futures([x.topic for x in new_topics],
                                              None,
                                              AdminClient._make_topics_result)

        super(AdminClient, self).create_topics(new_topics, f, **kwargs)

        return futmap

    def delete_topics(self, topics, **kwargs):
        """
        Delete one or more topics.

        :param list(str) topics: A list of topics to mark for deletion.
        :param float operation_timeout: The operation timeout in seconds,
                  controlling how long the DeleteTopics request will block
                  on the broker waiting for the topic deletion to propagate
                  in the cluster. A value of 0 returns immediately.
                  Default: `socket.timeout.ms/1000.0`
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures for each topic, keyed by the topic name.
                  The future result() method returns None.

        :rtype: dict(<topic_name, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f, futmap = AdminClient._make_futures(topics, None,
                                              AdminClient._make_topics_result)

        super(AdminClient, self).delete_topics(topics, f, **kwargs)

        return futmap

    def list_topics(self, *args, **kwargs):

        return super(AdminClient, self).list_topics(*args, **kwargs)

    def list_groups(self, *args, **kwargs):

        return super(AdminClient, self).list_groups(*args, **kwargs)

    def create_partitions(self, new_partitions, **kwargs):
        """
        Create additional partitions for the given topics.

        :param list(NewPartitions) new_partitions: New partitions to be created.
        :param float operation_timeout: The operation timeout in seconds,
                  controlling how long the CreatePartitions request will block
                  on the broker waiting for the partition creation to propagate
                  in the cluster. A value of 0 returns immediately.
                  Default: `socket.timeout.ms/1000.0`
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`
        :param bool validate_only: If true, the request is only validated
                  without creating the partitions. Default: False

        :returns: A dict of futures for each topic, keyed by the topic name.
                  The future result() method returns None.

        :rtype: dict(<topic_name, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f, futmap = AdminClient._make_futures([x.topic for x in new_partitions],
                                              None,
                                              AdminClient._make_topics_result)

        super(AdminClient, self).create_partitions(new_partitions, f, **kwargs)

        return futmap

    def describe_configs(self, resources, **kwargs):
        """
        Get the configuration of the specified resources.

        :warning: Multiple resources and resource types may be requested,
                  but at most one resource of type RESOURCE_BROKER is allowed
                  per call since these resource requests must be sent to the
                  broker specified in the resource.

        :param list(ConfigResource) resources: Resources to get the configuration for.
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures for each resource, keyed by the ConfigResource.
                  The type of the value returned by the future result() method is
                  dict(<configname, ConfigEntry>).

        :rtype: dict(<ConfigResource, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f, futmap = AdminClient._make_futures(resources, ConfigResource,
                                              AdminClient._make_resource_result)

        super(AdminClient, self).describe_configs(resources, f, **kwargs)

        return futmap

    def alter_configs(self, resources, **kwargs):
        """
        .. deprecated:: 2.2.0

        Update configuration properties for the specified resources.
        Updates are not transactional so they may succeed for a subset
        of the provided resources while the others fail.
        The configuration for a particular resource is updated atomically,
        replacing the specified values while reverting unspecified configuration
        entries to their default values.

        :warning: alter_configs() will replace all existing configuration for
                  the provided resources with the new configuration given,
                  reverting all other configuration for the resource back
                  to their default values.

        :warning: Multiple resources and resource types may be specified,
                  but at most one resource of type RESOURCE_BROKER is allowed
                  per call since these resource requests must be sent to the
                  broker specified in the resource.

        :param list(ConfigResource) resources: Resources to update configuration of.
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`.
        :param bool validate_only: If true, the request is validated only,
                  without altering the configuration. Default: False

        :returns: A dict of futures for each resource, keyed by the ConfigResource.
                  The future result() method returns None or throws a KafkaException.

        :rtype: dict(<ConfigResource, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeError: Invalid type.
        :raises ValueError: Invalid value.
        """
        warnings.warn(
            "alter_configs has been deprecated. Use incremental_alter_configs instead.",
            category=DeprecationWarning, stacklevel=2)

        f, futmap = AdminClient._make_futures(resources, ConfigResource,
                                              AdminClient._make_resource_result)

        super(AdminClient, self).alter_configs(resources, f, **kwargs)

        return futmap

    def incremental_alter_configs(self, resources, **kwargs):
        """
        Update configuration properties for the specified resources.
        Updates are incremental, i.e only the values mentioned are changed
        and rest remain as is.

        :param list(ConfigResource) resources: Resources to update configuration of.
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`.
        :param bool validate_only: If true, the request is validated only,
                  without altering the configuration. Default: False
        :param int broker: Broker id to send the request to. When
                  altering broker configurations, it's ignored because
                  the request needs to go to that broker only.
                  Default: controller broker.

        :returns: A dict of futures for each resource, keyed by the ConfigResource.
                  The future result() method returns None or throws a KafkaException.

        :rtype: dict(<ConfigResource, future>)

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeError: Invalid type.
        :raises ValueError: Invalid value.
        """
        f, futmap = AdminClient._make_futures_v2(resources, ConfigResource,
                                                 AdminClient._make_resource_result)

        super(AdminClient, self).incremental_alter_configs(resources, f, **kwargs)

        return futmap

    def create_acls(self, acls, **kwargs):
        """
        Create one or more ACL bindings.

        :param list(AclBinding) acls: A list of unique ACL binding specifications (:class:`.AclBinding`)
                         to create.
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures for each ACL binding, keyed by the :class:`AclBinding` object.
                  The future result() method returns None on success.

        :rtype: dict[AclBinding, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """
        if AdminClient._has_duplicates(acls):
            raise ValueError("duplicate ACL bindings not allowed")

        f, futmap = AdminClient._make_futures(acls, AclBinding,
                                              AdminClient._make_acls_result)

        super(AdminClient, self).create_acls(acls, f, **kwargs)

        return futmap

    def describe_acls(self, acl_binding_filter, **kwargs):
        """
        Match ACL bindings by filter.

        :param AclBindingFilter acl_binding_filter: a filter with attributes that
                  must match.
                  String attributes match exact values or any string if set to None.
                  Enums attributes match exact values or any value if equal to `ANY`.
                  If :class:`ResourcePatternType` is set to :attr:`ResourcePatternType.MATCH`
                  returns ACL bindings with:
                  :attr:`ResourcePatternType.LITERAL` pattern type with resource name equal
                  to the given resource name;
                  :attr:`ResourcePatternType.LITERAL` pattern type with wildcard resource name
                  that matches the given resource name;
                  :attr:`ResourcePatternType.PREFIXED` pattern type with resource name
                  that is a prefix of the given resource name
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A future returning a list(:class:`AclBinding`) as result

        :rtype: future

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        f = AdminClient._create_future()

        super(AdminClient, self).describe_acls(acl_binding_filter, f, **kwargs)

        return f

    def delete_acls(self, acl_binding_filters, **kwargs):
        """
        Delete ACL bindings matching one or more ACL binding filters.

        :param list(AclBindingFilter) acl_binding_filters: a list of unique ACL binding filters
                  to match ACLs to delete.
                  String attributes match exact values or any string if set to None.
                  Enums attributes match exact values or any value if equal to `ANY`.
                  If :class:`ResourcePatternType` is set to :attr:`ResourcePatternType.MATCH`
                  deletes ACL bindings with:
                  :attr:`ResourcePatternType.LITERAL` pattern type with resource name
                  equal to the given resource name;
                  :attr:`ResourcePatternType.LITERAL` pattern type with wildcard resource name
                  that matches the given resource name;
                  :attr:`ResourcePatternType.PREFIXED` pattern type with resource name
                  that is a prefix of the given resource name
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures for each ACL binding filter, keyed by the :class:`AclBindingFilter` object.
                  The future result() method returns a list of :class:`AclBinding`.

        :rtype: dict[AclBindingFilter, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """
        if AdminClient._has_duplicates(acl_binding_filters):
            raise ValueError("duplicate ACL binding filters not allowed")

        f, futmap = AdminClient._make_futures(acl_binding_filters, AclBindingFilter,
                                              AdminClient._make_acls_result)

        super(AdminClient, self).delete_acls(acl_binding_filters, f, **kwargs)

        return futmap

    def list_consumer_groups(self, **kwargs):
        """
        List consumer groups.

        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`
        :param set(ConsumerGroupState) states: only list consumer groups which are currently in
                  these states.
        :param set(ConsumerGroupType) types: only list consumer groups of
                  these types.

        :returns: a future. Result method of the future returns :class:`ListConsumerGroupsResult`.

        :rtype: future

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """
        if "states" in kwargs:
            states = kwargs["states"]
            if states is not None:
                if not isinstance(states, set):
                    raise TypeError("'states' must be a set")
                for state in states:
                    if not isinstance(state, _ConsumerGroupState):
                        raise TypeError("All elements of states must be of type ConsumerGroupState")
                kwargs["states_int"] = [state.value for state in states]
            kwargs.pop("states")
        if "types" in kwargs:
            types = kwargs["types"]
            if types is not None:
                if not isinstance(types, set):
                    raise TypeError("'types' must be a set")
                for type in types:
                    if not isinstance(type, _ConsumerGroupType):
                        raise TypeError("All elements of types must be of type ConsumerGroupType")
                kwargs["types_int"] = [type.value for type in types]
            kwargs.pop("types")

        f, _ = AdminClient._make_futures([], None, AdminClient._make_list_consumer_groups_result)

        super(AdminClient, self).list_consumer_groups(f, **kwargs)

        return f

    def describe_consumer_groups(self, group_ids, **kwargs):
        """
        Describe consumer groups.

        :param list(str) group_ids: List of group_ids which need to be described.
        :param bool include_authorized_operations: If True, fetches group AclOperations. Default: False
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures for each group, keyed by the group_id.
                  The future result() method returns :class:`ConsumerGroupDescription`.

        :rtype: dict[str, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        if not isinstance(group_ids, list):
            raise TypeError("Expected input to be list of group ids to be described")

        if len(group_ids) == 0:
            raise ValueError("Expected at least one group to be described")

        f, futmap = AdminClient._make_futures(group_ids, None,
                                              AdminClient._make_consumer_groups_result)

        super(AdminClient, self).describe_consumer_groups(group_ids, f, **kwargs)

        return futmap

    def describe_topics(self, topics, **kwargs):
        """
        Describe topics.

        :param TopicCollection topics: Collection of list of topic names to describe.
        :param bool include_authorized_operations: If True, fetches topic AclOperations. Default: False
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures for each topic, keyed by the topic.
                  The future result() method returns :class:`TopicDescription`.

        :rtype: dict[str, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeError: Invalid input type.
        :raises ValueError: Invalid input value.
        """

        if not isinstance(topics, _TopicCollection):
            raise TypeError("Expected input to be instance of TopicCollection")

        topic_names = topics.topic_names

        if not isinstance(topic_names, list):
            raise TypeError("Expected list of topic names to be described")

        f, futmap = AdminClient._make_futures_v2(topic_names, None,
                                                 AdminClient._make_futmap_result_from_list)

        super(AdminClient, self).describe_topics(topic_names, f, **kwargs)

        return futmap

    def describe_cluster(self, **kwargs):
        """
        Describe cluster.

        :param bool include_authorized_operations: If True, fetches topic AclOperations. Default: False
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A future returning description of the cluster as result

        :rtype: future containing the description of the cluster in result.

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeError: Invalid input type.
        :raises ValueError: Invalid input value.
        """

        f = AdminClient._create_future()

        super(AdminClient, self).describe_cluster(f, **kwargs)

        return f

    def delete_consumer_groups(self, group_ids, **kwargs):
        """
        Delete the given consumer groups.

        :param list(str) group_ids: List of group_ids which need to be deleted.
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures for each group, keyed by the group_id.
                  The future result() method returns None.

        :rtype: dict[str, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeError: Invalid input type.
        :raises ValueError: Invalid input value.
        """
        if not isinstance(group_ids, list):
            raise TypeError("Expected input to be list of group ids to be deleted")

        if len(group_ids) == 0:
            raise ValueError("Expected at least one group to be deleted")

        f, futmap = AdminClient._make_futures(group_ids, string_type, AdminClient._make_consumer_groups_result)

        super(AdminClient, self).delete_consumer_groups(group_ids, f, **kwargs)

        return futmap

    def list_consumer_group_offsets(self, list_consumer_group_offsets_request, **kwargs):
        """
        List offset information for the consumer group and (optional) topic partition provided in the request.

        :note: Currently, the API supports only a single group.

        :param list(ConsumerGroupTopicPartitions) list_consumer_group_offsets_request: List of
                    :class:`ConsumerGroupTopicPartitions` which consist of group name and topic
                    partition information for which offset detail is expected. If only group name is
                    provided, then offset information of all the topic and partition associated with
                    that group is returned.
        :param bool require_stable: If True, fetches stable offsets. Default: False
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures for each group, keyed by the group id.
                  The future result() method returns :class:`ConsumerGroupTopicPartitions`.

        :rtype: dict[str, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        AdminClient._check_list_consumer_group_offsets_request(list_consumer_group_offsets_request)

        f, futmap = AdminClient._make_futures([request.group_id for request in list_consumer_group_offsets_request],
                                              string_type,
                                              AdminClient._make_consumer_group_offsets_result)

        super(AdminClient, self).list_consumer_group_offsets(list_consumer_group_offsets_request, f, **kwargs)

        return futmap

    def alter_consumer_group_offsets(self, alter_consumer_group_offsets_request, **kwargs):
        """
        Alter offset for the consumer group and topic partition provided in the request.

        :note: Currently, the API supports only a single group.

        :param list(ConsumerGroupTopicPartitions) alter_consumer_group_offsets_request: List of
                    :class:`ConsumerGroupTopicPartitions` which consist of group name and topic
                    partition; and corresponding offset to be updated.
        :param float request_timeout: The overall request timeout in seconds,
                  including broker lookup, request transmission, operation time
                  on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures for each group, keyed by the group id.
                  The future result() method returns :class:`ConsumerGroupTopicPartitions`.

        :rtype: dict[ConsumerGroupTopicPartitions, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        :raises ValueException: Invalid input.
        """

        AdminClient._check_alter_consumer_group_offsets_request(alter_consumer_group_offsets_request)

        f, futmap = AdminClient._make_futures([request.group_id for request in alter_consumer_group_offsets_request],
                                              string_type,
                                              AdminClient._make_consumer_group_offsets_result)

        super(AdminClient, self).alter_consumer_group_offsets(alter_consumer_group_offsets_request, f, **kwargs)

        return futmap

    def set_sasl_credentials(self, username, password):
        """
        Sets the SASL credentials used for this client.
        These credentials will overwrite the old ones, and will be used the
        next time the client needs to authenticate.
        This method will not disconnect existing broker connections that
        have been established with the old credentials.
        This method is applicable only to SASL PLAIN and SCRAM mechanisms.

        :param str username: The username to set.
        :param str password: The password to set.

        :rtype: None

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeException: Invalid input.
        """
        super(AdminClient, self).set_sasl_credentials(username, password)

    def describe_user_scram_credentials(self, users=None, **kwargs):
        """
        Describe user SASL/SCRAM credentials.

        :param list(str) users: List of user names to describe.
               Duplicate users aren't allowed. Can be None
               to describe all user's credentials.
        :param float request_timeout: The overall request timeout in seconds,
               including broker lookup, request transmission, operation time
               on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: In case None is passed it returns a single future.
                  The future yields a dict[str, UserScramCredentialsDescription]
                  or raises a KafkaException

                  In case a list of user names is passed, it returns
                  a dict[str, future[UserScramCredentialsDescription]].
                  The futures yield a :class:`UserScramCredentialsDescription`
                  or raise a KafkaException

        :rtype: Union[future[dict[str, UserScramCredentialsDescription]],
                      dict[str, future[UserScramCredentialsDescription]]]

        :raises TypeError: Invalid input type.
        :raises ValueError: Invalid input value.
        """
        AdminClient._check_describe_user_scram_credentials_request(users)

        if users is None:
            internal_f, ret_fut = AdminClient._make_single_future_pair()
        else:
            internal_f, ret_fut = AdminClient._make_futures_v2(users, None,
                                                               AdminClient._make_futmap_result)
        super(AdminClient, self).describe_user_scram_credentials(users, internal_f, **kwargs)
        return ret_fut

    def alter_user_scram_credentials(self, alterations, **kwargs):
        """
        Alter user SASL/SCRAM credentials.

        :param list(UserScramCredentialAlteration) alterations: List of
               :class:`UserScramCredentialAlteration` to apply.
               The pair (user, mechanism) must be unique among alterations.
        :param float request_timeout: The overall request timeout in seconds,
               including broker lookup, request transmission, operation time
               on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures keyed by user name.
                  The future result() method returns None or
                  raises KafkaException

        :rtype: dict[str, future]

        :raises TypeError: Invalid input type.
        :raises ValueError: Invalid input value.
        """
        AdminClient._check_alter_user_scram_credentials_request(alterations)

        f, futmap = AdminClient._make_futures_v2(set([alteration.user for alteration in alterations]), None,
                                                 AdminClient._make_futmap_result)

        super(AdminClient, self).alter_user_scram_credentials(alterations, f, **kwargs)
        return futmap

    def list_offsets(self, topic_partition_offsets, **kwargs):
        """
        Enables to find the beginning offset,
        end offset as well as the offset matching a timestamp
        or the offset with max timestamp in partitions.

        :param dict([TopicPartition, OffsetSpec]) topic_partition_offsets: Dictionary of
               TopicPartition objects associated with the corresponding OffsetSpec to query for.
        :param IsolationLevel isolation_level: The isolation level to use when
               querying.
        :param float request_timeout: The overall request timeout in seconds,
               including broker lookup, request transmission, operation time
               on broker, and response. Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures keyed by TopicPartition.
                  The future result() method returns ListOffsetsResultInfo
                  raises KafkaException

        :rtype: dict[TopicPartition, future]

        :raises TypeError: Invalid input type.
        :raises ValueError: Invalid input value.
        """
        AdminClient._check_list_offsets_request(topic_partition_offsets, kwargs)

        if 'isolation_level' in kwargs:
            kwargs['isolation_level_value'] = kwargs['isolation_level'].value
            del kwargs['isolation_level']

        topic_partition_offsets_list = [
            _TopicPartition(topic_partition.topic, int(topic_partition.partition),
                            int(offset_spec._value))
            for topic_partition, offset_spec in topic_partition_offsets.items()]

        f, futmap = AdminClient._make_futures_v2(topic_partition_offsets_list,
                                                 _TopicPartition,
                                                 AdminClient._make_futmap_result)

        super(AdminClient, self).list_offsets(topic_partition_offsets_list, f, **kwargs)
        return futmap

    def delete_records(self, topic_partition_offsets, **kwargs):
        """
        Deletes all the records before the specified offsets (not including),
        in the specified topics and partitions.

        :param list(TopicPartition) topic_partition_offsets: A list of
               :class:`.TopicPartition` objects having `offset` field set to the offset
               before which all the records should be deleted.
               `offset` can be set to :py:const:`OFFSET_END` (-1) to delete all records
               in the partition.
        :param float request_timeout: The overall request timeout in seconds,
               including broker lookup, request transmission, operation time
               on broker, and response. Default: `socket.timeout.ms/1000.0`
        :param float operation_timeout: The operation timeout in seconds,
               controlling how long the `delete_records` request will block
               on the broker waiting for the record deletion to propagate
               in the cluster. A value of 0 returns immediately.
               Default: `socket.timeout.ms/1000.0`

        :returns: A dict of futures keyed by the :class:`.TopicPartition`.
                  The future result() method returns :class:`.DeletedRecords`
                  or raises :class:`.KafkaException`

        :rtype: dict[TopicPartition, future]

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeError: Invalid input type.
        :raises ValueError: Invalid input value.
        """
        AdminClient._check_delete_records(topic_partition_offsets)

        f, futmap = AdminClient._make_futures_v2(
            topic_partition_offsets, _TopicPartition, AdminClient._make_futmap_result)

        super(AdminClient, self).delete_records(topic_partition_offsets, f, **kwargs)
        return futmap

    def elect_leaders(self, election_type, partitions=None, **kwargs):
        """
        Perform Preferred or Unclean leader election for
        all the specified partitions or all partitions in the cluster.

        :param ElectionType election_type: The type of election to perform.
        :param List[TopicPartition]|None partitions: The topic partitions to perform
               the election on. Use ``None`` to perform on all the topic partitions.
        :param float request_timeout: The overall request timeout in seconds,
               including broker lookup, request transmission, operation time
               on broker, and response. Default: `socket.timeout.ms*1000.0`
        :param float operation_timeout: The operation timeout in seconds,
               controlling how long the 'elect_leaders' request will block
               on the broker waiting for the election to propagate
               in the cluster. A value of 0 returns immediately.
               Default: `socket.timeout.ms/1000.0`

        :returns: A future. Method result() of the future returns
                  dict[TopicPartition, KafkaException|None].

        :rtype: future

        :raises KafkaException: Operation failed locally or on broker.
        :raises TypeError: Invalid input type.
        :raises ValueError: Invalid input value.
        """

        AdminClient._check_elect_leaders(election_type, partitions)

        f = AdminClient._create_future()

        super(AdminClient, self).elect_leaders(election_type.value, partitions, f, **kwargs)

        return f
