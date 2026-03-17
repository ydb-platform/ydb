import abc


class ConsumerRebalanceListener(abc.ABC):
    """
    A callback interface that the user can implement to trigger custom actions
    when the set of partitions assigned to the consumer changes.

    This is applicable when the consumer is having Kafka auto-manage group
    membership. If the consumer's directly assign partitions, those
    partitions will never be reassigned and this callback is not applicable.

    When Kafka is managing the group membership, a partition re-assignment will
    be triggered any time the members of the group changes or the subscription
    of the members changes. This can occur when processes die, new process
    instances are added or old instances come back to life after failure.
    Rebalances can also be triggered by changes affecting the subscribed
    topics (e.g. when then number of partitions is administratively adjusted).

    There are many uses for this functionality. One common use is saving
    offsets in a custom store. By saving offsets in the
    :meth:`on_partitions_revoked`, call we can ensure that any time partition
    assignment changes the offset gets saved.

    Another use is flushing out any kind of cache of intermediate results the
    consumer may be keeping. For example, consider a case where the consumer is
    subscribed to a topic containing user page views, and the goal is to count
    the number of page views per users for each five minute window.  Let's say
    the topic is partitioned by the user id so that all events for a particular
    user will go to a single consumer instance. The consumer can keep in memory
    a running tally of actions per user and only flush these out to a remote
    data store when its cache gets too big. However if a partition is
    reassigned it may want to automatically trigger a flush of this cache,
    before the new owner takes over consumption.

    This callback will execute during the rebalance process, and Consumer will
    wait for callbacks to finish before proceeding with group join.

    It is guaranteed that all consumer processes will invoke
    :meth:`on_partitions_revoked` prior to any process invoking
    :meth:`on_partitions_assigned`. So if offsets or other state is saved in the
    :meth:`on_partitions_revoked` call, it should be saved by the time the process
    taking over that partition has their :meth:`on_partitions_assigned` callback
    called to load the state.
    """

    @abc.abstractmethod
    def on_partitions_revoked(self, revoked):
        """
        A coroutine or function the user can implement to provide cleanup or
        custom state save on the start of a rebalance operation.

        This method will be called *before* a rebalance operation starts and
        *after* the consumer stops fetching data.

        If you are using manual commit you have to commit all consumed offsets
        here, to avoid duplicate message delivery after rebalance is finished.

        .. note:: This method is only called before rebalances. It is not
            called prior to :meth:`.AIOKafkaConsumer.stop`

        Arguments:
            revoked (list(TopicPartition)): the partitions that were assigned
                to the consumer on the last rebalance
        """

    @abc.abstractmethod
    def on_partitions_assigned(self, assigned):
        """
        A coroutine or function the user can implement to provide load of
        custom consumer state or cache warmup on completion of a successful
        partition re-assignment.

        This method will be called *after* partition re-assignment completes
        and *before* the consumer starts fetching data again.

        It is guaranteed that all the processes in a consumer group will
        execute their :meth:`on_partitions_revoked` callback before any instance
        executes its :meth:`on_partitions_assigned` callback.

        Arguments:
            assigned (list(TopicPartition)): the partitions assigned to the
                consumer (may include partitions that were previously assigned)
        """


class AbstractTokenProvider(abc.ABC):
    """
    A Token Provider must be used for the `SASL OAuthBearer`_ protocol.

    The implementation should ensure token reuse so that multiple
    calls at connect time do not create multiple tokens.
    The implementation should also periodically refresh the token in order to
    guarantee that each call returns an unexpired token.

    A timeout error should be returned after a short period of inactivity so
    that the broker can log debugging info and retry.

    Token Providers MUST implement the :meth:`token` method

    .. _SASL OAuthBearer:
        https://docs.confluent.io/platform/current/kafka/authentication_sasl/authentication_sasl_oauth.html
    """

    @abc.abstractmethod
    async def token(self):
        """
        An async callback returning a :class:`str` ID/Access Token to be sent to
        the Kafka client. In case where a synchronous callback is needed,
        implementations like following can be used:

        .. code-block:: python

            from aiokafka.abc import AbstractTokenProvider

            class CustomTokenProvider(AbstractTokenProvider):
                async def token(self):
                    return await asyncio.get_running_loop().run_in_executor(
                        None, self._token)

                def _token(self):
                    # The actual synchronous token callback.
        """

    def extensions(self):
        """
        This is an OPTIONAL method that may be implemented.

        Returns a map of key-value pairs that can be sent with the
        SASL/OAUTHBEARER initial client request. If not implemented, the values
        are ignored

        This feature is only available in Kafka >= 2.1.0.
        """
        return {}


__all__ = [
    "AbstractTokenProvider",
    "ConsumerRebalanceListener",
]
