aiokafka
========
.. image:: https://github.com/aio-libs/aiokafka/actions/workflows/tests.yml/badge.svg?branch=master
    :target: https://github.com/aio-libs/aiokafka/actions/workflows/tests.yml?query=branch%3Amaster
    :alt: |Build status|
.. image:: https://codecov.io/github/aio-libs/aiokafka/coverage.svg?branch=master
    :target: https://codecov.io/gh/aio-libs/aiokafka/branch/master
    :alt: |Coverage|
.. image:: https://badges.gitter.im/Join%20Chat.svg
    :target: https://gitter.im/aio-libs/Lobby
    :alt: |Chat on Gitter|

asyncio client for Kafka


AIOKafkaProducer
****************

AIOKafkaProducer is a high-level, asynchronous message producer.

Example of AIOKafkaProducer usage:

.. code-block:: python

    from aiokafka import AIOKafkaProducer
    import asyncio

    async def send_one():
        producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
        # Get cluster layout and initial topic/partition leadership information
        await producer.start()
        try:
            # Produce message
            await producer.send_and_wait("my_topic", b"Super message")
        finally:
            # Wait for all pending messages to be delivered or expire.
            await producer.stop()

    asyncio.run(send_one())


AIOKafkaConsumer
****************

AIOKafkaConsumer is a high-level, asynchronous message consumer.
It interacts with the assigned Kafka Group Coordinator node to allow multiple
consumers to load balance consumption of topics (requires kafka >= 0.11).

Example of AIOKafkaConsumer usage:

.. code-block:: python

    from aiokafka import AIOKafkaConsumer
    import asyncio

    async def consume():
        consumer = AIOKafkaConsumer(
            'my_topic', 'my_other_topic',
            bootstrap_servers='localhost:9092',
            group_id="my-group")
        # Get cluster layout and join group `my-group`
        await consumer.start()
        try:
            # Consume messages
            async for msg in consumer:
                print("consumed: ", msg.topic, msg.partition, msg.offset,
                      msg.key, msg.value, msg.timestamp)
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await consumer.stop()

    asyncio.run(consume())


Documentation
-------------

https://aiokafka.readthedocs.io/


Running tests
-------------

Docker is required to run tests. See https://docs.docker.com/engine/installation for installation notes. Also note, that `lz4` compression libraries for python will require `python-dev` package,
or python source header files for compilation on Linux.
NOTE: You will also need a valid java installation. It's required for the ``keytool`` utility, used to
generate ssh keys for some tests.

Setting up tests requirements (assuming you're within virtualenv on ubuntu 14.04+)::

    sudo apt-get install -y libkrb5-dev krb5-user
    make setup

Running tests with coverage::

    make cov

To run tests with a specific version of Kafka (default one is 2.8.1) use KAFKA_VERSION variable::

    make cov SCALA_VERSION=2.11 KAFKA_VERSION=0.10.2.1

Test running cheat-sheet:

 * ``make test FLAGS="-l -x --ff"`` - run until 1 failure, rerun failed tests first. Great for cleaning up a lot of errors, say after a big refactor.
 * ``make test FLAGS="-k consumer"`` - run only the consumer tests.
 * ``make test FLAGS="-m 'not ssl'"`` - run tests excluding ssl.
 * ``make test FLAGS="--no-pull"`` - do not try to pull new docker image before test run.

