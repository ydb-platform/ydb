aiographite
===========

.. image:: https://travis-ci.org/zillow/aiographite.svg?branch=master
    :alt: build status
    :target: https://travis-ci.org/zillow/aiographite

.. image:: https://coveralls.io/repos/github/zillow/aiographite/badge.svg?branch=master
    :alt: coverage status
    :target: https://coveralls.io/github/zillow/aiographite?branch=master


An asyncio library for graphite.

You can find out more here:

http://aiographite.readthedocs.io/en/latest/


---------------------
What is aiographite ?
---------------------

aiographite is Python3 library ultilizing asyncio, designed
to help Graphite users to send data into graphite easily.


----------------------
Installing it globally
----------------------

You can install aiographite globally with any Python package manager:

.. code::

    pip install aiographite


----------------------
Quick start
----------------------

Let's get started.

.. code::

    from aiographite import connect
    from aiographite.protocol import PlaintextProtocol

    """
      Initialize a aiographite instance
    """
    loop = asyncio.get_event_loop()
    plaintext_protocol = PlaintextProtocol()
    graphite_conn = await connect(*httpd.address, plaintext_protocol, loop=loop)


    """
      Send a tuple (metric, value , timestamp)
    """
    await graphite_conn.send(metric, value, timestamp)


    """
      Send a list of tuples List[(metric, value , timestamp)]
    """
    await graphite_conn.send_multiple(list)


    """
      aiographite library also provides GraphiteEncoder module,
      which helps users to send valid metric name to graphite.
      For Example: (metric_parts, value ,timestamp)
    """
    metric = graphite_conn.clean_and_join_metric_parts(metric_parts)
    await graphite_conn.send(metric, value, timestamp)


    """
      Close connection
    """
    await graphite_conn.close()


----------------------
Example
----------------------

A simple example.

.. code::

    from aiographite.protocol import PlaintextProtocol
    from aiographite import connect
    import time
    import asyncio


    LOOP = asyncio.get_event_loop()
    SERVER = '127.0.0.1'
    PORT = 2003


    async def test_send_data():
      # Initiazlize an aiographite instance
      plaintext_protocol = PlaintextProtocol()
      graphite_conn = await connect(SERVER, PORT, plaintext_protocol, loop=LOOP)

      # Send data
      timestamp = time.time()
      for i in range(10):
        await graphite_conn.send("yun_test.aiographite", i, timestamp + 60 * i)))


    def main():
      LOOP.run_until_complete(test_send_data())
      LOOP.close()


    if __name__ == '__main__':
      main()


----------------------
Development
----------------------

Run unit tests.

.. code::

    ./uranium test


----------------------
Graphite setup
----------------------

Do not have graphite instances ? Set up a graphite instance on your local machine!

Please refer:

* https://github.com/yunstanford/MyGraphite
* https://github.com/yunstanford/GraphiteSetup
