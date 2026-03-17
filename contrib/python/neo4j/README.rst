****************************
Neo4j Bolt Driver for Python
****************************

This repository contains the official Neo4j driver for Python.

Driver upgrades within a major version will never contain breaking API changes.

For version compatibility with Neo4j server, please refer to:
https://neo4j.com/developer/kb/neo4j-supported-versions/

+ Python 3.14 supported.
+ Python 3.13 supported.
+ Python 3.12 supported.
+ Python 3.11 supported.
+ Python 3.10 supported.


Installation
============

To install the latest stable version, use:

.. code:: bash

    pip install neo4j


.. TODO: 7.0 - remove this note

.. note::

    ``neo4j-driver`` is the old name for this package. It is now deprecated and
    and will receive no further updates starting with 6.0.0. Make sure to
    install ``neo4j`` as shown above.


Alternative Installation for Better Performance
-----------------------------------------------

You may want to have a look at the available Rust extensions for this driver
for better performance. The Rust extensions are not installed by default. For
more information, see `neo4j-rust-ext`_.

.. _neo4j-rust-ext: https://github.com/neo4j/neo4j-python-driver-rust-ext


Quick Example
=============

.. code-block:: python

    from neo4j import GraphDatabase, RoutingControl


    URI = "neo4j://localhost:7687"
    AUTH = ("neo4j", "password")


    def add_friend(driver, name, friend_name):
        driver.execute_query(
            "MERGE (a:Person {name: $name}) "
            "MERGE (friend:Person {name: $friend_name}) "
            "MERGE (a)-[:KNOWS]->(friend)",
            name=name, friend_name=friend_name, database_="neo4j",
        )


    def print_friends(driver, name):
        records, _, _ = driver.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
            "RETURN friend.name ORDER BY friend.name",
            name=name, database_="neo4j", routing_=RoutingControl.READ,
        )
        for record in records:
            print(record["friend.name"])


    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        add_friend(driver, "Arthur", "Guinevere")
        add_friend(driver, "Arthur", "Lancelot")
        add_friend(driver, "Arthur", "Merlin")
        print_friends(driver, "Arthur")


Further Information
===================

* `The Neo4j Operations Manual`_ (docs on how to run a Neo4j server)
* `The Neo4j Python Driver Manual`_ (good introduction to this driver)
* `Python Driver API Documentation`_ (full API documentation for this driver)
* `Neo4j Cypher Cheat Sheet`_ (summary of Cypher syntax - Neo4j's graph query language)
* `Example Project`_ (small web application using this driver)
* `GraphAcademy`_ (interactive, free online trainings for Neo4j)
* `Driver Wiki`_ (includes change logs)
* `Neo4j Migration Guide`_

.. _`The Neo4j Operations Manual`: https://neo4j.com/docs/operations-manual/current/
.. _`The Neo4j Python Driver Manual`: https://neo4j.com/docs/python-manual/current/
.. _`Python Driver API Documentation`: https://neo4j.com/docs/api/python-driver/current/
.. _`Neo4j Cypher Cheat Sheet`: https://neo4j.com/docs/cypher-cheat-sheet/
.. _`Example Project`: https://github.com/neo4j-examples/movies-python-bolt
.. _`GraphAcademy`: https://graphacademy.neo4j.com/categories/python/
.. _`Driver Wiki`: https://github.com/neo4j/neo4j-python-driver/wiki
.. _`Neo4j Migration Guide`: https://neo4j.com/docs/migration-guide/current/
