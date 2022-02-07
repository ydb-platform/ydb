ydb
===

.. toctree::
   :caption: Contents:


.. module:: ydb

Module contents
---------------

Driver
^^^^^^

Driver object
~~~~~~~~~~~~~

.. autoclass:: Driver
    :members:
    :inherited-members:
    :undoc-members:


DriverConfig
~~~~~~~~~~~~

.. autoclass:: DriverConfig
    :members:
    :inherited-members:
    :undoc-members:
    :exclude-members: database, ca_cert, channel_options, secure_channel, endpoint, endpoints, credentials, use_all_nodes, root_certificates, certificate_chain, private_key, grpc_keep_alive_timeout, table_client_settings, primary_user_agent

------------------------

Table
^^^^^
TableClient
~~~~~~~~~~~
.. autoclass:: TableClient
    :members:
    :inherited-members:
    :undoc-members:

TableClientSettings
~~~~~~~~~~~~~~~~~~~

.. autoclass:: TableClientSettings
    :members:
    :inherited-members:
    :undoc-members:

Session
~~~~~~~

.. autoclass:: Session
   :members:
   :inherited-members:
   :undoc-members:

Transaction Context
~~~~~~~~~~~~~~~~~~~

.. autoclass:: TxContext
   :members:
   :inherited-members:
   :undoc-members:

--------------------------

Scheme
^^^^^^

SchemeClient
~~~~~~~~~~~~

.. autoclass:: SchemeClient
   :members:
   :inherited-members:
   :undoc-members:

------------------

Session Pool
^^^^^^^^^^^^

.. autoclass:: SessionPool
   :members:
   :inherited-members:
   :undoc-members:

-----------------------------


Result Sets
^^^^^^^^^^^

.. autoclass:: ydb.convert._ResultSet
   :members:
   :inherited-members:
   :undoc-members:

-----------------------------

DataQuery
^^^^^^^^^

.. autoclass:: DataQuery
   :members:
   :inherited-members:
   :undoc-members:
