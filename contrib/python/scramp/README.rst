======
Scramp
======

A Python implementation of the `SCRAM authentication protocol
<https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism>`_.
Scramp supports the following mechanisms:

- SCRAM-SHA-1
- SCRAM-SHA-1-PLUS
- SCRAM-SHA-256
- SCRAM-SHA-256-PLUS
- SCRAM-SHA-512
- SCRAM-SHA-512-PLUS
- SCRAM-SHA3-512
- SCRAM-SHA3-512-PLUS

.. contents:: Table of Contents
   :depth: 2
   :local:

Installation
------------

- Create a virtual environment: ``python3 -m venv venv``
- Activate the virtual environment: ``source venv/bin/activate``
- Install: ``pip install scramp``


Examples
--------

Client and Server
`````````````````

Here's an example using both the client and the server. It's a bit contrived as normally
you'd be using either the client or server on its own.

>>> from scramp import ScramClient, ScramMechanism
>>>
>>> USERNAME = 'user'
>>> PASSWORD = 'pencil'
>>> MECHANISMS = ['SCRAM-SHA-256']
>>>
>>>
>>> # Choose a mechanism for our server
>>> m = ScramMechanism()  # Default is SCRAM-SHA-256
>>>
>>> # On the server side we create the authentication information for each user
>>> # and store it in an authentication database. We'll use a dict:
>>> db = {}
>>>
>>> salt, stored_key, server_key, iteration_count = m.make_auth_info(PASSWORD)
>>>
>>> db[USERNAME] = salt, stored_key, server_key, iteration_count
>>>
>>> # Define your own function for retrieving the authentication information
>>> # from the database given a username
>>>
>>> def auth_fn(username):
...     return db[username]
>>>
>>> # Make the SCRAM server
>>> s = m.make_server(auth_fn)
>>>
>>> # Now set up the client and carry out authentication with the server
>>> c = ScramClient(MECHANISMS, USERNAME, PASSWORD)
>>> cfirst = c.get_client_first()
>>>
>>> s.set_client_first(cfirst)
>>> sfirst = s.get_server_first()
>>>
>>> c.set_server_first(sfirst)
>>> cfinal = c.get_client_final()
>>>
>>> s.set_client_final(cfinal)
>>> sfinal = s.get_server_final()
>>>
>>> c.set_server_final(sfinal)
>>>
>>> # If it all runs through without raising an exception, the authentication
>>> # has succeeded


Client only
```````````

Here's an example using just the client. The client nonce is specified in order to give
a reproducible example, but in production you'd omit the ``c_nonce`` parameter and let
``ScramClient`` generate a client nonce:

>>> from scramp import ScramClient
>>>
>>> USERNAME = 'user'
>>> PASSWORD = 'pencil'
>>> C_NONCE = 'rOprNGfwEbeRWgbNEkqO'
>>> MECHANISMS = ['SCRAM-SHA-256']
>>>
>>> # Normally the c_nonce would be omitted, in which case ScramClient will
>>> # generate the nonce itself.
>>>
>>> c = ScramClient(MECHANISMS, USERNAME, PASSWORD, c_nonce=C_NONCE)
>>>
>>> # Get the client first message and send it to the server
>>> cfirst = c.get_client_first()
>>> print(cfirst)
n,,n=user,r=rOprNGfwEbeRWgbNEkqO
>>>
>>> # Set the first message from the server
>>> c.set_server_first(
...     'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,'
...     's=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096')
>>>
>>> # Get the client final message and send it to the server
>>> cfinal = c.get_client_final()
>>> print(cfinal)
c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=
>>>
>>> # Set the final message from the server
>>> c.set_server_final('v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=')
>>>
>>> # If it all runs through without raising an exception, the authentication
>>> # has succeeded


Server only
```````````

Here's an example using just the server. The server nonce and salt is specified in order
to give a reproducible example, but in production you'd omit the ``s_nonce`` and
``salt`` parameters and let Scramp generate them:

>>> from scramp import ScramMechanism
>>>
>>> USERNAME = 'user'
>>> PASSWORD = 'pencil'
>>> S_NONCE = '%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0'
>>> SALT = b'[m\x99h\x9d\x125\x8e\xec\xa0K\x14\x126\xfa\x81'
>>>
>>> db = {}
>>>
>>> m = ScramMechanism()
>>>
>>> salt, stored_key, server_key, iteration_count = m.make_auth_info(
...     PASSWORD, salt=SALT)
>>>
>>> db[USERNAME] = salt, stored_key, server_key, iteration_count
>>>
>>> # Define your own function for getting a password given a username
>>> def auth_fn(username):
...     return db[username]
>>>
>>> # Normally the s_nonce parameter would be omitted, in which case the
>>> # server will generate the nonce itself.
>>>
>>> s = m.make_server(auth_fn, s_nonce=S_NONCE)
>>>
>>> # Set the first message from the client
>>> s.set_client_first('n,,n=user,r=rOprNGfwEbeRWgbNEkqO')
>>>
>>> # Get the first server message, and send it to the client
>>> sfirst = s.get_server_first()
>>> print(sfirst)
r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
>>>
>>> # Set the final message from the client
>>> s.set_client_final(
...     'c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,'
...     'p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=')
>>>
>>> # Get the final server message and send it to the client
>>> sfinal = s.get_server_final()
>>> print(sfinal)
v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=
>>>
>>> # If it all runs through without raising an exception, the authentication
>>> # has succeeded


Server only with passlib
````````````````````````

Here's an example using just the server and using the `passlib hashing library
<https://passlib.readthedocs.io/en/stable/index.html>`_. The server nonce and salt is
specified in order to give a reproducible example, but in production you'd omit the
``s_nonce`` and ``salt`` parameters and let Scramp generate them:

>>> from scramp import ScramMechanism
>>> from passlib.hash import scram
>>>
>>> USERNAME = 'user'
>>> PASSWORD = 'pencil'
>>> S_NONCE = '%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0'
>>> SALT = b'[m\x99h\x9d\x125\x8e\xec\xa0K\x14\x126\xfa\x81'
>>> ITERATION_COUNT = 4096
>>>
>>> db = {}
>>> hash = scram.using(salt=SALT, rounds=ITERATION_COUNT).hash(PASSWORD)
>>>
>>> salt, iteration_count, digest = scram.extract_digest_info(hash, 'sha-256')
>>> 
>>> stored_key, server_key = m.make_stored_server_keys(digest)
>>>
>>> db[USERNAME] = salt, stored_key, server_key, iteration_count
>>>
>>> # Define your own function for getting a password given a username
>>> def auth_fn(username):
...     return db[username]
>>>
>>> # Normally the s_nonce parameter would be omitted, in which case the
>>> # server will generate the nonce itself.
>>>
>>> m = ScramMechanism()
>>> s = m.make_server(auth_fn, s_nonce=S_NONCE)
>>>
>>> # Set the first message from the client
>>> s.set_client_first('n,,n=user,r=rOprNGfwEbeRWgbNEkqO')
>>>
>>> # Get the first server message, and send it to the client
>>> sfirst = s.get_server_first()
>>> print(sfirst)
r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
>>>
>>> # Set the final message from the client
>>> s.set_client_final(
...     'c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,'
...     'p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=')
>>>
>>> # Get the final server message and send it to the client
>>> sfinal = s.get_server_final()
>>> print(sfinal)
v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=
>>>
>>> # If it all runs through without raising an exception, the authentication
>>> # has succeeded


Server Error
````````````

Here's an example of when setting a message from the client causes an error. The server
nonce and salt is specified in order to give a reproducible example, but in production
you'd omit the ``s_nonce`` and ``salt`` parameters and let Scramp generate them:

>>> from scramp import ScramException, ScramMechanism
>>>
>>> USERNAME = 'user'
>>> PASSWORD = 'pencil'
>>> S_NONCE = '%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0'
>>> SALT = b'[m\x99h\x9d\x125\x8e\xec\xa0K\x14\x126\xfa\x81'
>>>
>>> db = {}
>>>
>>> m = ScramMechanism()
>>>
>>> salt, stored_key, server_key, iteration_count = m.make_auth_info(
...     PASSWORD, salt=SALT)
>>>
>>> db[USERNAME] = salt, stored_key, server_key, iteration_count
>>>
>>> # Define your own function for getting a password given a username
>>> def auth_fn(username):
...     return db[username]
>>>
>>> # Normally the s_nonce parameter would be omitted, in which case the
>>> # server will generate the nonce itself.
>>>
>>> s = m.make_server(auth_fn, s_nonce=S_NONCE)
>>>
>>> try:
...     # Set the first message from the client
...     s.set_client_first('p=tls-unique,,n=user,r=rOprNGfwEbeRWgbNEkqO')
... except ScramException as e:
...     print(e)
...     # Get the final server message and send it to the client
...     sfinal = s.get_server_final()
...     print(sfinal)
Received GS2 flag 'p' which indicates that the client requires channel binding, but the server does not: channel-binding-not-supported
e=channel-binding-not-supported


Standards
---------

`RFC 5802 <https://tools.ietf.org/html/rfc5802>`_
  Describes SCRAM.
`RFC 7677 <https://datatracker.ietf.org/doc/html/rfc7677>`_
  Registers SCRAM-SHA-256 and SCRAM-SHA-256-PLUS.
`draft-melnikov-scram-sha-512-02 <https://datatracker.ietf.org/doc/html/draft-melnikov-scram-sha-512>`_
  Registers SCRAM-SHA-512 and SCRAM-SHA-512-PLUS.
`draft-melnikov-scram-sha3-512 <https://datatracker.ietf.org/doc/html/draft-melnikov-scram-sha3-512>`_
  Registers SCRAM-SHA3-512 and SCRAM-SHA3-512-PLUS.
`RFC 5929 <https://datatracker.ietf.org/doc/html/rfc5929>`_
  Channel Bindings for TLS.
`draft-ietf-kitten-tls-channel-bindings-for-tls13 <https://datatracker.ietf.org/doc/html/draft-ietf-kitten-tls-channel-bindings-for-tls13>`_
  Defines the ``tls-exporter`` channel binding, which is `not yet supported by Scramp
  <https://github.com/tlocke/scramp/issues/9>`_.


API Docs
--------


scramp.MECHANISMS
`````````````````

A tuple of the supported mechanism names.


scramp.ScramClient
``````````````````

``ScramClient(mechanisms, username, password, channel_binding=None, c_nonce=None)``
  Constructor of the ``ScramClient`` class, with the following parameters:

  ``mechanisms``
    A list or tuple of mechanism names. ScramClient will choose the most secure. If
    ``cbind_data`` is ``None``, the '-PLUS' variants will be filtered out first. The
    chosen mechanism is available as the property ``mechanism_name``.

  ``username``

  ``password``

  ``channel_binding``
    Providing a value for this parameter allows channel binding to be used (ie. it lets
    you use mechanisms ending in '-PLUS'). The value for ``channel_binding`` is a tuple
    consisting of the channel binding name and the channel binding data. For example, if
    the channel binding name is ``tls-unique``, the ``channel_binding`` parameter would
    be ``('tls-unique', data)``, where ``data`` is obtained by calling
    `SSLSocket.get_channel_binding()
    <https://docs.python.org/3/library/ssl.html#ssl.SSLSocket.get_channel_binding>`_.
    The convenience function ``scramp.make_channel_binding()`` can be used to create a
    channel binding tuple.

  ``c_nonce``
    The client nonce. It's sometimes useful to set this when testing / debugging, but in
    production this should be omitted, in which case ``ScramClient`` will generate a
    client nonce.

The ``ScramClient`` object has the following methods and properties:

``get_client_first()``
  Get the client first message.
``set_server_first(message)``
    Set the first message from the server.
``get_client_final()``
  Get the final client message.
``set_server_final(message)``
  Set the final message from the server.
``mechanism_name``
  The mechanism chosen from the list given in the constructor.


scramp.ScramMechanism
`````````````````````

``ScramMechanism(mechanism='SCRAM-SHA-256')``
  Constructor of the ``ScramMechanism`` class, with the following parameter:

  ``mechanism``
    The SCRAM mechanism to use.

The ``ScramMechanism`` object has the following methods and properties:

``make_auth_info(password, iteration_count=None, salt=None)``
  returns the tuple ``(salt, stored_key, server_key, iteration_count)`` which is stored
  in the authentication database on the server side. It has the following parameters:

  ``password``
    The user's password as a ``str``.

  ``iteration_count``
    The rounds as an ``int``. If ``None`` then use the minimum associated with the
    mechanism.
  ``salt``
    It's sometimes useful to set this binary parameter when testing / debugging, but in
    production this should be omitted, in which case a salt will be generated.

``make_server(auth_fn, channel_binding=None, s_nonce=None)``
    returns a ``ScramServer`` object. It takes the following parameters:

  ``auth_fn``
    This is a function provided by the programmer that has one parameter, a username of
    type ``str`` and returns returns the tuple ``(salt, stored_key, server_key,
    iteration_count)``. Where ``salt``, ``stored_key`` and ``server_key`` are of a
    binary type, and ``iteration_count`` is an ``int``.

  ``channel_binding``
    Providing a value for this parameter allows channel binding to be used (ie.  it lets
    you use mechanisms ending in ``-PLUS``). The value for ``channel_binding`` is a
    tuple consisting of the channel binding name and the channel binding data. For
    example, if the channel binding name is 'tls-unique', the ``channel_binding``
    parameter would be ``('tls-unique', data)``, where ``data`` is obtained by calling
    `SSLSocket.get_channel_binding()
    <https://docs.python.org/3/library/ssl.html#ssl.SSLSocket.get_channel_binding>`_.
    The convenience function ``scramp.make_channel_binding()`` can be used to create a
    channel binding tuple. If ``channel_binding`` is provided and the mechanism isn't a
    ``-PLUS`` variant, then the server will negotiate with the client to use the
    ``-PLUS`` variant if the client supports it, or otherwise to use the mechanism
    without channel binding.

  ``s_nonce``
    The server nonce as a ``str``. It's sometimes useful to set this when testing /
    debugging, but in production this should be omitted, in which case ``ScramServer``
    will generate a server nonce.

``make_stored_server_keys(salted_password)``
    returns ``(stored_key, server_key)`` tuple of ``bytes`` objects given a salted
    password. This is useful if you want to use a separate hashing implementation from
    the one provided by Scramp. It takes the following parameter:

  ``salted_password``
    A binary object representing the hashed password.

``iteration_count``
    The minimum iteration count recommended for this mechanism.


scramp.ScramServer
``````````````````

The ``ScramServer`` object has the following methods:

``set_client_first(message)``
  Set the first message from the client.

``get_server_first()``
  Get the server first message.

``set_client_final(message)``
  Set the final client message.

``get_server_final()``
  Get the server final message.


scramp.make_channel_binding()
`````````````````````````````

``make_channel_binding(name, ssl_socket)``
  A helper function that makes a ``channel_binding`` tuple when given a channel binding
  name and an SSL socket. The parameters are:

  ``name``
    A channel binding name such as 'tls-unique' or 'tls-server-end-point'.

  ``ssl_socket``
    An instance of `ssl.SSLSocket
    <https://docs.python.org/3/library/ssl.html#ssl.SSLSocket>`_.


README.rst
----------

This file is written in the `reStructuredText
<https://docutils.sourceforge.io/docs/user/rst/quickref.html>`_ format. To generate an
HTML page from it, do:

- Activate the virtual environment: ``source venv/bin/activate``
- Install ``Sphinx``: ``pip install Sphinx``
- Run ``rst2html.py``: ``rst2html.py README.rst README.html``


Testing
-------

- Activate the virtual environment: ``source venv/bin/activate``
- Install ``tox``: ``pip install tox``
- Run ``tox``: ``tox``


Doing A Release Of Scramp
-------------------------

Run ``tox`` to make sure all tests pass, then update the release notes, then do::

  git tag -a x.y.z -m "version x.y.z"
  rm -r dist
  python -m build
  twine upload --sign dist/*


Release Notes
-------------

Version 1.4.4, 2022-11-01
`````````````````````````

- Tighten up parsing of messages to make sure that a ``ScramException`` is raised if a
  message is malformed.


Version 1.4.3, 2022-10-26
`````````````````````````

- The client now sends a gs2-cbind-flag of 'y' if the client supports channel
  binding, but thinks the server does not.


Version 1.4.2, 2022-10-22
`````````````````````````

- Switch to using the MIT-0 licence https://choosealicense.com/licenses/mit-0/

- When creating a ScramClient, allow non ``-PLUS`` variants, even if a
  ``channel_binding`` parameter is provided. Previously this would raise and
  exception.


Version 1.4.1, 2021-08-25
`````````````````````````

- When using ``make_channel_binding()`` to create a tls-server-end-point channel
  binding, support certificates with hash algorithm of sha512.


Version 1.4.0, 2021-03-28
`````````````````````````

- Raise an exception if the client receives an error from the server.


Version 1.3.0, 2021-03-28
`````````````````````````

- As the specification allows, server errors are now sent to the client in the
  ``server_final`` message, an exception is still thrown as before.


Version 1.2.2, 2021-02-13
`````````````````````````

- Fix bug in generating the AuthMessage. It was incorrect when channel binding
  was used. So now Scramp supports channel binding.


Version 1.2.1, 2021-02-07
`````````````````````````

- Add support for channel binding.

- Add support for SCRAM-SHA-512 and SCRAM-SHA3-512 and their channel binding
  variants.


Version 1.2.0, 2020-05-30
`````````````````````````

- This is a backwardly incompatible change on the server side, the client side will
  work as before. The idea of this change is to make it possible to have an
  authentication database. That is, the authentication information can be stored, and
  then retrieved when needed to authenticate the user.

- In addition, it's now possible on the server side to use a third party hashing library
  such as passlib as the hashing implementation.


Version 1.1.1, 2020-03-28
`````````````````````````

- Add the README and LICENCE to the distribution.


Version 1.1.0, 2019-02-24
`````````````````````````

- Add support for the SCRAM-SHA-1 mechanism.


Version 1.0.0, 2019-02-17
`````````````````````````

- Implement the server side as well as the client side.


Version 0.0.0, 2019-02-10
`````````````````````````

- Copied SCRAM implementation from `pg8000 <https://github.com/tlocke/pg8000>`_. The
  idea is to make it a general SCRAM implemtation. Credit to the `Scrampy
  <https://github.com/cagdass/scrampy>`_ project which I read through to help with this
  project. Also credit to the `passlib <https://github.com/efficks/passlib>`_ project
  from which I copied the ``saslprep`` function.
