pure-sasl
=========

.. image:: https://travis-ci.org/thobbs/pure-sasl.png?branch=master
   :target: https://travis-ci.org/thobbs/pure-sasl

pure-sasl is a pure python client-side SASL implementation.

At the moment, it supports the following mechanisms: ANONYMOUS, PLAIN, EXTERNAL,
CRAM-MD5, DIGEST-MD5, and GSSAPI. Support for other mechanisms may be added in the
future. Only GSSAPI supports a QOP higher than auth. Always use TLS!

Both Python 2 and Python 3 are supported.

Example Usage
-------------

.. code-block:: python

    from puresasl.client import SASLClient

    sasl = SASLClient('somehost2', 'customprotocol')
    conn = get_connection_to('somehost2')
    available_mechs = conn.get_mechanisms()
    sasl.choose_mechanism(available_mechs, allow_anonymous=False)
    while True:
        status, challenge = conn.get_challenge()
        if status == 'COMPLETE':
            break
        elif status == 'OK':
            response = sasl.process(challenge)
            conn.send_response(response)
        else:
            raise Exception(status)

    if not sasl.complete:
        raise Exception("SASL negotiation did not complete")

    # begin normal communication
    encoded = conn.fetch_data()
    decoded = sasl.unwrap(encoded)
    response = process_data(decoded)
    conn.send_data(sasl.wrap(response))


License
-------
Some of the mechanisms and utility functions are based on work done
by David Alan Cridland and Lance Stout in Suelta: https://github.com/dwd/Suelta

pure-sasl is open source under the
`MIT license <http://www.opensource.org/licenses/mit-license.php>`_.
