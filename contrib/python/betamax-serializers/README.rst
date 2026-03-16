betamax_serializers
===================

Experimental set of Serializers for `Betamax 
<https://github.com/sigmavirus24/betamax>`_ that may possibly end up in the 
main package.

Pretty JSON Serializer
----------------------

Usage:

.. code-block:: python

    from betamax_serializers.pretty_json import PrettyJSONSerializer

    from betamax import Betamax

    import requests

    Betamax.register_serializer(PrettyJSONSerializer)

    session = requests.Session()
    recorder = Betamax(session)
    with recorder.use_cassette('testpretty', serialize_with='prettyjson'):
        session.request(method=method, url=url, ...)

YAML 1.1 Serializer
-------------------

To use the YAML 1.1 Serializer, you **must** ensure that you have ``pyyaml``
installed either by using ``betamax_serializer[yaml11] >= 0.2.0`` as your
dependency or by explicity adding ``PyYAML`` to your list of dependencies.

Usage:

.. code-block:: python

    from betamax import Betamax
    from betamax_serializers.yaml11 import YAMLSerializer
    import requests

    Betamax.register_serializer(YAMLSerializer)

    session = requests.Session()
    recorder = Betamax(session)
    with recorder.use_cassette('testyaml', serialize_with='yaml11'):
        session.request(method=method, url=url, ...)
