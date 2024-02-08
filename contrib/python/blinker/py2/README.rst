Blinker
=======

Blinker provides a fast dispatching system that allows any number of
interested parties to subscribe to events, or "signals".

Signal receivers can subscribe to specific senders or receive signals
sent by any sender.

.. code-block:: pycon

    >>> from blinker import signal
    >>> started = signal('round-started')
    >>> def each(round):
    ...     print "Round %s!" % round
    ...
    >>> started.connect(each)

    >>> def round_two(round):
    ...     print "This is round two."
    ...
    >>> started.connect(round_two, sender=2)

    >>> for round in range(1, 4):
    ...     started.send(round)
    ...
    Round 1!
    Round 2!
    This is round two.
    Round 3!


Links
-----

-   Documentation: https://blinker.readthedocs.io/
-   Changes: https://blinker.readthedocs.io/#changes
-   PyPI Releases: https://pypi.org/project/blinker/
-   Source Code: https://github.com/pallets-eco/blinker/
-   Issue Tracker: https://github.com/pallets-eco/blinker/issues/
