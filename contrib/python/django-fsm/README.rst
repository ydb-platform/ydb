Django friendly finite state machine support
============================================

Django-fsm first came out in 2010 and had a big update in 2.0 release at 2014, making it incompatible with earlier versions. Now, ten years later at 2024, it's been updated to version 3.0 and renamed viewflow.fsm.

This new version has a different API that doesn't work with the old one but is better suited for today's needs.

Migration guide:

https://github.com/viewflow/viewflow/wiki/django%E2%80%90fsm-to-viewflow.fsm-Migration-Guide


About
=====

Finite state machine workflows is the declarative way to describe consecutive
operation through set of states and transitions between them.


:mod:`viewflow.fsm` can help you manage rules and restrictions around moving
from one state to another. The package could be used to get low level
db-independent fsm implementation, or to wrap existing database model, and
implement simple persistent workflow process with quickly bootstrapped UI.

Quick start
===========

All things are buit around :class:`viewflow.fsm.State`. It is the special class
slot, that can take a value only from a specific `python enum`_ or `django
enumeration type`_   and that value can't be changed with simple assignement.

.. code::

   from enum import Enum
   from viewflow.fsm import State

   class Stage(Enum):
      NEW = 1
      DONE = 2
      HIDDEN = 3


   class MyFlow(object):
      state = State(Stage, default=Stage.NEW)

      @state.transition(source=Stage.NEW, target=Stage.DONE)
      def complete():
          pass

      @state.transition(source=State.ANY, target=Stage.HIDDEN)
      def hide():
          pass

   flow = MyFlow()
   flow.state == Stage.NEW  # True
   flow.state = Stage.DONE  # Raises AttributeError

   flow.complete()
   flow.state == Stage.DONE  # True

   flow.complete()  # Now raises TransitionNotAllowed


Documentation
=============

Full documentation available at https://docs.viewflow.io/fsm/index.html

