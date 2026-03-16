# -*- coding: utf-8 -*-
"""Type aliases allowing to narrow down definition and reduce duplication

This file is part of PyVISA.

:copyright: 2020-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from typing import Any, Callable, NewType

from . import constants

#: Type alias used to identify VISA resource manager sessions
VISARMSession = NewType("VISARMSession", int)

#: Type alias used to identify VISA resource sessions
VISASession = NewType("VISASession", int)

#: Type alias used to identify an event context (created when handling an event)
VISAEventContext = NewType("VISAEventContext", int)

#: Type alias used to identify a job id created during an asynchronous operation
#: JobID should always be treated as opaque objects since their exact behavior
#: may depend on the backend in use.
VISAJobID = NewType("VISAJobID", object)

#: Type alias used to identify a memory address in a register based resource after
#: it has been mapped
VISAMemoryAddress = NewType("VISAMemoryAddress", int)

#: Type for event handler passed to the VISA library. The last argument is the
#: user handle specified when registering the handler. The value that will be
#: passed to the handler is the value as interpreted by the backend and returned
#: by the install_visa_handler method of the library object.
VISAHandler = Callable[[VISASession, constants.EventType, VISAEventContext, Any], None]
