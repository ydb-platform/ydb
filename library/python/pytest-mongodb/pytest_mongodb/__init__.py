"""Pytest MongoDB

Main aim of this package is to create integration tests (not small) with
mongodb.

This package allows you to launch and manage mongodb local instance through
pytest fixtures.

NOTE: MongoDB is NOT INCLUDED in this module, so you should provide it yourself.

EXAMPLE:
library/python/pytest-mongodb/tests/ya.make
>>>...
>>>PEERDIR(
>>>    library/python/pytest-mongodb
>>>    contrib/python/pymongo
>>>)
>>>
>>>BUILD_ONLY_IF(OS_LINUX OS_DARWIN)
>>>IF (OS_LINUX)
>>>    DATA(sbr://320653966)
>>>ELSEIF (OS_DARWIN)
>>>    DATA(sbr://769724493)
>>>ENDIF()
>>>...
"""
