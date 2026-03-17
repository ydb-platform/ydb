# Python imports
# Don't add any from __future__ imports here. This code should execute
# against standard Python.
import unittest
import random
import sys
import time

# Project imports
import sysv_ipc

IS_PY3 = (sys.version_info[0] == 3)


def make_key():
    """Generate a random key suitable for an IPC object."""
    return random.randint(sysv_ipc.KEY_MIN, sysv_ipc.KEY_MAX)


def sleep_past_granularity():
    """A utility method that encapsulates a type-specific detail of testing.

    I test all of the time-related variables in the IPC structs (o_time, shm_atime, shm_dtime,
    shm_ctime, msg_ctime, msg_stime, and msg_rtime) to ensure they change when they're supposed
    to (e.g. when a segment is detached, for shm_dtime). For variables that are initialized to 0
    (like o_time), it's easy to verify that they're 0 to start with and then non-zero after the
    change.

    Other variables (like shm_ctime) are trickier to test because they're already non-zero
    immediately after the object is created. My test has to save the value, do something that
    should change it, and then compare the saved value to the current one via assertNotEqual().

    Some (most? all?) systems define those time-related values as integral values (int or long),
    so their granularity is only 1 second. If I don't force at least 1 second to elapse between
    the statement where I save the value and the statement that should change it, they'll almost
    always happen in the same second and the assertNotEqual() even though all code (mine and the
    system) has behaved correctly.

    This method sleeps for 1.1 seconds to avoid the problem described above.
    """
    time.sleep(1.1)


class Base(unittest.TestCase):
    """Base class for test cases."""
    def assertWriteToReadOnlyPropertyFails(self, target_object, property_name,
                                           value):
        """test that writing to a readonly property raises an exception"""
        # The attributes tested with this code are implemented differently in C.
        # For instance, Semaphore.value is a 'getseters' with a NULL setter,
        # whereas Semaphore.name is a reference into the Semaphore member
        # definition.
        # Under Python 2.6, writing to sem.value raises AttributeError whereas
        # writing to sem.name raises TypeError. Under Python 3, both raise
        # AttributeError (but with different error messages!).
        # This illustrates that Python is a little unpredictable in this
        # matter. Rather than testing each of the numerous combinations of
        # of Python versions and attribute implementation, I just accept
        # both TypeError and AttributeError here.
        # ref: http://bugs.python.org/issue1687163
        # ref: http://bugs.python.org/msg127173
        with self.assertRaises((TypeError, AttributeError)):
            setattr(target_object, property_name, value)
