# Python imports
import unittest
import datetime
import time
import os

# Project imports
import sysv_ipc
from .base import Base, make_key

# Not tested --
# - mode seems to be settable and readable, but ignored by the OS
# - undo flag is hard to test without launching another process
# - P() and V() are simple aliases for acquire() and release(). I could repeat all of the
#   acquire() and release() tests using the aliases. A different implementation might be to
#   supply an __init__.py along with the .so binary that would alias these functions in Python
#   which would be testable.
# - Z() is not tested in the positive case, i.e. that it actually unblocks once the semaphore
#   hits zero. (Requires a separate process to test.)


# N_RELEASES is the number of times release() is called in test_release()
N_RELEASES = 1000000  # 1 million


class SemaphoreTestBase(Base):
    """base class for Semaphore test classes"""
    def setUp(self):
        self.sem = sysv_ipc.Semaphore(None, sysv_ipc.IPC_CREX, initial_value=1)

    def tearDown(self):
        if self.sem:
            self.sem.remove()

    def assertWriteToReadOnlyPropertyFails(self, property_name, value):
        """test that writing to a readonly property raises TypeError"""
        Base.assertWriteToReadOnlyPropertyFails(self, self.sem, property_name, value)

    def assertDeltasCloseEnough(self, delta_a, delta_b):
        """Compare two datetime.timedeltas and ensure they're within < 1 second of one another.

        This is useful when comparing actual times to expected times in tests where the actual
        time may have been a bit longer or a bit shorter than the expected.
        """
        # This is conceptually like computing abs(delta_a - delta_b).
        if delta_a > delta_b:
            delta = delta_a - delta_b
        else:
            delta = delta_b - delta_a

        self.assertEqual(delta.days, 0)
        self.assertEqual(delta.seconds, 0)
        # I don't test microseconds because that granularity isn't under the control of this module.


class TestSemaphoreCreation(SemaphoreTestBase):
    """Exercise stuff related to creating Semaphores"""
    def test_no_flags(self):
        """tests that opening a semaphore with no flags opens the existing
        semaphore and doesn't create a new semaphore"""
        sem_copy = sysv_ipc.Semaphore(self.sem.key)
        self.assertEqual(self.sem.key, sem_copy.key)

    def test_IPC_CREAT_existing(self):
        """tests sysv_ipc.IPC_CREAT to open an existing semaphore without IPC_EXCL"""
        sem_copy = sysv_ipc.Semaphore(self.sem.key, sysv_ipc.IPC_CREAT)

        self.assertEqual(self.sem.key, sem_copy.key)

    def test_IPC_CREAT_new(self):
        """tests sysv_ipc.IPC_CREAT to create a new semaphore without IPC_EXCL"""
        # I can't pass None for the name unless I also pass IPC_EXCL.
        key = make_key()

        # Note: this method of finding an unused key is vulnerable to a race
        # condition. It's good enough for test, but don't copy it for use in
        # production code!
        key_is_available = False
        while not key_is_available:
            try:
                sem = sysv_ipc.Semaphore(key)
                sem.close()
            except sysv_ipc.ExistentialError:
                key_is_available = True
            else:
                key = make_key()

        sem = sysv_ipc.Semaphore(key, sysv_ipc.IPC_CREAT)

        self.assertIsNotNone(sem)

        sem.remove()

    def test_IPC_EXCL(self):
        """tests IPC_CREAT | IPC_EXCL prevents opening an existing semaphore"""
        with self.assertRaises(sysv_ipc.ExistentialError):
            sysv_ipc.Semaphore(self.sem.key, sysv_ipc.IPC_CREX)

    def test_randomly_generated_key(self):
        """tests that the randomly-generated key works"""
        # This is tested implicitly elsewhere but I want to test it explicitly
        sem = sysv_ipc.Semaphore(None, sysv_ipc.IPC_CREX)
        self.assertIsNotNone(sem.key)
        self.assertGreaterEqual(sem.key, sysv_ipc.KEY_MIN)
        self.assertLessEqual(sem.key, sysv_ipc.KEY_MAX)
        sem.remove()

    # # don't bother testing mode, it's ignored by the OS?

    def test_default_initial_value(self):
        """tests that the initial value is 0 by default"""
        sem = sysv_ipc.Semaphore(None, sysv_ipc.IPC_CREX)
        self.assertEqual(sem.value, 0)
        sem.remove()

    def test_zero_initial_value(self):
        """tests that the initial value is 0 when assigned"""
        sem = sysv_ipc.Semaphore(None, sysv_ipc.IPC_CREX, initial_value=0)
        self.assertEqual(sem.value, 0)
        sem.remove()

    def test_nonzero_initial_value(self):
        """tests that the initial value is non-zero when assigned"""
        sem = sysv_ipc.Semaphore(None, sysv_ipc.IPC_CREX, initial_value=42)
        self.assertEqual(sem.value, 42)
        sem.remove()

    def test_kwargs(self):
        """ensure init accepts keyword args as advertised"""
        # mode 0x180 = 0600. Octal is difficult to express in Python 2/3 compatible code.
        sem = sysv_ipc.Semaphore(None, flags=sysv_ipc.IPC_CREX, mode=0x180, initial_value=0)
        sem.remove()


class TestSemaphoreAquisition(SemaphoreTestBase):
    """Exercise acquiring semaphores"""
    def test_simple_acquisition(self):
        """tests that acquisition works"""
        # I should be able to acquire this semaphore, but if I can't I don't want to hang the
        # test so I set block=False. If I can't acquire the semaphore, sysv_ipc will raise a
        # BusyError.
        self.sem.block = False
        # Should raise no error
        self.sem.acquire()

    def test_acquisition_delta(self):
        """tests that the delta param works"""
        self.sem.value = 42
        self.sem.acquire(None, 10)
        self.assertEqual(self.sem.value, 32)

    def test_acquisition_zero_delta(self):
        """tests that a zero delta is not allowed"""
        # acquire() w/zero delta is not allowed because at the C level, P(), V(), and Z() all
        # map to semop(), and the delta value is what differentiates P(), V(), and Z().
        with self.assertRaises(ValueError):
            self.sem.acquire(None, 0)

    def test_acquisition_non_blocking(self):
        """tests that a non-blocking attempt at acquisition works"""
        # Should raise no error
        self.sem.acquire()

        self.sem.block = False
        with self.assertRaises(sysv_ipc.BusyError):
            self.sem.acquire()

    # test acquisition failures
    # def test_acquisition_no_timeout(self):
    # FIXME
    # This is hard to test since it should wait infinitely. Probably the way
    # to do it is to spawn another process that holds the semaphore for
    # maybe 10 seconds and have this process wait on it. That's complicated
    # and not a really great test.

    @unittest.skipUnless(sysv_ipc.SEMAPHORE_TIMEOUT_SUPPORTED, "Requires Semaphore timeout support")
    def test_acquisition_zero_timeout(self):
        """tests that acquisition w/timeout=0 implements non-blocking behavior"""
        # Should not raise an error
        self.sem.acquire(0)
        with self.assertRaises(sysv_ipc.BusyError):
            self.sem.acquire(0)

    @unittest.skipUnless(sysv_ipc.SEMAPHORE_TIMEOUT_SUPPORTED, "Requires Semaphore timeout support")
    def test_acquisition_nonzero_int_timeout(self):
        """tests that acquisition w/timeout=an int is reasonably accurate"""
        # Should not raise an error
        self.sem.acquire(0)

        # This should raise a busy error
        wait_time = 1
        start = datetime.datetime.now()
        with self.assertRaises(sysv_ipc.BusyError):
            self.sem.acquire(wait_time)
        end = datetime.datetime.now()
        actual_delta = end - start
        expected_delta = datetime.timedelta(seconds=wait_time)

        self.assertDeltasCloseEnough(actual_delta, expected_delta)

    @unittest.skipUnless(sysv_ipc.SEMAPHORE_TIMEOUT_SUPPORTED, "Requires Semaphore timeout support")
    def test_acquisition_nonzero_float_timeout(self):
        """tests that acquisition w/timeout=a float is reasonably accurate"""
        # Should not raise an error
        self.sem.acquire(0)

        # This should raise a busy error
        wait_time = 1.5
        start = datetime.datetime.now()
        with self.assertRaises(sysv_ipc.BusyError):
            self.sem.acquire(wait_time)
        end = datetime.datetime.now()
        actual_delta = end - start
        expected_delta = datetime.timedelta(seconds=wait_time)

        self.assertDeltasCloseEnough(actual_delta, expected_delta)

    def test_acquire_kwargs(self):
        """Ensure acquire() takes kwargs as advertised"""
        self.sem.acquire(timeout=None, delta=1)

    def test_P_kwargs(self):
        """Ensure P() takes kwargs as advertised"""
        self.sem.P(timeout=None, delta=1)


class TestSemaphoreRelease(SemaphoreTestBase):
    """Exercise releasing semaphores"""
    def test_release(self):
        """tests that release works"""
        # Not only does it work, I can do it as many times as I want! I had
        # tried some code that called release() SEMAPHORE_VALUE_MAX times, but
        # on platforms where that's ~2 billion, the test takes too long to run.
        # So I'll stick to a lower (but still very large) number of releases.
        n_releases = min(N_RELEASES, sysv_ipc.SEMAPHORE_VALUE_MAX - 1)
        original_value = self.sem.value
        for i in range(n_releases):
            self.sem.release()
        self.assertEqual(self.sem.value, original_value + n_releases)

    def test_release_delta(self):
        """tests that release()'s delta param works"""
        original_value = self.sem.value
        self.sem.release(5)
        self.assertEqual(self.sem.value, original_value + 5)

    def test_release_kwargs(self):
        """Ensure release() takes kwargs as advertised"""
        self.sem.release(delta=1)

    def test_V_kwargs(self):
        """Ensure V() takes kwargs as advertised"""
        self.sem.V(delta=1)

    def test_context_manager(self):
        """tests that context manager acquire/release works"""
        with self.sem as sem:
            self.assertEqual(sem.value, 0)
            self.sem.block = False
            with self.assertRaises(sysv_ipc.BusyError):
                sem.acquire()

        self.assertEqual(sem.value, 1)

        # Should not raise an error.
        self.sem.block = False
        sem.acquire()


class TestSemaphoreZ(SemaphoreTestBase):
    """exercise Z() (block until zero)"""
    def test_Z_failure(self):
        """Ensure Z understands when the semaphore is non-zero"""
        self.sem.value = 42
        self.sem.block = False
        with self.assertRaises(sysv_ipc.BusyError):
            self.sem.Z()

    def test_Z_success(self):
        """Ensure Z understands when the semaphore is zero"""
        self.sem.value = 0
        self.sem.block = False
        # Should not raise BusyError
        self.sem.Z()

    @unittest.skipUnless(sysv_ipc.SEMAPHORE_TIMEOUT_SUPPORTED, "Requires Semaphore timeout support")
    def test_Z_zero_timeout(self):
        """tests that Z w/timeout=0 implements non-blocking behavior"""
        self.sem.value = 42
        with self.assertRaises(sysv_ipc.BusyError):
            self.sem.Z(0)

    @unittest.skipUnless(sysv_ipc.SEMAPHORE_TIMEOUT_SUPPORTED, "Requires Semaphore timeout support")
    def test_Z_nonzero_int_timeout(self):
        """tests that Z() w/timeout=an int is reasonably accurate"""
        self.sem.value = 42

        # This should raise a busy error
        wait_time = 1
        start = datetime.datetime.now()
        with self.assertRaises(sysv_ipc.BusyError):
            self.sem.Z(wait_time)
        end = datetime.datetime.now()
        actual_delta = end - start
        expected_delta = datetime.timedelta(seconds=wait_time)

        self.assertDeltasCloseEnough(actual_delta, expected_delta)

    @unittest.skipUnless(sysv_ipc.SEMAPHORE_TIMEOUT_SUPPORTED, "Requires Semaphore timeout support")
    def test_Z_nonzero_float_timeout(self):
        """tests that Z() w/timeout=a float is reasonably accurate"""
        self.sem.value = 42

        # This should raise a busy error
        wait_time = 1.5
        start = datetime.datetime.now()
        with self.assertRaises(sysv_ipc.BusyError):
            self.sem.Z(wait_time)
        end = datetime.datetime.now()
        actual_delta = end - start
        expected_delta = datetime.timedelta(seconds=wait_time)

        self.assertDeltasCloseEnough(actual_delta, expected_delta)

    def test_Z_kwargs(self):
        """Ensure Z() takes kwargs as advertised"""
        self.sem.value = 0
        self.sem.block = False
        self.sem.Z(timeout=None)


class TestSemaphoreRemove(SemaphoreTestBase):
    """Exercise sem.remove()"""
    def test_remove(self):
        """tests that sem.remove() works"""
        self.sem.remove()
        with self.assertRaises(sysv_ipc.ExistentialError):
            sysv_ipc.Semaphore(self.sem.key)
        # Wipe this out so that self.tearDown() doesn't crash.
        self.sem = None


class TestSemaphorePropertiesAndAttributes(SemaphoreTestBase):
    """Exercise props and attrs"""
    def test_property_key(self):
        """exercise Semaphore.key"""
        self.assertGreaterEqual(self.sem.key, sysv_ipc.KEY_MIN)
        self.assertLessEqual(self.sem.key, sysv_ipc.KEY_MAX)
        self.assertWriteToReadOnlyPropertyFails('key', 42)

    def test_property_id(self):
        """exercise Semaphore.id"""
        self.assertGreaterEqual(self.sem.id, 0)
        self.assertWriteToReadOnlyPropertyFails('id', 42)

    def test_attribute_value(self):
        """exercise Semaphore.value"""
        # test read, although this has been tested very thoroughly above
        self.assertEqual(self.sem.value, 1)
        self.sem.value = 42
        self.assertEqual(self.sem.value, 42)

        # test writing out of bounds values
        expected = "The semaphore's value must remain between 0 and SEMVMX"

        with self.assertRaises(ValueError) as context:
            self.sem.value = -1
        assert str(context.exception) == expected

        with self.assertRaises(ValueError) as context:
            self.sem.value = 999999
        assert str(context.exception) == expected

    def test_attribute_block(self):
        """exercise Semaphore.block"""
        # tested for semantics above, here I just test that it can be read.
        self.assertEqual(self.sem.block, True)
        self.sem.block = False
        self.assertEqual(self.sem.block, False)

    def test_attribute_uid(self):
        """exercise Semaphore.uid"""
        self.assertEqual(self.sem.uid, os.geteuid())

    def test_attribute_gid(self):
        """exercise Semaphore.gid"""
        self.assertEqual(self.sem.gid, os.getgid())

    def test_attribute_cuid(self):
        """exercise Semaphore.cuid"""
        self.assertEqual(self.sem.cuid, os.geteuid())
        self.assertWriteToReadOnlyPropertyFails('cuid', 42)

    def test_attribute_cgid(self):
        """exercise Semaphore.cgid"""
        self.assertEqual(self.sem.cgid, os.getgid())
        self.assertWriteToReadOnlyPropertyFails('cgid', 42)

    def test_attribute_last_pid(self):
        """exercise Semaphore.last_pid"""
        # All operating systems set last_pid when an operation like sem.release() occurs. Some
        # systems set it under other conditions, too, and some do not. The goal of this test is
        # not to exercise OS-specific quirks. In my tests, I assume the operating system is
        # well-behaved. My responsibility is only to ensure that my code correctly reports last_pid,
        # and that I can't write to it.
        #
        # https://github.com/osvenskan/sysv_ipc/blob/develop/USAGE.md#last_pid-behavior-depends-on-platform
        self.sem.release()
        self.assertEqual(self.sem.last_pid, os.getpid())
        self.assertWriteToReadOnlyPropertyFails('last_pid', 42)

    def test_attribute_waiting_for_nonzero(self):
        """exercise Semaphore.waiting_for_nonzero"""
        self.assertEqual(self.sem.waiting_for_nonzero, 0)
        self.assertWriteToReadOnlyPropertyFails('waiting_for_nonzero', 42)

    def test_attribute_waiting_for_zero(self):
        """exercise Semaphore.waiting_for_zero"""
        self.assertEqual(self.sem.waiting_for_zero, 0)
        self.assertWriteToReadOnlyPropertyFails('waiting_for_zero', 42)

    def test_attribute_o_time(self):
        """exercise Semaphore.o_time"""
        sem = sysv_ipc.Semaphore(None, sysv_ipc.IPC_CREX)
        self.assertEqual(self.sem.o_time, 0)
        # sem.release() will set o_time.
        sem.release()
        # I can't know precisely when o_time was set, but there should be < 10 seconds between
        # the sem.release() line above and the assertion below.
        self.assertLess(time.time() - sem.o_time, 10)
        self.assertWriteToReadOnlyPropertyFails('o_time', 42)
        sem.remove()


if __name__ == '__main__':
    unittest.main()
