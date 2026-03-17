# Python imports
import unittest
import time
import os
import numbers
import sys

# Project imports
import sysv_ipc
from .base import Base, make_key, sleep_past_granularity

# Not tested --
# - mode seems to be settable and readable, but ignored by the OS


class MessageQueueTestBase(Base):
    """base class for MessageQueue test classes"""
    def setUp(self):
        self.mq = sysv_ipc.MessageQueue(None, sysv_ipc.IPC_CREX)

    def tearDown(self):
        if self.mq:
            self.mq.remove()

    def assertWriteToReadOnlyPropertyFails(self, property_name, value):
        """test that writing to a readonly property raises TypeError"""
        Base.assertWriteToReadOnlyPropertyFails(self, self.mq, property_name, value)


class TestMessageQueueCreation(MessageQueueTestBase):
    """Exercise stuff related to creating MessageQueue"""

    def test_no_flags(self):
        """tests that opening a MessageQueue with no flags opens the existing
        MessageQueue and doesn't create a new MessageQueue"""
        mem_copy = sysv_ipc.MessageQueue(self.mq.key)
        self.assertEqual(self.mq.key, mem_copy.key)

    def test_IPC_CREAT_existing(self):
        """tests sysv_ipc.IPC_CREAT to open an existing MessageQueue without IPC_EXCL"""
        mem_copy = sysv_ipc.MessageQueue(self.mq.key, sysv_ipc.IPC_CREAT)

        self.assertEqual(self.mq.key, mem_copy.key)

    def test_IPC_CREAT_new(self):
        """tests sysv_ipc.IPC_CREAT to create a new MessageQueue without IPC_EXCL"""
        # I can't pass None for the name unless I also pass IPC_EXCL.
        key = make_key()

        # Note: this method of finding an unused key is vulnerable to a race
        # condition. It's good enough for test, but don't copy it for use in
        # production code!
        key_is_available = False
        while not key_is_available:
            try:
                mem = sysv_ipc.MessageQueue(key)
                mem.detach()
                mem.remove()
            except sysv_ipc.ExistentialError:
                key_is_available = True
            else:
                key = make_key()

        mq = sysv_ipc.MessageQueue(key, sysv_ipc.IPC_CREAT)

        self.assertIsNotNone(mq)

        mq.remove()

    def test_IPC_EXCL(self):
        """tests IPC_CREAT | IPC_EXCL prevents opening an existing MessageQueue"""
        with self.assertRaises(sysv_ipc.ExistentialError):
            sysv_ipc.MessageQueue(self.mq.key, sysv_ipc.IPC_CREX)

    def test_randomly_generated_key(self):
        """tests that the randomly-generated key works"""
        # This is tested implicitly elsewhere but I want to test it explicitly
        mq = sysv_ipc.MessageQueue(None, sysv_ipc.IPC_CREX)
        self.assertIsNotNone(mq.key)
        self.assertGreaterEqual(mq.key, sysv_ipc.KEY_MIN)
        self.assertLessEqual(mq.key, sysv_ipc.KEY_MAX)
        mq.remove()

    # don't bother testing mode, it's ignored by the OS?

    def test_default_flags(self):
        """tests that the flag is 0 by default (==> open existing)"""
        mq = sysv_ipc.MessageQueue(self.mq.key)
        self.assertEqual(self.mq.id, mq.id)

    def test_kwargs(self):
        """ensure init accepts keyword args as advertised"""
        mq = sysv_ipc.MessageQueue(None, flags=sysv_ipc.IPC_CREX, mode=0o0600,
                                   max_message_size=256)
        mq.remove()


class TestMessageQueueSendReceive(MessageQueueTestBase):
    """Exercise send() and receive()"""
    def test_simple_send_receive(self):
        test_string = b'abcdefg'
        self.mq.send(test_string)
        self.assertEqual(self.mq.receive(), (test_string, 1))

    def test_message_type_send(self):
        """test the msg type param of send()"""
        test_string = b'abcdefg'
        self.mq.send(test_string, type=2)
        self.assertEqual(self.mq.receive(), (test_string, 2))

        with self.assertRaises(ValueError):
            self.mq.send(test_string, type=-1)

    def test_message_type_receive_default_order(self):
        """test that receive() doesn't filter by type by default"""
        for i in range(1, 4):
            self.mq.send('type' + str(i), type=i)

        # default order is FIFO.
        self.assertEqual(self.mq.receive(), (b'type1', 1))
        self.assertEqual(self.mq.receive(), (b'type2', 2))
        self.assertEqual(self.mq.receive(), (b'type3', 3))

        self.assertEqual(self.mq.current_messages, 0)

    # Since the earliest commit I have for this file (2016), I've had to skip this test under Linux
    # because msgrcv() does not work correctly under Linux when using a negative type. See
    # https://github.com/osvenskan/sysv_ipc/issues/38 for details.
    # A less demanding version of this test follows so Linux doesn't go entirely untested.
    @unittest.skipIf(sys.platform.startswith('linux'),
                     'msgrcv() buggy on Linux: https://github.com/osvenskan/sysv_ipc/issues/38')
    def test_message_type_receive_specific_order(self):
        # Place messsages in Q w/highest type first
        for i in range(4, 0, -1):
            self.mq.send('type' + str(i), type=i)

        # receive(type=-2) should get "the first message of the lowest type that is <= the absolute
        # value of type." This assertion fails under Linux (but not under Mac or FreeBSD).
        self.assertEqual(self.mq.receive(type=-2), (b'type2', 2))

        # receive(type=3) should get "the first message of that type."
        self.assertEqual(self.mq.receive(type=3), (b'type3', 3))

        # Ensure the others are still there.
        self.assertEqual(self.mq.receive(), (b'type4', 4))
        self.assertEqual(self.mq.receive(), (b'type1', 1))

        self.assertEqual(self.mq.current_messages, 0)

    def test_message_type_receive_specific_order_no_negative_type(self):
        """test that receive() filters appropriately on positive msg type (softer test for Linux)"""
        # Place messsages in Q w/highest type first
        for i in range(4, 0, -1):
            self.mq.send('type' + str(i), type=i)

        # receive(type=3) should get "the first message of that type."
        self.assertEqual(self.mq.receive(type=3), (b'type3', 3))

        # Ensure the others are still there.
        self.assertEqual(self.mq.receive(), (b'type4', 4))
        self.assertEqual(self.mq.receive(), (b'type2', 2))
        self.assertEqual(self.mq.receive(), (b'type1', 1))

        self.assertEqual(self.mq.current_messages, 0)

    def test_send_non_blocking(self):
        """Test that send(block=False) raises BusyError as appropriate"""
        # This is a bit tricky since the OS has its own ideas about when the queue is full.
        # I would like to fill it precisely to the brim and then make one last call to send(),
        # but I don't know exactly when the OS will decide the Q is full. Instead, I just keep
        # stuffing messages in until I get some kind of an error. If it's a BusyError, all is well.
        done = False

        while not done:
            try:
                self.mq.send('x', block=False)
            except sysv_ipc.BusyError:
                done = True

    def test_receive_non_blocking(self):
        """Test that receive(block=False) raises BusyError as appropriate"""
        with self.assertRaises(sysv_ipc.BusyError):
            self.mq.receive(block=False)

        self.mq.send('x', type=3)
        with self.assertRaises(sysv_ipc.BusyError):
            self.mq.receive(type=2, block=False)

    def test_ascii_null(self):
        """ensure I can send & receive 0x00"""
        test_string = b'abc' + bytes(0) + b'def'
        self.mq.send(test_string)
        self.assertEqual(self.mq.receive(), (test_string, 1))

    def test_utf8(self):
        """Test writing encoded Unicode"""
        test_string = 'G' + '\u00F6' + 'teborg'
        test_string = test_string.encode('utf-8')
        self.mq.send(test_string)
        self.assertEqual(self.mq.receive(), (test_string, 1))

    def test_send_kwargs(self):
        """ensure send() accepts keyword args as advertised"""
        self.mq.send(b'x', block=True, type=1)

    def test_receive_kwargs(self):
        """ensure receive() accepts keyword args as advertised"""
        self.mq.send(b'x', block=True, type=1)
        self.mq.receive(block=False, type=0)

    def test_max_message_size_respected(self):
        '''ensure the max_message_size param is respected'''
        mq = sysv_ipc.MessageQueue(None, sysv_ipc.IPC_CREX, max_message_size=10)

        with self.assertRaises(ValueError):
            mq.send(b' ' * 11, block=False)

        mq.remove()


class TestMessageQueueRemove(MessageQueueTestBase):
    """Exercise mq.remove()"""
    def test_remove(self):
        """tests that mq.remove() works"""
        self.mq.remove()
        with self.assertRaises(sysv_ipc.ExistentialError):
            sysv_ipc.MessageQueue(self.mq.key)
        # Wipe this out so that self.tearDown() doesn't crash.
        self.mq = None


class TestMessageQueuePropertiesAndAttributes(MessageQueueTestBase):
    """Exercise props and attrs"""
    def test_property_key(self):
        """exercise MessageQueue.key"""
        self.assertGreaterEqual(self.mq.key, sysv_ipc.KEY_MIN)
        self.assertLessEqual(self.mq.key, sysv_ipc.KEY_MAX)
        self.assertWriteToReadOnlyPropertyFails('key', 42)

    # The POSIX spec says "msgget() shall return a non-negative integer", but OS X does not
    # respect this, I think due to a bug. See https://github.com/osvenskan/sysv_ipc/issues/63
    @unittest.skipIf(sys.platform.startswith('darwin'),
                     'msgget() buggy on Mac: https://github.com/osvenskan/sysv_ipc/issues/63')
    def test_property_id(self):
        """exercise MessageQueue.id"""
        self.assertGreaterEqual(self.mq.id, 0)
        self.assertWriteToReadOnlyPropertyFails('id', 42)

    def test_property_id_weak_for_darwin(self):
        """exercise MessageQueue.id with the Darwin-failing test removed"""
        self.assertIsInstance(self.mq.id, numbers.Integral)
        self.assertWriteToReadOnlyPropertyFails('id', 42)

    def test_attribute_max_size(self):
        """exercise MessageQueue.max_size"""
        self.assertGreaterEqual(self.mq.max_size, 0)
        # writing to max_size should not fail, and I test that here. However, as documented,
        # setting it is no guarantee that the OS will respect it. Caveat emptor.
        self.mq.max_size = 2048

    def test_property_last_send_time(self):
        """exercise MessageQueue.last_send_time"""
        self.assertEqual(self.mq.last_send_time, 0)
        # I can't record exactly when this send() happens, but as long as it is within 5 seconds
        # of the assertion happening, this test will pass.
        self.mq.send('x')
        self.assertLess(self.mq.last_send_time - time.time(), 5)
        self.assertWriteToReadOnlyPropertyFails('last_send_time', 42)

    def test_property_last_receive_time(self):
        """exercise MessageQueue.last_receive_time"""
        self.assertEqual(self.mq.last_receive_time, 0)
        self.mq.send('x')
        self.mq.receive()
        # I can't record exactly when this send() happens, but as long as it is within 5 seconds
        # of the assertion happening, this test will pass.
        self.assertLess(self.mq.last_receive_time - time.time(), 5)
        self.assertWriteToReadOnlyPropertyFails('last_receive_time', 42)

    def test_property_last_change_time(self):
        """exercise MessageQueue.last_change_time"""
        # Note that last_change_time doesn't start out as 0 (unlike e.g. last_receive_time), so
        # I don't test that here.
        original_last_change_time = self.mq.last_change_time
        sleep_past_granularity()
        # This might seem like a no-op, but setting the UID to any value triggers a call that
        # should set last_change_time.
        self.mq.uid = self.mq.uid
        self.assertLess(self.mq.last_change_time - time.time(), 5)
        # Ensure the time actually changed.
        self.assertNotEqual(self.mq.last_change_time, original_last_change_time)
        self.assertWriteToReadOnlyPropertyFails('last_change_time', 42)

    def test_property_last_send_pid(self):
        """exercise MessageQueue.last_send_pid"""
        self.assertEqual(self.mq.last_send_pid, 0)
        self.mq.send('x')
        self.assertEqual(self.mq.last_send_pid, os.getpid())
        self.assertWriteToReadOnlyPropertyFails('last_send_pid', 42)

    def test_property_last_receive_pid(self):
        """exercise MessageQueue.last_receive_pid"""
        self.assertEqual(self.mq.last_receive_pid, 0)
        self.mq.send('x')
        self.mq.receive()
        self.assertEqual(self.mq.last_receive_pid, os.getpid())
        self.assertWriteToReadOnlyPropertyFails('last_receive_pid', 42)

    def test_property_current_messages(self):
        """exercise MessageQueue.current_messages"""
        self.assertEqual(self.mq.current_messages, 0)
        self.mq.send('x')
        self.mq.send('x')
        self.mq.send('x')
        self.assertEqual(self.mq.current_messages, 3)
        self.mq.receive()
        self.assertEqual(self.mq.current_messages, 2)
        self.mq.receive()
        self.assertEqual(self.mq.current_messages, 1)
        self.mq.receive()
        self.assertEqual(self.mq.current_messages, 0)
        self.assertWriteToReadOnlyPropertyFails('current_messages', 42)

    def test_attribute_uid(self):
        """exercise MessageQueue.uid"""
        self.assertEqual(self.mq.uid, os.geteuid())

    def test_attribute_gid(self):
        """exercise MessageQueue.gid"""
        self.assertEqual(self.mq.gid, os.getgid())

    def test_attribute_cuid(self):
        """exercise MessageQueue.cuid"""
        self.assertEqual(self.mq.cuid, os.geteuid())
        self.assertWriteToReadOnlyPropertyFails('cuid', 42)

    def test_attribute_cgid(self):
        """exercise MessageQueue.cgid"""
        self.assertEqual(self.mq.cgid, os.getgid())
        self.assertWriteToReadOnlyPropertyFails('cgid', 42)


if __name__ == '__main__':
    unittest.main()
