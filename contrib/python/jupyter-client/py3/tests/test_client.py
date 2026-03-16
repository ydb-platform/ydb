"""Tests for the KernelClient"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import os
from threading import Event
from unittest import TestCase

import pytest
from IPython.utils.capture import capture_output
from traitlets import DottedObjectName, Type

from jupyter_client.client import validate_string_dict
from jupyter_client.kernelspec import KernelSpecManager, NoSuchKernel
from jupyter_client.manager import KernelManager, start_new_async_kernel, start_new_kernel
from jupyter_client.threaded import ThreadedKernelClient, ThreadedZMQSocketChannel

TIMEOUT = 60

pjoin = os.path.join


class TestKernelClient(TestCase):
    def setUp(self):
        try:
            KernelSpecManager().get_kernel_spec("echo")
        except NoSuchKernel:
            pytest.skip()
        self.km, self.kc = start_new_kernel(kernel_name="echo")

    def tearDown(self):
        self.km.shutdown_kernel()
        self.kc.stop_channels()
        return super().tearDown()

    def test_execute_interactive(self):
        kc = self.kc

        with capture_output() as io:
            reply = kc.execute_interactive("print('hello')", timeout=TIMEOUT)
        assert "hello" in io.stdout
        assert reply["content"]["status"] == "ok"

    def _check_reply(self, reply_type, reply):
        self.assertIsInstance(reply, dict)
        self.assertEqual(reply["header"]["msg_type"], reply_type + "_reply")
        self.assertEqual(reply["parent_header"]["msg_type"], reply_type + "_request")

    def test_history(self):
        kc = self.kc
        msg_id = kc.history(session=0)
        self.assertIsInstance(msg_id, str)
        # Drain the first reply to avoid race condition
        kc._recv_reply(msg_id, timeout=TIMEOUT)
        # Now test the reply=True convenience path
        reply = kc.history(session=0, reply=True, timeout=TIMEOUT)
        self._check_reply("history", reply)

    def test_inspect(self):
        kc = self.kc
        msg_id = kc.inspect("who cares")
        self.assertIsInstance(msg_id, str)
        # Drain the first reply to avoid race condition
        kc._recv_reply(msg_id, timeout=TIMEOUT)
        # Now test the reply=True convenience path
        reply = kc.inspect("code", reply=True, timeout=TIMEOUT)
        self._check_reply("inspect", reply)

    def test_complete(self):
        kc = self.kc
        msg_id = kc.complete("who cares")
        self.assertIsInstance(msg_id, str)
        # Drain the first reply to avoid race condition
        kc._recv_reply(msg_id, timeout=TIMEOUT)
        # Now test the reply=True convenience path
        reply = kc.complete("code", reply=True, timeout=TIMEOUT)
        self._check_reply("complete", reply)

    def test_kernel_info(self):
        kc = self.kc
        msg_id = kc.kernel_info()
        self.assertIsInstance(msg_id, str)
        # Drain the first reply to avoid race condition
        kc._recv_reply(msg_id, timeout=TIMEOUT)
        # Now test the reply=True convenience path
        reply = kc.kernel_info(reply=True, timeout=TIMEOUT)
        self._check_reply("kernel_info", reply)

    def test_comm_info(self):
        kc = self.kc
        msg_id = kc.comm_info()
        self.assertIsInstance(msg_id, str)
        # Drain the first reply to avoid race condition
        kc._recv_reply(msg_id, timeout=TIMEOUT)
        # Now test the reply=True convenience path
        reply = kc.comm_info(reply=True, timeout=TIMEOUT)
        self._check_reply("comm_info", reply)

    def test_shutdown(self):
        kc = self.kc
        reply = kc.shutdown(reply=True, timeout=TIMEOUT)
        self._check_reply("shutdown", reply)

    def test_shutdown_id(self):
        kc = self.kc
        msg_id = kc.shutdown()
        self.assertIsInstance(msg_id, str)


@pytest.fixture
def kc(jp_asyncio_loop):
    try:
        KernelSpecManager().get_kernel_spec("echo")
    except NoSuchKernel:
        pytest.skip()

    async def start():
        return await start_new_async_kernel(kernel_name="echo")

    km, kc = jp_asyncio_loop.run_until_complete(start())
    yield kc

    async def stop():
        await km.shutdown_kernel()

    jp_asyncio_loop.run_until_complete(stop())
    kc.stop_channels()


class TestAsyncKernelClient:
    async def test_execute_interactive(self, kc):
        reply = await kc.execute_interactive("hello", timeout=TIMEOUT)
        assert reply["content"]["status"] == "ok"

    def _check_reply(self, reply_type, reply):
        assert isinstance(reply, dict)
        assert reply["header"]["msg_type"] == reply_type + "_reply"
        assert reply["parent_header"]["msg_type"] == reply_type + "_request"

    async def test_input_request(self, kc):
        def handle_stdin(msg):
            kc.input("test")

        reply = await kc.execute_interactive(
            "a = input()",
            stdin_hook=handle_stdin,
            timeout=TIMEOUT,
        )
        assert reply["content"]["status"] == "ok"

    async def test_output_hook(self, kc):
        called = False

        def output_hook(msg):
            nonlocal called
            if msg["header"]["msg_type"] == "stream":
                called = True

        reply = await kc.execute_interactive(
            "print('hello')", timeout=TIMEOUT, output_hook=output_hook
        )
        assert reply["content"]["status"] == "ok"
        assert called

    async def test_history(self, kc):
        msg_id = kc.history(session=0)
        assert isinstance(msg_id, str)
        # Drain the first reply to avoid race condition
        await kc._async_recv_reply(msg_id, timeout=TIMEOUT)
        # Now test the reply=True convenience path
        reply = await kc.history(session=0, reply=True, timeout=TIMEOUT)
        self._check_reply("history", reply)

    async def test_inspect(self, kc):
        msg_id = kc.inspect("who cares")
        assert isinstance(msg_id, str)
        # Drain the first reply to avoid race condition
        await kc._async_recv_reply(msg_id, timeout=TIMEOUT)
        # Now test the reply=True convenience path
        reply = await kc.inspect("code", reply=True, timeout=TIMEOUT)
        self._check_reply("inspect", reply)

    async def test_complete(self, kc):
        msg_id = kc.complete("who cares")
        assert isinstance(msg_id, str)
        # Drain the first reply to avoid race condition
        await kc._async_recv_reply(msg_id, timeout=TIMEOUT)
        # Now test the reply=True convenience path
        reply = await kc.complete("code", reply=True, timeout=TIMEOUT)
        self._check_reply("complete", reply)

    async def test_is_complete(self, kc):
        msg_id = kc.is_complete("who cares")
        assert isinstance(msg_id, str)
        # Drain the first reply to avoid race condition
        await kc._async_recv_reply(msg_id, timeout=TIMEOUT)
        # Now test the reply=True convenience path
        reply = await kc.is_complete("code", reply=True, timeout=TIMEOUT)
        self._check_reply("is_complete", reply)

    async def test_kernel_info(self, kc):
        msg_id = kc.kernel_info()
        assert isinstance(msg_id, str)
        # Drain the first reply to avoid race condition
        await kc._async_recv_reply(msg_id, timeout=TIMEOUT)
        # Now test the reply=True convenience path
        reply = await kc.kernel_info(reply=True, timeout=TIMEOUT)
        self._check_reply("kernel_info", reply)

    async def test_comm_info(self, kc):
        msg_id = kc.comm_info()
        assert isinstance(msg_id, str)
        # Drain the first reply to avoid race condition
        await kc._async_recv_reply(msg_id, timeout=TIMEOUT)
        # Now test the reply=True convenience path
        reply = await kc.comm_info(reply=True, timeout=TIMEOUT)
        self._check_reply("comm_info", reply)

    async def test_shutdown(self, kc):
        reply = await kc.shutdown(reply=True, timeout=TIMEOUT)
        self._check_reply("shutdown", reply)

    async def test_shutdown_id(self, kc):
        msg_id = kc.shutdown()
        assert isinstance(msg_id, str)


class ThreadedKernelManager(KernelManager):
    client_class = DottedObjectName("tests.test_client.CustomThreadedKernelClient")


class CustomThreadedZMQSocketChannel(ThreadedZMQSocketChannel):
    last_msg = None

    def __init__(self, *args, **kwargs):
        self.msg_recv = Event()
        super().__init__(*args, **kwargs)

    def call_handlers(self, msg):
        self.last_msg = msg
        self.msg_recv.set()


class CustomThreadedKernelClient(ThreadedKernelClient):
    iopub_channel_class = Type(CustomThreadedZMQSocketChannel)  # type:ignore[arg-type]
    shell_channel_class = Type(CustomThreadedZMQSocketChannel)  # type:ignore[arg-type]
    stdin_channel_class = Type(CustomThreadedZMQSocketChannel)  # type:ignore[arg-type]
    control_channel_class = Type(CustomThreadedZMQSocketChannel)  # type:ignore[arg-type]


class TestThreadedKernelClient(TestKernelClient):
    def setUp(self):
        try:
            KernelSpecManager().get_kernel_spec("echo")
        except NoSuchKernel:
            pytest.skip()
        self.km = km = ThreadedKernelManager(kernel_name="echo")
        km.start_kernel()
        self.kc = kc = km.client()
        kc.start_channels()

    def tearDown(self):
        self.km.shutdown_kernel()
        self.kc.stop_channels()

    def _check_reply(self, reply_type, reply):
        self.assertIsInstance(reply, dict)
        self.assertEqual(reply["header"]["msg_type"], reply_type + "_reply")
        self.assertEqual(reply["parent_header"]["msg_type"], reply_type + "_request")

    def test_execute_interactive(self):
        pytest.skip("Not supported")

    def test_history(self):
        kc = self.kc
        msg_id = kc.history(session=0)
        self.assertIsInstance(msg_id, str)
        kc.history(session=0)
        kc.shell_channel.msg_recv.wait()
        reply = kc.shell_channel.last_msg
        self._check_reply("history", reply)

    def test_inspect(self):
        kc = self.kc
        msg_id = kc.inspect("who cares")
        self.assertIsInstance(msg_id, str)
        kc.inspect("code")
        kc.shell_channel.msg_recv.wait()
        reply = kc.shell_channel.last_msg
        self._check_reply("inspect", reply)

    def test_complete(self):
        kc = self.kc
        msg_id = kc.complete("who cares")
        self.assertIsInstance(msg_id, str)
        kc.complete("code")
        kc.shell_channel.msg_recv.wait()
        reply = kc.shell_channel.last_msg
        self._check_reply("complete", reply)

    def test_kernel_info(self):
        kc = self.kc
        msg_id = kc.kernel_info()
        self.assertIsInstance(msg_id, str)
        kc.kernel_info()
        kc.shell_channel.msg_recv.wait()
        reply = kc.shell_channel.last_msg
        self._check_reply("kernel_info", reply)

    def test_comm_info(self):
        kc = self.kc
        msg_id = kc.comm_info()
        self.assertIsInstance(msg_id, str)
        kc.shell_channel.msg_recv.wait()
        reply = kc.shell_channel.last_msg
        self._check_reply("comm_info", reply)

    def test_shutdown(self):
        kc = self.kc
        kc.shutdown()
        kc.control_channel.msg_recv.wait()
        reply = kc.control_channel.last_msg
        self._check_reply("shutdown", reply)

    def test_shutdown_id(self):
        kc = self.kc
        msg_id = kc.shutdown()
        self.assertIsInstance(msg_id, str)


def test_validate_string_dict():
    with pytest.raises(ValueError):
        validate_string_dict(dict(a=1))  # type:ignore
    with pytest.raises(ValueError):
        validate_string_dict({1: "a"})  # type:ignore
