"""Tests for the notebook kernel and session manager."""

import asyncio
import concurrent.futures
import os
import sys
import sysconfig
import uuid
from asyncio import ensure_future
from subprocess import PIPE
from unittest import TestCase

import pytest
import zmq
from jupyter_core import paths
from tornado.testing import AsyncTestCase, gen_test
from traitlets.config.loader import Config

from jupyter_client import AsyncKernelManager, KernelManager
from jupyter_client.localinterfaces import localhost
from jupyter_client.multikernelmanager import AsyncMultiKernelManager, MultiKernelManager

from tests.utils import (
    AsyncKMSubclass,
    AsyncMKMSubclass,
    SyncKMSubclass,
    SyncMKMSubclass,
    install_kernel,
    skip_win32,
)

TIMEOUT = 90

is_freethreaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


async def now(awaitable):
    """Use this function ensure that this awaitable
    happens before other awaitables defined after it.
    """
    (out,) = await asyncio.gather(awaitable)
    return out


class TestKernelManager(TestCase):
    def tearDown(self):
        zmq.Context.instance().destroy(linger=0)
        return super().tearDown()

    # static so picklable for multiprocessing on Windows

    @staticmethod
    def _get_tcp_km():
        c = Config()
        km = MultiKernelManager(config=c)
        return km

    @staticmethod
    def _get_tcp_km_sub():
        c = Config()
        km = SyncMKMSubclass(config=c)
        return km

    # static so picklable for multiprocessing on Windows
    @staticmethod
    def _get_ipc_km():
        c = Config()
        c.KernelManager.transport = "ipc"
        c.KernelManager.ip = "test"
        km = MultiKernelManager(config=c)
        return km

    # static so picklable for multiprocessing on Windows
    @staticmethod
    def _run_lifecycle(km, test_kid=None):
        if test_kid:
            kid = km.start_kernel(stdout=PIPE, stderr=PIPE, kernel_id=test_kid)
            assert kid == test_kid
        else:
            kid = km.start_kernel(stdout=PIPE, stderr=PIPE)
        assert km.is_alive(kid)
        assert km.get_kernel(kid).ready.done()
        assert kid in km
        assert kid in km.list_kernel_ids()
        assert len(km) == 1, f"{len(km)} != {1}"
        km.restart_kernel(kid, now=True)
        assert km.is_alive(kid)
        assert kid in km.list_kernel_ids()
        km.interrupt_kernel(kid)
        k = km.get_kernel(kid)
        kc = k.client()
        assert isinstance(k, KernelManager)
        km.shutdown_kernel(kid, now=True)
        assert kid not in km, f"{kid} not in {km}"
        kc.stop_channels()
        km.context.destroy(linger=0)

    def _run_cinfo(self, km, transport, ip):
        kid = km.start_kernel(stdout=PIPE, stderr=PIPE)
        km.get_kernel(kid)
        cinfo = km.get_connection_info(kid)
        self.assertEqual(transport, cinfo["transport"])
        self.assertEqual(ip, cinfo["ip"])
        self.assertTrue("stdin_port" in cinfo)
        self.assertTrue("iopub_port" in cinfo)
        stream = km.connect_iopub(kid)
        stream.close()
        self.assertTrue("shell_port" in cinfo)
        stream = km.connect_shell(kid)
        stream.close()
        self.assertTrue("hb_port" in cinfo)
        stream = km.connect_hb(kid)
        stream.close()
        km.shutdown_kernel(kid, now=True)
        km.context.destroy(linger=0)

    # static so picklable for multiprocessing on Windows
    @classmethod
    def test_tcp_lifecycle(cls):
        km = cls._get_tcp_km()
        cls._run_lifecycle(km)

    def test_tcp_lifecycle_with_kernel_id(self):
        km = self._get_tcp_km()
        self._run_lifecycle(km, test_kid=str(uuid.uuid4()))

    def test_shutdown_all(self):
        km = self._get_tcp_km()
        kid = km.start_kernel(stdout=PIPE, stderr=PIPE)
        self.assertIn(kid, km)
        km.shutdown_all()
        self.assertNotIn(kid, km)
        # shutdown again is okay, because we have no kernels
        km.shutdown_all()
        km.context.destroy(linger=0)

    def test_tcp_cinfo(self):
        km = self._get_tcp_km()
        self._run_cinfo(km, "tcp", localhost())

    @skip_win32
    def test_ipc_lifecycle(self):
        km = self._get_ipc_km()
        self._run_lifecycle(km)

    @skip_win32
    def test_ipc_cinfo(self):
        km = self._get_ipc_km()
        self._run_cinfo(km, "ipc", "test")

    def test_start_sequence_tcp_kernels(self):
        """Ensure that a sequence of kernel startups doesn't break anything."""
        self._run_lifecycle(self._get_tcp_km())
        self._run_lifecycle(self._get_tcp_km())
        self._run_lifecycle(self._get_tcp_km())

    @skip_win32
    def test_start_sequence_ipc_kernels(self):
        """Ensure that a sequence of kernel startups doesn't break anything."""
        self._run_lifecycle(self._get_ipc_km())
        self._run_lifecycle(self._get_ipc_km())
        self._run_lifecycle(self._get_ipc_km())

    def tcp_lifecycle_with_loop(self):
        # Ensure each thread has an event loop
        async def _task():
            return self.test_tcp_lifecycle()

        asyncio.run(_task())

    @pytest.mark.skipif(is_freethreaded, reason="Fail on free-threaded python")
    def test_start_parallel_thread_kernels(self):
        self.test_tcp_lifecycle()

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as thread_executor:
            future1 = thread_executor.submit(self.tcp_lifecycle_with_loop)
            future2 = thread_executor.submit(self.tcp_lifecycle_with_loop)
            future1.result()
            future2.result()

    @pytest.mark.skipif(
        sys.platform == "linux",
        reason="Kernel refuses to start in process pool",
    )
    def test_start_parallel_process_kernels(self):
        self.test_tcp_lifecycle()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as thread_executor:
            future1 = thread_executor.submit(self.tcp_lifecycle_with_loop)
            with concurrent.futures.ProcessPoolExecutor(max_workers=1) as process_executor:
                # Windows tests needs this target to be picklable:
                future2 = process_executor.submit(self.test_tcp_lifecycle)
                future2.result()
            future1.result()

    def test_subclass_callables(self):
        km = self._get_tcp_km_sub()

        km.reset_counts()
        kid = km.start_kernel(stdout=PIPE, stderr=PIPE)
        assert km.call_count("start_kernel") == 1
        assert isinstance(km.get_kernel(kid), SyncKMSubclass)
        assert km.get_kernel(kid).call_count("start_kernel") == 1
        assert km.get_kernel(kid).call_count("_async_launch_kernel") == 1

        assert km.is_alive(kid)
        assert kid in km
        assert kid in km.list_kernel_ids()
        assert len(km) == 1, f"{len(km)} != {1}"

        km.get_kernel(kid).reset_counts()
        km.reset_counts()
        km.restart_kernel(kid, now=True)
        assert km.call_count("restart_kernel") == 1
        assert km.call_count("get_kernel") == 1
        assert km.get_kernel(kid).call_count("restart_kernel") == 1
        assert km.get_kernel(kid).call_count("_async_shutdown_kernel") == 1
        assert km.get_kernel(kid).call_count("_async_interrupt_kernel") == 1
        assert km.get_kernel(kid).call_count("_async_kill_kernel") == 1
        assert km.get_kernel(kid).call_count("_async_cleanup_resources") == 1
        assert km.get_kernel(kid).call_count("_async_launch_kernel") == 1

        assert km.is_alive(kid)
        assert kid in km.list_kernel_ids()

        km.get_kernel(kid).reset_counts()
        km.reset_counts()
        km.interrupt_kernel(kid)
        assert km.call_count("interrupt_kernel") == 1
        assert km.call_count("get_kernel") == 1
        assert km.get_kernel(kid).call_count("interrupt_kernel") == 1

        km.get_kernel(kid).reset_counts()
        km.reset_counts()
        k = km.get_kernel(kid)
        assert isinstance(k, SyncKMSubclass)
        assert km.call_count("get_kernel") == 1

        km.get_kernel(kid).reset_counts()
        km.reset_counts()
        km.shutdown_all(now=True)
        assert km.call_count("remove_kernel") == 1
        assert km.call_count("request_shutdown") == 0
        assert km.call_count("finish_shutdown") == 0
        assert km.call_count("cleanup_resources") == 0

        assert kid not in km, f"{kid} not in {km}"

    def test_stream_on_recv(self):
        mkm = self._get_tcp_km()
        kid = mkm.start_kernel(stdout=PIPE, stderr=PIPE)
        stream = mkm.connect_iopub(kid)

        km = mkm.get_kernel(kid)
        client = km.client()
        session = km.session
        called = False

        def record_activity(msg_list):
            nonlocal called
            """Record an IOPub message arriving from a kernel"""
            _idents, fed_msg_list = session.feed_identities(msg_list)
            msg = session.deserialize(fed_msg_list, content=False)

            msg_type = msg["header"]["msg_type"]
            stream.send(msg)
            called = True

        stream.on_recv(record_activity)
        while True:
            client.kernel_info()
            import time

            time.sleep(0.1)
            if called:
                break
        stream.close()
        client.stop_channels()
        km.shutdown_kernel(now=True)


class TestAsyncKernelManager(AsyncTestCase):
    def tearDown(self):
        zmq.Context.instance().destroy(linger=0)
        return super().tearDown()

    # static so picklable for multiprocessing on Windows
    @staticmethod
    def _get_tcp_km():
        c = Config()
        km = AsyncMultiKernelManager(config=c)
        return km

    @staticmethod
    def _get_tcp_km_sub():
        c = Config()
        km = AsyncMKMSubclass(config=c)
        return km

    # static so picklable for multiprocessing on Windows
    @staticmethod
    def _get_ipc_km():
        c = Config()
        c.KernelManager.transport = "ipc"
        c.KernelManager.ip = "test"
        km = AsyncMultiKernelManager(config=c)
        return km

    @staticmethod
    def _get_pending_kernels_km():
        c = Config()
        c.AsyncMultiKernelManager.use_pending_kernels = True
        km = AsyncMultiKernelManager(config=c)
        return km

    # static so picklable for multiprocessing on Windows
    @staticmethod
    async def _run_lifecycle(km, test_kid=None):
        if test_kid:
            kid = await km.start_kernel(stdout=PIPE, stderr=PIPE, kernel_id=test_kid)
            assert kid == test_kid
        else:
            kid = await km.start_kernel(stdout=PIPE, stderr=PIPE)
        assert await km.is_alive(kid)
        assert kid in km
        assert kid in km.list_kernel_ids()
        assert len(km) == 1, f"{len(km)} != {1}"
        # Ensure we can interrupt during a restart.
        fut = km.restart_kernel(kid, now=True)
        await km.interrupt_kernel(kid)
        assert await km.is_alive(kid)
        await fut
        assert kid in km.list_kernel_ids()
        k = km.get_kernel(kid)
        assert isinstance(k, AsyncKernelManager)
        await km.shutdown_kernel(kid, now=True)
        assert kid not in km, f"{kid} not in {km}"
        await km.shutdown_all()
        km.context.destroy(linger=100)

    async def _run_cinfo(self, km, transport, ip):
        kid = await km.start_kernel(stdout=PIPE, stderr=PIPE)
        km.get_kernel(kid)
        cinfo = km.get_connection_info(kid)
        self.assertEqual(transport, cinfo["transport"])
        self.assertEqual(ip, cinfo["ip"])
        self.assertTrue("stdin_port" in cinfo)
        self.assertTrue("iopub_port" in cinfo)
        stream = km.connect_iopub(kid)
        stream.close()
        self.assertTrue("shell_port" in cinfo)
        stream = km.connect_shell(kid)
        stream.close()
        self.assertTrue("hb_port" in cinfo)
        stream = km.connect_hb(kid)
        stream.close()
        await km.shutdown_kernel(kid, now=True)
        self.assertNotIn(kid, km)
        km.context.destroy(linger=0)

    @gen_test
    async def test_tcp_lifecycle(self):
        await self.raw_tcp_lifecycle()

    @gen_test
    async def test_tcp_lifecycle_with_kernel_id(self):
        await self.raw_tcp_lifecycle(test_kid=str(uuid.uuid4()))

    @gen_test
    async def test_shutdown_all(self):
        km = self._get_tcp_km()
        kid = await km.start_kernel(stdout=PIPE, stderr=PIPE)
        self.assertIn(kid, km)
        await km.shutdown_all()
        self.assertNotIn(kid, km)
        # shutdown again is okay, because we have no kernels
        await km.shutdown_all()

    @gen_test(timeout=20)
    async def test_use_after_shutdown_all(self):
        km = self._get_tcp_km()
        kid = await km.start_kernel(stdout=PIPE, stderr=PIPE)
        self.assertIn(kid, km)
        await km.shutdown_all()
        self.assertNotIn(kid, km)

        # Start another kernel
        kid = await km.start_kernel(stdout=PIPE, stderr=PIPE)
        self.assertIn(kid, km)
        await km.shutdown_all()
        self.assertNotIn(kid, km)
        # shutdown again is okay, because we have no kernels
        await km.shutdown_all()

    @gen_test(timeout=20)
    async def test_shutdown_all_while_starting(self):
        km = self._get_tcp_km()
        kid_future = asyncio.ensure_future(km.start_kernel(stdout=PIPE, stderr=PIPE))
        # This is relying on the ordering of the asyncio queue, not sure if guaranteed or not:
        kid, _ = await asyncio.gather(kid_future, km.shutdown_all())
        self.assertNotIn(kid, km)

        # Start another kernel
        kid = await ensure_future(km.start_kernel(stdout=PIPE, stderr=PIPE))
        self.assertIn(kid, km)
        self.assertEqual(len(km), 1)
        await km.shutdown_all()
        self.assertNotIn(kid, km)
        # shutdown again is okay, because we have no kernels
        await km.shutdown_all()
        km.context.destroy(linger=0)

    @gen_test
    async def test_use_pending_kernels(self):
        km = self._get_pending_kernels_km()
        kid = await ensure_future(km.start_kernel(stdout=PIPE, stderr=PIPE))
        kernel = km.get_kernel(kid)
        assert not kernel.ready.done()
        assert kid in km
        assert kid in km.list_kernel_ids()
        assert len(km) == 1, f"{len(km)} != {1}"
        # Wait for the kernel to start.
        await kernel.ready
        await km.restart_kernel(kid, now=True)
        out = await km.is_alive(kid)
        assert out
        assert kid in km.list_kernel_ids()
        await km.interrupt_kernel(kid)
        k = km.get_kernel(kid)
        assert isinstance(k, AsyncKernelManager)
        await ensure_future(km.shutdown_kernel(kid, now=True))
        km.context.destroy(linger=0)
        # Wait for the kernel to shutdown
        await kernel.ready
        assert kid not in km, f"{kid} not in {km}"

    @gen_test
    async def test_use_pending_kernels_early_restart(self):
        km = self._get_pending_kernels_km()
        kid = await ensure_future(km.start_kernel(stdout=PIPE, stderr=PIPE))
        kernel = km.get_kernel(kid)
        assert not kernel.ready.done()
        with pytest.raises(RuntimeError):
            await km.restart_kernel(kid, now=True)
        await kernel.ready
        await ensure_future(km.shutdown_kernel(kid, now=True))
        # Wait for the kernel to shutdown
        await kernel.ready
        assert kid not in km, f"{kid} not in {km}"
        km.context.destroy(linger=0)

    @gen_test
    async def test_use_pending_kernels_early_shutdown(self):
        km = self._get_pending_kernels_km()
        kid = await ensure_future(km.start_kernel(stdout=PIPE, stderr=PIPE))
        kernel = km.get_kernel(kid)
        assert not kernel.ready.done()
        # Try shutting down while the kernel is pending
        await ensure_future(km.shutdown_kernel(kid, now=True))
        # Wait for the kernel to shutdown
        await kernel.ready
        assert kid not in km, f"{kid} not in {km}"
        km.context.destroy(linger=0)

    @gen_test
    async def test_use_pending_kernels_early_interrupt(self):
        km = self._get_pending_kernels_km()
        kid = await ensure_future(km.start_kernel(stdout=PIPE, stderr=PIPE))
        kernel = km.get_kernel(kid)
        assert not kernel.ready.done()
        with pytest.raises(RuntimeError):
            await km.interrupt_kernel(kid)
        # Now wait for the kernel to be ready.
        await kernel.ready
        await ensure_future(km.shutdown_kernel(kid, now=True))
        # Wait for the kernel to shutdown
        await kernel.ready
        assert kid not in km, f"{kid} not in {km}"
        km.context.destroy(linger=0)

    @gen_test
    async def test_tcp_cinfo(self):
        km = self._get_tcp_km()
        await self._run_cinfo(km, "tcp", localhost())
        km.context.destroy(linger=0)

    @skip_win32
    @gen_test
    async def test_ipc_lifecycle(self):
        km = self._get_ipc_km()
        await self._run_lifecycle(km)
        km.context.destroy(linger=0)

    @skip_win32
    @gen_test
    async def test_ipc_cinfo(self):
        km = self._get_ipc_km()
        await self._run_cinfo(km, "ipc", "test")
        km.context.destroy(linger=0)

    @gen_test
    async def test_start_sequence_tcp_kernels(self):
        """Ensure that a sequence of kernel startups doesn't break anything."""
        await self._run_lifecycle(self._get_tcp_km())
        await self._run_lifecycle(self._get_tcp_km())
        await self._run_lifecycle(self._get_tcp_km())

    @skip_win32
    @gen_test
    async def test_start_sequence_ipc_kernels(self):
        """Ensure that a sequence of kernel startups doesn't break anything."""
        await self._run_lifecycle(self._get_ipc_km())
        await self._run_lifecycle(self._get_ipc_km())
        await self._run_lifecycle(self._get_ipc_km())

    def tcp_lifecycle_with_loop(self):
        # Ensure each thread has an event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.raw_tcp_lifecycle())
        loop.close()

    # static so picklable for multiprocessing on Windows
    @classmethod
    async def raw_tcp_lifecycle(cls, test_kid=None):
        # Since @gen_test creates an event loop, we need a raw form of
        # test_tcp_lifecycle that assumes the loop already exists.
        km = cls._get_tcp_km()
        await cls._run_lifecycle(km, test_kid=test_kid)

    # static so picklable for multiprocessing on Windows
    @classmethod
    def raw_tcp_lifecycle_sync(cls, test_kid=None):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(cls.raw_tcp_lifecycle(test_kid=test_kid))
        loop.close()

    @gen_test
    async def test_start_parallel_thread_kernels(self):
        await self.raw_tcp_lifecycle()

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as thread_executor:
            future1 = thread_executor.submit(self.tcp_lifecycle_with_loop)
            future2 = thread_executor.submit(self.tcp_lifecycle_with_loop)
            future1.result()
            future2.result()

    @gen_test
    async def test_start_parallel_process_kernels(self):
        await self.raw_tcp_lifecycle()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as thread_executor:
            future1 = thread_executor.submit(self.tcp_lifecycle_with_loop)
            with concurrent.futures.ProcessPoolExecutor(max_workers=1) as process_executor:
                # Windows tests needs this target to be picklable:
                future2 = process_executor.submit(self.raw_tcp_lifecycle_sync)
                future2.result()
            future1.result()

    @gen_test
    async def test_subclass_callables(self):
        mkm = self._get_tcp_km_sub()

        mkm.reset_counts()
        kid = await mkm.start_kernel(stdout=PIPE, stderr=PIPE)
        assert mkm.call_count("start_kernel") == 1
        assert isinstance(mkm.get_kernel(kid), AsyncKMSubclass)
        assert mkm.get_kernel(kid).call_count("start_kernel") == 1
        assert mkm.get_kernel(kid).call_count("_async_launch_kernel") == 1

        assert await mkm.is_alive(kid)
        assert kid in mkm
        assert kid in mkm.list_kernel_ids()
        assert len(mkm) == 1, f"{len(mkm)} != {1}"

        mkm.get_kernel(kid).reset_counts()
        mkm.reset_counts()
        await mkm.restart_kernel(kid, now=True)
        assert mkm.call_count("restart_kernel") == 1
        assert mkm.call_count("get_kernel") == 1
        assert mkm.get_kernel(kid).call_count("restart_kernel") == 1
        assert mkm.get_kernel(kid).call_count("_async_interrupt_kernel") == 1
        assert mkm.get_kernel(kid).call_count("_async_kill_kernel") == 1
        assert mkm.get_kernel(kid).call_count("_async_cleanup_resources") == 1
        assert mkm.get_kernel(kid).call_count("_async_launch_kernel") == 1

        assert await mkm.is_alive(kid)
        assert kid in mkm.list_kernel_ids()

        mkm.get_kernel(kid).reset_counts()
        mkm.reset_counts()
        await mkm.interrupt_kernel(kid)
        assert mkm.call_count("interrupt_kernel") == 1
        assert mkm.call_count("get_kernel") == 1
        assert mkm.get_kernel(kid).call_count("interrupt_kernel") == 1

        mkm.get_kernel(kid).reset_counts()
        mkm.reset_counts()
        k = mkm.get_kernel(kid)
        assert isinstance(k, AsyncKMSubclass)
        assert mkm.call_count("get_kernel") == 1

        mkm.get_kernel(kid).reset_counts()
        mkm.reset_counts()
        await mkm.shutdown_all(now=True)
        mkm.context.destroy(linger=0)
        assert mkm.call_count("remove_kernel") == 1
        assert mkm.call_count("_async_request_shutdown") == 0
        assert mkm.call_count("_async_finish_shutdown") == 0
        assert mkm.call_count("_async_cleanup_resources") == 0

        assert kid not in mkm, f"{kid} not in {mkm}"

    @gen_test
    async def test_bad_kernelspec(self):
        km = self._get_tcp_km()
        install_kernel(
            os.path.join(paths.jupyter_data_dir(), "kernels"),
            argv=["non_existent_executable"],
            name="bad",
        )
        with pytest.raises(FileNotFoundError):
            await ensure_future(km.start_kernel(kernel_name="bad", stdout=PIPE, stderr=PIPE))
        km.context.destroy(linger=0)

    @gen_test
    async def test_bad_kernelspec_pending(self):
        km = self._get_pending_kernels_km()
        install_kernel(
            os.path.join(paths.jupyter_data_dir(), "kernels"),
            argv=["non_existent_executable"],
            name="bad",
        )
        kernel_id = await ensure_future(
            km.start_kernel(kernel_name="bad", stdout=PIPE, stderr=PIPE)
        )
        with pytest.raises(FileNotFoundError):
            await km.get_kernel(kernel_id).ready
        assert kernel_id in km.list_kernel_ids()
        await ensure_future(km.shutdown_kernel(kernel_id))
        assert kernel_id not in km.list_kernel_ids()
        km.context.destroy(linger=0)

    @gen_test(timeout=TIMEOUT)
    async def test_stream_on_recv(self):
        mkm = self._get_tcp_km()
        kid = await mkm.start_kernel(stdout=PIPE, stderr=PIPE)
        stream = mkm.connect_iopub(kid)

        km = mkm.get_kernel(kid)
        client = km.client()
        session = km.session
        called = False

        def record_activity(msg_list):
            nonlocal called
            """Record an IOPub message arriving from a kernel"""
            _idents, fed_msg_list = session.feed_identities(msg_list)
            msg = session.deserialize(fed_msg_list, content=False)

            msg_type = msg["header"]["msg_type"]
            stream.send(msg)
            called = True

        stream.on_recv(record_activity)
        while True:
            await client.kernel_info(reply=True)
            if called:
                break
            await asyncio.sleep(0.1)

        client.stop_channels()
        stream.close()
        await km.shutdown_kernel(now=True)
        km.context.destroy(linger=0)
