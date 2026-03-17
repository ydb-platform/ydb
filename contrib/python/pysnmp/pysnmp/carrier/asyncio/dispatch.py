#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
#
# Copyright (C) 2014, Zebra Technologies
# Authors: Matt Hooks <me@matthooks.com>
#          Zachary Lorusso <zlorusso@gmail.com>
#
# Copyright (C) 2024, LeXtudio Inc. <support@lextudio.com>
#
# License: https://www.pysnmp.com/pysnmp/license.html
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimer in the
#   documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
# THE POSSIBILITY OF SUCH DAMAGE.
#
import asyncio
import sys
import traceback
import warnings
from time import time
from typing import Tuple

from pysnmp.carrier.base import AbstractTransport, AbstractTransportDispatcher
from pysnmp.error import PySnmpError


class AsyncioDispatcher(AbstractTransportDispatcher):
    """AsyncioDispatcher based on asyncio event loop."""

    loop: asyncio.AbstractEventLoop
    __transport_count: int

    def __init__(self, *args, **kwargs):
        """Create an asyncio dispatcher object."""
        super().__init__()
        self.__transport_count = 0
        if "timeout" in kwargs:
            self.set_timer_resolution(kwargs["timeout"])
        self.loopingcall = None
        self.loop = kwargs.pop("loop", asyncio.get_event_loop())

    async def handle_timeout(self):
        """Handle timeout event with proper error handling."""
        try:
            while True:
                await asyncio.sleep(self.get_timer_resolution())
                self.handle_timer_tick(time())
        except asyncio.CancelledError:
            # Gracefully handle cancellation
            pass

    def run_dispatcher(self, timeout: float = 0.0):
        """Run the dispatcher loop."""
        if not self.loop.is_running():
            try:
                if timeout > 0:
                    self.loop.call_later(timeout, self.__close_dispatcher)
                self.loop.run_forever()
            except KeyboardInterrupt:
                raise
            except Exception:
                raise PySnmpError(";".join(traceback.format_exception(*sys.exc_info())))

    def __close_dispatcher(self):
        if self.loop.is_running():
            self.loop.stop()
        self.close_dispatcher()

    def close_dispatcher(self):
        super().close_dispatcher()
        self.__transport_count = 0
        self._cancel_loopingcall()

    def register_transport(
        self, tDomain: Tuple[int, ...], transport: AbstractTransport
    ):
        """Register transport associated with given transport domain."""
        if self.loopingcall is None and self.get_timer_resolution() > 0:
            self.loopingcall = asyncio.ensure_future(self.handle_timeout(), loop=self.loop)
        super().register_transport(tDomain, transport)
        self.__transport_count += 1

    def unregister_transport(self, tDomain: Tuple[int, ...]):
        """Unregister transport associated with given transport domain."""
        t = self.get_transport(tDomain)
        if t is not None:
            super().unregister_transport(tDomain)
            self.__transport_count -= 1

        if self.__transport_count == 0:
            # The last transport has been removed, stop the timeout
            self._cancel_loopingcall()

    def _cancel_loopingcall(self):
        if self.loopingcall is not None:
            if not self.loopingcall.done():
                loop = self.loopingcall.get_loop()
                if not loop.is_closed():
                    self.loopingcall.cancel()
            self.loopingcall = None

    def __del__(self):
        self._cancel_loopingcall()

    # compatibility with legacy code
    # Old to new attribute mapping
    deprecated_attributes = {
        "jobStarted": "job_started",
        "jobFinished": "job_finished",
        "runDispatcher": "run_dispatcher",
        "registerTransport": "register_transport",
        "closeDispatcher": "close_dispatcher",
    }

    def __getattr__(self, attr: str):
        """Handle deprecated attributes."""
        if new_attr := self.deprecated_attributes.get(attr):
            warnings.warn(
                f"{attr} is deprecated. Please use {new_attr} instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return getattr(self, new_attr)
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{attr}'"
        )
