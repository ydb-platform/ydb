# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

THREAD_ID: Final = "thread.id"
"""
Current "managed" thread ID (as opposed to OS thread ID).
Note: Examples of where the value can be extracted from:

| Language or platform | Source |
| --- | --- |
| JVM | `Thread.currentThread().threadId()` |
| .NET | `Thread.CurrentThread.ManagedThreadId` |
| Python | `threading.current_thread().ident` |
| Ruby | `Thread.current.object_id` |
| C++ | `std::this_thread::get_id()` |
| Erlang | `erlang:self()` |.
"""

THREAD_NAME: Final = "thread.name"
"""
Current thread name.
Note: Examples of where the value can be extracted from:

| Language or platform | Source |
| --- | --- |
| JVM | `Thread.currentThread().getName()` |
| .NET | `Thread.CurrentThread.Name` |
| Python | `threading.current_thread().name` |
| Ruby | `Thread.current.name` |
| Erlang | `erlang:process_info(self(), registered_name)` |.
"""
