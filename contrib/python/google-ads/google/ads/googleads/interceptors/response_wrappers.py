# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Wrapper classes used to modify the behavior of response objects."""

import grpc
from types import SimpleNamespace, TracebackType
from typing import Any, Callable, Iterator, Optional, Union

from google.ads.googleads import util
from google.ads.googleads.interceptors import MetadataType
from google.protobuf.message import Message as ProtobufMessageType


class _UnaryStreamWrapper(grpc.Call, grpc.Future):
    def __init__(
        self,
        underlay_call: Union[grpc.Call, grpc.Future],
        failure_handler: Callable[[Union[grpc.Call, grpc.Future]], grpc.Call],
        use_proto_plus: bool = False,
    ):
        super().__init__()
        self._underlay_call: Union[grpc.Call, grpc.Future] = underlay_call
        self._failure_handler: Callable[[Union[grpc.Call, grpc.Future]], grpc.Call] = (
            failure_handler
        )
        self._exception: Optional[grpc.RpcError] = None
        self._use_proto_plus: bool = use_proto_plus
        self._cache: SimpleNamespace = SimpleNamespace(
            **{"initial_response_object": None}
        )

    def initial_metadata(self) -> MetadataType:
        return self._underlay_call.initial_metadata()

    def trailing_metadata(self) -> MetadataType:
        return self._underlay_call.initial_metadata()

    def code(self) -> grpc.StatusCode:
        return self._underlay_call.code()

    def details(self) -> str:
        return self._underlay_call.details()

    def debug_error_string(self) -> str:
        return self._underlay_call.debug_error_string()

    def cancelled(self) -> bool:
        return self._underlay_call.cancelled()

    def running(self) -> bool:
        return self._underlay_call.running()

    def done(self) -> bool:
        return self._underlay_call.done()

    def result(self, timeout: Optional[float] = None) -> ProtobufMessageType:
        return self._underlay_call.result(timeout=timeout)

    def exception(self, timeout: Optional[float] = None) -> Optional[grpc.RpcError]:
        if self._exception:
            return self._exception
        else:
            return self._underlay_call.exception(timeout=timeout)

    def traceback(self, timeout: Optional[float] = None) -> Optional[TracebackType]:
        return self._underlay_call.traceback(timeout=timeout)

    def add_done_callback(self, fn: Callable[[grpc.Future], Any]) -> None:
        return self._underlay_call.add_done_callback(fn)

    def add_callback(self, callback: Callable[[grpc.Future], Any]) -> None:
        return self._underlay_call.add_callback(callback)

    def is_active(self) -> bool:
        return self._underlay_call.is_active()

    def time_remaining(self) -> Optional[float]:
        return self._underlay_call.time_remaining()

    def cancel(self) -> bool:
        return self._underlay_call.cancel()

    def __iter__(self) -> Iterator[ProtobufMessageType]:
        return self

    def __next__(self) -> ProtobufMessageType:
        try:
            message: ProtobufMessageType = next(self._underlay_call)
            # Store only the first streaming response object in _cache.initial_response_object.
            # Each streaming response object captures 10,000 rows.
            # The default log character limit is 5,000, so caching multiple
            # streaming response objects does not make sense in most cases,
            # as only [part of] 1 will get logged.
            if self._cache.initial_response_object is None:
                self._cache.initial_response_object = message
            if self._use_proto_plus is True:
                # By default this message is wrapped by proto-plus
                return message
            else:
                return util.convert_proto_plus_to_protobuf(message)
        except StopIteration:
            raise
        except Exception as e:
            try:
                self._failure_handler(self._underlay_call)
            except Exception as e_inner:
                self._exception = e
                raise e_inner

    def get_cache(self) -> SimpleNamespace:
        return self._cache


class _UnaryUnaryWrapper(grpc.Call, grpc.Future):
    def __init__(
        self,
        underlay_call: Union[grpc.Call, grpc.Future],
        use_proto_plus: bool = False,
    ):
        super().__init__()
        self._underlay_call: Union[grpc.Call, grpc.Future] = underlay_call
        self._use_proto_plus: bool = use_proto_plus
        self._exception: Optional[grpc.RpcError] = None

    def initial_metadata(self) -> MetadataType:
        return self._underlay_call.initial_metadata()

    def trailing_metadata(self) -> MetadataType:
        return self._underlay_call.initial_metadata()

    def code(self) -> grpc.StatusCode:
        return self._underlay_call.code()

    def details(self) -> str:
        return self._underlay_call.details()

    def debug_error_string(self) -> str:
        return self._underlay_call.debug_error_string()

    def cancelled(self) -> bool:
        return self._underlay_call.cancelled()

    def running(self) -> bool:
        return self._underlay_call.running()

    def done(self) -> bool:
        return self._underlay_call.done()

    def result(self, timeout: Optional[float] = None) -> ProtobufMessageType:
        message: ProtobufMessageType = self._underlay_call.result()
        if self._use_proto_plus is True:
            return message
        else:
            return util.convert_proto_plus_to_protobuf(message)

    def exception(self, timeout: Optional[float] = None) -> Optional[grpc.RpcError]:
        if self._exception:
            return self._exception
        else:
            return self._underlay_call.exception(timeout=timeout)

    def traceback(self, timeout: Optional[float] = None) -> Optional[TracebackType]:
        return self._underlay_call.traceback(timeout=timeout)

    def add_done_callback(self, fn: Callable[[grpc.Future], Any]) -> None:
        return self._underlay_call.add_done_callback(fn)

    def add_callback(self, callback: Callable[[grpc.Future], Any]) -> None:
        return self._underlay_call.add_callback(callback)

    def is_active(self) -> bool:
        return self._underlay_call.is_active()

    def time_remaining(self) -> Optional[float]:
        return self._underlay_call.time_remaining()

    def cancel(self) -> bool:
        return self._underlay_call.cancel()

    def __iter__(self) -> Iterator[ProtobufMessageType]:
        if self._use_proto_plus is True:
            return self
        else:
            return util.convert_proto_plus_to_protobuf(self)

    def __next__(self) -> ProtobufMessageType:
        return next(self._underlay_call)
