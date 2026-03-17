# Copyright 2018 Google LLC
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
"""Errors used by the Google Ads API library."""

import grpc
from proto import Message as ProtobufMessageType


class GoogleAdsException(Exception):
    """Exception thrown in response to an API error from GoogleAds servers."""

    def __init__(
        self,
        error: grpc.RpcError,
        call: grpc.Call,
        failure: ProtobufMessageType,
        request_id: str,
    ) -> None:
        """Initializer.

        Args:
            error: the grpc.RpcError raised by an rpc call.
            call: the grpc.Call object containing the details of the rpc call.
            failure: the GoogleAdsFailure instance describing how the
                GoogleAds API call failed.
            request_id: a str request ID associated with the GoogleAds API call.
        """
        self.error: grpc.RpcError = error
        self.call: grpc.Call = call
        self.failure: ProtobufMessageType = failure
        self.request_id: str = request_id
