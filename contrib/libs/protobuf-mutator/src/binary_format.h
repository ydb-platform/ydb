// Copyright 2017 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SRC_BINARY_FORMAT_H_
#define SRC_BINARY_FORMAT_H_

#include <string>

#include "port/protobuf.h"

namespace protobuf_mutator {

// Binary serialization of protos.
bool ParseBinaryMessage(const uint8_t* data, size_t size,
                        protobuf::Message* output);
bool ParseBinaryMessage(const TProtoStringType& data, protobuf::Message* output);
size_t SaveMessageAsBinary(const protobuf::Message& message, uint8_t* data,
                           size_t max_size);
TProtoStringType SaveMessageAsBinary(const protobuf::Message& message);

}  // namespace protobuf_mutator

#endif  // SRC_BINARY_FORMAT_H_
