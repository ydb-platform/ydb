/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef CODED_STREAM_WRAPPER_HH
#define CODED_STREAM_WRAPPER_HH

#include "Adaptor.hh"

DIAGNOSTIC_PUSH

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wshorten-64-to-32")
DIAGNOSTIC_IGNORE("-Wreserved-id-macro")
#endif

#if defined(__GNUC__) || defined(__clang__)
DIAGNOSTIC_IGNORE("-Wconversion")
#endif

#include <google/protobuf/io/coded_stream.h>

DIAGNOSTIC_POP

namespace orc {
  // Matches the Java reader's InStream.PROTOBUF_MESSAGE_MAX_LIMIT (1 GB) so both
  // implementations reject oversized messages identically.
  constexpr int PROTOBUF_MESSAGE_MAX_LIMIT = 1024 << 20;

  // Parse a protobuf message from a ZeroCopyInputStream while enforcing the
  // total byte limit above. Use this instead of Message::ParseFromZeroCopyStream
  // for any message read from file contents.
  template <typename Message>
  inline bool parseProtobufFromStream(Message* message,
                                      google::protobuf::io::ZeroCopyInputStream* input) {
    google::protobuf::io::CodedInputStream codedStream(input);
#if defined(GOOGLE_PROTOBUF_VERSION) && GOOGLE_PROTOBUF_VERSION < 3006000
    // The single-argument overload was added in protobuf 3.6.0; older versions
    // require a warning threshold, where -1 disables the warning.
    codedStream.SetTotalBytesLimit(PROTOBUF_MESSAGE_MAX_LIMIT, -1);
#else
    codedStream.SetTotalBytesLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
#endif
    return message->ParseFromCodedStream(&codedStream);
  }
}  // namespace orc

#endif
