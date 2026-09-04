#pragma clang system_header
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "contrib/libs/apache/arrow_next/src/arrow/util/config.h"

#ifdef ARROW_WITH_OPENTELEMETRY
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/status.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/telemetry/logging.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/util/macros.h"

namespace arrow20::flight {

ARROW_EXPORT Status
RegisterFlightOtelLoggers(const telemetry::OtelLoggingOptions& options);

}  // namespace arrow20::flight
#endif
