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

// IWYU pragma: begin_exports

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "contrib/libs/apache/arrow_next/cpp/src/arrow/array/data.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/buffer.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/chunked_array.h"

#include "contrib/libs/apache/arrow_next/cpp/src/arrow/compute/function.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/compute/kernel.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/compute/kernels/codegen_internal.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/compute/registry.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/datum.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/memory_pool.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/status.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/type.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/type_traits.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/util/checked_cast.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/util/logging.h"
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/util/macros.h"

// IWYU pragma: end_exports

namespace arrow20 {

using internal::checked_cast;
using internal::checked_pointer_cast;

}  // namespace arrow20
