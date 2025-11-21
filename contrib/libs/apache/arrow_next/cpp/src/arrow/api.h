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

// Coarse public API while the library is in development

#pragma once

#include "contrib/libs/apache/arrow_next/cpp/src/arrow/array.h"                    // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/array/array_run_end.h"      // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/array/concatenate.h"        // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/buffer.h"                   // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/builder.h"                  // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/chunked_array.h"            // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/compare.h"                  // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/config.h"                   // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/datum.h"                    // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/extension_type.h"           // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/memory_pool.h"              // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/pretty_print.h"             // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/record_batch.h"             // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/result.h"                   // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/status.h"                   // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/table.h"                    // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/table_builder.h"            // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/tensor.h"                   // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/type.h"                     // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/util/key_value_metadata.h"  // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/visit_array_inline.h"       // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/visit_scalar_inline.h"      // IWYU pragma: export
#include "contrib/libs/apache/arrow_next/cpp/src/arrow/visitor.h"                  // IWYU pragma: export

/// \brief Top-level namespace for Apache Arrow C++ API
namespace arrow20 {}
