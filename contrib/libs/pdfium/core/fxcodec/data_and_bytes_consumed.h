// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCODEC_DATA_AND_BYTES_CONSUMED_H_
#define CORE_FXCODEC_DATA_AND_BYTES_CONSUMED_H_

#include <stdint.h>

#include "core/fxcrt/data_vector.h"

namespace fxcodec {

struct DataAndBytesConsumed {
  DataAndBytesConsumed(DataVector<uint8_t> data, uint32_t bytes_consumed);
  DataAndBytesConsumed(const DataAndBytesConsumed&) = delete;
  DataAndBytesConsumed& operator=(const DataAndBytesConsumed&) = delete;
  DataAndBytesConsumed(DataAndBytesConsumed&&) noexcept;
  DataAndBytesConsumed& operator=(DataAndBytesConsumed&&) noexcept;
  ~DataAndBytesConsumed();

  DataVector<uint8_t> data;
  // TODO(thestig): Consider replacing with std::optional<size_t>.
  uint32_t bytes_consumed;
};

}  // namespace fxcodec

using DataAndBytesConsumed = fxcodec::DataAndBytesConsumed;

#endif  // CORE_FXCODEC_DATA_AND_BYTES_CONSUMED_H_
