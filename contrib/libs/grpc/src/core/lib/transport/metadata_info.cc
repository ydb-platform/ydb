// Copyright 2023 gRPC authors.
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

#include <grpc/support/port_platform.h>

#include "src/core/lib/transport/metadata_info.h"

#include "src/core/lib/slice/slice.h"

namespace grpc_core {

class MetadataSizesAnnotation::MetadataSizeEncoder {
 public:
  explicit MetadataSizeEncoder(TString& summary) : summary_(summary) {}

  void Encode(const Slice& key, const Slice& value) {
    AddToSummary(key.as_string_view(), value.size());
  }

  template <typename Key, typename Value>
  void Encode(Key, const Value& value) {
    AddToSummary(Key::key(), EncodedSizeOfKey(Key(), value));
  }

 private:
  void AddToSummary(y_absl::string_view key,
                    size_t value_length) GPR_ATTRIBUTE_NOINLINE {
    y_absl::StrAppend(&summary_, key, ":",
                    hpack_constants::SizeForEntry(key.size(), value_length),
                    ",");
  }
  TString& summary_;
};

TString MetadataSizesAnnotation::ToString() const {
  TString metadata_annotation =
      y_absl::StrCat("gRPC metadata soft_limit:", soft_limit_,
                   ",hard_limit:", hard_limit_, ",");
  MetadataSizeEncoder encoder(metadata_annotation);
  metadata_buffer_->Encode(&encoder);
  return metadata_annotation;
}

}  // namespace grpc_core