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

#include "src/core/ext/transport/chttp2/transport/hpack_parse_result.h"

#include <stddef.h>

#include "y_absl/strings/str_format.h"

#include "src/core/ext/transport/chttp2/transport/hpack_constants.h"
#include "src/core/lib/gprpp/status_helper.h"
#include "src/core/lib/iomgr/error.h"
#include "src/core/lib/slice/slice.h"

namespace grpc_core {

namespace {
class MetadataSizeLimitExceededEncoder {
 public:
  explicit MetadataSizeLimitExceededEncoder(TString& summary)
      : summary_(summary) {}

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
    y_absl::StrAppend(&summary_, " ", key, ":",
                    hpack_constants::SizeForEntry(key.size(), value_length),
                    "B");
  }
  TString& summary_;
};

y_absl::Status MakeStreamError(y_absl::Status error) {
  GPR_DEBUG_ASSERT(!error.ok());
  return grpc_error_set_int(std::move(error), StatusIntProperty::kStreamId, 0);
}
}  // namespace

y_absl::Status HpackParseResult::Materialize() const {
  if (state_ != nullptr && state_->materialized_status.has_value()) {
    return *state_->materialized_status;
  }
  y_absl::Status materialized_status = BuildMaterialized();
  if (!materialized_status.ok()) {
    // We can safely assume state_ is not null here, since BuildMaterialized
    // returns ok if it is.
    state_->materialized_status = materialized_status;
  }
  return materialized_status;
}

y_absl::Status HpackParseResult::BuildMaterialized() const {
  if (state_ == nullptr) return y_absl::OkStatus();
  switch (state_->status.get()) {
    case HpackParseStatus::kOk:
      return y_absl::OkStatus();
    case HpackParseStatus::kEof:
      Crash("Materialize() called on EOF");
      break;
    case HpackParseStatus::kMovedFrom:
      Crash("Materialize() called on moved-from object");
      break;
    case HpackParseStatus::kInvalidMetadata:
      if (state_->key.empty()) {
        return MakeStreamError(y_absl::InternalError(
            ValidateMetadataResultToString(state_->validate_metadata_result)));
      } else {
        return MakeStreamError(y_absl::InternalError(y_absl::StrCat(
            ValidateMetadataResultToString(state_->validate_metadata_result),
            ": ", state_->key)));
      }
    case HpackParseStatus::kSoftMetadataLimitExceeded:
    case HpackParseStatus::kHardMetadataLimitExceeded: {
      const auto& e = state_->metadata_limit_exceeded;
      // Collect a summary of sizes so far for debugging
      // Do not collect contents, for fear of exposing PII.
      TString summary;
      if (e.prior != nullptr) {
        MetadataSizeLimitExceededEncoder encoder(summary);
        e.prior->Encode(&encoder);
      }
      return MakeStreamError(y_absl::ResourceExhaustedError(y_absl::StrCat(
          "received metadata size exceeds ",
          state_->status.get() == HpackParseStatus::kSoftMetadataLimitExceeded
              ? "soft"
              : "hard",
          " limit (", e.frame_length, " vs. ", e.limit, ")",
          summary.empty() ? "" : "; ", summary)));
    }
    case HpackParseStatus::kHardMetadataLimitExceededByKey: {
      const auto& e = state_->metadata_limit_exceeded_by_atom;
      return MakeStreamError(y_absl::ResourceExhaustedError(
          y_absl::StrCat("received metadata size exceeds hard limit (key length ",
                       e.atom_length, " vs. ", e.limit, ")")));
    }
    case HpackParseStatus::kHardMetadataLimitExceededByValue: {
      const auto& e = state_->metadata_limit_exceeded_by_atom;
      return MakeStreamError(y_absl::ResourceExhaustedError(y_absl::StrCat(
          "received metadata size exceeds hard limit (value length ",
          e.atom_length, " vs. ", e.limit, ")")));
    }
    case HpackParseStatus::kMetadataParseError:
      if (!state_->key.empty()) {
        return MakeStreamError(y_absl::InternalError(
            y_absl::StrCat("Error parsing '", state_->key, "' metadata")));
      } else {
        return MakeStreamError(y_absl::InternalError("Error parsing metadata"));
      }
    case HpackParseStatus::kUnbase64Failed:
      if (!state_->key.empty()) {
        return MakeStreamError(y_absl::InternalError(
            y_absl::StrCat("Error parsing '", state_->key,
                         "' metadata: illegal base64 encoding")));
      } else {
        return MakeStreamError(y_absl::InternalError(
            y_absl::StrCat("Failed base64 decoding metadata")));
      }
    case HpackParseStatus::kIncompleteHeaderAtBoundary:
      return y_absl::InternalError(
          "Incomplete header at the end of a header/continuation sequence");
    case HpackParseStatus::kVarintOutOfRange:
      return y_absl::InternalError(y_absl::StrFormat(
          "integer overflow in hpack integer decoding: have 0x%08x, "
          "got byte 0x%02x",
          state_->varint_out_of_range.value,
          state_->varint_out_of_range.last_byte));
    case HpackParseStatus::kIllegalTableSizeChange:
      return y_absl::InternalError(y_absl::StrCat(
          "Attempt to make hpack table ",
          state_->illegal_table_size_change.new_size, " bytes when max is ",
          state_->illegal_table_size_change.max_size, " bytes"));
    case HpackParseStatus::kAddBeforeTableSizeUpdated:
      return y_absl::InternalError(
          y_absl::StrCat("HPACK max table size reduced to ",
                       state_->illegal_table_size_change.new_size,
                       " but not reflected by hpack stream (still at ",
                       state_->illegal_table_size_change.max_size, ")"));
    case HpackParseStatus::kParseHuffFailed:
      if (!state_->key.empty()) {
        return y_absl::InternalError(y_absl::StrCat("Failed huffman decoding '",
                                                state_->key, "' metadata"));
      } else {
        return y_absl::InternalError(
            y_absl::StrCat("Failed huffman decoding metadata"));
      }
      break;
    case HpackParseStatus::kTooManyDynamicTableSizeChanges:
      return y_absl::InternalError(
          "More than two max table size changes in a single frame");
    case HpackParseStatus::kMaliciousVarintEncoding:
      return y_absl::InternalError(
          "Malicious varint encoding detected in HPACK stream");
    case HpackParseStatus::kInvalidHpackIndex:
      return y_absl::InternalError(y_absl::StrFormat(
          "Invalid HPACK index received (%d)", state_->invalid_hpack_index));
    case HpackParseStatus::kIllegalHpackOpCode:
      return y_absl::InternalError("Illegal hpack op code");
  }
  GPR_UNREACHABLE_CODE(return y_absl::UnknownError("Should never reach here"));
}

}  // namespace grpc_core
