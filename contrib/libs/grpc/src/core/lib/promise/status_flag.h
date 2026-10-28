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

#ifndef GRPC_SRC_CORE_LIB_PROMISE_STATUS_FLAG_H
#define GRPC_SRC_CORE_LIB_PROMISE_STATUS_FLAG_H

#if defined(__GNUC__)
#pragma GCC system_header
#endif

#include <grpc/support/port_platform.h>

#include "y_absl/status/status.h"
#include "y_absl/status/statusor.h"
#include "y_absl/types/optional.h"

#include <grpc/support/log.h>

#include "src/core/lib/promise/detail/status.h"

namespace grpc_core {

struct Failure {};
struct Success {};

inline bool IsStatusOk(Failure) { return false; }
inline bool IsStatusOk(Success) { return true; }

template <>
struct StatusCastImpl<y_absl::Status, Success> {
  static y_absl::Status Cast(Success) { return y_absl::OkStatus(); }
};

template <>
struct StatusCastImpl<y_absl::Status, const Success&> {
  static y_absl::Status Cast(Success) { return y_absl::OkStatus(); }
};

template <>
struct StatusCastImpl<y_absl::Status, Failure> {
  static y_absl::Status Cast(Failure) { return y_absl::CancelledError(); }
};

template <typename T>
struct StatusCastImpl<y_absl::StatusOr<T>, Failure> {
  static y_absl::StatusOr<T> Cast(Failure) { return y_absl::CancelledError(); }
};

// A boolean representing whether an operation succeeded (true) or failed
// (false).
class StatusFlag {
 public:
  StatusFlag() : value_(true) {}
  explicit StatusFlag(bool value) : value_(value) {}
  // NOLINTNEXTLINE(google-explicit-constructor)
  StatusFlag(Failure) : value_(false) {}
  // NOLINTNEXTLINE(google-explicit-constructor)
  StatusFlag(Success) : value_(true) {}

  bool ok() const { return value_; }

  bool operator==(StatusFlag other) const { return value_ == other.value_; }

 private:
  bool value_;
};

inline bool IsStatusOk(const StatusFlag& flag) { return flag.ok(); }

template <>
struct StatusCastImpl<y_absl::Status, StatusFlag> {
  static y_absl::Status Cast(StatusFlag flag) {
    return flag.ok() ? y_absl::OkStatus() : y_absl::CancelledError();
  }
};

template <>
struct StatusCastImpl<y_absl::Status, StatusFlag&> {
  static y_absl::Status Cast(StatusFlag flag) {
    return flag.ok() ? y_absl::OkStatus() : y_absl::CancelledError();
  }
};

template <>
struct StatusCastImpl<y_absl::Status, const StatusFlag&> {
  static y_absl::Status Cast(StatusFlag flag) {
    return flag.ok() ? y_absl::OkStatus() : y_absl::CancelledError();
  }
};

template <typename T>
struct FailureStatusCastImpl<y_absl::StatusOr<T>, StatusFlag> {
  static y_absl::StatusOr<T> Cast(StatusFlag flag) {
    GPR_DEBUG_ASSERT(!flag.ok());
    return y_absl::CancelledError();
  }
};

template <typename T>
struct FailureStatusCastImpl<y_absl::StatusOr<T>, StatusFlag&> {
  static y_absl::StatusOr<T> Cast(StatusFlag flag) {
    GPR_DEBUG_ASSERT(!flag.ok());
    return y_absl::CancelledError();
  }
};

template <typename T>
struct FailureStatusCastImpl<y_absl::StatusOr<T>, const StatusFlag&> {
  static y_absl::StatusOr<T> Cast(StatusFlag flag) {
    GPR_DEBUG_ASSERT(!flag.ok());
    return y_absl::CancelledError();
  }
};

// A value if an operation was successful, or a failure flag if not.
template <typename T>
class ValueOrFailure {
 public:
  // NOLINTNEXTLINE(google-explicit-constructor)
  ValueOrFailure(T value) : value_(std::move(value)) {}
  // NOLINTNEXTLINE(google-explicit-constructor)
  ValueOrFailure(Failure) {}
  // NOLINTNEXTLINE(google-explicit-constructor)
  ValueOrFailure(StatusFlag status) { GPR_ASSERT(!status.ok()); }

  static ValueOrFailure FromOptional(y_absl::optional<T> value) {
    return ValueOrFailure{std::move(value)};
  }

  bool ok() const { return value_.has_value(); }
  StatusFlag status() const { return StatusFlag(ok()); }

  const T& value() const { return value_.value(); }
  T& value() { return value_.value(); }
  const T& operator*() const { return *value_; }
  T& operator*() { return *value_; }

  bool operator==(const ValueOrFailure& other) const {
    return value_ == other.value_;
  }

 private:
  y_absl::optional<T> value_;
};

template <typename T>
inline bool IsStatusOk(const ValueOrFailure<T>& value) {
  return value.ok();
}

template <typename T>
inline T TakeValue(ValueOrFailure<T>&& value) {
  return std::move(value.value());
}

template <typename T>
struct StatusCastImpl<y_absl::StatusOr<T>, ValueOrFailure<T>> {
  static y_absl::StatusOr<T> Cast(ValueOrFailure<T> value) {
    return value.ok() ? y_absl::StatusOr<T>(std::move(value.value()))
                      : y_absl::CancelledError();
  }
};

template <typename T>
struct StatusCastImpl<ValueOrFailure<T>, Failure> {
  static ValueOrFailure<T> Cast(Failure) {
    return ValueOrFailure<T>(Failure{});
  }
};

template <typename T>
struct StatusCastImpl<ValueOrFailure<T>, StatusFlag&> {
  static ValueOrFailure<T> Cast(StatusFlag f) {
    GPR_ASSERT(!f.ok());
    return ValueOrFailure<T>(Failure{});
  }
};

template <typename T>
struct StatusCastImpl<ValueOrFailure<T>, StatusFlag> {
  static ValueOrFailure<T> Cast(StatusFlag f) {
    GPR_ASSERT(!f.ok());
    return ValueOrFailure<T>(Failure{});
  }
};

}  // namespace grpc_core

#endif  // GRPC_SRC_CORE_LIB_PROMISE_STATUS_FLAG_H