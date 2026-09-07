#pragma once

#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <util/system/yassert.h>

namespace NKikimr::NSchemeShard {

enum class ESchemeOperationSupport {
    Implemented,
    Internal,
    Unsupported,
    Deprecated,
    Stub,
    Retired,
    Unknown,
};

struct TSchemeOperationInfo {
    NKikimrSchemeOp::EOperationType Type;
    ESchemeOperationSupport Support;
};

inline constexpr TSchemeOperationInfo SchemeOperations[] = {
#define SCHEME_OP_IMPLEMENTED(name, ...) {NKikimrSchemeOp::name, ESchemeOperationSupport::Implemented},
#define SCHEME_OP_INTERNAL(name, ...) {NKikimrSchemeOp::name, ESchemeOperationSupport::Internal},
#define SCHEME_OP_UNSUPPORTED(name, ...) {NKikimrSchemeOp::name, ESchemeOperationSupport::Unsupported},
#define SCHEME_OP_DEPRECATED(name, ...) {NKikimrSchemeOp::name, ESchemeOperationSupport::Deprecated},
#define SCHEME_OP_STUB(name, ...) {NKikimrSchemeOp::name, ESchemeOperationSupport::Stub},
#define SCHEME_OP_RETIRED(name, ...) {NKikimrSchemeOp::name, ESchemeOperationSupport::Retired},
#define SCHEME_OP_UNSUPPORTED_TX(...)
#define SCHEME_OP_TRANSIENT_TX(...)
#include "schemeshard_operation_registry.inc"
#undef SCHEME_OP_IMPLEMENTED
#undef SCHEME_OP_INTERNAL
#undef SCHEME_OP_UNSUPPORTED
#undef SCHEME_OP_DEPRECATED
#undef SCHEME_OP_STUB
#undef SCHEME_OP_RETIRED
#undef SCHEME_OP_UNSUPPORTED_TX
#undef SCHEME_OP_TRANSIENT_TX
};

constexpr ESchemeOperationSupport GetSchemeOperationSupport(NKikimrSchemeOp::EOperationType type) {
    switch (type) {
#define SCHEME_OP_IMPLEMENTED(name, ...) case NKikimrSchemeOp::name: return ESchemeOperationSupport::Implemented;
#define SCHEME_OP_INTERNAL(name, ...) case NKikimrSchemeOp::name: return ESchemeOperationSupport::Internal;
#define SCHEME_OP_UNSUPPORTED(name, ...) case NKikimrSchemeOp::name: return ESchemeOperationSupport::Unsupported;
#define SCHEME_OP_DEPRECATED(name, ...) case NKikimrSchemeOp::name: return ESchemeOperationSupport::Deprecated;
#define SCHEME_OP_STUB(name, ...) case NKikimrSchemeOp::name: return ESchemeOperationSupport::Stub;
#define SCHEME_OP_RETIRED(name, ...) case NKikimrSchemeOp::name: return ESchemeOperationSupport::Retired;
#define SCHEME_OP_UNSUPPORTED_TX(...)
#define SCHEME_OP_TRANSIENT_TX(...)
#include "schemeshard_operation_registry.inc"
#undef SCHEME_OP_IMPLEMENTED
#undef SCHEME_OP_INTERNAL
#undef SCHEME_OP_UNSUPPORTED
#undef SCHEME_OP_DEPRECATED
#undef SCHEME_OP_STUB
#undef SCHEME_OP_RETIRED
#undef SCHEME_OP_UNSUPPORTED_TX
#undef SCHEME_OP_TRANSIENT_TX
    }
    return ESchemeOperationSupport::Unknown;
}

#include <ydb/core/tx/schemeshard/generated/operation_registry_checks.inc>

template <NKikimrSchemeOp::EOperationType Type>
[[noreturn]] void AbortUnimplementedSchemeOperation() {
    static_assert(GetSchemeOperationSupport(Type) == ESchemeOperationSupport::Unsupported
            || GetSchemeOperationSupport(Type) == ESchemeOperationSupport::Stub,
        "Replace the unsupported dispatch when implementing an operation");
    Y_ABORT("Scheme operation %d is not implemented", static_cast<int>(Type));
}

} // namespace NKikimr::NSchemeShard
