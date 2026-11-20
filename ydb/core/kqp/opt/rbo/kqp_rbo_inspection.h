#pragma once

#include <ydb/core/kqp/tracing/kqp_compile_trace.h>

namespace NKikimr::NKqp {

// Offline inspection starts at the RBO transformer with already bound logical operators.
struct TImportedRboInspection final : TCompileTrace {
    using TCompileTrace::TCompileTrace;
    TVector<TIntrusivePtr<TOpRoot>> Roots;
};

} // namespace NKikimr::NKqp
