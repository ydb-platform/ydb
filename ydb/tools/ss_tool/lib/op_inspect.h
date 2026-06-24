#pragma once

#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NKikimr::NSchemeShard::NSsTool {

// One row in ss_tool's tables: identity (name + enum number) plus whether
// the op has migrated to the per-op handler pattern (sourced from
// op_handlers.h's IsRegistered_v predicate, which the codegen emits from
// op_handler_overrides.yaml).
struct TOpRow {
    TString Name;
    NKikimrSchemeOp::EOperationType Type{};
    bool IsRegistered = false;
};

TOpRow CollectRow(NKikimrSchemeOp::EOperationType opType);
TVector<TOpRow> AllOps();

} // namespace NKikimr::NSchemeShard::NSsTool
