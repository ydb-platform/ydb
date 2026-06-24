#pragma once

#include <ydb/core/tx/schemeshard/generated/op_handlers.h>

// Macros for defining per-op handlers gated by op_handler_overrides.yaml.
//
// Usage:
//
//   YDB_DEFINE_OP_FACTORY(ESchemeOpCreateTable, op, tx, ctx) {
//       return {CreateNewTable(op.NextPartId(), tx)};
//   }
//
//   YDB_DEFINE_OP_AUDIT(ESchemeOpCreateTable, tx, paths) {
//       paths.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(),
//                                              tx.GetCreateTable().GetName()}));
//   }
//
// The static_assert fires if `Op` is not listed in op_handler_overrides.yaml,
// pointing the developer to the file they need to edit. The function name
// and signature are baked in by the macro, so a typo or signature drift is
// also caught at compile time.

#define YDB_DEFINE_OP_FACTORY(Op, op, tx, ctx)                                  \
    static_assert(                                                              \
        ::NKikimr::NSchemeShard::NGenerated::NOpHandlers::IsRegistered_v<       \
            ::NKikimrSchemeOp::Op>,                                             \
        #Op " is not listed in op_handler_overrides.yaml; "                     \
        "add it there before defining the handler.");                           \
    ::TVector< ::NKikimr::NSchemeShard::ISubOperation::TPtr>                    \
    NKikimr::NSchemeShard::NGenerated::NOpHandlers::MakeOperationParts_##Op(    \
        const ::NKikimr::NSchemeShard::TOperation& op,                          \
        const ::NKikimrSchemeOp::TModifyScheme& tx,                             \
        ::NKikimr::NSchemeShard::TOperationContext& ctx)

#define YDB_DEFINE_OP_AUDIT(Op, tx, out)                                        \
    static_assert(                                                              \
        ::NKikimr::NSchemeShard::NGenerated::NOpHandlers::IsRegistered_v<       \
            ::NKikimrSchemeOp::Op>,                                             \
        #Op " is not listed in op_handler_overrides.yaml; "                     \
        "add it there before defining the handler.");                           \
    void                                                                        \
    NKikimr::NSchemeShard::NGenerated::NOpHandlers::CollectChangingPaths_##Op(  \
        const ::NKikimrSchemeOp::TModifyScheme& tx,                             \
        ::TVector< ::TString>& out)
