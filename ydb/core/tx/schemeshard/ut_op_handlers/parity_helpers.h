#pragma once

#include <ydb/core/tx/schemeshard/generated/op_handlers.h>
#include <ydb/core/tx/schemeshard/schemeshard_audit_log_fragment.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NSchemeShard::NTesting {

// Golden-output assertion helper for per-op audit handlers.
//
// Use at migration time to lock in the audit-path output for an op:
//
//   AssertOpAuditPaths(NSO::ESchemeOpFoo,
//       [](NSO::TModifyScheme& tx) {
//           tx.SetWorkingDir("/Root");
//           tx.MutableFoo()->SetName("X");
//       },
//       {"/Root/X"});
//
// The helper exercises both the direct dispatch (via TryCollectChangingPaths)
// AND the public surface (via MakeAuditLogFragment) and asserts both produce
// the expected paths. Catches "I migrated this op but routing is broken"
// and "the new handler diverges from what the legacy code did" in one shot.
//
// During migration the developer derives the expected vector from the
// legacy switch case being deleted in the same change. After migration the
// expected vector is the durable contract.
template <class TPopulate>
inline void AssertOpAuditPaths(
    NKikimrSchemeOp::EOperationType opType,
    TPopulate populate,
    const TVector<TString>& expected)
{
    NKikimrSchemeOp::TModifyScheme tx;
    tx.SetOperationType(opType);
    populate(tx);

    auto direct = NGenerated::NOpHandlers::TryCollectChangingPaths(tx);
    UNIT_ASSERT_C(direct.has_value(),
                  "TryCollectChangingPaths returned nullopt for migrated op "
                  << NKikimrSchemeOp::EOperationType_Name(opType));
    UNIT_ASSERT_VALUES_EQUAL(*direct, expected);

    const auto fragment = MakeAuditLogFragment(tx);
    UNIT_ASSERT_VALUES_EQUAL(fragment.Paths, expected);
}

} // namespace NKikimr::NSchemeShard::NTesting
