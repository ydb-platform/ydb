#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_operation_plan.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;

// The plan model is a plain value type over strings and enums: no tablet, no runtime. The
// properties pinned here are about the model itself.

namespace {

TDatabaseRelativePath Rel(const TString& absolute) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute("/MyRoot", absolute);
    UNIT_ASSERT_C(conclusion.IsSuccess(), "fixture path did not relativize: " << absolute);
    return conclusion.DetachResult();
}

} // namespace

Y_UNIT_TEST_SUITE(TOperationPlanTest) {

Y_UNIT_TEST(ReferenceCarriesNoEffect) {
    // A reference is a distinct alternative, not a schema effect with a permissive field: there
    // is no place in it to claim a mutation. Recorded as a compile-time property.
    static_assert(std::is_same_v<decltype(TLogicalPathEffect::Kind), std::variant<TSchemaEffect, TReference>>);
    static_assert(!std::is_default_constructible_v<TSchemaEffect> || sizeof(TSchemaEffect) > 0);

    TOperationPlanBuilder builder("/MyRoot");
    const auto ref = builder.AddReference(Rel("/MyRoot/Src"), "Src", std::nullopt,
        EPlanRole::Source, EPlanOrigin::RequestNamed);
    auto plan = builder.Seal();
    UNIT_ASSERT(!plan->Effect(ref).IsSchemaEffect());
    UNIT_ASSERT(plan->Effect(ref).AsSchemaEffect() == nullptr);
}

Y_UNIT_TEST(RecordProjectionExcludesReferencesAndDecomposition) {
    // A consumer reconstructs the parent DDL and the target database runs its own
    // decomposition; it must see neither a dependency nor a part-derived artefact.
    TOperationPlanBuilder builder("/MyRoot");
    builder.AddSchemaEffect(Rel("/MyRoot/Dst"), "Dst", std::nullopt,
        EPlanEffect::Create, EPlanRole::Target, EPlanOrigin::RequestNamed);
    builder.AddReference(Rel("/MyRoot/Src"), "Src", std::nullopt,
        EPlanRole::Source, EPlanOrigin::RequestNamed);
    builder.AddSchemaEffect(Rel("/MyRoot/Dst/Idx/indexImplTable"), "indexImplTable", std::nullopt,
        EPlanEffect::Create, EPlanRole::Target, EPlanOrigin::PartDerived);
    builder.AddSchemaEffect(Rel("/MyRoot/Dst/Idx"), "Idx", std::nullopt,
        EPlanEffect::Create, EPlanRole::Target, EPlanOrigin::RequestImplied);
    auto plan = builder.Seal();

    const auto forRecord = plan->SchemaEffectsForRecord();
    UNIT_ASSERT_VALUES_EQUAL(forRecord.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(TString(forRecord[0]->Path.Value()), "/Dst");
    UNIT_ASSERT_VALUES_EQUAL(TString(forRecord[1]->Path.Value()), "/Dst/Idx");
    UNIT_ASSERT_VALUES_EQUAL(plan->GetLogicalEffects().size(), 4u);
}

Y_UNIT_TEST(PhysicalWritesAreSeparateFromLogicalEffects) {
    // A generated directory is a physical write and nothing else: absent from the logical
    // effects, present in the allowance a write cross-check is measured against.
    TOperationPlanBuilder builder("/MyRoot");
    const auto target = builder.AddSchemaEffect(Rel("/MyRoot/DirA/Table1"), "Table1", std::nullopt,
        EPlanEffect::Create, EPlanRole::Target, EPlanOrigin::RequestNamed);
    builder.AddPhysicalWrite(Rel("/MyRoot/DirA/Table1"), "Table1", std::nullopt,
        EPlanObservation::MustWrite, EPhysicalWriteReason::LogicalEffect, target);
    builder.AddPhysicalWrite(Rel("/MyRoot/DirA"), "DirA", std::nullopt,
        EPlanObservation::MustWrite, EPhysicalWriteReason::GeneratedDirectory, std::nullopt);
    auto plan = builder.Seal();

    UNIT_ASSERT_VALUES_EQUAL(plan->GetLogicalEffects().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(plan->PathWriteAllowance().size(), 2u);
    UNIT_ASSERT(plan->PathWriteAllowance()[1].Reason == EPhysicalWriteReason::GeneratedDirectory);
    UNIT_ASSERT(!plan->PathWriteAllowance()[1].LogicalEffect.has_value());
}

Y_UNIT_TEST(MoveHalvesAreLinkedBothWays) {
    TOperationPlanBuilder builder("/MyRoot");
    const auto from = builder.AddSchemaEffect(Rel("/MyRoot/Old"), "Old", std::nullopt,
        EPlanEffect::MoveFrom, EPlanRole::Source, EPlanOrigin::RequestNamed);
    const auto to = builder.AddSchemaEffect(Rel("/MyRoot/New"), "New", std::nullopt,
        EPlanEffect::MoveTo, EPlanRole::Target, EPlanOrigin::RequestNamed);
    builder.Pair(from, to);
    auto plan = builder.Seal();

    UNIT_ASSERT_VALUES_EQUAL(*plan->Effect(from).AsSchemaEffect()->Related, to);
    UNIT_ASSERT_VALUES_EQUAL(*plan->Effect(to).AsSchemaEffect()->Related, from);
}

Y_UNIT_TEST(PathsAreDatabaseRelativeAndBridgedOnce) {
    TOperationPlanBuilder builder("/MyRoot");
    const auto id = builder.AddSchemaEffect(Rel("/MyRoot/DirA/Table1"), "Table1", std::nullopt,
        EPlanEffect::Create, EPlanRole::Target, EPlanOrigin::RequestNamed);
    const auto root = builder.AddSchemaEffect(Rel("/MyRoot"), "MyRoot", std::nullopt,
        EPlanEffect::ChildrenChanged, EPlanRole::Container, EPlanOrigin::RequestNamed);
    auto plan = builder.Seal();

    UNIT_ASSERT_VALUES_EQUAL(TString(plan->Effect(id).Path.Value()), "/DirA/Table1");
    UNIT_ASSERT_VALUES_EQUAL(plan->Absolute(plan->Effect(id).Path), "/MyRoot/DirA/Table1");
    UNIT_ASSERT_VALUES_EQUAL(TString(plan->Effect(root).Path.Value()), "/");
    UNIT_ASSERT_VALUES_EQUAL(plan->Absolute(plan->Effect(root).Path), "/MyRoot");
}

Y_UNIT_TEST(BlueprintsBindPartsById) {
    TOperationPlanBuilder builder("/MyRoot");
    const ui32 request = builder.AddRequest();
    const auto dir = builder.AddPhysicalWrite(Rel("/MyRoot/DirA"), "DirA", std::nullopt,
        EPlanObservation::MustWrite, EPhysicalWriteReason::GeneratedDirectory, std::nullopt);
    const auto dirContainer = builder.AddPhysicalWrite(Rel("/MyRoot"), "MyRoot", TPathId(1, 1),
        EPlanObservation::MustWrite, EPhysicalWriteReason::GeneratedDirectoryContainer, std::nullopt);
    const auto target = builder.AddSchemaEffect(Rel("/MyRoot/DirA/Dst"), "Dst", std::nullopt,
        EPlanEffect::Create, EPlanRole::Target, EPlanOrigin::RequestNamed);
    const auto container = builder.AddSchemaEffect(Rel("/MyRoot/DirA"), "DirA", std::nullopt,
        EPlanEffect::ChildrenChanged, EPlanRole::Container, EPlanOrigin::RequestNamed);
    const auto source = builder.AddReference(Rel("/MyRoot/Src"), "Src", TPathId(1, 7),
        EPlanRole::Source, EPlanOrigin::RequestNamed);
    const auto stream = builder.AddSchemaEffect(Rel("/MyRoot/Src/Stream"), "Stream", TPathId(1, 8),
        EPlanEffect::Drop, EPlanRole::Source, EPlanOrigin::RequestNamed);

    NKikimrSchemeOp::TModifyScheme mkdir;
    mkdir.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
    mkdir.SetWorkingDir("/MyRoot");
    mkdir.MutableMkDir()->SetName("DirA");
    const ui32 mkdirPart = builder.AddGeneratedDirPart(request, mkdir, TMkDirPartBindings{dir, dirContainer});

    NKikimrSchemeOp::TModifyScheme copy;
    copy.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
    copy.SetWorkingDir("/MyRoot/DirA");
    copy.MutableCreateTable()->SetName("Dst");
    copy.MutableCreateTable()->SetCopyFromTable("/MyRoot/Src");
    const ui32 copyPart = builder.AddPart(request, EPlannedPartKind::CopyTable, copy, TCopyTablePartBindings{target, container, source, {stream}});

    auto plan = builder.Seal();
    UNIT_ASSERT_VALUES_EQUAL(mkdirPart, 0u);
    UNIT_ASSERT_VALUES_EQUAL(copyPart, 1u);
    UNIT_ASSERT_VALUES_EQUAL(plan->GetRequests().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(plan->GetRequests()[0].GeneratedDirParts, TVector<ui32>{0});
    UNIT_ASSERT_VALUES_EQUAL(plan->GetRequests()[0].Parts, TVector<ui32>{1});

    UNIT_ASSERT_VALUES_EQUAL(plan->GetLogicalEffects().size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(plan->GetPhysicalWrites().size(), 2u);
    UNIT_ASSERT(plan->Effect(source).PathId == TPathId(1, 7));
    UNIT_ASSERT(!plan->Effect(target).PathId.has_value());
    UNIT_ASSERT(!plan->Effect(source).IsSchemaEffect());
    UNIT_ASSERT(plan->Effect(stream).AsSchemaEffect()->Effect == EPlanEffect::Drop);

    const auto* copyBlueprint = plan->FindPart(copyPart);
    UNIT_ASSERT(copyBlueprint);
    const auto& bindings = std::get<TCopyTablePartBindings>(copyBlueprint->Bindings);
    UNIT_ASSERT_VALUES_EQUAL(bindings.Target, target);
    UNIT_ASSERT_VALUES_EQUAL(bindings.Container, container);
    UNIT_ASSERT_VALUES_EQUAL(bindings.Source, source);
    UNIT_ASSERT_VALUES_EQUAL(bindings.DropStreams, TVector<TPlanEffectId>{stream});
    UNIT_ASSERT_VALUES_EQUAL(copyBlueprint->Tx.GetCreateTable().GetCopyFromTable(), "/MyRoot/Src");

    const auto* mkdirBlueprint = plan->FindPart(mkdirPart);
    UNIT_ASSERT(mkdirBlueprint);
    const auto& mkdirBindings = std::get<TMkDirPartBindings>(mkdirBlueprint->Bindings);
    UNIT_ASSERT_VALUES_EQUAL(mkdirBindings.Target, dir);
    UNIT_ASSERT_VALUES_EQUAL(mkdirBindings.Container, dirContainer);
    UNIT_ASSERT(plan->Write(dirContainer).PathId == TPathId(1, 1));
}

}
