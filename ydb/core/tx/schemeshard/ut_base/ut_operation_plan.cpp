#include <ydb/core/tx/schemeshard/schemeshard_operation_plan.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;

// The plan model is a plain value type over strings and enums: no tablet, no runtime. Keep it
// that way -- the properties worth pinning here are about the model itself, and a test that
// needed a schemeshard to check them would be checking something else.

namespace {

TDatabaseRelativePath Rel(const TString& absolute) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute("/MyRoot", absolute);
    UNIT_ASSERT_C(conclusion.IsSuccess(), "fixture path did not relativize: " << absolute);
    return conclusion.DetachResult();
}

} // namespace

Y_UNIT_TEST_SUITE(TOperationPlanTest) {

Y_UNIT_TEST(EffectCannotBeBuiltWithoutStatingWhatItIs) {
    // Not a runtime assertion -- a compile-time one, recorded here because it is the property
    // the whole model rests on. TPlannedPathEffect has no default constructor and no default
    // member initialisers for Class/Effect/Role/Origin, so a declaration cannot inherit a
    // claim nobody made. That is what makes migrating the remaining operations a set of
    // compile errors rather than an audit nobody finishes.
    static_assert(!std::is_default_constructible_v<TPlannedPathEffect>,
        "TPlannedPathEffect must not be default-constructible: defaulting a CDC-visible field "
        "is how an operation silently asserts something about a path");
}

Y_UNIT_TEST(RecordProjectionExcludesDecomposition) {
    // Consumers reconstruct the parent DDL, and the target database runs its own
    // decomposition -- so replaying a PartDerived path would collide with what the target
    // derives for itself. The plan keeps them; the record projection drops them.
    TLogicalOperationPlan plan;
    plan.Add(Rel("/MyRoot/DirA/Table1"), std::nullopt,
        EPlanEffectClass::SchemaEffect, EPlanEffect::Create, EPlanRole::Target,
        EPlanOrigin::RequestNamed, EPlanObservation::MustWrite);
    plan.Add(Rel("/MyRoot/DirA"), std::nullopt,
        EPlanEffectClass::BookkeepingInternal, EPlanEffect::Create, EPlanRole::Container,
        EPlanOrigin::PartDerived, EPlanObservation::MustWrite);

    const auto forRecord = plan.SchemaEffectsForRecord();
    UNIT_ASSERT_VALUES_EQUAL(forRecord.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(TString(forRecord[0]->Path.Value()), "/DirA/Table1");
}

Y_UNIT_TEST(WriteAllowanceKeepsWhatTheRecordDrops) {
    // The other half of the same point: the two projections are different questions over one
    // set. A generated directory is invisible to a consumer and entirely legitimate as a
    // path-row write, so a model that dropped it would make the write cross-check report it.
    TLogicalOperationPlan plan;
    plan.Add(Rel("/MyRoot/DirA/Table1"), std::nullopt,
        EPlanEffectClass::SchemaEffect, EPlanEffect::Create, EPlanRole::Target,
        EPlanOrigin::RequestNamed, EPlanObservation::MustWrite);
    plan.Add(Rel("/MyRoot/DirA"), std::nullopt,
        EPlanEffectClass::BookkeepingInternal, EPlanEffect::Create, EPlanRole::Container,
        EPlanOrigin::PartDerived, EPlanObservation::MustWrite);

    UNIT_ASSERT_VALUES_EQUAL(plan.PathWriteAllowance().size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(plan.GetEffects().size(), 2u);
}

Y_UNIT_TEST(ReferenceIsPlannedButNeverWritten) {
    // A dependency the DDL names without changing -- an external table's data source. It
    // belongs in the plan, because the operation genuinely depends on it, and it must not be
    // in the write allowance, because nothing writes its row.
    TLogicalOperationPlan plan;
    plan.Add(Rel("/MyRoot/ExternalTable"), std::nullopt,
        EPlanEffectClass::SchemaEffect, EPlanEffect::Create, EPlanRole::Target,
        EPlanOrigin::RequestNamed, EPlanObservation::MustWrite);
    plan.Add(Rel("/MyRoot/DataSource"), std::nullopt,
        EPlanEffectClass::Reference, EPlanEffect::Alter, EPlanRole::Source,
        EPlanOrigin::RequestNamed, EPlanObservation::ReferenceOnly);

    UNIT_ASSERT_VALUES_EQUAL(plan.PathWriteAllowance().size(), 1u);
    // Still visible to a consumer: the dependency is part of what the DDL means.
    UNIT_ASSERT_VALUES_EQUAL(plan.SchemaEffectsForRecord().size(), 1u);
}

Y_UNIT_TEST(MoveHalvesAreLinkedBothWays) {
    // Two effects on two different paths that are one logical rename. Without the pairing a
    // consumer sees an unrelated drop and create, which is exactly the reconstruction the
    // older model could not express -- it had no pairing field and both halves inherited the
    // same effect.
    TLogicalOperationPlan plan;
    const auto from = plan.Add(Rel("/MyRoot/Old"), std::nullopt,
        EPlanEffectClass::SchemaEffect, EPlanEffect::MoveFrom, EPlanRole::Source,
        EPlanOrigin::RequestNamed, EPlanObservation::MustWrite);
    const auto to = plan.Add(Rel("/MyRoot/New"), std::nullopt,
        EPlanEffectClass::SchemaEffect, EPlanEffect::MoveTo, EPlanRole::Target,
        EPlanOrigin::RequestNamed, EPlanObservation::MustWrite);
    plan.Pair(from, to);

    UNIT_ASSERT(plan.GetEffects()[from].Related.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*plan.GetEffects()[from].Related, to);
    UNIT_ASSERT_VALUES_EQUAL(*plan.GetEffects()[to].Related, from);
}

Y_UNIT_TEST(PathsAreDatabaseRelative) {
    // The contract is paths relative to the database root, so a consumer reattaching to
    // another database does not have to strip a foreign prefix. Pinned because the older
    // model stored a global absolute string and nothing noticed.
    TLogicalOperationPlan plan;
    plan.Add(Rel("/MyRoot/DirA/Table1"), std::nullopt,
        EPlanEffectClass::SchemaEffect, EPlanEffect::Create, EPlanRole::Target,
        EPlanOrigin::RequestNamed, EPlanObservation::MustWrite);

    UNIT_ASSERT_VALUES_EQUAL(TString(plan.GetEffects()[0].Path.Value()), "/DirA/Table1");
}

}
