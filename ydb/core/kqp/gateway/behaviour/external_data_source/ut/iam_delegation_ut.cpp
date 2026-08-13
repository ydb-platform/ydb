#include <ydb/core/kqp/gateway/behaviour/external_data_source/iam_delegation.h>
#include <ydb/core/kqp/gateway/behaviour/external_data_source/iam_delegation_ddl.h>
#include <ydb/core/kqp/gateway/behaviour/external_data_source/iam_object_lookup.h>
#include <ydb/core/protos/feature_flags.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp::NExternalDataSource {
namespace {

TIamDelegationSettings Settings() {
    return {
        .Endpoint = "service-control.test:4282",
        .ServiceId = "ydb",
        .MicroserviceId = "data-plane",
        .ResourceType = "resource-manager.cloud",
    };
}

TIamDelegation Delegation() {
    return {
        .ResourceId = "cloud-id",
        .ServiceAccountId = "target-sa-id",
        .ReferrerId = "72075186224037889:42",
    };
}

Y_UNIT_TEST_SUITE(IamDelegation) {
    Y_UNIT_TEST(EnsureEnabledRequest) {
        const auto request = MakeEnsureEnabledRequest(Settings(), Delegation());
        UNIT_ASSERT_VALUES_EQUAL(request.service_ids_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(request.service_ids(0), "ydb");
        UNIT_ASSERT_VALUES_EQUAL(request.resource().id(), "cloud-id");
        UNIT_ASSERT_VALUES_EQUAL(request.resource().type(), "resource-manager.cloud");
    }

    Y_UNIT_TEST(EnsureEnabledRequestDoesNotLeakDelegationFields) {
        const auto request = MakeEnsureEnabledRequest(Settings(), Delegation());
        UNIT_ASSERT_VALUES_EQUAL(request.service_ids_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(request.ShortDebugString().find("target-sa-id"), TString::npos);
        UNIT_ASSERT_VALUES_EQUAL(request.ShortDebugString().find("72075186224037889:42"), TString::npos);
    }

    Y_UNIT_TEST(SetupRequestContainsTrustedSubjectAndReference) {
        const auto request = MakeSetupDelegationRequest(Settings(), Delegation(), "user-id");
        UNIT_ASSERT_VALUES_EQUAL(request.service_id(), "ydb");
        UNIT_ASSERT_VALUES_EQUAL(request.microservice_id(), "data-plane");
        UNIT_ASSERT_VALUES_EQUAL(request.target_service_account_id(), "target-sa-id");
        UNIT_ASSERT_VALUES_EQUAL(request.resource().id(), "cloud-id");
        UNIT_ASSERT_VALUES_EQUAL(request.resource().type(), "resource-manager.cloud");
        UNIT_ASSERT_VALUES_EQUAL(request.on_behalf_of_subject_id(), "user-id");
        UNIT_ASSERT_VALUES_EQUAL(request.referrer().id(), "72075186224037889:42");
        UNIT_ASSERT_VALUES_EQUAL(request.referrer().type(), "ydb.externalDataSource");
        UNIT_ASSERT(request.with_references());
    }

    Y_UNIT_TEST(RequestBuildersHonorConfiguredReferrerType) {
        auto delegation = Delegation();
        delegation.ReferrerType = "custom.externalDataSource";

        const auto setup = MakeSetupDelegationRequest(Settings(), delegation, "user-id");
        const auto revoke = MakeRevokeDelegationRequest(Settings(), delegation);
        UNIT_ASSERT_VALUES_EQUAL(setup.referrer().type(), "custom.externalDataSource");
        UNIT_ASSERT_VALUES_EQUAL(revoke.referrer().type(), "custom.externalDataSource");
        UNIT_ASSERT_VALUES_EQUAL(revoke.service_id(), "ydb");
        UNIT_ASSERT_VALUES_EQUAL(revoke.microservice_id(), "data-plane");
    }

    Y_UNIT_TEST(RevokeRequestUsesSameReferenceAndHasNoUserIdentity) {
        const auto request = MakeRevokeDelegationRequest(Settings(), Delegation());
        UNIT_ASSERT_VALUES_EQUAL(request.target_service_account_id(), "target-sa-id");
        UNIT_ASSERT_VALUES_EQUAL(request.resource().id(), "cloud-id");
        UNIT_ASSERT_VALUES_EQUAL(request.resource().type(), "resource-manager.cloud");
        UNIT_ASSERT_VALUES_EQUAL(request.referrer().id(), "72075186224037889:42");
        UNIT_ASSERT(request.with_references());
    }

    Y_UNIT_TEST(NormalizeAccessServiceSubject) {
        UNIT_ASSERT_VALUES_EQUAL(NormalizeIamSubject("user-id@as"), "user-id");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeIamSubject("user-id"), "user-id");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeIamSubject("user@aside"), "user@aside");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeIamSubject("@as"), "");
        UNIT_ASSERT_VALUES_EQUAL(NormalizeIamSubject("user-id@as@as"), "user-id@as");
    }

    Y_UNIT_TEST(HumanReadableReferrerFitsIamLimit) {
        const TString id = MakeIamDelegationReferrerId(
            "Orders From Production", "12345678-1234-1234-1234-123456789012");
        UNIT_ASSERT_VALUES_EQUAL(id, "eds:orders-f:12345678-1234-1234-1234-123456789012");
        UNIT_ASSERT_LE(id.size(), 50);

        const TString punctuation = MakeIamDelegationReferrerId("!!!", "unique");
        UNIT_ASSERT_VALUES_EQUAL(punctuation, "eds:-:unique");
        const TString empty = MakeIamDelegationReferrerId("", "unique");
        UNIT_ASSERT_VALUES_EQUAL(empty, "eds:source:unique");
    }

    Y_UNIT_TEST(PrepareDelegationPersistsManagedHumanReadableReference) {
        NKikimrSchemeOp::TExternalDataSourceDescription description;
        description.MutableAuth()->MutableIam()->SetServiceAccountId("target-sa-id");

        const auto status = PrepareIamDelegation(description, "Orders From Production");
        UNIT_ASSERT(!status.IsFail());
        const auto& iam = description.GetAuth().GetIam();
        UNIT_ASSERT(iam.GetDelegationReferrerId().StartsWith("eds:orders-f:"));
        UNIT_ASSERT_LE(iam.GetDelegationReferrerId().size(), 50);

        TIamDelegation delegation{
            .ResourceId = "cloud-id",
            .ServiceAccountId = iam.GetServiceAccountId(),
            .ReferrerId = iam.GetDelegationReferrerId(),
        };
        UNIT_ASSERT(IsManagedIamDelegation(delegation));
    }

    Y_UNIT_TEST(PrepareDelegationRejectsMissingServiceAccount) {
        NKikimrSchemeOp::TExternalDataSourceDescription description;
        UNIT_ASSERT(PrepareIamDelegation(description, "source").IsFail());
        UNIT_ASSERT(!description.GetAuth().GetIam().HasDelegationReferrerId());
    }

    Y_UNIT_TEST(ReferrerSanitizationIsStableAndBounded) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeIamDelegationReferrerId("A  B/C_D-long-name", "id"),
            "eds:a-b-c_d-:id");

        const TString longUniqueId(100, 'x');
        const TString id = MakeIamDelegationReferrerId("orders", longUniqueId);
        UNIT_ASSERT_VALUES_EQUAL(id.size(), 50);
        UNIT_ASSERT(id.StartsWith("eds:orders:"));
    }

    Y_UNIT_TEST(OnlyPersistedCompleteDelegationIsManaged) {
        auto delegation = Delegation();
        UNIT_ASSERT(IsManagedIamDelegation(delegation));
        delegation.ReferrerId.clear();
        UNIT_ASSERT(!IsManagedIamDelegation(delegation));
        delegation = Delegation();
        delegation.ResourceId.clear();
        UNIT_ASSERT(!IsManagedIamDelegation(delegation));
        delegation = Delegation();
        delegation.ServiceAccountId.clear();
        UNIT_ASSERT(!IsManagedIamDelegation(delegation));
    }

    Y_UNIT_TEST(AlterDetectsReferrerChange) {
        auto oldDelegation = Delegation();
        auto newDelegation = oldDelegation;
        UNIT_ASSERT(IsSameIamDelegation(oldDelegation, newDelegation));
        newDelegation.ReferrerId = "eds:orders:another-id";
        UNIT_ASSERT(!IsSameIamDelegation(oldDelegation, newDelegation));
    }

    Y_UNIT_TEST(AlterDetectsEveryDelegationIdentityChange) {
        const auto oldDelegation = Delegation();

        auto changed = oldDelegation;
        changed.ResourceId = "another-cloud";
        UNIT_ASSERT(!IsSameIamDelegation(oldDelegation, changed));

        changed = oldDelegation;
        changed.ServiceAccountId = "another-sa";
        UNIT_ASSERT(!IsSameIamDelegation(oldDelegation, changed));

        // Referrer type is protocol configuration, not persisted delegation identity.
        changed = oldDelegation;
        changed.ReferrerType = "another-type";
        UNIT_ASSERT(IsSameIamDelegation(oldDelegation, changed));
    }

    Y_UNIT_TEST(FailedAlterCleansOnlyStagedDelegation) {
        auto previous = Delegation();
        auto staged = previous;
        staged.ReferrerId = "eds:orders:new-id";
        UNIT_ASSERT(SelectCleanupAfterSchemeRequest(false, previous, staged) ==
            EDelegationCleanup::Staged);
    }

    Y_UNIT_TEST(SuccessfulAlterCleansPreviousDelegation) {
        auto previous = Delegation();
        auto staged = previous;
        staged.ReferrerId = "eds:orders:new-id";
        UNIT_ASSERT(SelectCleanupAfterSchemeRequest(true, previous, staged) ==
            EDelegationCleanup::Previous);
    }

    Y_UNIT_TEST(DropRevokesOnlyAfterSchemeShardCommit) {
        UNIT_ASSERT(SelectCleanupAfterSchemeRequest(false, Delegation(), {}) ==
            EDelegationCleanup::None);
        UNIT_ASSERT(SelectCleanupAfterSchemeRequest(true, Delegation(), {}) ==
            EDelegationCleanup::Previous);
        UNIT_ASSERT(SelectCleanupAfterSchemeRequest(true, {}, {}) ==
            EDelegationCleanup::None);
    }

    Y_UNIT_TEST(RetryWithSamePersistedDelegationNeedsNoCleanup) {
        const auto delegation = Delegation();
        UNIT_ASSERT(SelectCleanupAfterSchemeRequest(true, delegation, delegation) ==
            EDelegationCleanup::None);
        UNIT_ASSERT(SelectCleanupAfterSchemeRequest(false, delegation, delegation) ==
            EDelegationCleanup::Staged);
    }

    Y_UNIT_TEST(IncompleteDelegationsAreNeverSelectedForCleanup) {
        auto incompletePrevious = Delegation();
        incompletePrevious.ReferrerId.clear();
        auto incompleteStaged = Delegation();
        incompleteStaged.ResourceId.clear();

        UNIT_ASSERT(SelectCleanupAfterSchemeRequest(true, incompletePrevious, {}) ==
            EDelegationCleanup::None);
        UNIT_ASSERT(SelectCleanupAfterSchemeRequest(false, {}, incompleteStaged) ==
            EDelegationCleanup::None);
    }

    Y_UNIT_TEST(DelegationFeatureIsOptIn) {
        NKikimrConfig::TFeatureFlags flags;
        UNIT_ASSERT(!flags.GetEnableExternalDataSourceIamDelegation());
    }

    Y_UNIT_TEST(DelegationRouteLeavesNonIamCreateOnLegacyPath) {
        NKikimrSchemeOp::TModifyScheme schemeTx;
        schemeTx.MutableCreateExternalDataSource()->MutableAuth()->MutableNone();

        UNIT_ASSERT(SelectIamDelegationDdlRoute(
            true,
            schemeTx,
            NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) ==
            EIamDelegationDdlRoute::Legacy);
    }

    Y_UNIT_TEST(DelegationRouteSelectsOnlyIamCreate) {
        NKikimrSchemeOp::TModifyScheme schemeTx;
        schemeTx.MutableCreateExternalDataSource()->MutableAuth()->MutableIam();

        UNIT_ASSERT(SelectIamDelegationDdlRoute(
            true,
            schemeTx,
            NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) ==
            EIamDelegationDdlRoute::IamOperation);
        UNIT_ASSERT(SelectIamDelegationDdlRoute(
            false,
            schemeTx,
            NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) ==
            EIamDelegationDdlRoute::Legacy);
    }

    Y_UNIT_TEST(DelegationRouteGuardsOperationsThatMayRemoveIam) {
        NKikimrSchemeOp::TModifyScheme replacement;
        replacement.SetReplaceIfExists(true);
        replacement.MutableCreateExternalDataSource()->MutableAuth()->MutableBasic();
        UNIT_ASSERT(SelectIamDelegationDdlRoute(
            true,
            replacement,
            NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) ==
            EIamDelegationDdlRoute::LegacyWithIamCleanup);

        NKikimrSchemeOp::TModifyScheme drop;
        UNIT_ASSERT(SelectIamDelegationDdlRoute(
            true,
            drop,
            NKqpProto::TKqpSchemeOperation::kDropExternalDataSource) ==
            EIamDelegationDdlRoute::LegacyWithIamCleanup);
    }
    Y_UNIT_TEST(OnlyConfirmedMissingObjectCanBeIgnored) {
        using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;
        UNIT_ASSERT(ClassifyIamObjectLookup(EStatus::RootUnknown, false) ==
            EIamObjectLookupResult::NotFound);
        UNIT_ASSERT(ClassifyIamObjectLookup(EStatus::PathErrorUnknown, false) ==
            EIamObjectLookupResult::NotFound);
        UNIT_ASSERT(ClassifyIamObjectLookup(EStatus::Ok, true) ==
            EIamObjectLookupResult::Found);
        UNIT_ASSERT(ClassifyIamObjectLookup(EStatus::LookupError, false) ==
            EIamObjectLookupResult::Error);
        UNIT_ASSERT(ClassifyIamObjectLookup(EStatus::RedirectLookupError, false) ==
            EIamObjectLookupResult::Error);
        UNIT_ASSERT(ClassifyIamObjectLookup(EStatus::AccessDenied, false) ==
            EIamObjectLookupResult::Error);
        UNIT_ASSERT(ClassifyIamObjectLookup(EStatus::Ok, false) ==
            EIamObjectLookupResult::Error);
    }
}

} // anonymous namespace
} // namespace NKikimr::NKqp::NExternalDataSource
