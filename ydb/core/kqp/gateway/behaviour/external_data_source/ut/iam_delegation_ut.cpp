#include <ydb/core/kqp/gateway/behaviour/external_data_source/iam_delegation.h>
#include <ydb/core/kqp/gateway/behaviour/external_data_source/iam_delegation_ddl.h>
#include <ydb/core/kqp/gateway/behaviour/external_data_source/iam_object_lookup.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/public/api/client/yc_private/iam/operation_service.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/descriptor.h>

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
        UNIT_ASSERT(request.on_behalf_of_subject_id().empty());
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

    Y_UNIT_TEST(VerifiedSubjectRequiresAndReturnsUserBearerToken) {
        const NACLib::TUserToken accessServiceUser({
            .OriginalUserToken = "user-iam-token",
            .UserSID = "user-id@as",
            .AuthType = "AccessService",
        });
        UNIT_ASSERT(IsVerifiedIamDelegationSubject(accessServiceUser));
        UNIT_ASSERT_VALUES_EQUAL(
            GetIamDelegationBearerToken(accessServiceUser), "user-iam-token");

        const NACLib::TUserToken missingBearer({
            .UserSID = "user-id@as",
            .AuthType = "AccessService",
        });
        UNIT_ASSERT(!IsVerifiedIamDelegationSubject(missingBearer));
        UNIT_ASSERT(GetIamDelegationBearerToken(missingBearer).empty());

        const NACLib::TUserToken loginUser({
            .OriginalUserToken = "user-iam-token",
            .UserSID = "user-id@as",
            .AuthType = "Login",
        });
        UNIT_ASSERT(!IsVerifiedIamDelegationSubject(loginUser));

        const NACLib::TUserToken missingSubject({
            .OriginalUserToken = "user-iam-token",
            .AuthType = "AccessService",
        });
        UNIT_ASSERT(!IsVerifiedIamDelegationSubject(missingSubject));
    }

    Y_UNIT_TEST(UnfinishedIamOperationMustBePolled) {
        ydb::yc::priv::operation::Operation operation;
        operation.set_id("operation-id");
        UNIT_ASSERT(ClassifyIamOperation(operation) ==
            EIamOperationState::InProgress);

        operation.set_done(true);
        UNIT_ASSERT(ClassifyIamOperation(operation) ==
            EIamOperationState::Succeeded);

        operation.mutable_error()->set_message("failed");
        UNIT_ASSERT(ClassifyIamOperation(operation) ==
            EIamOperationState::Failed);
    }

    Y_UNIT_TEST(VendoredIamProtoMatchesCanonicalLifecycleSurface) {
        const auto* pool = google::protobuf::DescriptorPool::generated_pool();
        const auto* service = pool->FindServiceByName(
            "yandex.cloud.priv.iam.v1.ServiceControlService");
        UNIT_ASSERT(service);
        UNIT_ASSERT(service->FindMethodByName("EnsureEnabled"));
        UNIT_ASSERT(service->FindMethodByName("CanEnsureEnabled"));
        UNIT_ASSERT(service->FindMethodByName("SetupDelegation"));
        UNIT_ASSERT(service->FindMethodByName("RevokeDelegation"));
        UNIT_ASSERT(!service->FindMethodByName("IsEnabled"));

        const auto* ensure = pool->FindMessageTypeByName(
            "yandex.cloud.priv.iam.v1.EnsureServicesEnabledRequest");
        UNIT_ASSERT(ensure);
        UNIT_ASSERT_VALUES_EQUAL(
            ensure->FindFieldByName("service_ids")->number(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            ensure->FindFieldByName("resource")->number(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            ensure->FindFieldByName("on_behalf_of_subject_id")->number(), 3);

        const auto* setup = pool->FindMessageTypeByName(
            "yandex.cloud.priv.iam.v1.SetupDelegationRequest");
        UNIT_ASSERT(setup);
        UNIT_ASSERT_VALUES_EQUAL(
            setup->FindFieldByName("target_service_account_id")->number(), 4);
        UNIT_ASSERT_VALUES_EQUAL(
            setup->FindFieldByName("referrer")->number(), 5);
        UNIT_ASSERT_VALUES_EQUAL(
            setup->FindFieldByName("on_behalf_of_subject_id")->number(), 6);
        UNIT_ASSERT_VALUES_EQUAL(
            setup->FindFieldByName("with_references")->number(), 7);

        const auto* revoke = pool->FindMessageTypeByName(
            "yandex.cloud.priv.iam.v1.RevokeDelegationRequest");
        UNIT_ASSERT(revoke);
        UNIT_ASSERT(!revoke->FindFieldByName("on_behalf_of_subject_id"));
        UNIT_ASSERT_VALUES_EQUAL(
            revoke->FindFieldByName("with_references")->number(), 6);

        const auto* operationService = pool->FindServiceByName(
            "yandex.cloud.priv.iam.v1.OperationService");
        UNIT_ASSERT(operationService);
        const auto* get = operationService->FindMethodByName("Get");
        UNIT_ASSERT(get);
        UNIT_ASSERT_VALUES_EQUAL(
            get->input_type()->FindFieldByName("operation_id")->number(), 1);
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

    Y_UNIT_TEST(CreateIfNotExistsSkipsSetupForExistingObject) {
        NKikimrSchemeOp::TModifyScheme schemeTx;
        schemeTx.SetFailedOnAlreadyExists(false);

        UNIT_ASSERT(ShouldSkipIamDelegationSetup(schemeTx, false));
        UNIT_ASSERT(!ShouldSkipIamDelegationSetup(schemeTx, true));

        schemeTx.SetReplaceIfExists(true);
        UNIT_ASSERT(!ShouldSkipIamDelegationSetup(schemeTx, false));

        schemeTx.SetReplaceIfExists(false);
        schemeTx.SetFailedOnAlreadyExists(true);
        UNIT_ASSERT(!ShouldSkipIamDelegationSetup(schemeTx, false));
    }

    Y_UNIT_TEST(ReplacementCarriesSnapshotCompareAndSwap) {
        NKikimrSchemeOp::TModifyScheme schemeTx;
        AddIamPathVersionPrecondition(schemeTx, 42, 17);

        UNIT_ASSERT_VALUES_EQUAL(schemeTx.ApplyIfSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(schemeTx.GetApplyIf(0).GetPathId(), 42);
        UNIT_ASSERT_VALUES_EQUAL(schemeTx.GetApplyIf(0).GetPathVersion(), 17);
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
