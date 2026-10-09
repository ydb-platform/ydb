#include "ut_helpers.h"

#include <util/generic/vector.h>

namespace NKikimr::NIamDelegation::NTests {
namespace {

namespace NProto = NKikimrIamDelegation;

NProto::TDatabaseIdentity DatabaseIdentity(
    const TString& incarnation = "database-incarnation-1",
    const TString& path = "/Root/database",
    const TString& databaseId = "database-id-1")
{
    NProto::TDatabaseIdentity identity;
    identity.SetIncarnation(incarnation);
    identity.SetPath(path);
    identity.SetDatabaseId(databaseId);
    return identity;
}

NProto::TSecretIdentity SecretIdentity(ui64 localId = 42,
    const TString& incarnation = "database-incarnation-1")
{
    NProto::TSecretIdentity identity;
    identity.SetDatabaseIncarnation(incarnation);
    identity.SetPathOwnerId(100);
    identity.SetPathLocalId(localId);
    return identity;
}

NProto::TIamBinding Binding(const TString& referrerId = "ydb.delegation.first") {
    NProto::TIamBinding binding;
    binding.SetServiceAccountId("service-account-1");
    binding.SetCloudId("cloud-1");
    binding.SetResourceType("resource-manager.cloud");
    binding.SetReferrerId(referrerId);
    binding.SetReferrerType("ydb.secret");
    binding.SetServiceId("ydb");
    binding.SetMicroserviceId("data-plane");
    binding.SetReferencePolicy(NProto::WITHOUT_REFERENCES);
    return binding;
}

NProto::TRequest RegisterRequest(const NProto::TDatabaseIdentity& identity) {
    NProto::TRequest request;
    request.MutableRegisterDatabase()->MutableIdentity()->CopyFrom(identity);
    return request;
}

NProto::TRequest StageCreateRequest(const TString& operationId = "create-1",
    const NProto::TIamBinding& binding = Binding(),
    const TString& secretPath = "/Root/database/secret",
    const TString& incarnation = "database-incarnation-1",
    ui64 createTxId = 10)
{
    NProto::TRequest request;
    auto* stage = request.MutableStage();
    stage->SetMode(NProto::CREATE);
    stage->SetOperationId(operationId);
    stage->SetDatabaseIncarnation(incarnation);
    stage->SetSecretPath(secretPath);
    stage->SetCreateTxId(createTxId);
    stage->MutableBinding()->CopyFrom(binding);
    return request;
}

NProto::TRequest BindRequest(const NProto::TDelegation& delegation,
    const NProto::TSecretIdentity& secret = SecretIdentity())
{
    NProto::TRequest request;
    auto* bind = request.MutableBindSecret();
    bind->SetOperationId(delegation.GetOperationId());
    bind->SetExpectedRevision(delegation.GetRevision());
    bind->MutableSecret()->CopyFrom(secret);
    return request;
}

NProto::TRequest StartRequest(const NProto::TDelegation& delegation) {
    NProto::TRequest request;
    auto* start = request.MutableStartSetup();
    start->SetOperationId(delegation.GetOperationId());
    start->SetExpectedRevision(delegation.GetRevision());
    return request;
}

NProto::TRequest SetupResultRequest(const NProto::TDelegation& delegation,
    NProto::ESetupState outcome, const TString& operationId = "iam-setup-1")
{
    NProto::TRequest request;
    auto* result = request.MutableSetSetupResult();
    result->SetOperationId(delegation.GetOperationId());
    result->SetExpectedRevision(delegation.GetRevision());
    result->SetOutcome(outcome);
    result->SetSetupOperationId(operationId);
    return request;
}

NProto::TDelegation GetDelegation(TTestContext& ctx, const TString& operationId) {
    NProto::TRequest request;
    request.MutableGetDelegation()->SetOperationId(operationId);
    return ctx.Call(std::move(request)).GetDelegation();
}

NProto::TSecretRecord GetSecret(TTestContext& ctx, const NProto::TSecretIdentity& identity = SecretIdentity()) {
    NProto::TRequest request;
    request.MutableGetSecret()->MutableSecret()->CopyFrom(identity);
    return ctx.Call(std::move(request)).GetSecret();
}

void AssertBinding(const NProto::TDelegation& delegation, const NProto::TIamBinding& expected) {
    UNIT_ASSERT_VALUES_EQUAL(delegation.GetBinding().SerializeAsString(), expected.SerializeAsString());
}

} // namespace

Y_UNIT_TEST_SUITE(IamDelegationTablet) {
    Y_UNIT_TEST(DatabaseRegistrationIsDurableAndImmutable) {
        TTestContext ctx;
        for (const TString path : {"/", "/Root/A/", "/Root//A", "/Root/../A"}) {
            ctx.Call(RegisterRequest(DatabaseIdentity("invalid", path, "")), NProto::INVALID_ARGUMENT);
        }
        const auto identity = DatabaseIdentity();
        const auto registered = ctx.Call(RegisterRequest(identity));
        UNIT_ASSERT_VALUES_EQUAL(registered.GetDatabase().GetIdentity().SerializeAsString(), identity.SerializeAsString());
        ctx.Reboot();
        const auto restored = ctx.Call(RegisterRequest(identity));
        UNIT_ASSERT_VALUES_EQUAL(restored.SerializeAsString(), registered.SerializeAsString());
        auto conflicting = identity;
        conflicting.SetDatabaseId("different-database-id");
        ctx.Call(RegisterRequest(conflicting), NProto::CONFLICT);
        const auto second = DatabaseIdentity("database-incarnation-2", identity.GetPath(), "");
        const auto replacement = ctx.Call(RegisterRequest(second));
        UNIT_ASSERT_VALUES_EQUAL(replacement.GetDatabase().GetIdentity().GetIncarnation(), second.GetIncarnation());
    }

    Y_UNIT_TEST(IntentPathsMustStayWithinCanonicalDatabase) {
        TTestContext ctx;
        for (const TString path : {"/", "/Root/A/", "/Root//A", "/Root/../A"}) {
            ctx.Call(RegisterRequest(DatabaseIdentity("database-A", path, "")), NProto::INVALID_ARGUMENT);
        }
        ctx.Call(RegisterRequest(DatabaseIdentity("database-A", "/Root/A", "")));
        for (const TString path : {
                "/Root/B/secret", "/Root/AB/secret", "/Root/A", "/Root/A//secret",
                "/Root/A/secret/", "/Root/A/./secret", "/Root/A/../B/secret"}) {
            ctx.Call(StageCreateRequest("create-A", Binding(), path, "database-A"), NProto::INVALID_ARGUMENT);
        }
        ctx.Call(StageCreateRequest("create-A", Binding(), "/Root/A/secret", "database-A"));
        ctx.Call(StageCreateRequest("nested-A", Binding("ydb.delegation.nested"),
            "/Root/A/nested/secret", "database-A", 11));
        ctx.Reboot();
    }

    Y_UNIT_TEST(UnboundIntentIsDurableAndReplayChecksEntireBinding) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto request = StageCreateRequest();
        const auto staged = ctx.Call(request).GetDelegation();
        UNIT_ASSERT_VALUES_EQUAL(staged.GetSetupState(), NProto::SETUP_NOT_STARTED);
        UNIT_ASSERT_VALUES_EQUAL(staged.GetState(), NProto::PENDING);
        UNIT_ASSERT(!staged.HasSecret());

        ctx.Reboot();
        const auto restored = GetDelegation(ctx, staged.GetOperationId());
        UNIT_ASSERT_VALUES_EQUAL(restored.SerializeAsString(), staged.SerializeAsString());
        const auto replay = ctx.Call(request).GetDelegation();
        UNIT_ASSERT_VALUES_EQUAL(replay.GetRevision(), staged.GetRevision());

        auto conflict = request;
        conflict.MutableStage()->MutableBinding()->SetCloudId("changed-cloud");
        ctx.Call(conflict, NProto::CONFLICT);
        conflict = request;
        conflict.MutableStage()->MutableBinding()->SetReferencePolicy(NProto::WITH_REFERENCES);
        ctx.Call(conflict, NProto::CONFLICT);
        conflict = request;
        conflict.MutableStage()->SetCreateTxId(999);
        ctx.Call(conflict, NProto::CONFLICT);
        conflict = request;
        conflict.MutableStage()->SetOperationId("conflicting-create");
        conflict.MutableStage()->MutableBinding()->SetReferrerId("ydb.delegation.conflict");
        ctx.Call(conflict, NProto::CONFLICT);
        AssertBinding(GetDelegation(ctx, staged.GetOperationId()), Binding());
    }

    Y_UNIT_TEST(SetupRequiresBoundIntentAndDurableStart) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto staged = ctx.Call(StageCreateRequest()).GetDelegation();
        ctx.Call(StartRequest(staged), NProto::PRECONDITION_FAILED);
        ctx.Call(SetupResultRequest(staged, NProto::SETUP_SUCCEEDED), NProto::PRECONDITION_FAILED);
        auto wrongIncarnation = SecretIdentity();
        wrongIncarnation.SetDatabaseIncarnation("different-incarnation");
        ctx.Call(BindRequest(staged, wrongIncarnation), NProto::CONFLICT);

        const auto bound = ctx.Call(BindRequest(staged)).GetDelegation();
        ctx.Call(SetupResultRequest(bound, NProto::SETUP_SUCCEEDED), NProto::PRECONDITION_FAILED);
        const auto started = ctx.Call(StartRequest(bound)).GetDelegation();
        UNIT_ASSERT_VALUES_EQUAL(started.GetSetupState(), NProto::SETUP_STARTED);
        ctx.Call(StartRequest(bound), NProto::PRECONDITION_FAILED);
        ctx.Call(StartRequest(started), NProto::PRECONDITION_FAILED);
        ctx.Reboot();
        const auto restored = GetDelegation(ctx, started.GetOperationId());
        UNIT_ASSERT_VALUES_EQUAL(restored.GetSetupState(), NProto::SETUP_STARTED);
        ctx.Call(StartRequest(restored), NProto::PRECONDITION_FAILED);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetSecret().GetPathLocalId(), SecretIdentity().GetPathLocalId());
        const auto setup = ctx.Call(SetupResultRequest(restored, NProto::SETUP_SUCCEEDED));
        UNIT_ASSERT_VALUES_EQUAL(setup.GetDelegation().GetSetupState(), NProto::SETUP_SUCCEEDED);
        UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx).GetPendingOperationId(), staged.GetOperationId());
        AssertBinding(setup.GetDelegation(), Binding());
    }

    Y_UNIT_TEST(StaleRevisionCannotChangeSetupOutcome) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto staged = ctx.Call(StageCreateRequest());
        const auto bound = ctx.Call(BindRequest(staged.GetDelegation()));
        const auto started = ctx.Call(StartRequest(bound.GetDelegation()));
        const auto unknown = ctx.Call(SetupResultRequest(started.GetDelegation(), NProto::SETUP_UNKNOWN));
        ctx.Call(SetupResultRequest(started.GetDelegation(), NProto::SETUP_SUCCEEDED), NProto::PRECONDITION_FAILED);
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").GetSetupState(), NProto::SETUP_UNKNOWN);
        const auto failed = ctx.Call(SetupResultRequest(unknown.GetDelegation(), NProto::SETUP_FAILED));
        UNIT_ASSERT_VALUES_EQUAL(failed.GetDelegation().GetSetupState(), NProto::SETUP_FAILED);
        ctx.Call(SetupResultRequest(failed.GetDelegation(), NProto::SETUP_SUCCEEDED), NProto::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(KnownIamOperationCannotBeReplacedByAnotherResult) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto staged = ctx.Call(StageCreateRequest());
        const auto bound = ctx.Call(BindRequest(staged.GetDelegation()));
        const auto started = ctx.Call(StartRequest(bound.GetDelegation()));
        const auto unknown = ctx.Call(SetupResultRequest(started.GetDelegation(),
            NProto::SETUP_UNKNOWN, "iam-original"));

        ctx.Call(SetupResultRequest(unknown.GetDelegation(), NProto::SETUP_SUCCEEDED,
            "iam-different"), NProto::CONFLICT);
        const auto saved = GetDelegation(ctx, "create-1");
        UNIT_ASSERT_VALUES_EQUAL(saved.GetSetupState(), NProto::SETUP_UNKNOWN);
        UNIT_ASSERT_VALUES_EQUAL(saved.GetSetupOperationId(), "iam-original");
        const auto succeeded = ctx.Call(SetupResultRequest(saved, NProto::SETUP_SUCCEEDED, ""));
        UNIT_ASSERT_VALUES_EQUAL(succeeded.GetDelegation().GetSetupOperationId(), "iam-original");
    }
}

} // namespace NKikimr::NIamDelegation::NTests
