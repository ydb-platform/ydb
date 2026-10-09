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

NProto::TRequest StageAlterRequest(const NProto::TSecretRecord& secret,
    const TString& operationId, const NProto::TIamBinding& binding)
{
    NProto::TRequest request;
    auto* stage = request.MutableStage();
    stage->SetMode(NProto::ALTER);
    stage->SetOperationId(operationId);
    stage->SetDatabaseIncarnation(secret.GetIdentity().GetDatabaseIncarnation());
    stage->SetSecretPath(secret.GetSecretPath());
    stage->SetCreateTxId(secret.GetCreateTxId());
    stage->MutableSecret()->CopyFrom(secret.GetIdentity());
    stage->SetExpectedSecretRevision(secret.GetRevision());
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

NProto::TRequest PromoteRequest(const NProto::TDelegation& delegation, ui64 secretRevision) {
    NProto::TRequest request;
    auto* promote = request.MutablePromote();
    promote->SetOperationId(delegation.GetOperationId());
    promote->SetExpectedRevision(delegation.GetRevision());
    promote->SetExpectedSecretRevision(secretRevision);
    return request;
}

NProto::TRequest DropRequest(const NProto::TSecretRecord& secret) {
    NProto::TRequest request;
    auto* drop = request.MutableDropSecret();
    drop->MutableSecret()->CopyFrom(secret.GetIdentity());
    drop->SetExpectedRevision(secret.GetRevision());
    return request;
}

NProto::TRequest DropRequest(const NProto::TDelegation& delegation) {
    NProto::TRequest request;
    auto* drop = request.MutableDropSecret();
    drop->SetOperationId(delegation.GetOperationId());
    drop->SetExpectedRevision(delegation.GetRevision());
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

NProto::TRequest ListRequest(const TString& incarnation = "database-incarnation-1", ui32 pageSize = 100) {
    NProto::TRequest request;
    auto* list = request.MutableListInventory();
    list->SetDatabaseIncarnation(incarnation);
    list->SetPageSize(pageSize);
    return request;
}

NProto::TRequest ClaimRequest(const TString& workerId = "worker-1", ui32 limit = 100) {
    NProto::TRequest request;
    auto* claim = request.MutableClaimRevocations();
    claim->SetWorkerId(workerId);
    claim->SetLimit(limit);
    claim->SetLeaseUs(TDuration::Seconds(10).MicroSeconds());
    return request;
}

NProto::TRequest FinishRequest(const NProto::TDelegation& delegation,
    NProto::ERevokeOutcome outcome = NProto::REVOKE_SUCCEEDED,
    TDuration retryAfter = TDuration::Zero())
{
    NProto::TRequest request;
    auto* finish = request.MutableFinishRevocation();
    finish->SetOperationId(delegation.GetOperationId());
    finish->MutableClaim()->CopyFrom(delegation.GetClaim());
    finish->SetOutcome(outcome);
    finish->SetRevokeOperationId("iam-revoke-1");
    finish->SetRetryAfterUs(retryAfter.MicroSeconds());
    return request;
}

NProto::TResponse CompleteSetup(TTestContext& ctx, const NProto::TDelegation& delegation) {
    const auto started = ctx.Call(StartRequest(delegation));
    return ctx.Call(SetupResultRequest(started.GetDelegation(), NProto::SETUP_SUCCEEDED));
}

NProto::TResponse CreateActive(TTestContext& ctx) {
    ctx.Call(RegisterRequest(DatabaseIdentity()));
    const auto staged = ctx.Call(StageCreateRequest());
    const auto bound = ctx.Call(BindRequest(staged.GetDelegation()));
    const auto setup = CompleteSetup(ctx, bound.GetDelegation());
    return ctx.Call(PromoteRequest(setup.GetDelegation(), GetSecret(ctx).GetRevision()));
}

void AssertBinding(const NProto::TDelegation& delegation, const NProto::TIamBinding& expected) {
    UNIT_ASSERT_VALUES_EQUAL(delegation.GetBinding().SerializeAsString(), expected.SerializeAsString());
}

} // namespace

Y_UNIT_TEST_SUITE(IamDelegationTablet) {
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
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest("database-A")).DelegationsSize(), 0);
        ctx.Call(StageCreateRequest("create-A", Binding(), "/Root/A/secret", "database-A"));
        ctx.Call(StageCreateRequest("nested-A", Binding("ydb.delegation.nested"),
            "/Root/A/nested/secret", "database-A", 11));
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest("database-A")).DelegationsSize(), 2);
    }

    Y_UNIT_TEST(KnownEmptyDatabaseAndIncarnationIsolation) {
        TTestContext ctx;
        ctx.Call(ListRequest(), NProto::NOT_FOUND);
        const auto identity = DatabaseIdentity();
        const auto registered = ctx.Call(RegisterRequest(identity));
        UNIT_ASSERT_VALUES_EQUAL(registered.GetDatabase().GetIdentity().GetIncarnation(), identity.GetIncarnation());
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).DelegationsSize(), 0);

        ctx.Reboot();
        ctx.Call(RegisterRequest(identity));
        auto conflicting = identity;
        conflicting.SetDatabaseId("different-database-id");
        ctx.Call(RegisterRequest(conflicting), NProto::CONFLICT);

        const auto second = DatabaseIdentity("database-incarnation-2", identity.GetPath(), "database-id-2");
        ctx.Call(RegisterRequest(second));
        ctx.Call(StageCreateRequest());
        ctx.Call(StageCreateRequest("create-2", Binding("ydb.delegation.second"),
            "/Root/database/secret", second.GetIncarnation(), 11));
        const auto firstInventory = ctx.Call(ListRequest());
        const auto secondInventory = ctx.Call(ListRequest(second.GetIncarnation()));
        UNIT_ASSERT_VALUES_EQUAL(firstInventory.DelegationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(secondInventory.DelegationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(firstInventory.GetDelegations(0).GetOperationId(), "create-1");
        UNIT_ASSERT_VALUES_EQUAL(secondInventory.GetDelegations(0).GetOperationId(), "create-2");
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
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).DelegationsSize(), 1);

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
        const auto secret = GetSecret(ctx);
        const auto promoted = ctx.Call(PromoteRequest(setup.GetDelegation(), secret.GetRevision()));
        UNIT_ASSERT_VALUES_EQUAL(promoted.GetDelegation().GetState(), NProto::ACTIVE);
        UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx).GetCurrentOperationId(), staged.GetOperationId());
        AssertBinding(promoted.GetDelegation(), Binding());
    }

    Y_UNIT_TEST(PromotionAtomicallyQueuesPredecessorAndPersistsBinding) {
        TTestContext ctx;
        CreateActive(ctx);
        const auto original = GetDelegation(ctx, "create-1");
        const auto oldSecret = GetSecret(ctx);
        auto replacementBinding = Binding("ydb.delegation.replacement");
        replacementBinding.SetCloudId("replacement-cloud");
        replacementBinding.SetServiceAccountId("replacement-service-account");
        const auto staged = ctx.Call(StageAlterRequest(oldSecret, "alter-1", replacementBinding));
        ctx.Call(PromoteRequest(staged.GetDelegation(), GetSecret(ctx).GetRevision()),
            NProto::PRECONDITION_FAILED);
        const auto setup = CompleteSetup(ctx, staged.GetDelegation());
        ctx.Call(PromoteRequest(setup.GetDelegation(), oldSecret.GetRevision()),
            NProto::PRECONDITION_FAILED);
        const auto promoteRequest = PromoteRequest(setup.GetDelegation(), GetSecret(ctx).GetRevision());
        const auto promoted = ctx.Call(promoteRequest);
        ctx.Reboot();

        const auto secret = GetSecret(ctx);
        UNIT_ASSERT_VALUES_EQUAL(secret.GetCurrentOperationId(), "alter-1");
        UNIT_ASSERT(secret.GetPendingOperationId().empty());
        const auto predecessor = GetDelegation(ctx, "create-1");
        const auto replacement = GetDelegation(ctx, "alter-1");
        UNIT_ASSERT_VALUES_EQUAL(predecessor.GetState(), NProto::REVOCATION_PENDING);
        UNIT_ASSERT_VALUES_EQUAL(replacement.GetState(), NProto::ACTIVE);
        AssertBinding(predecessor, original.GetBinding());
        AssertBinding(replacement, replacementBinding);
        const auto replay = ctx.Call(promoteRequest);
        UNIT_ASSERT_VALUES_EQUAL(replay.GetDelegation().GetRevision(), promoted.GetDelegation().GetRevision());

        const auto claimed = ctx.Call(ClaimRequest());
        UNIT_ASSERT_VALUES_EQUAL(claimed.DelegationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(claimed.GetDelegations(0).GetOperationId(), "create-1");
        AssertBinding(claimed.GetDelegations(0), original.GetBinding());
    }

    Y_UNIT_TEST(DroppedUnknownSetupRemainsVisibleAndCannotBeClaimed) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto staged = ctx.Call(StageCreateRequest());
        const auto bound = ctx.Call(BindRequest(staged.GetDelegation()));
        const auto started = ctx.Call(StartRequest(bound.GetDelegation()));
        ctx.Call(SetupResultRequest(started.GetDelegation(), NProto::SETUP_UNKNOWN, ""));
        ctx.Call(DropRequest(GetSecret(ctx)));
        ctx.Reboot();

        const auto dropped = GetSecret(ctx);
        UNIT_ASSERT_VALUES_EQUAL(dropped.GetState(), NProto::SECRET_DROPPED);
        const auto unknown = GetDelegation(ctx, "create-1");
        UNIT_ASSERT_VALUES_EQUAL(unknown.GetState(), NProto::WAITING_SETUP);
        UNIT_ASSERT_VALUES_EQUAL(unknown.GetSetupState(), NProto::SETUP_UNKNOWN);
        UNIT_ASSERT(unknown.GetSetupOperationId().empty());
        const auto inventory = ctx.Call(ListRequest());
        UNIT_ASSERT_VALUES_EQUAL(inventory.DelegationsSize(), 1);
        AssertBinding(inventory.GetDelegations(0), Binding());
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        ctx.AdvanceTime(TDuration::Days(1));
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);

        const auto resolved = ctx.Call(SetupResultRequest(unknown, NProto::SETUP_SUCCEEDED));
        UNIT_ASSERT_VALUES_EQUAL(resolved.GetDelegation().GetState(), NProto::REVOCATION_PENDING);
        const auto claimed = ctx.Call(ClaimRequest());
        UNIT_ASSERT_VALUES_EQUAL(claimed.DelegationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(claimed.GetDelegations(0).GetOperationId(), "create-1");
    }

    Y_UNIT_TEST(DropBeforeSetupCancelsIntentAndReleasesName) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto staged = ctx.Call(StageCreateRequest()).GetDelegation();
        ctx.Call(DropRequest(staged));
        ctx.Reboot();
        const auto cancelled = GetDelegation(ctx, staged.GetOperationId());
        UNIT_ASSERT_VALUES_EQUAL(cancelled.GetState(), NProto::CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        ctx.Call(StartRequest(cancelled), NProto::PRECONDITION_FAILED);
        const auto replacement = ctx.Call(StageCreateRequest("create-2", Binding("ydb.delegation.second"),
            "/Root/database/secret", "database-incarnation-1", 20));
        UNIT_ASSERT_VALUES_EQUAL(replacement.GetDelegation().GetState(), NProto::PENDING);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).DelegationsSize(), 1);
    }

    Y_UNIT_TEST(StaleUnboundDropCannotRetireBoundSecret) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto staged = ctx.Call(StageCreateRequest()).GetDelegation();
        const auto unboundDrop = DropRequest(staged);
        const auto bound = ctx.Call(BindRequest(staged));
        UNIT_ASSERT_VALUES_EQUAL(staged.GetRevision(), bound.GetSecret().GetRevision());
        UNIT_ASSERT(bound.GetDelegation().GetRevision() > staged.GetRevision());

        ctx.Call(unboundDrop, NProto::PRECONDITION_FAILED);
        ctx.Reboot();
        const auto secret = GetSecret(ctx);
        UNIT_ASSERT_VALUES_EQUAL(secret.GetState(), NProto::SECRET_LIVE);
        UNIT_ASSERT_VALUES_EQUAL(secret.GetPendingOperationId(), staged.GetOperationId());
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, staged.GetOperationId()).GetState(), NProto::PENDING);
    }

    Y_UNIT_TEST(ClaimLeaseAndTabletGenerationFenceAcknowledgements) {
        TTestContext ctx;
        CreateActive(ctx);
        ctx.Call(DropRequest(GetSecret(ctx)));
        const auto firstBatch = ctx.Call(ClaimRequest("worker-1"));
        UNIT_ASSERT_VALUES_EQUAL(firstBatch.DelegationsSize(), 1);
        const auto first = firstBatch.GetDelegations(0);
        UNIT_ASSERT_VALUES_EQUAL(first.GetState(), NProto::REVOKING);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest("worker-2")).DelegationsSize(), 0);

        ctx.AdvanceTime(TDuration::Seconds(11));
        const auto secondBatch = ctx.Call(ClaimRequest("worker-2"));
        UNIT_ASSERT_VALUES_EQUAL(secondBatch.DelegationsSize(), 1);
        const auto second = secondBatch.GetDelegations(0);
        UNIT_ASSERT(second.GetClaim().GetAttempt() > first.GetClaim().GetAttempt());
        ctx.Call(FinishRequest(first), NProto::STALE_CLAIM);
        ctx.Reboot();
        ctx.Call(FinishRequest(second), NProto::STALE_CLAIM);

        ctx.AdvanceTime(TDuration::Seconds(11));
        const auto thirdBatch = ctx.Call(ClaimRequest("worker-3"));
        UNIT_ASSERT_VALUES_EQUAL(thirdBatch.DelegationsSize(), 1);
        const auto third = thirdBatch.GetDelegations(0);
        UNIT_ASSERT(third.GetClaim().GetGeneration() > second.GetClaim().GetGeneration());
        UNIT_ASSERT(third.GetClaim().GetAttempt() > second.GetClaim().GetAttempt());
        ctx.Call(FinishRequest(third));
        ctx.Reboot();
        const auto revoked = GetDelegation(ctx, "create-1");
        UNIT_ASSERT_VALUES_EQUAL(revoked.GetState(), NProto::REVOKED);
        UNIT_ASSERT_VALUES_EQUAL(revoked.GetRevokeOperationId(), "iam-revoke-1");
        AssertBinding(revoked, Binding());
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).DelegationsSize(), 0);
    }

    Y_UNIT_TEST(RetryDefersRevocationAndCannotOverwriteNewClaim) {
        TTestContext ctx;
        CreateActive(ctx);
        ctx.Call(DropRequest(GetSecret(ctx)));
        const auto firstBatch = ctx.Call(ClaimRequest());
        UNIT_ASSERT_VALUES_EQUAL(firstBatch.DelegationsSize(), 1);
        const auto first = firstBatch.GetDelegations(0);
        ctx.Call(FinishRequest(first, NProto::REVOKE_RETRY, TDuration::Seconds(30)));
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        ctx.AdvanceTime(TDuration::Seconds(31));
        const auto secondBatch = ctx.Call(ClaimRequest("worker-2"));
        UNIT_ASSERT_VALUES_EQUAL(secondBatch.DelegationsSize(), 1);
        const auto second = secondBatch.GetDelegations(0);
        AssertBinding(second, Binding());
        ctx.Call(FinishRequest(first), NProto::STALE_CLAIM);
        ctx.Call(FinishRequest(second));
    }

    Y_UNIT_TEST(InventoryPaginationSurvivesRebootAndExpiresOnMutation) {
        TTestContext ctx;
        CreateActive(ctx);
        const auto active = GetSecret(ctx);
        const auto pending = ctx.Call(StageAlterRequest(active, "operation-2", Binding("ydb.delegation.second")));
        ctx.Call(StageCreateRequest("operation-3", Binding("ydb.delegation.third"), "/Root/database/third", "database-incarnation-1", 30));
        const auto pageRequest = ListRequest("database-incarnation-1", 2);
        const auto first = ctx.Call(pageRequest);
        UNIT_ASSERT_VALUES_EQUAL(first.DelegationsSize(), 2);
        UNIT_ASSERT(!first.GetNextAfterOperationId().empty());
        auto continuation = pageRequest;
        continuation.MutableListInventory()->SetAfterOperationId(first.GetNextAfterOperationId());
        continuation.MutableListInventory()->SetInventoryRevision(first.GetDatabase().GetInventoryRevision());
        const auto second = ctx.Call(continuation);
        UNIT_ASSERT_VALUES_EQUAL(second.DelegationsSize(), 1);
        UNIT_ASSERT(second.GetNextAfterOperationId().empty());
        TVector<TString> operationIds;
        for (const auto& item : first.GetDelegations()) {
            operationIds.push_back(item.GetOperationId());
        }
        operationIds.push_back(second.GetDelegations(0).GetOperationId());
        UNIT_ASSERT_VALUES_EQUAL(operationIds[0], "create-1");
        UNIT_ASSERT_VALUES_EQUAL(operationIds[1], "operation-2");
        UNIT_ASSERT_VALUES_EQUAL(operationIds[2], "operation-3");

        ctx.Reboot();
        const auto replay = ctx.Call(continuation);
        UNIT_ASSERT_VALUES_EQUAL(replay.SerializeAsString(), second.SerializeAsString());
        ctx.Call(DropRequest(GetSecret(ctx)));
        ctx.Call(continuation, NProto::SNAPSHOT_EXPIRED);
        const auto inventory = ctx.Call(ListRequest());
        UNIT_ASSERT_VALUES_EQUAL(inventory.DelegationsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").GetState(), NProto::REVOCATION_PENDING);
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, pending.GetDelegation().GetOperationId()).GetState(), NProto::CANCELLED);
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
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).DelegationsSize(), 0);
    }

    Y_UNIT_TEST(FailedAlterReleasesPendingWithoutLosingCurrent) {
        TTestContext ctx;
        CreateActive(ctx);
        const auto staged = ctx.Call(StageAlterRequest(GetSecret(ctx), "failed-alter",
            Binding("ydb.delegation.failed")));
        const auto started = ctx.Call(StartRequest(staged.GetDelegation()));
        ctx.Call(SetupResultRequest(started.GetDelegation(), NProto::SETUP_FAILED));
        ctx.Reboot();

        const auto secret = GetSecret(ctx);
        UNIT_ASSERT_VALUES_EQUAL(secret.GetState(), NProto::SECRET_LIVE);
        UNIT_ASSERT_VALUES_EQUAL(secret.GetCurrentOperationId(), "create-1");
        UNIT_ASSERT(secret.GetPendingOperationId().empty());
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").GetState(), NProto::ACTIVE);
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "failed-alter").GetState(), NProto::CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);

        const auto replacement = ctx.Call(StageAlterRequest(secret, "next-alter",
            Binding("ydb.delegation.next")));
        UNIT_ASSERT_VALUES_EQUAL(replacement.GetDelegation().GetState(), NProto::PENDING);
        UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx).GetCurrentOperationId(), "create-1");
        UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx).GetPendingOperationId(), "next-alter");
    }

    Y_UNIT_TEST(FailedCreateKeepsNameUntilDropAndRetainsTombstones) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto originalRequest = StageCreateRequest();
        const auto staged = ctx.Call(originalRequest);
        const auto bound = ctx.Call(BindRequest(staged.GetDelegation()));
        const auto started = ctx.Call(StartRequest(bound.GetDelegation()));
        ctx.Call(SetupResultRequest(started.GetDelegation(), NProto::SETUP_FAILED));
        ctx.Reboot();

        const auto replay = ctx.Call(originalRequest);
        UNIT_ASSERT_VALUES_EQUAL(replay.GetDelegation().GetState(), NProto::CANCELLED);
        const auto replacement = StageCreateRequest("create-2", Binding("ydb.delegation.second"),
            "/Root/database/secret", "database-incarnation-1", 20);
        ctx.Call(replacement, NProto::CONFLICT);
        ctx.Call(DropRequest(GetSecret(ctx)));
        const auto replacementIntent = ctx.Call(replacement).GetDelegation();
        ctx.Call(BindRequest(replacementIntent), NProto::CONFLICT);
        ctx.Call(BindRequest(replacementIntent, SecretIdentity(43)));

        auto reuseOperation = replacement;
        reuseOperation.MutableStage()->SetOperationId("create-1");
        ctx.Call(reuseOperation, NProto::CONFLICT);
        ctx.Call(StageCreateRequest("reuse-referrer", Binding(), "/Root/database/other",
            "database-incarnation-1", 30), NProto::CONFLICT);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).DelegationsSize(), 1);
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
