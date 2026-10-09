#include "requests.h"

namespace NKikimr::NIamDelegation::NTests {
namespace {

void AssertMissingDelegation(TTestContext& ctx, const TString& operationId) {
    NProto::TRequest request;
    request.MutableGetDelegation()->SetOperationId(operationId);
    ctx.Call(std::move(request), NProto::NOT_FOUND);
}

void AssertMissingSecret(TTestContext& ctx) {
    NProto::TRequest request;
    request.MutableGetSecret()->MutableSecret()->CopyFrom(SecretIdentity());
    ctx.Call(std::move(request), NProto::NOT_FOUND);
}

void AssertInventoryMatchesCanonicalRecords(TTestContext& ctx, ui32 expectedSize) {
    const auto inventory = ctx.Call(ListRequest());
    UNIT_ASSERT_VALUES_EQUAL(inventory.DelegationsSize(), expectedSize);
    TString previousOperation;
    for (const auto& record : inventory.GetDelegations()) {
        UNIT_ASSERT(record.GetOperationId() > previousOperation);
        previousOperation = record.GetOperationId();
        const auto canonical = GetDelegation(ctx, record.GetOperationId());
        UNIT_ASSERT_VALUES_EQUAL(record.SerializeAsString(), canonical.SerializeAsString());
    }
}

// Register each crash point independently, so a failure identifies its durable
// boundary and no later scenario can conceal an earlier failed injection.
#define Y_UNIT_TEST_DURABLE_BOUNDARIES(Name) \
    void Name(ECrashPoint point); \
    Y_UNIT_TEST(Name##BeforeCommit) { Name(ECrashPoint::BeforeCommit); } \
    Y_UNIT_TEST(Name##AfterDurableCommit) { Name(ECrashPoint::AfterDurableCommitBeforeComplete); } \
    Y_UNIT_TEST(Name##LostResponse) { Name(ECrashPoint::ResponseLost); } \
    void Name(ECrashPoint point)

} // namespace

Y_UNIT_TEST_SUITE(IamDelegationDurability) {
    Y_UNIT_TEST_DURABLE_BOUNDARIES(RegisterDatabase) {
        TTestContext ctx;
        const auto request = RegisterRequest(DatabaseIdentity());
        ctx.CrashCall(request, point);
        if (point == ECrashPoint::BeforeCommit) {
            ctx.Call(ListRequest(), NProto::NOT_FOUND);
        } else {
            const auto restored = ctx.Call(ListRequest());
            UNIT_ASSERT_VALUES_EQUAL(restored.GetDatabase().GetIdentity().SerializeAsString(),
                DatabaseIdentity().SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(restored.DelegationsSize(), 0);
        }
        const auto registered = ctx.Call(request);
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(request).SerializeAsString(), registered.SerializeAsString());
        auto conflict = DatabaseIdentity();
        conflict.SetDatabaseId("different-database");
        ctx.Call(RegisterRequest(conflict), NProto::CONFLICT);
    }

    Y_UNIT_TEST_DURABLE_BOUNDARIES(CreateIntent) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto request = StageCreateRequest();
        ctx.CrashCall(request, point);
        if (point == ECrashPoint::BeforeCommit) {
            AssertMissingDelegation(ctx, "create-1");
            AssertInventoryMatchesCanonicalRecords(ctx, 0);
        } else {
            const auto record = GetDelegation(ctx, "create-1");
            UNIT_ASSERT_VALUES_EQUAL(record.GetState(), NProto::PENDING);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSetupState(), NProto::SETUP_NOT_STARTED);
            UNIT_ASSERT(!record.HasSecret());
            AssertBinding(record, Binding());
            AssertInventoryMatchesCanonicalRecords(ctx, 1);
        }
        const auto replay = ctx.Call(request).GetDelegation();
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(request).GetDelegation().SerializeAsString(), replay.SerializeAsString());
        ctx.Call(StageCreateRequest("same-name", Binding("other-referrer")), NProto::CONFLICT);
        ctx.Call(StageCreateRequest("same-referrer", Binding(), "/Root/database/other"), NProto::CONFLICT);
        AssertInventoryMatchesCanonicalRecords(ctx, 1);
    }

    Y_UNIT_TEST_DURABLE_BOUNDARIES(BindIntent) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto staged = ctx.Call(StageCreateRequest()).GetDelegation();
        const auto request = BindRequest(staged);
        ctx.CrashCall(request, point);
        if (point == ECrashPoint::BeforeCommit) {
            AssertMissingSecret(ctx);
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), staged.SerializeAsString());
        } else {
            const auto record = GetDelegation(ctx, "create-1");
            const auto secret = GetSecret(ctx);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSecret().SerializeAsString(), secret.GetIdentity().SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(secret.GetCreateOperationId(), "create-1");
            UNIT_ASSERT_VALUES_EQUAL(secret.GetPendingOperationId(), "create-1");
            UNIT_ASSERT_VALUES_EQUAL(secret.GetState(), NProto::SECRET_LIVE);
            ctx.Call(DropRequest(staged), NProto::PRECONDITION_FAILED);
        }
        const auto replay = ctx.Call(request);
        ctx.Reboot();
        const auto repeated = ctx.Call(request);
        UNIT_ASSERT_VALUES_EQUAL(repeated.GetDelegation().SerializeAsString(), replay.GetDelegation().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(repeated.GetSecret().SerializeAsString(), replay.GetSecret().SerializeAsString());
        ctx.Call(BindRequest(staged, SecretIdentity(43)), NProto::CONFLICT);
        AssertInventoryMatchesCanonicalRecords(ctx, 1);
    }

    Y_UNIT_TEST_DURABLE_BOUNDARIES(StartDispatchPermission) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto staged = ctx.Call(StageCreateRequest());
        const auto bound = ctx.Call(BindRequest(staged.GetDelegation())).GetDelegation();
        const auto request = StartRequest(bound);
        ctx.CrashCall(request, point);
        if (point == ECrashPoint::BeforeCommit) {
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), bound.SerializeAsString());
            ctx.Call(request);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").GetSetupState(), NProto::SETUP_STARTED);
        }
        // A lost successful response is uncertainty, never a second grant to
        // dispatch IAM setup, even when the caller refreshes its CAS revision.
        ctx.Call(request, NProto::PRECONDITION_FAILED);
        ctx.Reboot();
        const auto started = GetDelegation(ctx, "create-1");
        ctx.Call(StartRequest(started), NProto::PRECONDITION_FAILED);
        ctx.Call(SetupResultRequest(started, NProto::SETUP_UNKNOWN, "recover-operation"));
        AssertInventoryMatchesCanonicalRecords(ctx, 1);
    }

    Y_UNIT_TEST_DURABLE_BOUNDARIES(SetupSuccess) {
        TTestContext ctx;
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        const auto staged = ctx.Call(StageCreateRequest());
        const auto bound = ctx.Call(BindRequest(staged.GetDelegation()));
        const auto started = ctx.Call(StartRequest(bound.GetDelegation())).GetDelegation();
        const auto request = SetupResultRequest(started, NProto::SETUP_SUCCEEDED, "saved-setup-operation");
        ctx.CrashCall(request, point);
        const auto restored = GetDelegation(ctx, "create-1");
        if (point == ECrashPoint::BeforeCommit) {
            UNIT_ASSERT_VALUES_EQUAL(restored.SerializeAsString(), started.SerializeAsString());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(restored.GetSetupState(), NProto::SETUP_SUCCEEDED);
            UNIT_ASSERT_VALUES_EQUAL(restored.GetSetupOperationId(), "saved-setup-operation");
            UNIT_ASSERT_VALUES_EQUAL(restored.GetState(), NProto::PENDING);
        }
        const auto replay = ctx.Call(request).GetDelegation();
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(request).GetDelegation().SerializeAsString(), replay.SerializeAsString());
        AssertBinding(GetDelegation(ctx, "create-1"), Binding());
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        AssertInventoryMatchesCanonicalRecords(ctx, 1);
    }

    Y_UNIT_TEST_DURABLE_BOUNDARIES(PromotionAndPredecessorOutbox) {
        TTestContext ctx;
        CreateActive(ctx);
        const auto oldBinding = GetDelegation(ctx, "create-1").GetBinding();
        auto replacementBinding = Binding("replacement-referrer");
        replacementBinding.SetServiceAccountId("replacement-account");
        replacementBinding.SetCloudId("replacement-cloud");
        replacementBinding.SetReferencePolicy(NProto::WITH_REFERENCES);
        const auto staged = ctx.Call(StageAlterRequest(GetSecret(ctx), "alter-1", replacementBinding));
        const auto setup = CompleteSetup(ctx, staged.GetDelegation()).GetDelegation();
        const auto oldSecret = GetSecret(ctx);
        const auto request = PromoteRequest(setup, oldSecret.GetRevision());
        ctx.CrashCall(request, point);
        if (point == ECrashPoint::BeforeCommit) {
            UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx).SerializeAsString(), oldSecret.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").GetState(), NProto::ACTIVE);
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "alter-1").GetState(), NProto::PENDING);
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        } else {
            const auto secret = GetSecret(ctx);
            UNIT_ASSERT_VALUES_EQUAL(secret.GetCurrentOperationId(), "alter-1");
            UNIT_ASSERT(secret.GetPendingOperationId().empty());
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").GetState(), NProto::REVOCATION_PENDING);
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "alter-1").GetState(), NProto::ACTIVE);
        }
        const auto promoted = ctx.Call(request).GetDelegation();
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(request).GetDelegation().SerializeAsString(), promoted.SerializeAsString());
        AssertBinding(GetDelegation(ctx, "alter-1"), replacementBinding);
        AssertInventoryMatchesCanonicalRecords(ctx, 2);
        const auto claims = ctx.Call(ClaimRequest());
        UNIT_ASSERT_VALUES_EQUAL(claims.DelegationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(claims.GetDelegations(0).GetOperationId(), "create-1");
        AssertBinding(claims.GetDelegations(0), oldBinding);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest("second-worker")).DelegationsSize(), 0);
    }

    Y_UNIT_TEST_DURABLE_BOUNDARIES(DropWithUncertainReplacement) {
        TTestContext ctx;
        CreateActive(ctx);
        const auto staged = ctx.Call(StageAlterRequest(GetSecret(ctx), "alter-1", Binding("replacement-referrer")));
        const auto started = ctx.Call(StartRequest(staged.GetDelegation()));
        ctx.Call(SetupResultRequest(started.GetDelegation(), NProto::SETUP_UNKNOWN, "pending-setup"));
        const auto oldSecret = GetSecret(ctx);
        const auto request = DropRequest(oldSecret);
        ctx.CrashCall(request, point);
        if (point == ECrashPoint::BeforeCommit) {
            UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx).SerializeAsString(), oldSecret.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").GetState(), NProto::ACTIVE);
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "alter-1").GetState(), NProto::PENDING);
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        } else {
            const auto secret = GetSecret(ctx);
            UNIT_ASSERT_VALUES_EQUAL(secret.GetState(), NProto::SECRET_DROPPED);
            UNIT_ASSERT(secret.GetCurrentOperationId().empty());
            UNIT_ASSERT(secret.GetPendingOperationId().empty());
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").GetState(), NProto::REVOCATION_PENDING);
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "alter-1").GetState(), NProto::WAITING_SETUP);
        }
        const auto dropped = ctx.Call(request).GetSecret();
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(request).GetSecret().SerializeAsString(), dropped.SerializeAsString());
        AssertInventoryMatchesCanonicalRecords(ctx, 2);
        const auto claims = ctx.Call(ClaimRequest());
        UNIT_ASSERT_VALUES_EQUAL(claims.DelegationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(claims.GetDelegations(0).GetOperationId(), "create-1");
        ctx.Call(FinishRequest(claims.GetDelegations(0)));
        ctx.AdvanceTime(TDuration::Days(1));
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        const auto unknown = GetDelegation(ctx, "alter-1");
        UNIT_ASSERT_VALUES_EQUAL(unknown.GetSetupOperationId(), "pending-setup");
        UNIT_ASSERT_VALUES_EQUAL(unknown.GetState(), NProto::WAITING_SETUP);
        ctx.Call(SetupResultRequest(unknown, NProto::SETUP_SUCCEEDED, "pending-setup"));
        const auto lateClaims = ctx.Call(ClaimRequest());
        UNIT_ASSERT_VALUES_EQUAL(lateClaims.DelegationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(lateClaims.GetDelegations(0).GetOperationId(), "alter-1");
        UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx).GetState(), NProto::SECRET_DROPPED);
    }

    Y_UNIT_TEST_DURABLE_BOUNDARIES(ClaimLease) {
        TTestContext ctx;
        CreateActive(ctx);
        ctx.Call(DropRequest(GetSecret(ctx)));
        const auto queued = GetDelegation(ctx, "create-1");
        const auto generation = ctx.Generation();
        ctx.CrashCall(ClaimRequest("lost-worker"), point);
        const auto restored = GetDelegation(ctx, "create-1");
        if (point == ECrashPoint::BeforeCommit) {
            UNIT_ASSERT_VALUES_EQUAL(restored.SerializeAsString(), queued.SerializeAsString());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(restored.GetState(), NProto::REVOKING);
            UNIT_ASSERT_VALUES_EQUAL(restored.GetClaim().GetGeneration(), generation);
            UNIT_ASSERT_VALUES_EQUAL(restored.GetClaim().GetWorkerId(), "lost-worker");
            UNIT_ASSERT_VALUES_EQUAL(restored.GetClaim().GetAttempt(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest("other-worker")).DelegationsSize(), 0);
            ctx.Call(FinishRequest(restored), NProto::STALE_CLAIM);
            ctx.AdvanceTime(TDuration::Seconds(11));
        }
        const auto reclaimed = ctx.Call(ClaimRequest("new-worker"));
        UNIT_ASSERT_VALUES_EQUAL(reclaimed.DelegationsSize(), 1);
        const auto claim = reclaimed.GetDelegations(0);
        UNIT_ASSERT_VALUES_EQUAL(claim.GetClaim().GetGeneration(), ctx.Generation());
        UNIT_ASSERT(claim.GetClaim().GetAttempt() > restored.GetClaim().GetAttempt());
        AssertBinding(claim, Binding());
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest("competing-worker")).DelegationsSize(), 0);
        ctx.Call(FinishRequest(claim));
        ctx.Reboot();
        AssertInventoryMatchesCanonicalRecords(ctx, 0);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
    }

    Y_UNIT_TEST_DURABLE_BOUNDARIES(FinishSuccess) {
        TTestContext ctx;
        CreateActive(ctx);
        ctx.Call(DropRequest(GetSecret(ctx)));
        const auto claimed = ctx.Call(ClaimRequest()).GetDelegations(0);
        const auto request = FinishRequest(claimed);
        ctx.CrashCall(request, point);
        auto restored = GetDelegation(ctx, "create-1");
        if (point == ECrashPoint::BeforeCommit) {
            UNIT_ASSERT_VALUES_EQUAL(restored.SerializeAsString(), claimed.SerializeAsString());
            AssertInventoryMatchesCanonicalRecords(ctx, 1);
            ctx.Call(request, NProto::STALE_CLAIM);
            ctx.AdvanceTime(TDuration::Seconds(11));
            const auto reclaimed = ctx.Call(ClaimRequest("recovery-worker"));
            UNIT_ASSERT_VALUES_EQUAL(reclaimed.DelegationsSize(), 1);
            restored = ctx.Call(FinishRequest(reclaimed.GetDelegations(0))).GetDelegation();
        } else {
            UNIT_ASSERT_VALUES_EQUAL(restored.GetState(), NProto::REVOKED);
            UNIT_ASSERT_VALUES_EQUAL(restored.GetRevokeOperationId(), "iam-revoke-1");
            UNIT_ASSERT_VALUES_EQUAL(restored.GetDueAtUs(), 0);
            // Generation fencing takes precedence over replay. The committed
            // tombstone still tells an uncertain caller that cleanup succeeded.
            ctx.Call(request, NProto::STALE_CLAIM);
        }
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), restored.SerializeAsString());
        AssertBinding(restored, Binding());
        AssertInventoryMatchesCanonicalRecords(ctx, 0);
        ctx.AdvanceTime(TDuration::Days(1));
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        ctx.Call(StageCreateRequest("reuse-referrer", Binding(), "/Root/database/other"), NProto::CONFLICT);
        const auto replay = ctx.Call(StageCreateRequest()).GetDelegation();
        UNIT_ASSERT_VALUES_EQUAL(replay.GetState(), NProto::REVOKED);
    }

    Y_UNIT_TEST_DURABLE_BOUNDARIES(FinishRetry) {
        TTestContext ctx;
        CreateActive(ctx);
        ctx.Call(DropRequest(GetSecret(ctx)));
        const auto claimed = ctx.Call(ClaimRequest()).GetDelegations(0);
        const auto request = FinishRequest(claimed, NProto::REVOKE_RETRY, TDuration::Seconds(30));
        ctx.CrashCall(request, point);
        const auto restored = GetDelegation(ctx, "create-1");
        if (point == ECrashPoint::BeforeCommit) {
            UNIT_ASSERT_VALUES_EQUAL(restored.SerializeAsString(), claimed.SerializeAsString());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(restored.GetState(), NProto::REVOCATION_PENDING);
            UNIT_ASSERT(restored.GetDueAtUs() > claimed.GetClaim().GetDeadlineUs());
            UNIT_ASSERT_VALUES_EQUAL(restored.GetRevokeOperationId(), "iam-revoke-1");
        }
        ctx.Call(request, NProto::STALE_CLAIM);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest("early-worker")).DelegationsSize(), 0);
        ctx.AdvanceTime(TDuration::Seconds(11));
        if (point != ECrashPoint::BeforeCommit) {
            // An obsolete lease-deadline row would make cleanup claimable here.
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest("still-early-worker")).DelegationsSize(), 0);
            ctx.Reboot();
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), restored.SerializeAsString());
            ctx.AdvanceTime(TDuration::Seconds(20));
        }
        const auto retried = ctx.Call(ClaimRequest("retry-worker"));
        UNIT_ASSERT_VALUES_EQUAL(retried.DelegationsSize(), 1);
        const auto claim = retried.GetDelegations(0);
        UNIT_ASSERT(claim.GetClaim().GetAttempt() > claimed.GetClaim().GetAttempt());
        ctx.Call(FinishRequest(claimed), NProto::STALE_CLAIM);
        ctx.Call(FinishRequest(claim));
        AssertInventoryMatchesCanonicalRecords(ctx, 0);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
    }
}

#undef Y_UNIT_TEST_DURABLE_BOUNDARIES

} // namespace NKikimr::NIamDelegation::NTests
