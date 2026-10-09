#include "requests.h"

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>

namespace NKikimr::NIamDelegation::NTests {
namespace {

using TRecords = TMap<TString, NProto::TDelegation>;

void AdvanceTo(TTestContext& ctx, ui64 timeUs) {
    const auto currentUs = ctx.GetRuntime().GetCurrentTime().MicroSeconds();
    UNIT_ASSERT_C(currentUs <= timeUs, "The target boundary must still be in the future");
    ctx.AdvanceTime(TDuration::MicroSeconds(timeUs - currentUs));
    UNIT_ASSERT_VALUES_EQUAL(ctx.GetRuntime().GetCurrentTime().MicroSeconds(), timeUs);
}

NProto::TResponse CallWithoutClockAdvance(TTestContext& ctx, const NProto::TRequest& request,
    NProto::EStatus status = NProto::SUCCESS)
{
    const auto before = ctx.GetRuntime().GetCurrentTime();
    const auto response = ctx.Call(request, status);
    UNIT_ASSERT_VALUES_EQUAL_C(ctx.GetRuntime().GetCurrentTime(), before,
        "Boundary assertion must not cross its deadline during request delivery");
    return response;
}

NProto::TDelegation QueueDefault(TTestContext& ctx) {
    CreateActive(ctx);
    ctx.Call(DropRequest(GetSecret(ctx)));
    return GetDelegation(ctx, "create-1");
}

NProto::TDelegation ClaimWithMaxLease(TTestContext& ctx) {
    auto request = ClaimRequest();
    request.MutableClaimRevocations()->SetLeaseUs(TDuration::Minutes(1).MicroSeconds());
    const auto response = ctx.Call(request);
    UNIT_ASSERT_VALUES_EQUAL(response.DelegationsSize(), 1);
    return response.GetDelegations(0);
}

TRecords AssertBatch(TTestContext& ctx, const NProto::TResponse& response,
    const TRecords& expected, ui64 attempt)
{
    UNIT_ASSERT_VALUES_EQUAL(response.DelegationsSize(), expected.size());
    TRecords claimed;
    for (const auto& record : response.GetDelegations()) {
        const auto it = expected.find(record.GetOperationId());
        UNIT_ASSERT_C(it != expected.end(), record.GetOperationId());
        UNIT_ASSERT_C(claimed.emplace(record.GetOperationId(), record).second, record.GetOperationId());
        UNIT_ASSERT_VALUES_EQUAL(record.GetState(), NProto::REVOKING);
        UNIT_ASSERT_VALUES_EQUAL(record.GetSetupState(), NProto::SETUP_SUCCEEDED);
        UNIT_ASSERT_VALUES_EQUAL(record.GetDatabaseIncarnation(), it->second.GetDatabaseIncarnation());
        UNIT_ASSERT_VALUES_EQUAL(record.GetClaim().GetAttempt(), attempt);
        UNIT_ASSERT_VALUES_EQUAL(record.GetClaim().GetGeneration(), ctx.Generation());
        AssertBinding(record, it->second.GetBinding());
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, record.GetOperationId()).SerializeAsString(),
            record.SerializeAsString());
    }
    return claimed;
}

void FinishBatch(TTestContext& ctx, const TRecords& records) {
    for (const auto& [operation, record] : records) {
        Y_UNUSED(operation);
        ctx.Call(FinishRequest(record));
    }
}

void CrossDatabaseClaimBoundary(ECrashPoint point) {
    TTestContext ctx;
    ctx.Call(RegisterRequest(DatabaseIdentity("database-0", "/Root/db0")));
    ctx.Call(RegisterRequest(DatabaseIdentity("database-1", "/Root/db1")));
    ctx.Call(RegisterRequest(DatabaseIdentity("untouched", "/Root/untouched")));
    TRecords firstFour;
    TRecords remainder;
    for (ui32 index = 0; index < 6; ++index) {
        const TString incarnation = "database-" + ToString(index % 2);
        const TString operation = "operation-" + ToString(index);
        const TString path = "/Root/db" + ToString(index % 2) + "/secret-" + ToString(index);
        const auto identity = SecretIdentity(index + 1, incarnation);
        auto binding = Binding("referrer-" + operation);
        binding.SetCloudId("original-cloud-" + ToString(index));
        binding.SetReferencePolicy(index % 2 ? NProto::WITH_REFERENCES : NProto::WITHOUT_REFERENCES);
        const auto staged = ctx.Call(StageCreateRequest(operation, binding, path, incarnation, index + 1)).GetDelegation();
        const auto bound = ctx.Call(BindRequest(staged, identity)).GetDelegation();
        const auto ready = CompleteSetup(ctx, bound).GetDelegation();
        ctx.Call(PromoteRequest(ready, GetSecret(ctx, identity).GetRevision()));
        ctx.Call(DropRequest(GetSecret(ctx, identity)));
        auto& expected = index < 4 ? firstFour : remainder;
        expected.emplace(operation, GetDelegation(ctx, operation));
        // Make due ordering independent of lexical operation names and dispatch speed.
        ctx.AdvanceTime(TDuration::MicroSeconds(1));
    }
    TMap<TString, ui64> revisions;
    for (const TString incarnation : {"database-0", "database-1", "untouched"}) {
        revisions.emplace(incarnation, ctx.Call(ListRequest(incarnation)).GetDatabase().GetInventoryRevision());
    }
    const auto generation = ctx.Generation();
    auto request = ClaimRequest("interrupted-worker", 4);
    request.MutableClaimRevocations()->SetLeaseUs(TDuration::Minutes(1).MicroSeconds());
    ctx.CrashCall(request, point);

    TRecords abandoned;
    ui64 deadlineUs = 0;
    for (const auto& [operation, before] : firstFour) {
        const auto after = GetDelegation(ctx, operation);
        AssertBinding(after, before.GetBinding());
        if (point == ECrashPoint::BeforeCommit) {
            UNIT_ASSERT_VALUES_EQUAL(after.SerializeAsString(), before.SerializeAsString());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(after.GetState(), NProto::REVOKING);
            UNIT_ASSERT_VALUES_EQUAL(after.GetRevision(), before.GetRevision() + 1);
            UNIT_ASSERT_VALUES_EQUAL(after.GetClaim().GetGeneration(), generation);
            UNIT_ASSERT_VALUES_EQUAL(after.GetClaim().GetAttempt(), 1);
            UNIT_ASSERT_VALUES_EQUAL(after.GetClaim().GetWorkerId(), "interrupted-worker");
            if (!deadlineUs) {
                deadlineUs = after.GetClaim().GetDeadlineUs();
            }
            UNIT_ASSERT_VALUES_EQUAL(after.GetClaim().GetDeadlineUs(), deadlineUs);
            UNIT_ASSERT_VALUES_EQUAL(after.GetDueAtUs(), deadlineUs);
            abandoned.emplace(operation, after);
        }
    }
    for (const auto& [operation, before] : remainder) {
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, operation).SerializeAsString(), before.SerializeAsString());
    }
    for (const auto& [incarnation, before] : revisions) {
        const auto inventory = ctx.Call(ListRequest(incarnation));
        const ui64 increment = incarnation != "untouched" && point != ECrashPoint::BeforeCommit ? 1 : 0;
        UNIT_ASSERT_VALUES_EQUAL(inventory.GetDatabase().GetInventoryRevision(), before + increment);
        UNIT_ASSERT_VALUES_EQUAL(inventory.DelegationsSize(), incarnation == "untouched" ? 0 : 3);
        for (const auto& record : inventory.GetDelegations()) {
            UNIT_ASSERT_VALUES_EQUAL(record.SerializeAsString(), GetDelegation(ctx, record.GetOperationId()).SerializeAsString());
        }
    }

    if (point == ECrashPoint::BeforeCommit) {
        FinishBatch(ctx, AssertBatch(ctx, ctx.Call(ClaimRequest("recovery-worker", 4)), firstFour, 1));
    } else {
        for (const auto& [operation, record] : abandoned) {
            ctx.Call(FinishRequest(record), NProto::STALE_CLAIM);
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, operation).SerializeAsString(), record.SerializeAsString());
        }
    }
    FinishBatch(ctx, AssertBatch(ctx, ctx.Call(ClaimRequest("remainder-worker")), remainder, 1));
    UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
    if (point != ECrashPoint::BeforeCommit) {
        AdvanceTo(ctx, deadlineUs);
        const auto recovered = AssertBatch(ctx, ctx.Call(ClaimRequest("new-generation-worker")), firstFour, 2);
        for (const auto& [operation, old] : abandoned) {
            ctx.Call(FinishRequest(old), NProto::STALE_CLAIM);
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, operation).SerializeAsString(), recovered.at(operation).SerializeAsString());
        }
        FinishBatch(ctx, recovered);
    }
    ctx.Reboot();
    UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
    for (const TString incarnation : {"database-0", "database-1", "untouched"}) {
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest(incarnation)).DelegationsSize(), 0);
    }
    for (const auto* records : {&firstFour, &remainder}) {
        for (const auto& [operation, before] : *records) {
            const auto terminal = GetDelegation(ctx, operation);
            UNIT_ASSERT_VALUES_EQUAL(terminal.GetState(), NProto::REVOKED);
            AssertBinding(terminal, before.GetBinding());
        }
    }
}

void FinishReplay(NProto::ERevokeOutcome outcome) {
    TTestContext ctx;
    QueueDefault(ctx);
    const auto claimed = ClaimWithMaxLease(ctx);
    const auto request = FinishRequest(claimed, outcome,
        outcome == NProto::REVOKE_RETRY ? TDuration::Minutes(5) : TDuration::Zero());
    const auto completed = ctx.Call(request).GetDelegation();
    const auto inventory = ctx.Call(ListRequest());
    ctx.AdvanceTime(TDuration::Seconds(1));
    const auto replay = ctx.Call(request).GetDelegation();
    UNIT_ASSERT_VALUES_EQUAL(replay.SerializeAsString(), completed.SerializeAsString());
    UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).SerializeAsString(), inventory.SerializeAsString());

    auto changed = request;
    changed.MutableFinishRevocation()->SetRevokeOperationId("different-remote-operation");
    ctx.Call(changed, NProto::CONFLICT);
    changed = request;
    changed.MutableFinishRevocation()->SetRetryAfterUs(request.GetFinishRevocation().GetRetryAfterUs() + 1);
    ctx.Call(changed, NProto::CONFLICT);
    changed = request;
    changed.MutableFinishRevocation()->SetOutcome(outcome == NProto::REVOKE_RETRY
        ? NProto::REVOKE_SUCCEEDED : NProto::REVOKE_RETRY);
    ctx.Call(changed, NProto::STALE_CLAIM);
    UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), completed.SerializeAsString());
    UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).SerializeAsString(), inventory.SerializeAsString());

    ctx.Reboot();
    ctx.Call(request, NProto::STALE_CLAIM);
    UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), completed.SerializeAsString());
    if (outcome == NProto::REVOKE_RETRY) {
        AdvanceTo(ctx, completed.GetDueAtUs());
        const auto newer = ClaimWithMaxLease(ctx);
        UNIT_ASSERT_VALUES_EQUAL(newer.GetClaim().GetAttempt(), claimed.GetClaim().GetAttempt() + 1);
        ctx.Call(request, NProto::STALE_CLAIM);
        ctx.Call(FinishRequest(claimed), NProto::STALE_CLAIM);
        auto finish = FinishRequest(newer);
        finish.MutableFinishRevocation()->ClearRevokeOperationId();
        const auto terminal = ctx.Call(finish).GetDelegation();
        UNIT_ASSERT_VALUES_EQUAL(terminal.GetRevokeOperationId(), completed.GetRevokeOperationId());
    }
    UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).DelegationsSize(), 0);
    UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
}

} // namespace

Y_UNIT_TEST_SUITE(IamDelegationRevocationBoundaries) {
    Y_UNIT_TEST(CrossDatabaseClaimBeforeCommit) {
        CrossDatabaseClaimBoundary(ECrashPoint::BeforeCommit);
    }

    Y_UNIT_TEST(CrossDatabaseClaimAfterDurableCommit) {
        CrossDatabaseClaimBoundary(ECrashPoint::AfterDurableCommitBeforeComplete);
    }

    Y_UNIT_TEST(CrossDatabaseClaimLostResponse) {
        CrossDatabaseClaimBoundary(ECrashPoint::ResponseLost);
    }

    Y_UNIT_TEST(FinishSucceedsOneMicrosecondBeforeLeaseDeadline) {
        TTestContext ctx;
        QueueDefault(ctx);
        const auto claim = ClaimWithMaxLease(ctx);
        AdvanceTo(ctx, claim.GetClaim().GetDeadlineUs() - 1);
        UNIT_ASSERT_VALUES_EQUAL(CallWithoutClockAdvance(ctx, ClaimRequest("competitor")).DelegationsSize(), 0);
        const auto terminal = ctx.Call(FinishRequest(claim)).GetDelegation();
        UNIT_ASSERT_VALUES_EQUAL(terminal.GetState(), NProto::REVOKED);
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), terminal.SerializeAsString());
    }

    Y_UNIT_TEST(LeaseExpiresExactlyAtDeadlineAndOldClaimCannotFinishReplacement) {
        TTestContext ctx;
        QueueDefault(ctx);
        const auto claim = ClaimWithMaxLease(ctx);
        AdvanceTo(ctx, claim.GetClaim().GetDeadlineUs() - 1);
        UNIT_ASSERT_VALUES_EQUAL(CallWithoutClockAdvance(ctx, ClaimRequest("competitor")).DelegationsSize(), 0);
        AdvanceTo(ctx, claim.GetClaim().GetDeadlineUs());
        CallWithoutClockAdvance(ctx, FinishRequest(claim), NProto::STALE_CLAIM);
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), claim.SerializeAsString());
        const auto replacement = ClaimWithMaxLease(ctx);
        // The durable lease timestamps the decision; commit acknowledgement
        // may advance the runtime clock after that decision has already passed.
        UNIT_ASSERT_VALUES_EQUAL_C(replacement.GetClaim().GetDeadlineUs(),
            claim.GetClaim().GetDeadlineUs() + TDuration::Minutes(1).MicroSeconds(),
            "The replacement must be claimed exactly at the expired lease deadline");
        UNIT_ASSERT_VALUES_EQUAL(replacement.GetClaim().GetAttempt(), claim.GetClaim().GetAttempt() + 1);
        ctx.Call(FinishRequest(claim, NProto::REVOKE_RETRY, TDuration::Days(1)), NProto::STALE_CLAIM);
        ctx.Call(FinishRequest(claim), NProto::STALE_CLAIM);
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), replacement.SerializeAsString());
        ctx.Call(FinishRequest(replacement));
    }

    Y_UNIT_TEST(RetryBecomesClaimableExactlyAtStoredDueTime) {
        TTestContext ctx;
        QueueDefault(ctx);
        const auto claim = ClaimWithMaxLease(ctx);
        const auto deferred = ctx.Call(FinishRequest(claim, NProto::REVOKE_RETRY, TDuration::Minutes(5))).GetDelegation();
        ctx.Reboot();
        // Complete post-reboot resolver/pipe recovery before positioning the clock
        // one microsecond from the boundary; a connection retry advances time.
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), deferred.SerializeAsString());
        AdvanceTo(ctx, deferred.GetDueAtUs() - 1);
        UNIT_ASSERT_VALUES_EQUAL(CallWithoutClockAdvance(ctx, ClaimRequest()).DelegationsSize(), 0);
        AdvanceTo(ctx, deferred.GetDueAtUs());
        const auto retry = ClaimWithMaxLease(ctx);
        UNIT_ASSERT_VALUES_EQUAL_C(retry.GetClaim().GetDeadlineUs(),
            deferred.GetDueAtUs() + TDuration::Minutes(1).MicroSeconds(),
            "The retry must be claimed exactly at its stored due time");
        UNIT_ASSERT_VALUES_EQUAL(retry.GetClaim().GetAttempt(), claim.GetClaim().GetAttempt() + 1);
        ctx.Call(FinishRequest(claim), NProto::STALE_CLAIM);
        ctx.Call(FinishRequest(retry));
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).DelegationsSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
    }

    Y_UNIT_TEST(EveryClaimTokenFieldIsFencedWithoutMutation) {
        TTestContext ctx;
        QueueDefault(ctx);
        const auto claim = ClaimWithMaxLease(ctx);
        const auto inventory = ctx.Call(ListRequest());
        TVector<NProto::TRequest> invalid;
        auto append = [&] () -> NProto::TClaim* {
            invalid.push_back(FinishRequest(claim));
            return invalid.back().MutableFinishRevocation()->MutableClaim();
        };
        append()->SetGeneration(claim.GetClaim().GetGeneration() + 1);
        append()->SetAttempt(0);
        append()->SetAttempt(claim.GetClaim().GetAttempt() + 1);
        append()->SetWorkerId("another-worker");
        append()->SetDeadlineUs(claim.GetClaim().GetDeadlineUs() + 1);
        append()->Clear();
        for (const auto& request : invalid) {
            ctx.Call(request, NProto::STALE_CLAIM);
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), claim.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).SerializeAsString(), inventory.SerializeAsString());
        }
        ctx.Call(FinishRequest(claim));
    }

    Y_UNIT_TEST(FinishSuccessReplayPreservesTombstoneAndRejectsChangedPayload) {
        FinishReplay(NProto::REVOKE_SUCCEEDED);
    }

    Y_UNIT_TEST(FinishRetryReplayCannotPostponeDueTimeOrOverwriteNewAttempt) {
        FinishReplay(NProto::REVOKE_RETRY);
    }

    Y_UNIT_TEST(InvalidClaimAndFinishBoundsLeaveOutboxUnchanged) {
        TTestContext ctx;
        const auto queued = QueueDefault(ctx);
        const auto before = ctx.Call(ListRequest());
        TVector<NProto::TRequest> invalidClaims;
        auto appendClaim = [&] () -> NProto::TClaimRevocationsRequest* {
            invalidClaims.push_back(ClaimRequest());
            return invalidClaims.back().MutableClaimRevocations();
        };
        appendClaim()->ClearWorkerId();
        appendClaim()->SetWorkerId(TString(257, 'w'));
        appendClaim()->SetLimit(0);
        appendClaim()->SetLimit(101);
        appendClaim()->SetLeaseUs(TDuration::Seconds(1).MicroSeconds() - 1);
        appendClaim()->SetLeaseUs(TDuration::Minutes(1).MicroSeconds() + 1);
        for (const auto& request : invalidClaims) {
            ctx.Call(request, NProto::INVALID_ARGUMENT);
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), queued.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).SerializeAsString(), before.SerializeAsString());
        }
        const auto claim = ClaimWithMaxLease(ctx);
        const auto claimedInventory = ctx.Call(ListRequest());
        TVector<NProto::TRequest> invalidFinishes;
        auto appendFinish = [&] () -> NProto::TFinishRevocationRequest* {
            invalidFinishes.push_back(FinishRequest(claim));
            return invalidFinishes.back().MutableFinishRevocation();
        };
        appendFinish()->ClearOutcome();
        appendFinish()->SetRetryAfterUs(TDuration::Days(1).MicroSeconds() + 1);
        appendFinish()->SetRevokeOperationId(TString(257, 'r'));
        for (const auto& request : invalidFinishes) {
            ctx.Call(request, NProto::INVALID_ARGUMENT);
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), claim.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).SerializeAsString(), claimedInventory.SerializeAsString());
        }
        const auto immediate = ctx.Call(FinishRequest(claim, NProto::REVOKE_RETRY)).GetDelegation();
        UNIT_ASSERT_VALUES_EQUAL(immediate.GetState(), NProto::REVOCATION_PENDING);
        const auto next = ClaimWithMaxLease(ctx);
        UNIT_ASSERT_VALUES_EQUAL(next.GetClaim().GetAttempt(), claim.GetClaim().GetAttempt() + 1);
        ctx.Call(FinishRequest(next));
    }
}

} // namespace NKikimr::NIamDelegation::NTests
