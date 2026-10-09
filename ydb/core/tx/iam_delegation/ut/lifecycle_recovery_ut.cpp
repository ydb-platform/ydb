#include "requests.h"

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>

namespace NKikimr::NIamDelegation::NTests {
namespace {

using TBindings = TMap<TString, NProto::TIamBinding>;

// These are tablet protocol scenarios. Setup outcomes model authoritative IAM
// replies; the production IAM/KQP/CMS integration is outside this fixture.
NProto::TDelegation CreateActiveAt(TTestContext& ctx, const TString& operationId,
    const NProto::TIamBinding& binding, const TString& path,
    const NProto::TSecretIdentity& identity, ui64 createTxId)
{
    const auto staged = ctx.Call(StageCreateRequest(operationId, binding, path,
        identity.GetDatabaseIncarnation(), createTxId)).GetDelegation();
    const auto bound = ctx.Call(BindRequest(staged, identity)).GetDelegation();
    const auto started = ctx.Call(StartRequest(bound)).GetDelegation();
    const auto ready = ctx.Call(SetupResultRequest(started, NProto::SETUP_SUCCEEDED,
        "setup-" + operationId)).GetDelegation();
    return ctx.Call(PromoteRequest(ready, GetSecret(ctx, identity).GetRevision())).GetDelegation();
}

void AssertInventory(TTestContext& ctx, const TString& incarnation, const TBindings& expected) {
    auto request = ListRequest(incarnation, 1);
    TSet<TString> observed;
    ui64 revision = 0;
    TString previous;
    for (size_t page = 0; page <= expected.size(); ++page) {
        const auto response = ctx.Call(request);
        UNIT_ASSERT_VALUES_EQUAL(response.GetDatabase().GetIdentity().GetIncarnation(), incarnation);
        if (!revision) {
            revision = response.GetDatabase().GetInventoryRevision();
        }
        UNIT_ASSERT_VALUES_EQUAL(response.GetDatabase().GetInventoryRevision(), revision);
        UNIT_ASSERT(response.DelegationsSize() <= 1);
        for (const auto& record : response.GetDelegations()) {
            const auto it = expected.find(record.GetOperationId());
            UNIT_ASSERT_C(it != expected.end(), record.GetOperationId());
            UNIT_ASSERT_C(observed.insert(record.GetOperationId()).second, record.GetOperationId());
            UNIT_ASSERT(previous.empty() || previous < record.GetOperationId());
            previous = record.GetOperationId();
            UNIT_ASSERT_VALUES_EQUAL(record.GetDatabaseIncarnation(), incarnation);
            UNIT_ASSERT(record.GetState() != NProto::REVOKED && record.GetState() != NProto::CANCELLED);
            AssertBinding(record, it->second);
            UNIT_ASSERT_VALUES_EQUAL(record.SerializeAsString(),
                GetDelegation(ctx, record.GetOperationId()).SerializeAsString());
        }
        if (response.GetNextAfterOperationId().empty()) {
            UNIT_ASSERT_VALUES_EQUAL(observed.size(), expected.size());
            return;
        }
        UNIT_ASSERT_VALUES_EQUAL(response.DelegationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetNextAfterOperationId(), previous);
        request.MutableListInventory()->SetAfterOperationId(response.GetNextAfterOperationId());
        request.MutableListInventory()->SetInventoryRevision(revision);
    }
    UNIT_FAIL("Inventory failed to terminate within the expected number of pages");
}

void AssertClaimed(const NProto::TResponse& response, const TBindings& expected) {
    UNIT_ASSERT_VALUES_EQUAL(response.DelegationsSize(), expected.size());
    TSet<TString> observed;
    for (const auto& record : response.GetDelegations()) {
        const auto it = expected.find(record.GetOperationId());
        UNIT_ASSERT_C(it != expected.end(), record.GetOperationId());
        UNIT_ASSERT_C(observed.insert(record.GetOperationId()).second, record.GetOperationId());
        UNIT_ASSERT_VALUES_EQUAL(record.GetState(), NProto::REVOKING);
        UNIT_ASSERT_VALUES_EQUAL(record.GetSetupState(), NProto::SETUP_SUCCEEDED);
        AssertBinding(record, it->second);
    }
}

void RunDroppedAlterWithRecreatedName(NProto::ESetupState lateOutcome) {
    TTestContext ctx;
    ctx.Call(RegisterRequest(DatabaseIdentity()));
    const auto oldIdentity = SecretIdentity();
    const auto newIdentity = SecretIdentity(43);
    const auto bindingA = Binding("referrer-a");
    auto bindingB = Binding("referrer-b");
    bindingB.SetCloudId("old-cloud-b");
    bindingB.SetServiceAccountId("old-account-b");
    bindingB.SetServiceId("old-service-b");
    bindingB.SetMicroserviceId("old-microservice-b");
    bindingB.SetResourceType("old-resource-b");
    bindingB.SetReferrerType("old-referrer-type-b");
    bindingB.SetReferencePolicy(NProto::WITH_REFERENCES);
    const auto bindingC = Binding("referrer-c");
    const auto originalRequest = StageCreateRequest("a", bindingA);
    CreateActiveAt(ctx, "a", bindingA, "/Root/database/secret", oldIdentity, 10);
    ctx.Reboot();

    const auto oldActive = GetSecret(ctx, oldIdentity);
    const auto alterRequest = StageAlterRequest(oldActive, "b", bindingB);
    const auto stagedB = ctx.Call(alterRequest).GetDelegation();
    const auto startedB = ctx.Call(StartRequest(stagedB)).GetDelegation();
    const auto unknownB = ctx.Call(SetupResultRequest(startedB,
        NProto::SETUP_UNKNOWN, "setup-b")).GetDelegation();
    const auto stalePromote = PromoteRequest(unknownB, GetSecret(ctx, oldIdentity).GetRevision());
    ctx.Reboot();
    UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx, oldIdentity).GetCurrentOperationId(), "a");
    UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx, oldIdentity).GetPendingOperationId(), "b");

    const auto dropRequest = DropRequest(GetSecret(ctx, oldIdentity));
    ctx.Call(dropRequest);
    ctx.Reboot();
    UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "a").GetState(), NProto::REVOCATION_PENDING);
    UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "b").GetState(), NProto::WAITING_SETUP);
    AssertInventory(ctx, "database-incarnation-1", {{"a", bindingA}, {"b", bindingB}});

    CreateActiveAt(ctx, "c", bindingC, "/Root/database/secret", newIdentity, 20);
    ctx.Reboot();
    const auto recreated = GetSecret(ctx, newIdentity);
    UNIT_ASSERT_VALUES_EQUAL(recreated.GetCurrentOperationId(), "c");
    UNIT_ASSERT_VALUES_EQUAL(recreated.GetState(), NProto::SECRET_LIVE);

    ctx.Call(dropRequest);
    ctx.Call(stalePromote, NProto::PRECONDITION_FAILED);
    ctx.Call(StartRequest(unknownB), NProto::PRECONDITION_FAILED);
    ctx.Call(BindRequest(GetDelegation(ctx, "a"), newIdentity), NProto::CONFLICT);
    UNIT_ASSERT_VALUES_EQUAL(ctx.Call(originalRequest).GetDelegation().GetState(), NProto::REVOCATION_PENDING);
    UNIT_ASSERT_VALUES_EQUAL(ctx.Call(alterRequest).GetDelegation().GetState(), NProto::WAITING_SETUP);
    UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx, newIdentity).SerializeAsString(), recreated.SerializeAsString());

    const auto firstClaims = ctx.Call(ClaimRequest("first-worker"));
    AssertClaimed(firstClaims, {{"a", bindingA}});
    ctx.Call(FinishRequest(firstClaims.GetDelegations(0)));
    ctx.AdvanceTime(TDuration::Days(2));
    ctx.Reboot();
    UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
    AssertInventory(ctx, "database-incarnation-1", {{"b", bindingB}, {"c", bindingC}});

    const auto waitingB = GetDelegation(ctx, "b");
    const auto resultRequest = SetupResultRequest(waitingB, lateOutcome, "setup-b");
    ctx.Call(resultRequest);
    ctx.Reboot();
    ctx.Call(resultRequest);
    ctx.Call(SetupResultRequest(GetDelegation(ctx, "b"),
        lateOutcome == NProto::SETUP_SUCCEEDED ? NProto::SETUP_FAILED : NProto::SETUP_SUCCEEDED,
        "setup-b"), NProto::PRECONDITION_FAILED);

    if (lateOutcome == NProto::SETUP_SUCCEEDED) {
        AssertInventory(ctx, "database-incarnation-1", {{"b", bindingB}, {"c", bindingC}});
        const auto secondClaims = ctx.Call(ClaimRequest("second-worker"));
        AssertClaimed(secondClaims, {{"b", bindingB}});
        ctx.Call(FinishRequest(secondClaims.GetDelegations(0), NProto::REVOKE_RETRY,
            TDuration::Minutes(5)));
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        ctx.AdvanceTime(TDuration::Minutes(6));
        const auto retried = ctx.Call(ClaimRequest("third-worker"));
        AssertClaimed(retried, {{"b", bindingB}});
        ctx.Call(FinishRequest(secondClaims.GetDelegations(0)), NProto::STALE_CLAIM);
        ctx.Call(FinishRequest(retried.GetDelegations(0)));
    } else {
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "b").GetState(), NProto::CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
    }
    ctx.Reboot();
    AssertInventory(ctx, "database-incarnation-1", {{"c", bindingC}});
    AssertBinding(GetDelegation(ctx, "a"), bindingA);
    AssertBinding(GetDelegation(ctx, "b"), bindingB);
    UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "a").GetState(), NProto::REVOKED);
    UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx, oldIdentity).GetState(), NProto::SECRET_DROPPED);
    UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx, newIdentity).SerializeAsString(), recreated.SerializeAsString());
    UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
}

} // namespace

Y_UNIT_TEST_SUITE(TIamDelegationLifecycleRecovery) {
    Y_UNIT_TEST(LateAlterSuccessAfterDropAndNameReuseRevokesOnlyOldBindings) {
        RunDroppedAlterWithRecreatedName(NProto::SETUP_SUCCEEDED);
    }

    Y_UNIT_TEST(LateAlterFailureAfterDropAndNameReusePreservesNewSecret) {
        RunDroppedAlterWithRecreatedName(NProto::SETUP_FAILED);
    }

    Y_UNIT_TEST(EveryCreateBoundarySurvivesRepeatedReboot) {
        TTestContext ctx;
        const auto registered = ctx.Call(RegisterRequest(DatabaseIdentity())).GetDatabase();
        ctx.Reboot();
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(RegisterRequest(DatabaseIdentity())).GetDatabase().SerializeAsString(),
            registered.SerializeAsString());

        const auto staged = ctx.Call(StageCreateRequest()).GetDelegation();
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), staged.SerializeAsString());
        UNIT_ASSERT(!GetDelegation(ctx, "create-1").HasSecret());
        AssertInventory(ctx, "database-incarnation-1", {{"create-1", Binding()}});

        const auto bound = ctx.Call(BindRequest(staged)).GetDelegation();
        const auto boundSecret = GetSecret(ctx);
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), bound.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx).SerializeAsString(), boundSecret.SerializeAsString());

        const auto started = ctx.Call(StartRequest(bound)).GetDelegation();
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), started.SerializeAsString());
        ctx.Call(StartRequest(bound), NProto::PRECONDITION_FAILED);
        ctx.Call(StartRequest(started), NProto::PRECONDITION_FAILED);

        const auto unknown = ctx.Call(SetupResultRequest(started, NProto::SETUP_UNKNOWN,
            "durable-setup-id")).GetDelegation();
        ctx.AdvanceTime(TDuration::Days(30));
        ctx.Reboot();
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), unknown.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);

        const auto succeeded = ctx.Call(SetupResultRequest(unknown, NProto::SETUP_SUCCEEDED, "")).GetDelegation();
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), succeeded.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(succeeded.GetSetupOperationId(), "durable-setup-id");
        UNIT_ASSERT(GetSecret(ctx).GetCurrentOperationId().empty());
        const auto active = ctx.Call(PromoteRequest(succeeded, GetSecret(ctx).GetRevision())).GetDelegation();
        const auto activeSecret = GetSecret(ctx);
        ctx.Reboot();
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), active.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx).SerializeAsString(), activeSecret.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(active.GetState(), NProto::ACTIVE);
        UNIT_ASSERT_VALUES_EQUAL(activeSecret.GetCurrentOperationId(), "create-1");
        UNIT_ASSERT(activeSecret.GetPendingOperationId().empty());
        AssertInventory(ctx, "database-incarnation-1", {{"create-1", Binding()}});
    }

    Y_UNIT_TEST(ReusedDatabasePathAndPathIdNeverMixIncarnations) {
        TTestContext ctx;
        const TString oldIncarnation = "old-database";
        const TString newIncarnation = "new-database";
        const TString path = "/Root/reused";
        ctx.Call(RegisterRequest(DatabaseIdentity(oldIncarnation, path, "same-cloud-database")));
        ctx.Call(RegisterRequest(DatabaseIdentity(newIncarnation, path, "same-cloud-database")));
        const auto oldIdentity = SecretIdentity(42, oldIncarnation);
        const auto newIdentity = SecretIdentity(42, newIncarnation);
        const auto oldBinding = Binding("old-database-referrer");
        const auto newBinding = Binding("new-database-referrer");
        const auto oldActive = CreateActiveAt(ctx, "old-operation", oldBinding,
            path + "/secret", oldIdentity, 1);
        CreateActiveAt(ctx, "new-operation", newBinding, path + "/secret", newIdentity, 1);
        ctx.Reboot();
        const auto newSecret = GetSecret(ctx, newIdentity);
        AssertInventory(ctx, oldIncarnation, {{"old-operation", oldBinding}});
        AssertInventory(ctx, newIncarnation, {{"new-operation", newBinding}});

        const auto dropOld = DropRequest(GetSecret(ctx, oldIdentity));
        ctx.Call(dropOld);
        ctx.Reboot();
        ctx.Call(dropOld);
        ctx.Call(BindRequest(oldActive, newIdentity), NProto::CONFLICT);
        auto wrongAlter = StageAlterRequest(GetSecret(ctx, newIdentity), "wrong-alter", Binding("wrong-referrer"));
        wrongAlter.MutableStage()->SetDatabaseIncarnation(oldIncarnation);
        ctx.Call(wrongAlter, NProto::INVALID_ARGUMENT);
        ctx.Call(StageCreateRequest("reuse-referrer", oldBinding, path + "/other", newIncarnation, 2), NProto::CONFLICT);
        const auto claimed = ctx.Call(ClaimRequest());
        AssertClaimed(claimed, {{"old-operation", oldBinding}});
        ctx.Call(FinishRequest(claimed.GetDelegations(0)));
        ctx.Reboot();
        AssertInventory(ctx, oldIncarnation, {});
        AssertInventory(ctx, newIncarnation, {{"new-operation", newBinding}});
        UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx, newIdentity).SerializeAsString(), newSecret.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "old-operation").GetState(), NProto::REVOKED);
        AssertBinding(GetDelegation(ctx, "old-operation"), oldBinding);
        ctx.Call(ListRequest("unregistered-database"), NProto::NOT_FOUND);
        ctx.Call(StageCreateRequest("reuse-terminal-referrer", oldBinding,
            path + "/other", newIncarnation, 3), NProto::CONFLICT);
    }

    Y_UNIT_TEST(BoundedWorkersRecoverMixedDatabaseBatchesWithoutLosingRetryWork) {
        TTestContext ctx;
        TMap<TString, TBindings> inventories;
        TBindings outstanding;
        TMap<TString, TString> databases;
        for (ui64 database = 0; database != 2; ++database) {
            const TString incarnation = "database-" + ToString(database);
            const TString path = "/Root/db" + ToString(database);
            ctx.Call(RegisterRequest(DatabaseIdentity(incarnation, path, "shared-cloud-alias")));
            for (ui64 secret = 0; secret != 3; ++secret) {
                const TString operation = "operation-" + ToString(secret) + "-" + ToString(database);
                const auto binding = Binding("referrer-" + operation);
                const auto identity = SecretIdentity(secret + 1, incarnation);
                CreateActiveAt(ctx, operation, binding, path + "/secret" + ToString(secret), identity, secret + 1);
                ctx.Call(DropRequest(GetSecret(ctx, identity)));
                inventories[incarnation].emplace(operation, binding);
                outstanding.emplace(operation, binding);
                databases.emplace(operation, incarnation);
            }
        }
        ctx.Reboot();
        for (const auto& [incarnation, expected] : inventories) {
            AssertInventory(ctx, incarnation, expected);
        }

        TVector<NProto::TResponse> batches;
        TSet<TString> claimedIds;
        for (ui64 worker = 0; worker != 3; ++worker) {
            auto request = ClaimRequest("worker-" + ToString(worker), 2);
            request.MutableClaimRevocations()->SetLeaseUs(TDuration::Minutes(1).MicroSeconds());
            batches.push_back(ctx.Call(request));
            UNIT_ASSERT_VALUES_EQUAL(batches.back().DelegationsSize(), 2);
            for (const auto& record : batches.back().GetDelegations()) {
                UNIT_ASSERT_C(claimedIds.insert(record.GetOperationId()).second, record.GetOperationId());
                AssertBinding(record, outstanding.at(record.GetOperationId()));
                UNIT_ASSERT_VALUES_EQUAL(record.GetDatabaseIncarnation(), databases.at(record.GetOperationId()));
                UNIT_ASSERT_VALUES_EQUAL(record.GetClaim().GetWorkerId(), "worker-" + ToString(worker));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(claimedIds.size(), outstanding.size());
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest("fourth-worker")).DelegationsSize(), 0);

        TBindings deferred;
        TBindings abandoned;
        for (const auto& record : batches[0].GetDelegations()) {
            ctx.Call(FinishRequest(record, NProto::REVOKE_RETRY, TDuration::Minutes(5)));
            deferred.emplace(record.GetOperationId(), outstanding.at(record.GetOperationId()));
        }
        for (const auto& record : batches[1].GetDelegations()) {
            ctx.Call(FinishRequest(record));
            inventories.at(record.GetDatabaseIncarnation()).erase(record.GetOperationId());
            outstanding.erase(record.GetOperationId());
        }
        for (const auto& record : batches[2].GetDelegations()) {
            abandoned.emplace(record.GetOperationId(), outstanding.at(record.GetOperationId()));
        }
        ctx.Reboot();
        for (const auto& batch : batches) {
            for (const auto& record : batch.GetDelegations()) {
                ctx.Call(FinishRequest(record), NProto::STALE_CLAIM);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        for (const auto& [incarnation, expected] : inventories) {
            AssertInventory(ctx, incarnation, expected);
        }

        ctx.AdvanceTime(TDuration::Seconds(61));
        const auto recovered = ctx.Call(ClaimRequest("replacement-worker"));
        AssertClaimed(recovered, abandoned);
        for (const auto& record : recovered.GetDelegations()) {
            UNIT_ASSERT(record.GetClaim().GetAttempt() > 1);
            ctx.Call(FinishRequest(record));
            inventories.at(record.GetDatabaseIncarnation()).erase(record.GetOperationId());
        }
        ctx.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        ctx.AdvanceTime(TDuration::Minutes(5));
        const auto due = ctx.Call(ClaimRequest("retry-worker"));
        AssertClaimed(due, deferred);
        for (const auto& record : due.GetDelegations()) {
            UNIT_ASSERT(record.GetClaim().GetAttempt() > 1);
            ctx.Call(FinishRequest(record));
            inventories.at(record.GetDatabaseIncarnation()).erase(record.GetOperationId());
        }
        ctx.Reboot();
        for (const auto& [incarnation, expected] : inventories) {
            UNIT_ASSERT(expected.empty());
            AssertInventory(ctx, incarnation, expected);
        }
        for (const auto& batch : batches) {
            for (const auto& record : batch.GetDelegations()) {
                const auto saved = GetDelegation(ctx, record.GetOperationId());
                UNIT_ASSERT_VALUES_EQUAL(saved.GetState(), NProto::REVOKED);
                AssertBinding(saved, record.GetBinding());
                auto reuse = StageCreateRequest("reuse-" + record.GetOperationId(), record.GetBinding(),
                    "/Root/db0/unused", "database-0", 100);
                ctx.Call(reuse, NProto::CONFLICT);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
    }
}

} // namespace NKikimr::NIamDelegation::NTests
