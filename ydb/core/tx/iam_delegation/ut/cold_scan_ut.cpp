#include "requests.h"

#include <ydb/core/base/localdb.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tx/iam_delegation/schema.h>

#include <util/generic/set.h>
#include <util/string/cast.h>

namespace NKikimr::NIamDelegation::NTests {
namespace {

void EnableSmallCompactions(TTestContext& ctx, std::initializer_list<ui32> tables) {
    auto event = MakeHolder<TEvTablet::TEvLocalSchemeTx>();
    for (const ui32 table : tables) {
        auto* delta = event->Record.MutableSchemeChanges()->AddDelta();
        delta->SetDeltaType(NTabletFlatScheme::TAlterRecord::SetCompactionPolicy);
        delta->SetTableId(table);
        NLocalDb::TCompactionPolicy policy;
        policy.InMemSizeToSnapshot = 16 * 1024;
        policy.InMemStepsToSnapshot = 1;
        policy.InMemForceStepsToSnapshot = 16;
        policy.InMemForceSizeToSnapshot = 32 * 1024;
        policy.MinDataPageSize = 1024;
        policy.Serialize(*delta->MutableCompactionPolicy());
    }
    auto& runtime = ctx.GetRuntime();
    const auto edge = runtime.AllocateEdgeActor();
    runtime.SendToPipe(TTestContext::TabletId, edge, event.Release(), 0, GetPipeConfigWithRetries());
    const auto response = runtime.GrabEdgeEvent<TEvTablet::TEvLocalSchemeTxResponse>(edge);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrProto::OK);
}

NProto::TIamBinding WideBinding(const TString& operation) {
    auto binding = Binding("referrer-" + operation);
    binding.SetCloudId(TString(200, 'c'));
    binding.SetServiceAccountId(TString(200, 'a'));
    binding.SetServiceId(TString(200, 's'));
    binding.SetMicroserviceId(TString(200, 'm'));
    binding.SetResourceType(TString(200, 'r'));
    return binding;
}

class TPageReadEvidence {
public:
    explicit TPageReadEvidence(TTestContext& ctx)
        : Compactions(ctx.GetRuntime().AddObserver<NSharedCache::TEvSaveCompactedPages>([this](auto& event) {
            if (event->Get()->PageCollection->Label().TabletID() == TTestContext::TabletId) {
                ++CompactedCollections;
            }
        }))
        , Reads(ctx.GetRuntime().AddObserver<NSharedCache::TEvRequest>([this](auto& event) {
            if (event->Get()->PageCollection->Label().TabletID() == TTestContext::TabletId
                && event->Cookie == static_cast<ui64>(NSharedCache::ERequestTypeCookie::Transaction)) {
                ++TransactionFetches;
            }
        }))
    {}

    ui64 CompactedCollections = 0;
    ui64 TransactionFetches = 0;

private:
    TTestActorRuntime::TEventObserverHolder Compactions;
    TTestActorRuntime::TEventObserverHolder Reads;
};

} // namespace

Y_UNIT_TEST_SUITE(IamDelegationColdScans) {
    Y_UNIT_TEST(PaginationRetriesColdPagesWithoutDuplicatesOrShortNonfinalPages) {
        TTestContext ctx;
        TPageReadEvidence evidence(ctx);
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        EnableSmallCompactions(ctx, {TSchema::Inventory::TableId});
        constexpr ui32 count = 257;
        TSet<TString> expected;
        for (ui32 index = 0; index < count; ++index) {
            const TString operation = "operation-" + ToString(index);
            ctx.Call(StageCreateRequest(operation, WideBinding(operation),
                "/Root/database/secret-" + ToString(index)));
            expected.insert(operation);
        }
        UNIT_ASSERT_C(evidence.CompactedCollections > 0, "The test must read persisted parts, not only the memtable");
        ctx.Reboot();
        evidence.TransactionFetches = 0;

        auto request = ListRequest("database-incarnation-1", 64);
        TSet<TString> actual;
        ui64 revision = 0;
        ui32 pages = 0;
        ui64 firstPageFetches = 0;
        TString previous;
        while (true) {
            const auto response = ctx.Call(request);
            UNIT_ASSERT(++pages <= 5);
            if (pages == 1) {
                firstPageFetches = evidence.TransactionFetches;
            }
            if (!revision) {
                revision = response.GetDatabase().GetInventoryRevision();
            }
            UNIT_ASSERT_VALUES_EQUAL(response.GetDatabase().GetInventoryRevision(), revision);
            for (const auto& record : response.GetDelegations()) {
                UNIT_ASSERT(actual.insert(record.GetOperationId()).second);
                UNIT_ASSERT(previous.empty() || previous < record.GetOperationId());
                previous = record.GetOperationId();
                AssertBinding(record, WideBinding(record.GetOperationId()));
            }
            if (response.GetNextAfterOperationId().empty()) {
                UNIT_ASSERT_VALUES_EQUAL(response.DelegationsSize(), 1);
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL(response.DelegationsSize(), 64);
            UNIT_ASSERT_VALUES_EQUAL(response.GetNextAfterOperationId(), previous);
            request.MutableListInventory()->SetAfterOperationId(previous);
            request.MutableListInventory()->SetInventoryRevision(revision);
        }
        UNIT_ASSERT_VALUES_EQUAL(pages, 5);
        UNIT_ASSERT(actual == expected);
        UNIT_ASSERT_C(evidence.TransactionFetches > firstPageFetches,
            "The first bounded page must not preload the entire inventory");
        UNIT_ASSERT_C(evidence.TransactionFetches > 1, "Cold transaction page reads must actually exercise Execute retries");
    }

    Y_UNIT_TEST(ClaimBatchRetriesColdReadsWithoutDuplicateClaimsOrPartialWrites) {
        TTestContext ctx;
        TPageReadEvidence evidence(ctx);
        ctx.Call(RegisterRequest(DatabaseIdentity()));
        EnableSmallCompactions(ctx, {TSchema::Delegations::TableId, TSchema::Revocations::TableId});
        constexpr ui32 count = 40;
        TSet<TString> expected;
        for (ui32 index = 0; index < count; ++index) {
            const TString operation = "operation-" + ToString(index);
            const auto staged = ctx.Call(StageCreateRequest(operation, WideBinding(operation),
                "/Root/database/secret-" + ToString(index))).GetDelegation();
            const auto identity = SecretIdentity(index + 1);
            const auto bound = ctx.Call(BindRequest(staged, identity)).GetDelegation();
            const auto ready = CompleteSetup(ctx, bound).GetDelegation();
            ctx.Call(PromoteRequest(ready, GetSecret(ctx, identity).GetRevision()));
            ctx.Call(DropRequest(GetSecret(ctx, identity)));
            expected.insert(operation);
        }
        UNIT_ASSERT(evidence.CompactedCollections > 0);
        ctx.Reboot();
        evidence.TransactionFetches = 0;
        TSet<TString> actual;
        for (ui32 batch = 0; batch < 3; ++batch) {
            const auto response = ctx.Call(ClaimRequest("worker-" + ToString(batch), 16));
            if (batch == 0) {
                UNIT_ASSERT_C(evidence.TransactionFetches > 1, "The first claim must fault before verification reads warm its pages");
            }
            UNIT_ASSERT_VALUES_EQUAL(response.DelegationsSize(), batch == 2 ? 8 : 16);
            for (const auto& record : response.GetDelegations()) {
                UNIT_ASSERT(actual.insert(record.GetOperationId()).second);
                UNIT_ASSERT_VALUES_EQUAL(record.GetState(), NProto::REVOKING);
                UNIT_ASSERT_VALUES_EQUAL(record.GetClaim().GetAttempt(), 1);
                AssertBinding(record, WideBinding(record.GetOperationId()));
                UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, record.GetOperationId()).SerializeAsString(), record.SerializeAsString());
            }
        }
        UNIT_ASSERT(actual == expected);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
        UNIT_ASSERT_C(evidence.TransactionFetches > 1, "Claim reads must actually fault into persisted parts");
        ctx.Reboot();
        for (const auto& operation : expected) {
            const auto record = GetDelegation(ctx, operation);
            UNIT_ASSERT_VALUES_EQUAL(record.GetState(), NProto::REVOKING);
            UNIT_ASSERT_VALUES_EQUAL(record.GetClaim().GetAttempt(), 1);
        }
    }
}

} // namespace NKikimr::NIamDelegation::NTests
