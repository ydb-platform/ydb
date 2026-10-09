#include "requests.h"

#include <util/folder/tempdir.h>

namespace NKikimr::NIamDelegation::NTests {
namespace {

enum class EStorageOpenMode { Format, Reopen };

NFake::TStorage DiskStorage(const TTempDir& directory, EStorageOpenMode mode) {
    NFake::TStorage storage;
    storage.UseDisk = true;
    storage.FormatDisk = mode == EStorageOpenMode::Format;
    storage.DiskPath = directory.Name() + "/";
    // Exercise the real PDisk/VDisk path, with the same file across runtimes.
    storage.DiskSize = 8ull << 30;
    storage.SectorSize = 4096;
    storage.ChunkSize = 32ull << 20;
    return storage;
}

} // namespace

Y_UNIT_TEST_SUITE(IamDelegationStorageReopen) {
    Y_UNIT_TEST(DatabaseAndUnboundIntentSurviveWholeRuntimeReplacement) {
        TTempDir directory;
        NProto::TResponse inventory;
        NProto::TDelegation intent;
        ui32 generation = 0;
        {
            TTestContext ctx(DiskStorage(directory, EStorageOpenMode::Format));
            ctx.Call(RegisterRequest(DatabaseIdentity()));
            intent = ctx.Call(StageCreateRequest()).GetDelegation();
            inventory = ctx.Call(ListRequest());
            generation = ctx.Generation();
        }
        UNIT_ASSERT((directory.Path() / "pdisk_1.dat").IsFile());
        for (ui32 restart = 0; restart < 3; ++restart) {
            // Nothing from the previous runtime survives except the disk file.
            TTestContext ctx(DiskStorage(directory, EStorageOpenMode::Reopen));
            UNIT_ASSERT(ctx.Generation() > generation);
            generation = ctx.Generation();
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), intent.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).SerializeAsString(), inventory.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(StageCreateRequest()).GetDelegation().SerializeAsString(), intent.SerializeAsString());
            ctx.Call(StageCreateRequest("duplicate-name", Binding("unused-referrer")), NProto::CONFLICT);
            ctx.Call(StageCreateRequest("duplicate-referrer", Binding(), "/Root/database/other"), NProto::CONFLICT);
        }
    }

    Y_UNIT_TEST(MixedLifecycleAndCleanupSurviveWholeRuntimeReplacement) {
        TTempDir directory;
        NProto::TDelegation claimed;
        NProto::TDelegation unknown;
        NProto::TSecretRecord secret;
        NProto::TResponse inventory;
        NProto::TDelegation cancelled;
        TInstant clock;
        ui32 generation = 0;
        {
            TTestContext ctx(DiskStorage(directory, EStorageOpenMode::Format));
            CreateActive(ctx);
            const auto replacement = ctx.Call(StageAlterRequest(GetSecret(ctx), "replacement",
                Binding("replacement-referrer"))).GetDelegation();
            const auto ready = CompleteSetup(ctx, replacement).GetDelegation();
            ctx.Call(PromoteRequest(ready, GetSecret(ctx).GetRevision()));
            const auto claims = ctx.Call(ClaimRequest());
            UNIT_ASSERT_VALUES_EQUAL(claims.DelegationsSize(), 1);
            claimed = claims.GetDelegations(0);
            const auto pending = ctx.Call(StageAlterRequest(GetSecret(ctx), "unknown",
                Binding("unknown-referrer"))).GetDelegation();
            const auto started = ctx.Call(StartRequest(pending)).GetDelegation();
            unknown = ctx.Call(SetupResultRequest(started, NProto::SETUP_UNKNOWN, "uncertain-operation")).GetDelegation();
            const auto unbound = ctx.Call(StageCreateRequest("cancelled", Binding("cancelled-referrer"),
                "/Root/database/cancelled")).GetDelegation();
            cancelled = ctx.Call(DropRequest(unbound)).GetDelegation();
            secret = GetSecret(ctx);
            inventory = ctx.Call(ListRequest());
            UNIT_ASSERT_VALUES_EQUAL(inventory.DelegationsSize(), 3);
            clock = ctx.GetRuntime().GetCurrentTime();
            generation = ctx.Generation();
        }
        {
            TTestContext ctx(DiskStorage(directory, EStorageOpenMode::Reopen));
            ctx.GetRuntime().UpdateCurrentTime(Max(clock, ctx.GetRuntime().GetCurrentTime()));
            UNIT_ASSERT(ctx.Generation() > generation);
            generation = ctx.Generation();
            UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx).SerializeAsString(), secret.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "create-1").SerializeAsString(), claimed.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "unknown").SerializeAsString(), unknown.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "cancelled").SerializeAsString(), cancelled.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).SerializeAsString(), inventory.SerializeAsString());
            ctx.Call(FinishRequest(claimed), NProto::STALE_CLAIM);
            ctx.AdvanceTime(TDuration::Seconds(11));
            const auto claims = ctx.Call(ClaimRequest("recovered-worker"));
            UNIT_ASSERT_VALUES_EQUAL(claims.DelegationsSize(), 1);
            claimed = claims.GetDelegations(0);
            AssertBinding(claimed, Binding());
            ctx.Call(FinishRequest(claimed, NProto::REVOKE_RETRY, TDuration::Seconds(30)));
            ctx.Call(DropRequest(GetSecret(ctx)));
            const auto retiredUnknown = GetDelegation(ctx, "unknown");
            UNIT_ASSERT_VALUES_EQUAL(retiredUnknown.GetState(), NProto::WAITING_SETUP);
            ctx.Call(SetupResultRequest(retiredUnknown, NProto::SETUP_SUCCEEDED, "uncertain-operation"));
            inventory = ctx.Call(ListRequest());
            clock = ctx.GetRuntime().GetCurrentTime();
        }
        {
            TTestContext ctx(DiskStorage(directory, EStorageOpenMode::Reopen));
            ctx.GetRuntime().UpdateCurrentTime(Max(clock, ctx.GetRuntime().GetCurrentTime()));
            UNIT_ASSERT(ctx.Generation() > generation);
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).SerializeAsString(), inventory.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(GetSecret(ctx).GetState(), NProto::SECRET_DROPPED);
            const auto due = ctx.Call(ClaimRequest("third-runtime"));
            UNIT_ASSERT_VALUES_EQUAL(due.DelegationsSize(), 2);
            for (const auto& record : due.GetDelegations()) {
                UNIT_ASSERT(record.GetOperationId() == "replacement" || record.GetOperationId() == "unknown");
                ctx.Call(FinishRequest(record));
            }
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
            ctx.AdvanceTime(TDuration::Seconds(31));
            const auto retry = ctx.Call(ClaimRequest());
            UNIT_ASSERT_VALUES_EQUAL(retry.DelegationsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(retry.GetDelegations(0).GetOperationId(), "create-1");
            ctx.Call(FinishRequest(retry.GetDelegations(0)));
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).DelegationsSize(), 0);
        }
        {
            TTestContext ctx(DiskStorage(directory, EStorageOpenMode::Reopen));
            for (const TString operation : {"create-1", "replacement", "unknown"}) {
                UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, operation).GetState(), NProto::REVOKED);
            }
            UNIT_ASSERT_VALUES_EQUAL(GetDelegation(ctx, "cancelled").GetState(), NProto::CANCELLED);
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ListRequest()).DelegationsSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(ctx.Call(ClaimRequest()).DelegationsSize(), 0);
            ctx.Call(StageCreateRequest("reuse-referrer", Binding(), "/Root/database/new"), NProto::CONFLICT);
        }
    }
}

} // namespace NKikimr::NIamDelegation::NTests
