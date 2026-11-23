#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blob_depot/events.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/testlib/helpers.h>

#ifndef KIKIMR_DISABLE_S3_OPS
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blob_depot/s3_router_events.h>
#include <ydb/core/blob_depot/schema.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/blob_depot/types.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <util/generic/scope.h>
#include <util/string/hex.h>
#include <util/string/printf.h>
#include <ydb/core/wrappers/fake_storage.h>
#endif

#include "blob_depot_test_functions.h"

#include <algorithm>
#include <array>

using namespace NKikimr::NBlobDepot;

namespace {

std::optional<ui64> TryGetBlobDepotTabletId(TEnvironmentSetup& env, ui32 groupId) {
    const NKikimrBlobStorage::TBaseConfig baseConfig = env.FetchBaseConfig();
    for (const auto& group : baseConfig.GetGroup()) {
        if (group.GetGroupId() != groupId) {
            continue;
        }
        const ui64 depotTabletId = group.GetVirtualGroupInfo().GetBlobDepotId();
        if (depotTabletId) {
            return depotTabletId;
        }
    }
    return std::nullopt;
}

void SetAllNodesBlobDepotMaxLoadedTrashRecords(TEnvironmentSetup& env, ui64 limit) {
    for (const ui32 node : env.Runtime->GetNodes()) {
        TControlBoard::SetValue(limit,
            env.Runtime->GetNode(node)->AppData->Icb->BlobDepotControls.MaxLoadedTrashRecords);
    }
}

void RebootBlobDepotTablet(TEnvironmentSetup& env, ui64 blobDepotTabletId) {
    auto& runtime = *env.Runtime;
    const TActorId sender = runtime.AllocateEdgeActor(1);

    auto* poison = new NActors::TEvents::TEvPoison();
    auto* nested = new IEventHandle(TActorId(), sender, poison);
    runtime.Send(new IEventHandle(MakeTabletResolverID(), sender, new TEvTabletResolver::TEvForward(blobDepotTabletId, nested, {},
                 TEvTabletResolver::TEvForward::EActor::Tablet)),
        sender.NodeId());

    {
        auto forwardResult = env.WaitForEdgeActorEvent<TEvTabletResolver::TEvForwardResult>(sender, false);
        UNIT_ASSERT(forwardResult);
        UNIT_ASSERT_VALUES_EQUAL_C(forwardResult->Get()->Status, NKikimrProto::OK, forwardResult->Get()->ToString());
    }

    env.Sim(TDuration::Seconds(5));
    runtime.Send(new IEventHandle(MakeTabletResolverID(), sender, new TEvTabletResolver::TEvTabletProblem(blobDepotTabletId, TActorId())), sender.NodeId());
    env.Sim(TDuration::MilliSeconds(100));
    runtime.DestroyActor(sender);
}

void LoadSeed(ui32& seed) {
    ui32 constantSeed = 0;
    if (TryIntFromString<10, ui32>(GetEnv("MERSENNE_SEED"), constantSeed)) {
        seed = constantSeed;
    } else {
        Seed().LoadOrFail(&seed, sizeof(seed));
    }
}

void CheckRangeWaitsForFinalResolve(bool reverse, bool finalError) {
    ui32 seed;
    LoadSeed(seed);
    TBlobDepotTestEnvironment tenv(seed);
    auto& env = *tenv.Env;
    const TString data = "paginated range body";
    const std::array<TLogoBlobID, 2> ids{
        TLogoBlobID(100500, 1, 1, 0, data.size(), 0),
        TLogoBlobID(100500, 1, 2, 0, data.size(), 0),
    };
    const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    for (const auto& id : ids) {
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, tenv.BlobDepot, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
    }

    TActorId agent;
    std::unique_ptr<IEventHandle> lastPage;
    ui32 lastPageNodeId = 0;
    bool firstReadCompleted = false;
    bool rangeReplied = false;
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (!lastPage && ev->GetTypeRewrite() == TEvBlobDepot::TEvResolveResult::EventType) {
            auto& record = ev->Get<TEvBlobDepot::TEvResolveResult>()->Record;
            if (record.GetStatus() == NKikimrProto::OK && record.ResolvedKeysSize() == 2 &&
                    record.GetResolvedKeys(0).GetKey() == ids[reverse ? 1 : 0].AsBinaryString() &&
                    record.GetResolvedKeys(1).GetKey() == ids[reverse ? 0 : 1].AsBinaryString()) {
                // Split a real tablet response at a smaller page boundary, keeping the stream's request cookie.
                auto final = std::make_unique<TEvBlobDepot::TEvResolveResult>();
                final->Record.CopyFrom(record);
                final->Record.MutableResolvedKeys()->DeleteSubrange(0, 1);
                if (finalError) {
                    final->Record.SetStatus(NKikimrProto::ERROR);
                    final->Record.SetErrorReason("final resolve page failed");
                    final->Record.ClearResolvedKeys();
                }
                agent = ev->Recipient;
                lastPage = std::make_unique<IEventHandle>(ev->Recipient, ev->Sender, final.release(), ev->Flags, ev->Cookie);
                lastPageNodeId = nodeId;
                record.SetStatus(NKikimrProto::OVERRUN);
                record.MutableResolvedKeys()->DeleteSubrange(1, 1);
            }
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->Recipient == agent) {
            firstReadCompleted = true;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvRangeResult && ev->Recipient == edge) {
            rangeReplied = true;
            return false; // the edge actor is not waiting yet; assert the premature completion below
        }
        return true;
    };
    env.Runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, tenv.BlobDepot, new TEvBlobStorage::TEvRange(ids[0].TabletID(),
            ids[reverse ? 1 : 0], ids[reverse ? 0 : 1], false, TInstant::Max(), false));
    });
    for (ui32 i = 0; i < 100 && !firstReadCompleted; ++i) {
        env.Sim(TDuration::MilliSeconds(100));
    }
    UNIT_ASSERT(lastPage);
    UNIT_ASSERT(firstReadCompleted);
    env.Sim(TDuration::MilliSeconds(100)); // process the physical completion and any resulting client reply
    UNIT_ASSERT_C(!rangeReplied, "Range completed before the terminal Resolve page");

    env.Runtime->FilterFunction = {};
    env.Runtime->Send(lastPage.release(), lastPageNodeId);
    auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge, false,
        env.Runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT(result);
    if (finalError) {
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->ErrorReason, "final resolve page failed");
    } else {
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses.size(), ids.size());
        for (size_t i = 0; i < ids.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[i].Id, ids[reverse ? 1 - i : i]);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[i].Buffer, data);
        }
    }
}

enum class EReadFailure { HeldResolve, HeldRead, PhysicalTimeout, ResolveError };

template<typename TRequest, typename TResult>
void CheckReadFailure(EReadFailure failure) {
    const bool holdResolve = failure == EReadFailure::HeldResolve;
    const bool physicalTimeout = failure == EReadFailure::PhysicalTimeout;
    const bool resolveError = failure == EReadFailure::ResolveError;
    ui32 seed;
    LoadSeed(seed);
    TBlobDepotTestEnvironment tenv(seed);
    auto& env = *tenv.Env;
    const TString data = "read with a deadline";
    const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
    const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    env.Runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, tenv.BlobDepot, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
    });
    UNIT_ASSERT_VALUES_EQUAL(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false)->Get()->Status,
        NKikimrProto::OK);

    auto sendRead = [&](TInstant deadline) {
        env.Runtime->WrapInActorContext(edge, [&] {
            TRequest *request;
            if constexpr (std::is_same_v<TRequest, TEvBlobStorage::TEvGet>) {
                request = new TRequest(id, 0, 0, deadline, NKikimrBlobStorage::FastRead);
            } else if constexpr (std::is_same_v<TRequest, TEvBlobStorage::TEvRange>) {
                request = new TRequest(id.TabletID(), id, id, false, deadline, false);
            } else if constexpr (std::is_same_v<TRequest, TEvBlobStorage::TEvDiscover>) {
                request = new TRequest(id.TabletID(), 0, true, false, deadline, 0, false);
            } else {
                request = new TRequest(id, deadline, NKikimrBlobStorage::FastRead);
            }
            SendToBSProxy(edge, tenv.BlobDepot, request);
        });
    };
    TActorId agent;
    std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> held;
    std::optional<TInstant> physicalDeadline;
    bool timedOut = false;
    bool timeoutInjected = false;
    const TInstant deadline = env.Runtime->GetClock() + TDuration::Seconds(2);
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (timedOut) {
            UNIT_ASSERT_C(ev->Recipient != edge || ev->GetTypeRewrite() != TResult::EventType,
                "late dependency response completed the timed-out query again");
            return true;
        }
        bool hold = false;
        if (ev->GetTypeRewrite() == TEvBlobDepot::TEvResolveResult::EventType) {
            auto& record = ev->Get<TEvBlobDepot::TEvResolveResult>()->Record;
            if (record.ResolvedKeysSize() == 1 && record.GetResolvedKeys(0).GetKey() == id.AsBinaryString()) {
                agent = ev->Recipient;
                if (resolveError) {
                    record.SetStatus(NKikimrProto::ERROR);
                    record.SetErrorReason("resolve failed before enumerating keys");
                    record.ClearResolvedKeys();
                } else {
                    hold = holdResolve;
                }
            }
        } else if (agent && ev->Sender == agent && ev->GetTypeRewrite() == TEvBlobStorage::EvGet) {
            physicalDeadline = ev->Get<TEvBlobStorage::TEvGet>()->Deadline;
        } else if (agent && ev->Sender == agent && ev->GetTypeRewrite() == TEvBlobStorage::EvCheckIntegrity) {
            physicalDeadline = ev->Get<TEvBlobStorage::TEvCheckIntegrity>()->Deadline;
        } else if (agent && ev->Recipient == agent &&
                (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult ||
                    ev->GetTypeRewrite() == TEvBlobStorage::EvCheckIntegrityResult)) {
            if (physicalTimeout) {
                UNIT_ASSERT(ev->GetTypeRewrite() == TEvBlobStorage::EvCheckIntegrityResult);
                auto& msg = *ev->Get<TEvBlobStorage::TEvCheckIntegrityResult>();
                msg.Status = NKikimrProto::TIMEOUT;
                msg.ErrorReason = "physical integrity check timed out";
                timeoutInjected = true;
            } else {
                hold = !holdResolve;
            }
        }
        if (hold) {
            held.emplace_back(nodeId, std::move(ev));
            return false;
        }
        return true;
    };
    sendRead(deadline);
    auto result = env.WaitForEdgeActorEvent<TResult>(edge, false, deadline + TDuration::Seconds(2));
    UNIT_ASSERT_C(result, "read did not finish at its caller's deadline");
    if (resolveError) {
        if constexpr (std::is_same_v<TResult, TEvBlobStorage::TEvGetResult>) {
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::ERROR);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
            UNIT_ASSERT_STRING_CONTAINS(result->Get()->ErrorReason, "resolve failed before enumerating keys");
        }
    } else {
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::TIMEOUT, result->Get()->ToString());
        UNIT_ASSERT(physicalTimeout ? timeoutInjected : !held.empty());
    }
    if (!holdResolve && !resolveError) {
        UNIT_ASSERT(physicalDeadline);
        UNIT_ASSERT_VALUES_EQUAL(*physicalDeadline, deadline);
    }
    timedOut = true;
    for (auto& [nodeId, ev] : held) {
        env.Runtime->Send(ev.release(), nodeId);
    }
    env.Sim(TDuration::Seconds(1));

    env.Runtime->FilterFunction = {};
    sendRead(TInstant::Max());
    auto retry = env.WaitForEdgeActorEvent<TResult>(edge, false, env.Runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT(retry);
    UNIT_ASSERT_VALUES_EQUAL_C(retry->Get()->Status, NKikimrProto::OK, retry->Get()->ToString());
    if constexpr (std::is_same_v<TResult, TEvBlobStorage::TEvGetResult>) {
        UNIT_ASSERT_VALUES_EQUAL(retry->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(retry->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(retry->Get()->Responses[0].Buffer.ConvertToString(), data);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(BlobDepot) {
    Y_UNIT_TEST_TWIN(GetDeadline, holdResolve) {
        CheckReadFailure<TEvBlobStorage::TEvGet, TEvBlobStorage::TEvGetResult>(
            holdResolve ? EReadFailure::HeldResolve : EReadFailure::HeldRead);
    }

    Y_UNIT_TEST_TWIN(RangeDeadline, holdResolve) {
        CheckReadFailure<TEvBlobStorage::TEvRange, TEvBlobStorage::TEvRangeResult>(
            holdResolve ? EReadFailure::HeldResolve : EReadFailure::HeldRead);
    }

    Y_UNIT_TEST_TWIN(DiscoverDeadline, holdResolve) {
        CheckReadFailure<TEvBlobStorage::TEvDiscover, TEvBlobStorage::TEvDiscoverResult>(
            holdResolve ? EReadFailure::HeldResolve : EReadFailure::HeldRead);
    }

    Y_UNIT_TEST_TWIN(CheckIntegrityDeadline, holdResolve) {
        CheckReadFailure<TEvBlobStorage::TEvCheckIntegrity, TEvBlobStorage::TEvCheckIntegrityResult>(
            holdResolve ? EReadFailure::HeldResolve : EReadFailure::HeldRead);
    }

    Y_UNIT_TEST(CheckIntegrityPhysicalTimeoutIsNotSuccess) {
        CheckReadFailure<TEvBlobStorage::TEvCheckIntegrity, TEvBlobStorage::TEvCheckIntegrityResult>(EReadFailure::PhysicalTimeout);
    }

    Y_UNIT_TEST(GetResolveFailureIsNotAbsence) {
        CheckReadFailure<TEvBlobStorage::TEvGet, TEvBlobStorage::TEvGetResult>(EReadFailure::ResolveError);
    }

    Y_UNIT_TEST(CheckIntegrityResolveFailureIsNotAbsence) {
        CheckReadFailure<TEvBlobStorage::TEvCheckIntegrity, TEvBlobStorage::TEvCheckIntegrityResult>(EReadFailure::ResolveError);
    }

    Y_UNIT_TEST(RangeWaitsForFinalResolvePage) {
        CheckRangeWaitsForFinalResolve(false, false);
    }

    Y_UNIT_TEST(ReverseRangeWaitsForFinalResolvePage) {
        CheckRangeWaitsForFinalResolve(true, false);
    }

    Y_UNIT_TEST(RangeReportsFinalResolveError) {
        CheckRangeWaitsForFinalResolve(false, true);
    }

    void TestMaxGenerationBlock(bool collectByCompleteDeletionBlock) {
        ui32 seed;
        LoadSeed(seed);
        TFeatureFlags featureFlags;
        featureFlags.SetEnableCollectByCompleteDeletionBlock(collectByCompleteDeletionBlock);
        TBlobDepotTestEnvironment tenv(seed, 1, 8, TBlobStorageGroupType::ErasureMirror3of4, featureFlags);

        auto& env = *tenv.Env;
        const ui32 nodeId = 1;
        const ui32 groupId = tenv.BlobDepot;
        const ui64 tabletId = 100;
        auto sender = env.Runtime->AllocateEdgeActor(nodeId);

        const TString data = tenv.DataGen(100);
        const TLogoBlobID id(tabletId, 1, 1, 0, data.size(), 0);

        auto checkBlob = [&](NKikimrProto::EReplyStatus expected) {
            SendTEvGet(env, sender, groupId, id);
            auto getResult = CaptureTEvGetResult(env, sender, false);
            UNIT_ASSERT_VALUES_EQUAL_C(getResult->Get()->Status, NKikimrProto::OK, getResult->Get()->ToString());
            UNIT_ASSERT_VALUES_EQUAL(getResult->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL_C(getResult->Get()->Responses[0].Status, expected, getResult->Get()->ToString());
        };

        SendTEvPut(env, sender, groupId, id, data);
        auto putResult = CaptureTEvPutResult(env, sender, false);
        UNIT_ASSERT_VALUES_EQUAL_C(putResult->Get()->Status, NKikimrProto::OK, putResult->Get()->ToString());

        checkBlob(NKikimrProto::OK);

        // Hive deletes the tablet for good: with EnableCollectByCompleteDeletionBlock the Max generation
        // block alone has to collect its data, because the hard barrier that normally follows may never be
        // observed -- a VDisk that has seen this block is free to drop the barrier records of this tablet;
        // without it the data stays until that hard barrier
        SendTEvBlock(env, sender, groupId, tabletId, Max<ui32>());
        auto blockResult = CaptureTEvBlockResult(env, sender, false);
        UNIT_ASSERT_VALUES_EQUAL_C(blockResult->Get()->Status, NKikimrProto::OK, blockResult->Get()->ToString());

        env.Sim(TDuration::Seconds(1));

        checkBlob(collectByCompleteDeletionBlock ? NKikimrProto::NODATA : NKikimrProto::OK);

        // With the flag, the hard barrier Hive sends after the block is redundant by now and must not be
        // recorded: Hive retries it, and each retry would otherwise bring back a barrier row we have purged.
        for (int attempt = 0; attempt < 2; ++attempt) {
            SendTEvCollectGarbage(env, sender, groupId, tabletId, Max<ui32>(), Max<ui32>(), id.Channel(),
                true, Max<ui32>(), Max<ui32>(), nullptr, nullptr, false, true);
            auto collectResult = CaptureTEvCollectGarbageResult(env, sender, false);
            UNIT_ASSERT_VALUES_EQUAL_C(collectResult->Get()->Status, NKikimrProto::OK,
                collectResult->Get()->ToString());
        }

        env.Sim(TDuration::Seconds(1));

        checkBlob(NKikimrProto::NODATA);
    }

    Y_UNIT_TEST(BasicPutAndGet) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestBasicPutAndGet(tenv, 1, tenv.RegularGroups[0]);
        TestBasicPutAndGet(tenv, 11, tenv.BlobDepot);
    }

    Y_UNIT_TEST(TestBlockedEvGetRequest) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        constexpr ui32 tabletId = 10;
        constexpr ui32 cookie = 1;
        constexpr ui32 tabletGeneration = 3;

        TBlobInfo blob(tenv.DataGen(100), tabletId, cookie, tabletGeneration);

        auto& env = *tenv.Env;
        constexpr ui32 nodeId = 1;
        auto groupId = tenv.BlobDepot;

        TBSState state;
        state[tabletId];

        // write blob to blob depot
        VerifiedPut(env, nodeId, groupId, blob, state);

        // block all tablet generations <= tabletGeneration
        VerifiedBlock(env, nodeId, groupId, tabletId, tabletGeneration, state);

        // do TEvGet with Reader* params
        auto mustRestoreFirst = false;
        auto isIndexOnly = false;

        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(
            blob.Id,
            0,
            blob.Id.BlobSize(),
            TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead,
            mustRestoreFirst,
            isIndexOnly);
        ev->ReaderTabletData = {tabletId, tabletGeneration};

        auto sender = tenv.Env->Runtime->AllocateEdgeActor(nodeId);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev.release(), cookie);
        });

        auto res = CaptureTEvGetResult(env, sender, true, true);

        // check that TEvGet returns BLOCKED
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::BLOCKED);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::BLOCKED);
    }

    Y_UNIT_TEST(BasicRange) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestBasicRange(tenv, 1, tenv.RegularGroups[0]);
        TestBasicRange(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(BasicDiscover) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestBasicDiscover(tenv, 1000, tenv.RegularGroups[0]);
        TestBasicDiscover(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(BasicBlock) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestBasicBlock(tenv, 15, tenv.RegularGroups[0]);
        TestBasicBlock(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(StorageInfoVersion) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        auto& env = *tenv.Env;
        const TActorId sender = env.Runtime->AllocateEdgeActor(1);
        const ui64 tabletId = 100;
        const ui64 issuerGuid = 1;

        auto block = [&](ui32 generation, ui32 version) {
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, tenv.BlobDepot, new TEvBlobStorage::TEvBlock(tabletId, generation,
                    TInstant::Max(), issuerGuid, TWriteSource::Unknown, version));
            });
            return CaptureTEvBlockResult(env, sender, false);
        };

        auto result = block(10, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        result = block(20, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(result->Get()->IsTabletStorageInfoVersionObsolete);

        result = block(11, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        result = block(10, 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(!result->Get()->IsTabletStorageInfoVersionObsolete);

        // The rejected version bump must not mutate either value.
        result = block(12, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        result = block(13, 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, tenv.BlobDepot, new TEvBlobStorage::TEvBlock(~tabletId, 4, TInstant::Max(),
                TWriteSource::SyncerMergeBlock));
        });
        result = CaptureTEvBlockResult(env, sender, false);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        const std::optional<ui64> blobDepotTabletId = TryGetBlobDepotTabletId(env, tenv.BlobDepot);
        UNIT_ASSERT(blobDepotTabletId);
        RebootBlobDepotTablet(env, *blobDepotTabletId);

        result = block(14, 3);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(result->Get()->IsTabletStorageInfoVersionObsolete);

        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, tenv.BlobDepot, new TEvBlobStorage::TEvBlock(tabletId, 16,
                TInstant::Max(), issuerGuid));
        });
        result = CaptureTEvBlockResult(env, sender, false);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        result = block(17, 3);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(result->Get()->IsTabletStorageInfoVersionObsolete);
    }

    Y_UNIT_TEST(BasicCollectGarbage) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestBasicCollectGarbage(tenv, 15, tenv.RegularGroups[0]);
        TestBasicCollectGarbage(tenv, 100, tenv.BlobDepot);
    }

    Y_UNIT_TEST(CollectGarbageAfterMaxGenerationBlock) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        auto& env = *tenv.Env;
        const ui32 nodeId = 1;
        const ui32 groupId = tenv.BlobDepot;
        const ui64 tabletId = 100;
        const ui32 channel = 0;
        auto sender = env.Runtime->AllocateEdgeActor(nodeId);

        SendTEvBlock(env, sender, groupId, tabletId, Max<ui32>());
        auto blockResult = CaptureTEvBlockResult(env, sender, false);
        UNIT_ASSERT_VALUES_EQUAL(blockResult->Get()->Status, NKikimrProto::OK);

        SendTEvCollectGarbage(env, sender, groupId, tabletId, Max<ui32>(), Max<ui32>(), channel,
            true, Max<ui32>(), Max<ui32>(), nullptr, nullptr, false, true);
        auto collectResult = CaptureTEvCollectGarbageResult(env, sender);
        UNIT_ASSERT_VALUES_EQUAL_C(collectResult->Get()->Status, NKikimrProto::OK, collectResult->Get()->ToString());
    }

    Y_UNIT_TEST(MaxGenerationBlockCollectsBlobs) {
        TestMaxGenerationBlock(true);
    }

    Y_UNIT_TEST(MaxGenerationBlockWaitsForBarrierWhenDisabled) {
        TestMaxGenerationBlock(false);
    }

    Y_UNIT_TEST(TrashBatchReloadAfterRestartWithTinyLimit) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        auto& env = *tenv.Env;
        const ui32 nodeId = 1;
        const ui32 groupId = tenv.BlobDepot;

        std::vector<TBlobInfo> blobs;
        constexpr ui64 tabletId = 100;
        const ui64 tablet2 = tabletId + 1;
        TBSState state;
        state[tabletId];
        state[tablet2];

        for (ui32 i = 0; i < 10; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 1, i + 1, 0));
        }
        for (ui32 i = 10; i < 20; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 1, i + 1, (i % 2)));
        }
        for (ui32 i = 0; i < 10; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 2, i + 1, 0));
        }
        for (ui32 i = 0; i < 10; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 3 + i, 1, 0));
        }
        for (ui32 i = 0; i < 5; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tablet2, 1, 1, 1 + i, 0));
        }
        for (ui32 i = 0; i < 5; ++i) {
            blobs.push_back(TBlobInfo(tenv.DataGen(100), tablet2, 1, 2 + i, 1, 0));
        }

        for (auto& blob : blobs) {
            VerifiedPut(env, nodeId, groupId, blob, state);
        }

        ui32 gen = 2;
        ui32 perGenCtr = 1;

        VerifiedCollectGarbage(env, nodeId, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 2, nullptr, nullptr, false,
            false, blobs, state);

        SetAllNodesBlobDepotMaxLoadedTrashRecords(env, 1);

        const auto blobDepotTabletId = TryGetBlobDepotTabletId(env, groupId);
        UNIT_ASSERT_C(blobDepotTabletId.has_value(), "blob depot virtual group must report BlobDepotId in base config");

        RebootBlobDepotTablet(env, *blobDepotTabletId);

        VerifiedGet(env, nodeId, groupId, blobs[1], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[2], false, false, std::nullopt, state);

        VerifiedGet(env, nodeId, groupId, blobs[20], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[30], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[31], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[40], false, false, std::nullopt, state);

        VerifiedCollectGarbage(env, nodeId, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 1, nullptr, nullptr, false, false, blobs, state);

        {
            TBlobInfo blob(tenv.DataGen(100), tabletId, 99, 1, 1, 0);
            VerifiedPut(env, nodeId, groupId, blob, state);
            blobs.push_back(blob);
        }

        VerifiedCollectGarbage(env, nodeId, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 3, nullptr, nullptr, false, true, blobs, state);

        {
            TBlobInfo blob(tenv.DataGen(100), tabletId, 99, 1, 3, 0);
            VerifiedPut(env, nodeId, groupId, blob, state);
            blobs.push_back(blob);
        }
        VerifiedRange(env, nodeId, groupId, tabletId, blobs[1].Id, blobs[1].Id, false, false, blobs, state);

        VerifiedGet(env, nodeId, groupId, blobs[1], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[2], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[3], false, false, std::nullopt, state);

        VerifiedGet(env, nodeId, groupId, blobs[20], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[30], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[31], false, false, std::nullopt, state);
        VerifiedGet(env, nodeId, groupId, blobs[40], false, false, std::nullopt, state);

        VerifiedCollectGarbage(env, nodeId, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 1, nullptr, nullptr, false, true, blobs, state);
    }

    Y_UNIT_TEST(VerifiedRandom) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        // TestVerifiedRandom(tenv, 8, 15, tenv.RegularGroups[0], 1000);
        TestVerifiedRandom(tenv, 8, 100, tenv.BlobDepot, 1000);
    }

    Y_UNIT_TEST(LoadPutAndRead) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        // TestLoadPutAndGet(tenv, 100, tenv.BlobDepot, 1 << 10, 1 << 15, 500);
        TestLoadPutAndGet(tenv, 100, tenv.BlobDepot, 100, 1 << 10, 500);
    }

    Y_UNIT_TEST(DecommitPutAndRead) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestLoadPutAndGet(tenv, 15, tenv.RegularGroups[0], 100, 1 << 10, 500, true, 10, { 5, 1, 5, 1, 1, 0 });
        // no blob depot restarts performed
    }

    Y_UNIT_TEST(DecommitVerifiedRandom) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);

        TestVerifiedRandom(tenv, 8, 15, tenv.RegularGroups[0], 1000, 500, 10, { 10, 10, 3, 3, 2, 1, 1, 3, 3, 0 });
        // no blob depot restarts performed
    }

    Y_UNIT_TEST(CheckIntegrity) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed, 1, 8, TBlobStorageGroupType::Erasure4Plus2Block);

        TestBasicCheckIntegrity(tenv, 1, tenv.RegularGroups[0]);
        TestBasicCheckIntegrity(tenv, 1, tenv.BlobDepot);
    }

    // The agent does not forward the write, it re-originates it under a blob id of its own, so
    // every field the underlying group needs has to be copied across by hand. Without that a system
    // tablet writing through a virtual group reaches the disks as user data.
    Y_UNIT_TEST(DataKindSurvivesTheAgent) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);
        auto& env = *tenv.Env;

        ui64 tabletId = 100500;
        for (const auto dataKind : {NKikimrBlobStorage::TDataKind::USER, NKikimrBlobStorage::TDataKind::SYSTEM}) {
            const TString data = TStringBuilder() << "data_" << static_cast<int>(dataKind);

            // Both the original write into the virtual group and the one the agent relays into a
            // real group carry this exact buffer, so matching on it picks up both hops.
            std::vector<NKikimrBlobStorage::TDataKind::E> seen;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                    auto *put = ev->Get<TEvBlobStorage::TEvPut>();
                    if (put->Buffer.ConvertToString() == data) {
                        seen.push_back(put->DataKind);
                    }
                }
                return true;
            };

            const TLogoBlobID id(tabletId++, 1, 1, 0, data.size(), 0);
            const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, tenv.BlobDepot, new TEvBlobStorage::TEvPut(TEvBlobStorage::TEvPut::TParameters{
                    .BlobId = id,
                    .Buffer = TRope(data),
                    .Deadline = TInstant::Max(),
                    .DataKind = dataKind,
                }));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

            env.Runtime->FilterFunction = nullptr;

            UNIT_ASSERT_C(seen.size() >= 2, "the agent did not relay the write, nothing was proven");
            for (const auto kind : seen) {
                UNIT_ASSERT_EQUAL(kind, dataKind);
            }
        }
    }

    // A read that asks for MustRestoreFirst turns into a resolve, and in decommission mode the
    // tablet answers it by copying the blob into its own storage. The kind of the read is the only
    // thing that can tell that copy whether it is allowed to happen in a group short of space, so
    // it has to travel with the resolve.
    Y_UNIT_TEST(DataKindReachesTheResolve) {
        ui32 seed;
        LoadSeed(seed);
        TBlobDepotTestEnvironment tenv(seed);
        auto& env = *tenv.Env;

        ui64 tabletId = 100600;
        for (const auto dataKind : {NKikimrBlobStorage::TDataKind::USER, NKikimrBlobStorage::TDataKind::SYSTEM}) {
            const TString data = "hello";
            const TLogoBlobID id(tabletId, 1, 1, 0, data.size(), 0);
            const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, tenv.BlobDepot, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            auto putRes = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(putRes->Get()->Status, NKikimrProto::OK);

            // A range resolve names the tablet it scans, which keeps unrelated resolves -- the agent
            // issues them for its own housekeeping too -- out of the measurement.
            std::vector<NKikimrBlobStorage::TDataKind::E> seen;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobDepot::EvResolve) {
                    const auto& record = ev->Get<TEvBlobDepot::TEvResolve>()->Record;
                    for (const auto& item : record.GetItems()) {
                        if (item.GetTabletId() == tabletId) {
                            seen.push_back(record.GetDataKind());
                        }
                    }
                }
                return true;
            };

            const TLogoBlobID from(tabletId, 0, 0, 0, 0, 0);
            const TLogoBlobID to(tabletId, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
            env.Runtime->WrapInActorContext(sender, [&] {
                auto range = std::make_unique<TEvBlobStorage::TEvRange>(tabletId, from, to,
                    true /*mustRestoreFirst*/, TInstant::Max(), false /*isIndexOnly*/);
                range->DataKind = dataKind;
                SendToBSProxy(sender, tenv.BlobDepot, range.release());
            });
            auto rangeRes = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(rangeRes->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(rangeRes->Get()->Responses.size(), 1);

            env.Runtime->FilterFunction = nullptr;

            UNIT_ASSERT_C(!seen.empty(), "the read did not resolve anything, so it proves nothing");
            for (const auto kind : seen) {
                UNIT_ASSERT_EQUAL(kind, dataKind);
            }

            ++tabletId;
        }
    }

    // the agent of the virtual group with no physical group behind it has nothing to assimilate; it answers with error
    // rather than forwarding the request nowhere
    Y_UNIT_TEST(AssimilateWithoutPhysicalGroup) {
        TBlobDepotTestEnvironment tenv;
        auto& env = *tenv.Env;
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString data = "written through the virtual group";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, tenv.BlobDepot, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        UNIT_ASSERT_VALUES_EQUAL(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false)->Get()->Status,
            NKikimrProto::OK);

        std::optional<NKikimrProto::EReplyStatus> status;
        bool undelivered = false;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() != edge) {
                return true;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilateResult) {
                status = ev->Get<TEvBlobStorage::TEvAssimilateResult>()->Status;
            } else if (ev->GetTypeRewrite() == TEvents::TSystem::Undelivered) {
                undelivered = true;
            }
            return false;
        };
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, tenv.BlobDepot, new TEvBlobStorage::TEvAssimilate(std::nullopt, std::nullopt, std::nullopt,
                true, false));
        });
        env.Sim(TDuration::Minutes(1));
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT(!undelivered);
        UNIT_ASSERT(status);
        UNIT_ASSERT_VALUES_EQUAL(*status, NKikimrProto::ERROR);
    }

    // the put waits for BlobSeqId allocation when the BlobDepot tablet goes away; the allocation is lost along with the
    // connection, so the put ends with error, as the other queries waiting for the tablet do, instead of waiting for
    // the agent to reconnect
    Y_UNIT_TEST(PutWaitingForIdEndsOnDisconnect) {
        TBlobDepotTestEnvironment tenv;
        auto& env = *tenv.Env;
        const std::optional<ui64> tabletId = TryGetBlobDepotTabletId(env, tenv.BlobDepot);
        UNIT_ASSERT(tabletId);

        bool dropAllocations = true;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return !dropAllocations || ev->GetTypeRewrite() != TEvBlobDepot::EvAllocateIdsResult;
        };

        // write until the agent runs out of BlobSeqIds
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString data = "data";
        auto put = [&](ui32 step) {
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, tenv.BlobDepot, new TEvBlobStorage::TEvPut(TLogoBlobID(100500, 1, step, 0,
                    data.size(), 0), data, TInstant::Max()));
            });
        };
        auto waitPut = [&](TDuration timeout) {
            const TInstant deadline = env.Runtime->GetClock() + timeout;
            return env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false, deadline);
        };
        ui32 step = 1;
        for (;; ++step) {
            UNIT_ASSERT_C(step <= 10000, "agent does not run out of BlobSeqIds");
            put(step);
            if (auto res = waitPut(TDuration::Seconds(10))) {
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            } else {
                break; // this put waits for allocation
            }
        }
        Cerr << "*** put of step " << step << " waits for BlobSeqId" << Endl;

        env.Runtime->WrapInActorContext(edge, [&] {
            TActivationContext::Register(CreateTabletKiller(*tabletId));
        });
        auto res = waitPut(TDuration::Minutes(1));
        UNIT_ASSERT_C(res, "put waiting for BlobSeqId got no answer after the tablet disconnected");
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::ERROR, res->Get()->ToString());

        // the agent reconnects and allocates BlobSeqIds again
        dropAllocations = false;
        put(step + 1);
        res = waitPut(TDuration::Minutes(1));
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        env.Runtime->FilterFunction = {};
    }

#ifndef KIKIMR_DISABLE_S3_OPS
    void WaitForS3Condition(TEnvironmentSetup& env, auto predicate, TDuration timeout = TDuration::Seconds(30)) {
        const auto deadline = env.Runtime->GetClock() + timeout;
        env.Runtime->Sim([&] { return !predicate() && env.Runtime->GetClock() < deadline; });
        UNIT_ASSERT_C(predicate() && env.Runtime->GetClock() <= deadline, "timed out waiting for S3 condition");
    }

    ui64 GetUnfinishedPutWatchCount(TEnvironmentSetup& env, ui64 tabletId) {
        const auto sender = env.Runtime->AllocateEdgeActor(1);
        env.Runtime->SendToPipe(tabletId, sender, new TEvTablet::TEvGetCounters, 0,
            TTestActorSystem::GetPipeConfigWithRetries());
        const auto reply = env.WaitForEdgeActorEvent<TEvTablet::TEvGetCountersResponse>(sender);
        for (const auto& counter : reply->Get()->Record.GetTabletCounters().GetAppCounters().GetSimpleCounters()) {
            if (counter.GetName() == "BlobDepot/Cooling/UnfinishedPutWatches") {
                return counter.GetValue();
            }
        }
        UNIT_FAIL("missing BlobDepot/Cooling/UnfinishedPutWatches counter");
        return 0;
    }

    TLogoBlobID CompactBlobDepotTable(TEnvironmentSetup& env, ui64 tabletId, ui32 tableId) {
        auto& runtime = *env.Runtime;
        // The blob-storage fixture normally keeps tablet tables in memory.
        for (const auto node : runtime.GetNodes()) {
            const auto& counters = runtime.GetNode(node)->AppData->Counters;
            runtime.RegisterService(NResourceBroker::MakeResourceBrokerID(),
                runtime.Register(NResourceBroker::CreateResourceBrokerActor(NResourceBroker::MakeDefaultConfig(), counters), node));
            runtime.RegisterService(NSharedCache::MakeSharedPageCacheId(),
                runtime.Register(NSharedCache::CreateSharedPageCache({}, counters), node));
        }
        TLogoBlobID collection;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NSharedCache::TEvAttach::EventType) {
                const auto label = ev->Get<NSharedCache::TEvAttach>()->PageCollection->Label();
                if (label.TabletID() == tabletId) {
                    collection = label;
                }
            }
            return true;
        };
        const auto resolverEdge = runtime.AllocateEdgeActor(1);
        const auto tabletEdge = runtime.AllocateEdgeActor(1);
        auto* request = new NMon::TEvRemoteHttpInfo(TStringBuilder() << "/executorInternals?force_compaction="
            << tableId);
        auto* nested = new IEventHandle(TActorId(), tabletEdge, request);
        runtime.Send(new IEventHandle(MakeTabletResolverID(), resolverEdge, new TEvTabletResolver::TEvForward(tabletId,
            nested, {}, TEvTabletResolver::TEvForward::EActor::Tablet)), resolverEdge.NodeId());
        const auto deadline = runtime.GetClock() + TDuration::Seconds(10);
        const auto forwarded = env.WaitForEdgeActorEvent<TEvTabletResolver::TEvForwardResult>(resolverEdge, true, deadline);
        UNIT_ASSERT(forwarded && forwarded->Get()->Status == NKikimrProto::OK);
        const auto reply = env.WaitForEdgeActorEvent<NMon::TEvRemoteHttpInfoRes>(tabletEdge, true, deadline);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_STRING_CONTAINS(reply->Get()->Html, "Table will be compacted");
        WaitForS3Condition(env, [&] { return bool(collection); });
        runtime.FilterFunction = {};
        return collection;
    }

    bool IsBlobDepotTablePage(IEventHandle& ev, TLogoBlobID collection) {
        if (ev.GetTypeRewrite() == NSharedCache::TEvResult::EventType) {
            const auto& page = *ev.Get<NSharedCache::TEvResult>();
            return page.WaitPad && page.PageCollection->Label() == collection;
        }
        return false;
    }

    void SetDataCoolingEnabled(TTestActorSystem& runtime, bool enabled) {
        for (const auto node : runtime.GetNodes()) {
            runtime.GetNode(node)->AppData->FeatureFlags.SetEnableBlobDepotDataCooling(enabled);
        }
    }

    TEnvironmentSetup::TSettings MakeCoolingTestSettings(bool enabled = true) {
        TFeatureFlags flags;
        flags.SetEnableBlobDepotDataCooling(enabled);
        return {
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
            .SetupHive = true,
            .FeatureFlags = flags,
        };
    }

    NKikimrBlobDepot::TS3BackendSettings MakeS3Settings(const TString& bucket,
            const NKikimrBlobDepot::TS3BackendSettings::TAsyncMode* async = nullptr, const TString& keyPrefix = {}) {
        NKikimrBlobDepot::TS3BackendSettings s3;
        s3.MutableSettings()->SetEndpoint("fake.fake");
        s3.MutableSettings()->SetBucket(bucket);
        s3.MutableSettings()->SetObjectKeyPattern(keyPrefix);
        if (async) {
            s3.MutableAsyncMode()->CopyFrom(*async);
        } else {
            s3.MutableSyncMode();
        }
        return s3;
    }

    struct TS3ReconfigureOptions {
        bool ConfirmRouteChange = false;
        bool ConfirmNamespaceMigration = false;
        bool ConfirmEmpty = false;
        std::optional<TString> MigratedS3Name;
    };

    auto TryReconfigureS3Group(TEnvironmentSetup& env, std::optional<NKikimrBlobDepot::TS3BackendSettings> settings = {},
            const TString& groupName = "vg", const TS3ReconfigureOptions& options = {}) {
        NKikimrBlobStorage::TConfigRequest request;
        auto* change = request.AddCommand()->MutableReconfigureVirtualGroup();
        change->SetName(groupName);
        change->SetConfirmS3RouteChange(options.ConfirmRouteChange);
        change->SetConfirmS3NamespaceMigration(options.ConfirmNamespaceMigration);
        change->SetConfirmS3Empty(options.ConfirmEmpty);
        if (options.MigratedS3Name) {
            change->SetMigratedS3Name(*options.MigratedS3Name);
        }
        if (settings) {
            *change->MutableS3BackendSettings() = std::move(*settings);
        }
        return env.Invoke(request);
    }

    void ReconfigureS3Group(TEnvironmentSetup& env, std::optional<NKikimrBlobDepot::TS3BackendSettings> settings = {},
            const TString& groupName = "vg") {
        const auto response = TryReconfigureS3Group(env, std::move(settings), groupName);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    void AddS3VirtualGroup(TEnvironmentSetup& env, NKikimrBlobStorage::TConfigRequest& request,
            NKikimrBlobDepot::TS3BackendSettings settings) {
        auto *cmd = request.AddCommand()->MutableAllocateVirtualGroup();
        cmd->SetName("vg");
        cmd->SetHiveId(env.Runtime->GetDomainsInfo()->GetHive());
        cmd->SetStoragePoolName("virtual_pool");
        auto *prof = cmd->AddChannelProfiles();
        prof->SetStoragePoolName(env.StoragePoolName);
        prof->SetCount(2);
        prof = cmd->AddChannelProfiles();
        prof->SetStoragePoolName(env.StoragePoolName);
        prof->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
        prof->SetCount(2);
        *cmd->MutableS3BackendSettings() = std::move(settings);
    }

    ui64 WaitForS3VirtualGroup(TEnvironmentSetup& env, ui32 groupId) {
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
        for (;;) {
            if (const auto tabletId = TryGetBlobDepotTabletId(env, groupId)) {
                return *tabletId;
            }
            UNIT_ASSERT_C(env.Runtime->GetClock() < deadline, "BSC did not publish the BlobDepot tablet ID");
            env.Sim(TDuration::MilliSeconds(100));
        }
    }

    ui32 CreateS3VirtualGroup(TEnvironmentSetup& env, NKikimrBlobDepot::TS3BackendSettings settings) {
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(20));
        Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->Clear();

        NKikimrBlobStorage::TConfigRequest request;
        {
            auto *cmd = request.AddCommand()->MutableDefineStoragePool();
            cmd->SetBoxId(1);
            cmd->SetName("virtual_pool");
            cmd->SetErasureSpecies("none");
            cmd->SetVDiskKind("Default");
        }
        AddS3VirtualGroup(env, request, std::move(settings));
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        const ui32 groupId = response.GetStatus(1).GetGroupId(0);
        WaitForS3VirtualGroup(env, groupId);
        return groupId;
    }

    ui32 CreateS3VirtualGroup(TEnvironmentSetup& env, const TString& bucket,
            const NKikimrBlobDepot::TS3BackendSettings::TAsyncMode* async = nullptr, const TString& keyPrefix = {}) {
        return CreateS3VirtualGroup(env, MakeS3Settings(bucket, async, keyPrefix));
    }

    void PutToS3VirtualGroup(TEnvironmentSetup& env, const TActorId& edge, ui32 groupId, const TLogoBlobID& id,
            const TString& data) {
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, deadline));
        });
        auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false, deadline + TDuration::Seconds(1));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::OK, result->Get()->ToString());
    }

    TActorId ConnectToVirtualGroupAgent(TEnvironmentSetup& env, TActorId edge, ui32 groupId) {
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvStatus(deadline));
        });
        const auto reply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(edge, false, deadline);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Status, NKikimrProto::OK, reply->Get()->ToString());
        return reply->Sender;
    }

    TString ReadFromS3VirtualGroup(TEnvironmentSetup& env, const TActorId& edge, ui32 groupId, TLogoBlobID id,
            ui32 shift = 0, ui32 size = 0) {
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(10);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, shift, size, deadline, NKikimrBlobStorage::FastRead));
        });
        auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false, deadline + TDuration::Seconds(1));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::OK, result->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Responses[0].Status, NKikimrProto::OK, result->Get()->ToString());
        return result->Get()->Responses[0].Buffer.ConvertToString();
    }

    NKikimrProto::EReplyStatus CheckS3VirtualGroupIntegrity(TEnvironmentSetup& env, const TActorId& edge, ui32 groupId,
            TLogoBlobID id) {
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(10);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvCheckIntegrity(id, deadline, NKikimrBlobStorage::FastRead));
        });
        auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCheckIntegrityResult>(edge, false,
            deadline + TDuration::Seconds(1));
        UNIT_ASSERT(result);
        return result->Get()->Status;
    }

    NKikimrBlobDepot::TEvResolveResult WaitForPublishedS3Range(TEnvironmentSetup& env, const TActorId& reader,
            ui32 groupId, TLogoBlobID first, TLogoBlobID second) {
        auto& runtime = *env.Runtime;
        const auto firstKey = first.AsBinaryString();
        const auto secondKey = second.AsBinaryString();
        std::optional<NKikimrBlobDepot::TEvResolveResult> packedResolve;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::TEvResolveResult::EventType) {
                const auto& record = ev->Get<TEvBlobDepot::TEvResolveResult>()->Record;
                if (record.GetStatus() == NKikimrProto::OK && record.ResolvedKeysSize() &&
                        record.GetResolvedKeys(0).GetKey() == firstKey && record.GetResolvedKeys().rbegin()->GetKey() == secondKey &&
                        std::ranges::all_of(record.GetResolvedKeys(), [](const auto& key) {
                            return key.ValueChainSize() == 1 && key.GetValueChain(0).HasS3Locator();
                        })) {
                    packedResolve = record;
                }
            }
            return true;
        };
        const auto packDeadline = runtime.GetClock() + TDuration::Seconds(30);
        while (!packedResolve && runtime.GetClock() < packDeadline) {
            runtime.WrapInActorContext(reader, [&] {
                SendToBSProxy(reader, groupId, new TEvBlobStorage::TEvRange(first.TabletID(), first, second,
                    false /* mustRestoreFirst */, packDeadline, true /* indexOnly */));
            });
            const auto reply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(reader, false, packDeadline);
            UNIT_ASSERT(reply);
            UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Status, NKikimrProto::OK, reply->Get()->ToString());
            if (!packedResolve) {
                env.Sim(TDuration::MilliSeconds(10));
            }
        }
        UNIT_ASSERT_C(packedResolve, "the resolved range did not acquire S3 locators");
        runtime.FilterFunction = {};
        return std::move(*packedResolve);
    }

    Y_UNIT_TEST(S3RejectsDuplicateEffectivePrefix) {
        TEnvironmentSetup env(MakeCoolingTestSettings());
        const TString bucket = "duplicate-prefix";
        CreateS3VirtualGroup(env, bucket, nullptr, "archive/team/");

        NKikimrBlobStorage::TConfigRequest allocate;
        AddS3VirtualGroup(env, allocate, MakeS3Settings(bucket, nullptr, "archive"));
        auto* group = allocate.MutableCommand(0)->MutableAllocateVirtualGroup();
        group->SetName("team/vg");
        auto* s3 = group->MutableS3BackendSettings();
        const auto duplicate = env.Invoke(allocate);
        UNIT_ASSERT(!duplicate.GetSuccess());
        UNIT_ASSERT_C(TStringBuf(duplicate.GetErrorDescription()).Contains("effective key prefix"), duplicate.DebugString());

        s3->MutableSettings()->SetObjectKeyPattern("elsewhere/../archive");
        const auto dotSegment = env.Invoke(allocate);
        UNIT_ASSERT(!dotSegment.GetSuccess());
        UNIT_ASSERT_C(TStringBuf(dotSegment.GetErrorDescription()).Contains("dot segments"), dotSegment.DebugString());

        s3->MutableSettings()->SetObjectKeyPattern("elsewhere");
        const auto distinct = env.Invoke(allocate);
        UNIT_ASSERT_C(distinct.GetSuccess(), distinct.GetErrorDescription());

        s3->MutableSettings()->SetObjectKeyPattern("archive");
        const auto collision = TryReconfigureS3Group(env, *s3, "team/vg", {.ConfirmNamespaceMigration = true});
        UNIT_ASSERT(!collision.GetSuccess());
        UNIT_ASSERT_C(TStringBuf(collision.GetErrorDescription()).Contains("effective key prefix"), collision.DebugString());

        s3->MutableSettings()->SetObjectKeyPattern("unclaimed");
        const auto move = TryReconfigureS3Group(env, *s3, "team/vg");
        UNIT_ASSERT(!move.GetSuccess());
        UNIT_ASSERT_C(TStringBuf(move.GetErrorDescription()).Contains("requires migrating"), move.DebugString());
    }

    Y_UNIT_TEST(S3NamespaceClaimsFollowConfigTransaction) {
        TEnvironmentSetup env(MakeCoolingTestSettings());
        CreateS3VirtualGroup(env, "existing");
        const TString bucket = "batch-namespace";
        auto allocate = [&](NKikimrBlobStorage::TConfigRequest& request, TString name, TString prefix) {
            AddS3VirtualGroup(env, request, MakeS3Settings(bucket, nullptr, prefix));
            auto* cmd = request.MutableCommand(request.CommandSize() - 1)->MutableAllocateVirtualGroup();
            cmd->SetName(name);
            return cmd;
        };

        NKikimrBlobStorage::TConfigRequest batch;
        auto* first = allocate(batch, "first", "archive/team/");
        first->ClearS3BackendSettings();
        auto* configure = batch.AddCommand()->MutableReconfigureVirtualGroup();
        configure->SetName("first");
        *configure->MutableS3BackendSettings() = MakeS3Settings(bucket, nullptr, "archive/team/");
        allocate(batch, "team/first", "archive");
        const auto collision = env.Invoke(batch);
        UNIT_ASSERT(!collision.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(collision.GetErrorDescription(), "effective key prefix");

        NKikimrBlobStorage::TConfigRequest retry;
        allocate(retry, "team/first", "archive");
        const auto success = env.Invoke(retry);
        UNIT_ASSERT_C(success.GetSuccess(), success.GetErrorDescription());
        UNIT_ASSERT(!success.GetStatus(0).GetAlready());

        env.RebootTablet(MakeBSControllerID());
        NKikimrBlobStorage::TConfigRequest afterRestart;
        allocate(afterRestart, "first", "archive/team/");
        const auto recovered = env.Invoke(afterRestart);
        UNIT_ASSERT(!recovered.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(recovered.GetErrorDescription(), "effective key prefix");
    }

    Y_UNIT_TEST(S3ValidatesPhysicalKeyLength) {
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        CreateS3VirtualGroup(env, "existing");
        NKikimrBlobStorage::TConfigRequest allocate;
        AddS3VirtualGroup(env, allocate, MakeS3Settings("key-length", nullptr, TString(984, 'p')));
        auto* group = allocate.MutableCommand(0)->MutableAllocateVirtualGroup();
        group->SetName("new");
        const auto rejected = env.Invoke(allocate);
        UNIT_ASSERT(!rejected.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(rejected.GetErrorDescription(), "1024 bytes");

        // A local group also has a stable physical suffix before S3 is first configured.
        group->ClearS3BackendSettings();
        TString storageName;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                const auto& name = event->Get<TEvBlobDepot::TEvApplyConfig>()->Record.GetConfig().GetName();
                if (TStringBuf(name).StartsWith("new/")) {
                    storageName = name;
                }
            }
            return true;
        };
        const auto local = env.Invoke(allocate);
        UNIT_ASSERT_C(local.GetSuccess(), local.GetErrorDescription());
        WaitForS3Condition(env, [&] { return !storageName.empty(); });
        env.Runtime->FilterFunction = {};

        const TS3Locator largest{.Generation = Max<ui32>(), .KeyId = Max<ui64>()};
        const size_t maxPrefix = 1024 - largest.MakeObjectName(storageName).size() - 1;
        auto s3 = MakeS3Settings("key-length", nullptr, TString(maxPrefix + 1, 'p'));
        const auto tooLong = TryReconfigureS3Group(env, s3, "new");
        UNIT_ASSERT(!tooLong.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(tooLong.GetErrorDescription(), "1024 bytes");
        s3.MutableSettings()->SetObjectKeyPattern(TString(maxPrefix, 'p'));
        ReconfigureS3Group(env, s3, "new");
    }

    NKikimrMiniKQL::TResult RunBscLocalQuery(TEnvironmentSetup& env, const TString& program) {
        auto request = std::make_unique<TEvTablet::TEvLocalMKQL>();
        request->Record.MutableProgram()->MutableProgram()->SetText(program);
        const auto sender = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
        env.Runtime->SendToPipe(env.TabletId, sender, request.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
        const auto response = env.WaitForEdgeActorEvent<TEvTablet::TEvLocalMKQLResponse>(sender);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL_C(record.GetStatus(), NKikimrProto::OK, record.GetMiniKQLErrors());
        return record.GetExecutionEngineEvaluatedResponse();
    }

    // Seed a pre-upgrade BSC row: current allocation rules cannot create these legacy namespaces.
    void StoreLegacyS3Config(TEnvironmentSetup& env, NKikimrBlobDepot::TBlobDepotConfig legacy, const TString& groupName) {
        legacy.SetName(groupName);
        legacy.ClearS3NamespacePinned();
        legacy.ClearS3NamespaceValidated();
        legacy.ClearEverAuthorizedDataCooling();
        RunBscLocalQuery(env, Sprintf(R"(
            ((return (AsList
                (UpdateRow 'Group '('('ID (Uint32 '%u)))
                    '('('BlobDepotConfig (String 'x"%s")))))))
        )", legacy.GetVirtualGroupId(), HexEncode(legacy.SerializeAsString()).c_str()));
    }

    Y_UNIT_TEST_TWIN(S3LegacyCredentialRotationPreservesAuthorization, dotSegment) {
        const auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        runtime.GetNode(env.Settings.ControllerNodeId)->AppData->FunctionRegistry = registry.Get();
        THashMap<ui32, NKikimrBlobDepot::TBlobDepotConfig> applied;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                const auto& config = ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.GetConfig();
                applied[config.GetVirtualGroupId()] = config;
            }
            return true;
        };
        const TString bucket = "legacy-rotation";
        const auto syncId = CreateS3VirtualGroup(env, bucket);
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        NKikimrBlobStorage::TConfigRequest allocate;
        AddS3VirtualGroup(env, allocate, MakeS3Settings(bucket, &async, "elsewhere"));
        const TString asyncName = "team/vg";
        allocate.MutableCommand(0)->MutableAllocateVirtualGroup()->SetName(asyncName);
        const auto created = env.Invoke(allocate);
        UNIT_ASSERT_C(created.GetSuccess(), created.GetErrorDescription());
        const auto asyncId = created.GetStatus(0).GetGroupId(0);
        WaitForS3VirtualGroup(env, asyncId);
        auto syncConfig = applied.at(syncId);
        auto asyncConfig = applied.at(asyncId);
        syncConfig.MutableS3BackendSettings()->MutableSettings()->SetObjectKeyPattern(dotSegment ? "archive/../archive" : "archive/team/");
        asyncConfig.MutableS3BackendSettings()->MutableSettings()->SetObjectKeyPattern("archive");
        StoreLegacyS3Config(env, syncConfig, "vg");
        StoreLegacyS3Config(env, asyncConfig, asyncName);
        env.RebootTablet(env.TabletId, TDuration::Seconds(5));

        for (const bool enableCooling : {false, true}) {
            SetDataCoolingEnabled(runtime, enableCooling);
            for (const ui32 groupId : {syncId, asyncId}) {
                const TString name = groupId == syncId ? "vg" : asyncName;
                auto settings = (groupId == syncId ? syncConfig : asyncConfig).GetS3BackendSettings();
                const TString key = enableCooling ? "rotated-again" : "rotated";
                settings.MutableSettings()->SetAccessKey(key);
                settings.MutableSettings()->SetSecretKey(key);
                settings.MutableSettings()->SetScheme(NKikimrSchemeOp::TS3Settings::HTTPS); // same effective route
                ReconfigureS3Group(env, settings, name);
                WaitForS3Condition(env, [&] {
                    return applied.at(groupId).GetS3BackendSettings().GetSettings().GetAccessKey() == key;
                });
                UNIT_ASSERT(!applied.at(groupId).GetS3NamespaceValidated());
                UNIT_ASSERT(!applied.at(groupId).GetEverAuthorizedDataCooling());
            }
        }
        if (!dotSegment) {
            // Removing only the trailing slash would enable the unvalidated synchronous depot's startup scan.
            auto normalized = applied.at(syncId).GetS3BackendSettings();
            normalized.MutableSettings()->SetObjectKeyPattern("archive/team");
            const auto scan = TryReconfigureS3Group(env, normalized);
            UNIT_ASSERT(!scan.GetSuccess());
            UNIT_ASSERT_STRING_CONTAINS(scan.GetErrorDescription(), "effective key prefix");
        }
        const auto activation = TryReconfigureS3Group(env, {}, asyncName);
        UNIT_ASSERT(!activation.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(activation.GetErrorDescription(), dotSegment ? "ambiguous key prefix" : "effective key prefix");

        auto rerouted = applied.at(asyncId).GetS3BackendSettings();
        rerouted.MutableSettings()->SetRegion("another-region");
        const auto redirect = TryReconfigureS3Group(env, rerouted, asyncName, {.ConfirmRouteChange = true});
        UNIT_ASSERT(!redirect.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(redirect.GetErrorDescription(), dotSegment ? "ambiguous key prefix" : "effective key prefix");

        auto migrated = applied.at(syncId).GetS3BackendSettings();
        auto* storage = Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>();
        const TString originalSecret = storage->GetSecretKey();
        Y_DEFER { storage->SetSecretKey(originalSecret); };
        storage->SetSecretKey(migrated.GetSettings().GetSecretKey());
        migrated.MutableSettings()->SetObjectKeyPattern("dedicated");
        const auto fixed = TryReconfigureS3Group(env, migrated, "vg", {.ConfirmNamespaceMigration = true});
        UNIT_ASSERT_C(fixed.GetSuccess(), fixed.GetErrorDescription());
        ReconfigureS3Group(env, {}, asyncName);
        WaitForS3Condition(env, [&] { return applied.at(asyncId).GetEverAuthorizedDataCooling(); });
        runtime.FilterFunction = {};
    }

    Y_UNIT_TEST(S3CorrectsMalformedLegacyNamespace) {
        const auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        runtime.GetNode(env.Settings.ControllerNodeId)->AppData->FunctionRegistry = registry.Get();
        const TString bucket = "correct-legacy-namespace";
        CreateS3VirtualGroup(env, bucket);

        const TString name = "legacy/../vg";
        NKikimrBlobDepot::TBlobDepotConfig applied;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                applied = ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.GetConfig();
            }
            return true;
        };
        NKikimrBlobStorage::TConfigRequest allocate;
        AddS3VirtualGroup(env, allocate, MakeS3Settings(bucket, nullptr, {}));
        auto* group = allocate.MutableCommand(0)->MutableAllocateVirtualGroup();
        group->SetName(name);
        group->ClearS3BackendSettings();
        const auto local = env.Invoke(allocate);
        UNIT_ASSERT_C(local.GetSuccess(), local.GetErrorDescription());
        const auto groupId = local.GetStatus(0).GetGroupId(0);
        WaitForS3VirtualGroup(env, groupId);
        auto legacy = applied;
        *legacy.MutableS3BackendSettings() = MakeS3Settings(bucket);
        StoreLegacyS3Config(env, legacy, name);
        env.RebootTablet(env.TabletId, TDuration::Seconds(5));

        group->SetName("unrelated");
        *group->MutableS3BackendSettings() = MakeS3Settings(bucket, nullptr, "elsewhere");
        const auto blocked = env.Invoke(allocate);
        UNIT_ASSERT(!blocked.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(blocked.GetErrorDescription(), "ambiguous key prefix");

        const auto dest = MakeS3Settings(bucket, nullptr, "clean");
        TS3ReconfigureOptions migration{.ConfirmNamespaceMigration = true, .MigratedS3Name = "corrected"};
        const auto corrected = TryReconfigureS3Group(env, dest, name, migration);
        UNIT_ASSERT_C(corrected.GetSuccess(), corrected.GetErrorDescription());
        WaitForS3Condition(env, [&] { return applied.GetName() == "corrected"; });
        env.RebootTablet(env.TabletId, TDuration::Seconds(5));
        const auto peer = env.Invoke(allocate);
        UNIT_ASSERT_C(peer.GetSuccess(), peer.GetErrorDescription());
        const auto repeat = TryReconfigureS3Group(env, dest, name, migration);
        UNIT_ASSERT_C(repeat.GetSuccess(), repeat.GetErrorDescription());
        runtime.FilterFunction = {};
    }

    Y_UNIT_TEST_TWIN(S3ValidatesLegacyPrefixBeforeCooling, alreadyAsync) {
        const auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        runtime.GetNode(env.Settings.ControllerNodeId)->AppData->FunctionRegistry = registry.Get();
        NKikimrBlobDepot::TBlobDepotConfig legacy;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                legacy = ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.GetConfig();
            }
            return true;
        };
        const TString bucket = "legacy-key-length";
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        CreateS3VirtualGroup(env, bucket, alreadyAsync ? &async : nullptr);
        WaitForS3Condition(env, [&] { return legacy.GetS3BackendSettings().GetSettings().GetBucket() == bucket; });
        runtime.FilterFunction = {};

        // Older controllers admitted this path and used the group name directly on S3.
        legacy.MutableS3BackendSettings()->MutableSettings()->SetObjectKeyPattern(TString(1014, 'p'));
        StoreLegacyS3Config(env, legacy, "vg");

        SetDataCoolingEnabled(runtime, true);
        env.RebootTablet(env.TabletId, TDuration::Seconds(5));
        std::optional<NKikimrBlobDepot::TS3BackendSettings> updated;
        if (!alreadyAsync) {
            updated.emplace(legacy.GetS3BackendSettings()).MutableAsyncMode();
        }
        const auto rejected = TryReconfigureS3Group(env, std::move(updated));
        UNIT_ASSERT(!rejected.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(rejected.GetErrorDescription(), "1024 bytes");
    }

    struct TDelayedCoolingPut {
        TString Key;
        TString Body;
    };

    TDelayedCoolingPut DelayCoolingPut(TEnvironmentSetup& env, const TActorId& edge, ui32 group, TLogoBlobID id,
            const TString& body) {
        using namespace NWrappers::NExternalStorage;
        TDelayedCoolingPut delayed;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPutObjectRequest::EventType) {
                const auto& put = *ev->Get<TEvPutObjectRequest>();
                delayed.Key = put.GetRequest().GetKey();
                delayed.Body = put.Body;
                return false;
            }
            return true;
        };
        PutToS3VirtualGroup(env, edge, group, id, body);
        WaitForS3Condition(env, [&] { return !delayed.Key.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(delayed.Body, body);
        env.Runtime->FilterFunction = {};
        return delayed;
    }

    void CheckReplacementS3ScanAfterRestart(TEnvironmentSetup& env, ui64 tabletId, const TString& bucketName,
            const TString& oldKey, const TString& newKey) {
        using namespace NWrappers::NExternalStorage;
        auto& runtime = *env.Runtime;
        TActorId scanner;
        bool scanned = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvListObjectsRequest::EventType) {
                const auto& list = ev->Get<TEvListObjectsRequest>()->GetRequest();
                if (list.GetBucket() == bucketName) {
                    const auto& awsPrefix = list.GetPrefix();
                    const TStringBuf prefix(awsPrefix.data(), awsPrefix.size());
                    UNIT_ASSERT(newKey.StartsWith(prefix));
                    UNIT_ASSERT(!oldKey.StartsWith(prefix));
                    scanner = ev->Sender;
                }
            } else if (scanner && ev->GetRecipientRewrite() == scanner && ev->GetTypeRewrite() == TEvListObjectsResponse::EventType) {
                const auto& result = *ev->Get<TEvListObjectsResponse>();
                scanned |= result.IsSuccess() && !result.Result.GetResult().GetIsTruncated();
            }
            return true;
        };
        RebootBlobDepotTablet(env, tabletId);
        WaitForS3Condition(env, [&] { return scanned; });
        runtime.FilterFunction = {};
    }

    Y_UNIT_TEST(AsyncS3IsolatesLatePutFromRetiredGroup) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings());
        auto& runtime = *env.Runtime;
        const TString bucketName = "retired-s3-writer";
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        async.SetUploadPutsInFlight(2);
        const ui32 retired = CreateS3VirtualGroup(env, bucketName, &async, "archive");
        const auto retiredTabletId = TryGetBlobDepotTabletId(env, retired);
        UNIT_ASSERT(retiredTabletId);
        const auto writer = runtime.AllocateEdgeActor(1);
        const auto reader = runtime.AllocateEdgeActor(2);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        auto& bucket = Singleton<TFakeExternalStorage>()->MutableBucket(bucketName);
        const auto [oldKey, oldBody] = DelayCoolingPut(env, writer, retired, id, "old-data");
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 0);

        const TLogoBlobID other(100500, 1, 2, 0, 8, 0);
        PutToS3VirtualGroup(env, writer, retired, other, "old-pack");
        const auto oldMapping = WaitForPublishedS3Range(env, reader, retired, other, other);
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 1);
        const TString published = bucket.GetObjectKeys().front();
        std::optional<TInstant> firstSweep;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (!firstSweep && ev->GetTypeRewrite() == TEvBlobDepot::EvRetiredS3SweepComplete &&
                    ev->Get<TEvBlobDepot::TEvRetiredS3SweepComplete>()->Success) {
                firstSweep = runtime.GetClock();
            }
            return true;
        };
        NKikimrBlobStorage::TConfigRequest replace;
        replace.AddCommand()->MutableDeleteSpecificGroups()->AddGroupIds(retired);
        AddS3VirtualGroup(env, replace, MakeS3Settings(bucketName, &async, "archive"));
        // An explicitly attached tablet can request the exact physical namespace of the removed group.
        const auto oldLocator = TS3Locator::FromProto(oldMapping.GetResolvedKeys(0).GetValueChain(0).GetS3Locator());
        TStringBuf storageName(published);
        UNIT_ASSERT(storageName.SkipPrefix("archive/") && storageName.ChopSuffix("/" + oldLocator.MakeObjectName({})));
        auto claimRetired = replace;
        auto* claimant = claimRetired.MutableCommand(1)->MutableAllocateVirtualGroup();
        claimant->SetName(TString(storageName));
        claimant->SetBlobDepotId(*retiredTabletId);
        const auto rejected = env.Invoke(claimRetired);
        UNIT_ASSERT(!rejected.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(rejected.GetErrorDescription(), "reserved for retired BlobDepot cleanup");
        const auto response = env.Invoke(replace);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        const ui32 replacement = response.GetStatus(1).GetGroupId(0);
        UNIT_ASSERT_UNEQUAL(replacement, retired);
        const ui64 tabletId = WaitForS3VirtualGroup(env, replacement);
        UNIT_ASSERT_UNEQUAL(tabletId, *retiredTabletId);
        WaitForS3Condition(env, [&] { return firstSweep && !bucket.GetObject(published); });
        runtime.FilterFunction = {};
        PutToS3VirtualGroup(env, writer, replacement, id, "new-data");
        const auto resolved = WaitForPublishedS3Range(env, reader, replacement, id, id);
        const auto locator = TS3Locator::FromProto(resolved.GetResolvedKeys(0).GetValueChain(0).GetS3Locator());
        const TString suffix = "/" + locator.MakeObjectName({});
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 1);
        const TString newKey = bucket.GetObjectKeys().front();
        UNIT_ASSERT_C(oldKey.EndsWith(suffix) && newKey.EndsWith(suffix), oldKey << " vs " << newKey);
        UNIT_ASSERT_UNEQUAL(oldKey, newKey);
        // The retired tablet's already-issued PUT reaches storage after the replacement publishes.
        bucket.PutObject(oldKey, oldBody);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, reader, replacement, id), "new-data");

        CheckReplacementS3ScanAfterRestart(env, tabletId, bucketName, oldKey, newKey);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, runtime.AllocateEdgeActor(3), replacement, id), "new-data");
        UNIT_ASSERT(bucket.GetObject(oldKey));

        // Retirement outlives the group and its Hive deletion. The reloaded BSC must
        // discover a PUT that landed after the initial sweep without touching the replacement.
        const TString nestedKey = oldKey.substr(0, oldKey.size() - suffix.size()) + "/nested" + suffix;
        bucket.PutObject(nestedKey, "nested depot");
        std::optional<TInstant> nextSweep;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (!nextSweep && ev->GetTypeRewrite() == TEvListObjectsRequest::EventType) {
                const auto& list = ev->Get<TEvListObjectsRequest>()->GetRequest();
                const auto& prefix = list.GetPrefix();
                if (list.GetBucket() == bucketName && oldKey.StartsWith(TStringBuf(prefix.data(), prefix.size()))) {
                    nextSweep = runtime.GetClock();
                }
            }
            return true;
        };
        env.RebootTablet(MakeBSControllerID());
        WaitForS3Condition(env, [&] { return !bucket.GetObject(oldKey); }, TDuration::Minutes(3));
        UNIT_ASSERT(nextSweep && *nextSweep >= *firstSweep + TDuration::Minutes(2));
        runtime.FilterFunction = {};
        claimRetired.MutableCommand()->DeleteSubrange(0, 1);
        const auto stillReserved = env.Invoke(claimRetired);
        UNIT_ASSERT(!stillReserved.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(stillReserved.GetErrorDescription(), "reserved for retired BlobDepot cleanup");
        UNIT_ASSERT(bucket.GetObject(nestedKey));
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, reader, replacement, id), "new-data");
    }

    NKikimrBlobDepot::TBlobDepotConfig ReadRetiredS3Config(TEnvironmentSetup& env, ui32 group) {
        const auto result = RunBscLocalQuery(env, Sprintf(R"(
            ((let key '('('GroupId (Uint32 '%u))))
             (return (AsList (SetResult 'row (SelectRow 'RetiredS3Namespaces key '('S3Config))))))
        )", group));
        const auto& row = result.GetValue().GetStruct(0).GetOptional().GetOptional();
        NKikimrBlobDepot::TBlobDepotConfig config;
        UNIT_ASSERT(row.StructSize() == 1 && config.ParseFromString(row.GetStruct(0).GetOptional().GetBytes()));
        return config;
    }

    Y_UNIT_TEST(S3RetirementDiscardsCredentialsAfterFinalSweep) {
        using namespace NWrappers::NExternalStorage;
        const auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        TEnvironmentSetup env(MakeCoolingTestSettings());
        auto& runtime = *env.Runtime;
        runtime.GetNode(env.Settings.ControllerNodeId)->AppData->FunctionRegistry = registry.Get();
        const TString bucketName = "retirement-final-sweep";
        auto* storage = Singleton<TFakeExternalStorage>();
        const TString originalSecret = storage->GetSecretKey();
        Y_DEFER { storage->SetSecretKey(originalSecret); };
        auto settings = MakeS3Settings(bucketName);
        settings.MutableSettings()->SetAccessKey("test-key");
        settings.MutableSettings()->SetSecretKey("test-secret");
        storage->SetSecretKey(settings.GetSettings().GetSecretKey());
        const ui32 group = CreateS3VirtualGroup(env, std::move(settings));
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, runtime.AllocateEdgeActor(1), group, id, "old-data");
        auto& bucket = Singleton<TFakeExternalStorage>()->MutableBucket(bucketName);
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 1);
        const TString key = bucket.GetObjectKeys().front();

        size_t successful = 0;
        bool failed = false;
        bool dropped = false;
        TActorId scheduler;
        TInstant secondSweep;
        TInstant finalDue;
        runtime.FilterEnqueue = [&](ui32, std::unique_ptr<IEventHandle>& ev, ISchedulerCookie*, TInstant when) {
            if (scheduler && ev->Recipient == scheduler && ev->GetTypeRewrite() == TEvents::TSystem::Wakeup &&
                    secondSweep && when >= secondSweep + TDuration::Days(1)) {
                finalDue = when;
            }
            return true;
        };
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvRetiredS3SweepComplete) {
                scheduler = ev->Recipient;
                if (ev->Get<TEvBlobDepot::TEvRetiredS3SweepComplete>()->Success) {
                    ++successful;
                    if (successful == 2) {
                        secondSweep = runtime.GetClock();
                    }
                } else {
                    failed = true;
                }
            } else if (successful == 2 && !dropped && ev->GetTypeRewrite() == TEvListObjectsRequest::EventType &&
                    ev->Get<TEvListObjectsRequest>()->GetRequest().GetBucket() == bucketName) {
                dropped = true;
                return false;
            }
            return true;
        };
        NKikimrBlobStorage::TConfigRequest retire;
        retire.AddCommand()->MutableDeleteSpecificGroups()->AddGroupIds(group);
        UNIT_ASSERT(env.Invoke(retire).GetSuccess());
        WaitForS3Condition(env, [&] { return bool(finalDue); }, TDuration::Minutes(3));
        runtime.FilterEnqueue = {};
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 0);
        bucket.PutObject(key, "late-put");
        // Model a BSC restart after the durable timer has elapsed, without running a day
        // of unrelated simulated actors. Keep the recorded cleanup phase and credentials.
        RunBscLocalQuery(env, Sprintf(R"(
            ((return (AsList (UpdateRow 'RetiredS3Namespaces '('('GroupId (Uint32 '%u)))
                '('('NextS3Sweep (Uint64 '1)))))))
        )", group));
        env.RebootTablet(MakeBSControllerID());
        WaitForS3Condition(env, [&] { return failed; }, TDuration::Minutes(3));
        UNIT_ASSERT(dropped);
        UNIT_ASSERT(bucket.GetObject(key));
        const auto active = ReadRetiredS3Config(env, group);
        UNIT_ASSERT_VALUES_EQUAL(active.GetS3BackendSettings().GetSettings().GetSecretKey(), "test-secret");
        UNIT_ASSERT(key.StartsWith(active.GetName() + "/"));
        WaitForS3Condition(env, [&] { return successful == 3 && !runtime.GetActor(scheduler); }, TDuration::Minutes(2));
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 0);
        runtime.FilterFunction = {};

        env.RebootTablet(MakeBSControllerID());
        const auto reservation = ReadRetiredS3Config(env, group);
        NKikimrBlobDepot::TBlobDepotConfig expected;
        expected.SetName(active.GetName());
        expected.MutableS3BackendSettings()->MutableSettings()->SetBucket(bucketName);
        UNIT_ASSERT_VALUES_EQUAL(reservation.SerializeAsString(), expected.SerializeAsString());
    }

    Y_UNIT_TEST(S3RetirementHonorsCoolingRollout) {
        using namespace NWrappers::NExternalStorage;
        for (bool previouslyAuthorized : {false, true}) {
            TEnvironmentSetup env(MakeCoolingTestSettings(previouslyAuthorized));
            auto& runtime = *env.Runtime;
            const TString bucketName = "retired-s3-rollout";
            NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
            async.SetTargetObjectBytes(8);
            const ui32 group = CreateS3VirtualGroup(env, bucketName, previouslyAuthorized ? &async : nullptr);
            const auto edge = runtime.AllocateEdgeActor(1);
            const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
            PutToS3VirtualGroup(env, edge, group, id, "old-data");
            const auto& bucket = Singleton<TFakeExternalStorage>()->GetBucket(bucketName);
            WaitForS3Condition(env, [&] { return bucket.GetSize() == 1; });
            if (previouslyAuthorized) {
                WaitForPublishedS3Range(env, edge, group, id, id);
                ReconfigureS3Group(env, MakeS3Settings(bucketName));
            }
            SetDataCoolingEnabled(runtime, false);
            NKikimrBlobStorage::TConfigRequest retire;
            retire.AddCommand()->MutableDeleteSpecificGroups()->AddGroupIds(group);
            const auto response = env.Invoke(retire);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            env.RebootTablet(MakeBSControllerID());
            if (!previouslyAuthorized) {
                // Wait for BSC recovery before reopening the rollout window.
                UNIT_ASSERT(!TryGetBlobDepotTabletId(env, group));
                UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 1);
                SetDataCoolingEnabled(runtime, true);
                env.RebootTablet(MakeBSControllerID());
            }
            WaitForS3Condition(env, [&] { return bucket.GetSize() == 0; });
        }
    }

    Y_UNIT_TEST(S3ReadsExistingGroupNamespaceAfterRestart) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        // Model the configuration persisted by BSC before it assigned per-allocation namespaces.
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.MutableConfig()->SetName("vg");
            }
            return true;
        };
        const TString bucketName = "existing-s3-writer";
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, nullptr, "archive");
        runtime.FilterFunction = {};
        const auto tabletId = TryGetBlobDepotTabletId(env, groupId);
        UNIT_ASSERT(tabletId);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, runtime.AllocateEdgeActor(1), groupId, id, "original");
        const auto& bucket = Singleton<TFakeExternalStorage>()->GetBucket(bucketName);
        const auto keys = bucket.GetObjectKeys();
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        TString error;
        TStringBuf key = keys.front();
        UNIT_ASSERT(key.SkipPrefix("archive/vg/"));
        UNIT_ASSERT_C(TS3Locator::FromObjectName(TString(key), 8, &error), error);
        RebootBlobDepotTablet(env, *tabletId);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, runtime.AllocateEdgeActor(2), groupId, id), "original");
    }

    Y_UNIT_TEST(AsyncS3RequiresBSCNamespaceValidation) {
        using namespace NWrappers::NExternalStorage;

        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        const TString bucketName = "async-legacy-namespace";
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        async.SetMaxPendingBytes(MaxWriteCreditBytes);
        bool appliedUnvalidatedConfig = false;
        bool scanStarted = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                const auto& config = ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.GetConfig();
                if (config.GetS3BackendSettings().HasAsyncMode()) {
                    UNIT_ASSERT(!config.GetS3NamespaceValidated());
                    appliedUnvalidatedConfig = true;
                }
            } else if (ev->GetTypeRewrite() == TEvListObjectsRequest::EventType) {
                scanStarted = true;
            }
            return true;
        };
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, &async);
        UNIT_ASSERT(appliedUnvalidatedConfig);
        const auto tabletId = TryGetBlobDepotTabletId(env, groupId);
        UNIT_ASSERT(tabletId);
        const auto& bucket = Singleton<TFakeExternalStorage>()->GetBucket(bucketName);
        const auto edge = runtime.AllocateEdgeActor(1);
        const TLogoBlobID first(100500, 1, 1, 0, 8, 0);
        const TLogoBlobID second(100500, 1, 2, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, groupId, first, "abcdefgh");
        const auto collection = CompactBlobDepotTable(env, *tabletId, NBlobDepot::Schema::Data::TableId);
        // Keep the other row in the log so a point operation can recover it independently.
        PutToS3VirtualGroup(env, edge, groupId, second, "ijklmnop");
        SetDataCoolingEnabled(runtime, true);
        std::unique_ptr<IEventHandle> dataPage;
        ui32 dataNode = 0;
        runtime.FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (!dataNode && IsBlobDepotTablePage(*ev, collection)) {
                dataNode = node;
                dataPage = std::move(ev);
                return false;
            }
            scanStarted |= ev->GetTypeRewrite() == TEvListObjectsRequest::EventType;
            return true;
        };
        RebootBlobDepotTablet(env, *tabletId);
        WaitForS3Condition(env, [&] { return bool(dataPage); });
        const auto newAgent = runtime.AllocateEdgeActor(2);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, newAgent, groupId, second), "ijklmnop");
        PutToS3VirtualGroup(env, newAgent, groupId, second, "ijklmnop");
        runtime.Send(std::move(dataPage), dataNode);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, first), "abcdefgh");
        UNIT_ASSERT(!scanStarted);
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 0);
        runtime.FilterFunction = {};

        ReconfigureS3Group(env);
        WaitForS3Condition(env, [&] { return bucket.GetSize() == 2; });
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, first), "abcdefgh");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, second), "ijklmnop");
    }

    Y_UNIT_TEST(AsyncS3ConfirmedRouteChangePreservesPublishedData) {
        TEnvironmentSetup env(MakeCoolingTestSettings());
        const TString bucket = "confirmed-s3-route";
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        const auto groupId = CreateS3VirtualGroup(env, bucket, &async);
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, groupId, id, "original");
        WaitForPublishedS3Range(env, edge, groupId, id, id);

        auto rerouted = MakeS3Settings(bucket);
        rerouted.MutableSettings()->SetRegion("new-region");
        const auto denied = TryReconfigureS3Group(env, rerouted, "vg", {.ConfirmNamespaceMigration = true});
        UNIT_ASSERT(!denied.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(denied.GetErrorDescription(), "ConfirmS3RouteChange");

        auto migrated = rerouted;
        migrated.MutableSettings()->SetObjectKeyPattern("other-prefix");
        const auto moved = TryReconfigureS3Group(env, migrated, "vg", {.ConfirmRouteChange = true});
        UNIT_ASSERT(!moved.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(moved.GetErrorDescription(), "requires migrating");

        const auto tabletId = WaitForS3VirtualGroup(env, groupId);
        bool applied = false;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvNodeWardenAcquireBlobDepotS3Router) {
                const auto& acquire = *ev->Get<NStorage::TEvNodeWardenAcquireBlobDepotS3Router>();
                applied |= acquire.TabletId == tabletId && acquire.Settings.HasSyncMode() &&
                    acquire.Settings.GetSettings().GetRegion() == "new-region";
            }
            return true;
        };
        const auto confirmed = TryReconfigureS3Group(env, rerouted, "vg", {.ConfirmRouteChange = true});
        UNIT_ASSERT_C(confirmed.GetSuccess(), confirmed.GetErrorDescription());
        WaitForS3Condition(env, [&] { return applied; });
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, id), "original");

        // The operator's confirmation applies to one command, including after the change to SyncMode.
        rerouted.MutableSettings()->SetEndpoint("unconfirmed-host");
        UNIT_ASSERT(!TryReconfigureS3Group(env, rerouted).GetSuccess());
    }

    Y_UNIT_TEST(S3RouterTracksConsumerGenerations) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings());
        auto& runtime = *env.Runtime;
        const ui32 node = 1;
        const ui64 tablet = 100500;
        const auto warden = MakeBlobStorageNodeWardenID(node);
        const auto service = MakeBlobDepotS3RouterID(tablet);
        const auto first = runtime.AllocateEdgeActor(node);
        const auto second = runtime.AllocateEdgeActor(node);
        const auto getRouter = [&] { return runtime.GetNode(node)->ActorSystem->LookupLocalService(service); };
        auto acquire = [&](TActorId sender, ui32 generation, const TString& bucket) {
            runtime.Send(new IEventHandle(warden, sender, new NStorage::TEvNodeWardenAcquireBlobDepotS3Router(
                tablet, generation, MakeS3Settings(bucket))), node);
        };
        auto release = [&](TActorId sender, ui32 generation) {
            runtime.Send(new IEventHandle(warden, sender, new NStorage::TEvNodeWardenReleaseBlobDepotS3Router(
                tablet, generation)), node);
        };

        const TString source = "router-migration-source";
        const TString destination = "router-migration-destination";
        acquire(first, 1, source);
        WaitForS3Condition(env, [&] { return bool(getRouter()); });
        const auto old = getRouter();
        acquire(second, 2, destination);
        WaitForS3Condition(env, [&] { return getRouter() && getRouter() != old; });
        const auto current = getRouter();

        ui32 releases = 0;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            releases += ev->GetTypeRewrite() == TEvBlobStorage::EvNodeWardenReleaseBlobDepotS3Router;
            return true;
        };
        acquire(second, 1, source); // a delayed acquire from the same agent's older registration
        release(second, 1);
        release(first, 1);
        WaitForS3Condition(env, [&] { return releases == 2; });
        runtime.FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL(getRouter(), current);
        auto request = Aws::S3::Model::PutObjectRequest().WithBucket(destination).WithKey("new-key");
        runtime.Send(new IEventHandle(service, second, new TEvPutObjectRequest(request, "value")), node);
        const auto result = env.WaitForEdgeActorEvent<TEvPutObjectResponse>(second, false, runtime.GetClock() + TDuration::Seconds(10));
        UNIT_ASSERT(result && result->Get()->IsSuccess());
        auto* storage = Singleton<TFakeExternalStorage>();
        UNIT_ASSERT(!storage->GetBucket(source).GetObject("new-key"));
        UNIT_ASSERT_VALUES_EQUAL(storage->GetBucket(destination).GetObject("new-key").value(), "value");

        release(second, 2);
        WaitForS3Condition(env, [&] { return !getRouter(); });
    }

    Y_UNIT_TEST(S3DisablesEmptyNamespace) {
        TEnvironmentSetup env(MakeCoolingTestSettings());
        const TString bucket = "misspelled-bucket";
        const auto group = CreateS3VirtualGroup(env, bucket);
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const auto agent = ConnectToVirtualGroupAgent(env, edge, group);
        const NKikimrBlobDepot::TS3BackendSettings disabled;
        UNIT_ASSERT(!TryReconfigureS3Group(env, disabled).GetSuccess());
        UNIT_ASSERT(!TryReconfigureS3Group(env, {}, "vg", {.ConfirmEmpty = true}).GetSuccess());
        bool registeredWithoutS3 = false;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                UNIT_ASSERT(!ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.GetConfig().HasS3BackendSettings());
            } else if (ev->GetTypeRewrite() == TEvBlobDepot::EvRegisterAgentResult && ev->GetRecipientRewrite() == agent) {
                const auto& config = ev->Get<TEvBlobDepot::TEvRegisterAgentResult>()->Record;
                registeredWithoutS3 = !config.HasS3BackendSettings();
            }
            return true;
        };
        const auto result = TryReconfigureS3Group(env, disabled, "vg", {.ConfirmEmpty = true});
        UNIT_ASSERT_C(result.GetSuccess(), result.GetErrorDescription());
        WaitForS3Condition(env, [&] { return registeredWithoutS3; });
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, group, id, "new-data");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, group, id), "new-data");
        UNIT_ASSERT_VALUES_EQUAL(Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket(bucket).GetSize(), 0);
        env.Runtime->FilterFunction = {};
        const auto repeated = TryReconfigureS3Group(env, disabled, "vg", {.ConfirmEmpty = true});
        UNIT_ASSERT_C(repeated.GetSuccess(), repeated.GetErrorDescription());
    }

    Y_UNIT_TEST(AsyncS3ReauthorizesNamespaceMigratedWithCoolingDisabled) {
        TEnvironmentSetup env(MakeCoolingTestSettings());
        auto& runtime = *env.Runtime;
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        async.SetMaxPendingBytes(MaxWriteCreditBytes);
        const auto group = CreateS3VirtualGroup(env, "reauthorize-source", &async);
        const auto edge = runtime.AllocateEdgeActor(1);
        const auto agent = ConnectToVirtualGroupAgent(env, edge, group);
        const TString destination = "reauthorize-destination";
        auto settings = MakeS3Settings(destination, &async);
        SetDataCoolingEnabled(runtime, false);
        std::optional<NKikimrBlobDepot::TBlobDepotConfig> applied;
        bool registeredWithAdmission = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                const auto& config = ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.GetConfig();
                if (config.GetS3BackendSettings().GetSettings().GetBucket() == destination) {
                    applied = config;
                }
            } else if (ev->GetTypeRewrite() == TEvBlobDepot::EvRegisterAgentResult && ev->GetRecipientRewrite() == agent) {
                const auto& registration = ev->Get<TEvBlobDepot::TEvRegisterAgentResult>()->Record;
                const auto& s3 = registration.GetS3BackendSettings().GetSettings();
                registeredWithAdmission = registration.HasHotWriteCreditEpoch() && s3.GetBucket() == destination &&
                    s3.GetAccessKey() == "rotated";
            }
            return true;
        };
        const auto move = TryReconfigureS3Group(env, settings, "vg", {.ConfirmNamespaceMigration = true});
        UNIT_ASSERT_C(move.GetSuccess(), move.GetErrorDescription());
        WaitForS3Condition(env, [&] { return applied.has_value(); });
        UNIT_ASSERT(applied->GetEverAuthorizedDataCooling());
        UNIT_ASSERT(!applied->GetS3NamespaceValidated());

        SetDataCoolingEnabled(runtime, true);
        settings.MutableSettings()->SetAccessKey("rotated");
        ReconfigureS3Group(env, settings);
        WaitForS3Condition(env, [&] { return applied->GetS3BackendSettings().GetSettings().GetAccessKey() == "rotated"; });
        UNIT_ASSERT(!applied->GetS3NamespaceValidated());
        ReconfigureS3Group(env);
        WaitForS3Condition(env, [&] { return applied->GetS3NamespaceValidated() && registeredWithAdmission; });
        runtime.FilterFunction = {};

        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, group, id, "new-data");
        WaitForPublishedS3Range(env, edge, group, id, id);
        UNIT_ASSERT_VALUES_EQUAL(Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket(destination).GetSize(), 1);
    }

    Y_UNIT_TEST(AsyncS3ConfirmedNamespaceMigrationPreservesPublishedData) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings());
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        const TString source = "namespace-migration-source";
        const TString destination = "namespace-migration-destination";
        const auto group = CreateS3VirtualGroup(env, source, &async);
        const auto tablet = WaitForS3VirtualGroup(env, group);
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, group, id, "original");
        WaitForPublishedS3Range(env, edge, group, id, id);
        const auto drop = TryReconfigureS3Group(env, NKikimrBlobDepot::TS3BackendSettings{}, "vg",
            {.ConfirmNamespaceMigration = true});
        UNIT_ASSERT(!drop.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(drop.GetErrorDescription(), "ConfirmS3Empty");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, group, id), "original");
        auto* storage = Singleton<TFakeExternalStorage>();
        const auto& oldBucket = storage->GetBucket(source);
        UNIT_ASSERT_VALUES_EQUAL(oldBucket.GetSize(), 1);
        const TString key = oldBucket.GetObjectKeys().front();
        const TString newKey = "migrated/" + key;
        storage->MutableBucket(destination).PutObject(newKey, *oldBucket.GetObject(key));

        auto settings = MakeS3Settings(destination, &async, "migrated");
        settings.MutableSettings()->SetRegion("destination-region");
        const auto denied = TryReconfigureS3Group(env, settings);
        UNIT_ASSERT(!denied.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(denied.GetErrorDescription(), "ConfirmS3NamespaceMigration");
        bool acquired = false;
        bool readNewKey = false;
        std::unique_ptr<IEventHandle> earlierConfig;
        ui32 configNode = 0;
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (!configNode && ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                configNode = node;
                earlierConfig = std::move(ev);
                return false;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvNodeWardenAcquireBlobDepotS3Router) {
                const auto& msg = *ev->Get<NStorage::TEvNodeWardenAcquireBlobDepotS3Router>();
                acquired |= msg.TabletId == tablet && msg.Settings.GetSettings().GetBucket() == destination &&
                    msg.Settings.GetSettings().GetRegion() == "destination-region";
            } else if (ev->GetTypeRewrite() == TEvGetObjectRequest::EventType) {
                readNewKey |= TStringBuf(ev->Get<TEvGetObjectRequest>()->GetRequest().GetKey().c_str()) == newKey;
            }
            return true;
        };
        // The earlier credential rotation is already committed at BSC, but has not reached the tablet yet.
        auto credentials = MakeS3Settings(source, &async);
        credentials.MutableSettings()->SetAccessKey("rotated");
        ReconfigureS3Group(env, credentials);
        WaitForS3Condition(env, [&] { return bool(earlierConfig); });
        const auto moved = TryReconfigureS3Group(env, settings, "vg", {.ConfirmNamespaceMigration = true});
        UNIT_ASSERT_C(moved.GetSuccess(), moved.GetErrorDescription());
        env.Runtime->Send(std::move(earlierConfig), configNode);
        WaitForS3Condition(env, [&] { return acquired; });
        const auto reader = env.Runtime->AllocateEdgeActor(2);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, reader, group, id), "original");
        UNIT_ASSERT(readNewKey);
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(AsyncS3PacksAndRepacks) {
        TEnvironmentSetup env(MakeCoolingTestSettings());
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(16);
        const TString bucketName = "async-pack-and-repack";
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, &async);
        const auto& bucket = Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket(bucketName);
        const ui64 tabletId = 100500;
        const TLogoBlobID large(tabletId, 1, 1, 0, 12, 0);
        const TLogoBlobID small(tabletId, 1, 2, 0, 4, 0);
        const auto writer = env.Runtime->AllocateEdgeActor(1);
        const auto reader = env.Runtime->AllocateEdgeActor(2);
        const auto checker = env.Runtime->AllocateEdgeActor(3);

        PutToS3VirtualGroup(env, writer, groupId, large, "abcdefghijkl");
        PutToS3VirtualGroup(env, writer, groupId, small, "wxyz");
        WaitForS3Condition(env, [&] { return bucket.GetSize() == 1; });
        const auto objects = bucket.GetObjectKeys();
        UNIT_ASSERT_VALUES_EQUAL(objects.size(), 1);
        const auto packed = bucket.GetObject(objects.front());
        UNIT_ASSERT(packed);
        UNIT_ASSERT_VALUES_EQUAL(*packed, "abcdefghijklwxyz");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, reader, groupId, large), "abcdefghijkl");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, reader, groupId, small, 1, 2), "xy");
        UNIT_ASSERT_VALUES_EQUAL(CheckS3VirtualGroupIntegrity(env, checker, groupId, small), NKikimrProto::OK);

        auto s3 = MakeS3Settings(bucketName + "-other", &async);
        const auto reconfigured = TryReconfigureS3Group(env, s3);
        UNIT_ASSERT(!reconfigured.GetSuccess());

        s3.MutableSettings()->SetBucket(bucketName);
        s3.MutableSettings()->SetEndpoint("another.fake");
        const auto redirected = TryReconfigureS3Group(env, s3);
        UNIT_ASSERT_C(!redirected.GetSuccess(), "reconfiguration redirected existing objects to another S3 endpoint");
        UNIT_ASSERT_C(TStringBuf(redirected.GetErrorDescription()).Contains("configured S3 route"), redirected.DebugString());

        SendTEvCollectGarbage(env, writer, groupId, tabletId, 1, 1, large.Channel(), true, 1, 1,
            nullptr, nullptr, false, true);
        auto collected = CaptureTEvCollectGarbageResult(env, writer, false);
        UNIT_ASSERT_VALUES_EQUAL_C(collected->Get()->Status, NKikimrProto::OK, collected->Get()->ToString());
        WaitForS3Condition(env, [&] { return bucket.GetSize() == 1 && !bucket.GetObject(objects.front()); });

        UNIT_ASSERT(!bucket.GetObject(objects.front()));
        const auto compacted = bucket.GetObjectKeys();
        UNIT_ASSERT_VALUES_EQUAL(compacted.size(), 1);
        const auto remaining = bucket.GetObject(compacted.front());
        UNIT_ASSERT(remaining);
        UNIT_ASSERT_VALUES_EQUAL(*remaining, "wxyz");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, reader, groupId, small), "wxyz");
        UNIT_ASSERT_VALUES_EQUAL(CheckS3VirtualGroupIntegrity(env, checker, groupId, small), NKikimrProto::OK);
    }

    Y_UNIT_TEST(AsyncS3ConcurrentRepacksResumeAtFirstUnconsumedKey) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings());
        auto& runtime = *env.Runtime;
        const TString bucketName = "concurrent-cold-repacks";
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(73);
        async.SetRepackBelowLivePercent(0);
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, &async);
        const auto& bucket = Singleton<TFakeExternalStorage>()->GetBucket(bucketName);
        const auto edge = runtime.AllocateEdgeActor(1);
        const TLogoBlobID large(100500, 1, 1, 0, 48, 0);
        const auto small = [&](ui32 i) { return TLogoBlobID(large.TabletID(), 1, 2 + i, 0, 5, 0); };
        const auto body = [](ui32 i) { return TString(5, 'a' + i); };
        PutToS3VirtualGroup(env, edge, groupId, large, TString(48, 'x'));
        for (ui32 i = 0; i < 5; ++i) {
            PutToS3VirtualGroup(env, edge, groupId, small(i), body(i));
        }
        WaitForPublishedS3Range(env, edge, groupId, large, small(4));
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 1);
        const auto original = bucket.GetObjectKeys().front();
        SendTEvCollectGarbage(env, edge, groupId, large.TabletID(), 1, 1, large.Channel(), true, 1, 1,
            nullptr, nullptr, false, true);
        const auto collected = CaptureTEvCollectGarbageResult(env, edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(collected->Get()->Status, NKikimrProto::OK, collected->Get()->ToString());

        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> replies;
        runtime.FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPutObjectResponse::EventType) {
                replies.emplace_back(node, std::move(ev));
                return false;
            }
            return true;
        };
        async.SetTargetObjectBytes(11); // The third 5-byte key must be retried in the next pack.
        async.SetRepackBelowLivePercent(50);
        async.SetUploadPutsInFlight(3);
        ReconfigureS3Group(env, MakeS3Settings(bucketName, &async));
        WaitForS3Condition(env, [&] { return replies.size() == 3; });
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 4);
        runtime.FilterFunction = {};
        for (auto& [node, reply] : replies) {
            runtime.Send(std::move(reply), node);
        }
        WaitForS3Condition(env, [&] { return !bucket.GetObject(original); });
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 3);
        for (ui32 i = 0; i < 5; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, small(i)), body(i));
        }
    }

    Y_UNIT_TEST(AsyncS3RejectsLimitsBelowOneMaximumWrite) {
        TEnvironmentSetup env(MakeCoolingTestSettings());
        const TString bucket = "async-write-limit";
        CreateS3VirtualGroup(env, bucket);
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetMaxPendingBytes(MaxWriteCreditBytes - 1);
        const auto rejected = TryReconfigureS3Group(env, MakeS3Settings(bucket, &async));
        UNIT_ASSERT(!rejected.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(rejected.GetErrorDescription(), "maximum-sized blob");
        async.SetMaxPendingBytes(MaxWriteCreditBytes);
        ReconfigureS3Group(env, MakeS3Settings(bucket, &async));
    }

    Y_UNIT_TEST(AsyncS3CoolsBelowThrottleStart) {
        TEnvironmentSetup env(MakeCoolingTestSettings());
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetMaxPendingBytes(MaxWriteCreditBytes);
        async.SetHotTargetBytes(32);
        async.SetThrottleStartBytes(16);
        async.SetThrottleMaxBytesPerSecond(64);
        async.SetTargetObjectBytes(16);
        const ui32 group = CreateS3VirtualGroup(env, "throttle-target", &async);
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 16, 0);
        const TString data(16, 'x');
        PutToS3VirtualGroup(env, edge, group, id, data);
        WaitForPublishedS3Range(env, edge, group, id, id);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, group, id), data);
    }

    Y_UNIT_TEST(AsyncS3AdmissionWaitsForDurableMapping) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings());
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetMaxPendingBytes(MaxWriteCreditBytes);
        async.SetHotTargetBytes(8);
        async.SetTargetObjectBytes(MaxBlobSize);
        const TString bucketName = "async-admission";
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, &async);
        const auto router = MakeBlobDepotS3RouterID(WaitForS3VirtualGroup(env, groupId));
        const auto& bucket = Singleton<TFakeExternalStorage>()->GetBucket(bucketName);
        const auto first = env.Runtime->AllocateEdgeActor(1);
        const auto second = env.Runtime->AllocateEdgeActor(2);
        const TString firstValue(MaxBlobSize, 'h');
        const TString waitingValue(32, 'w');
        const TLogoBlobID firstId(100500, 1, 1, 0, firstValue.size(), 0);
        const TLogoBlobID finiteHead(100500, 1, 2, 0, waitingValue.size(), 0);
        const TLogoBlobID unlimitedHead(100500, 1, 3, 0, waitingValue.size(), 0);
        const TLogoBlobID finiteTail(100500, 1, 4, 0, waitingValue.size(), 0);

        std::unique_ptr<IEventHandle> uploadReply;
        ui32 uploadReplyNode = 0;
        ui32 uploadRequests = 0;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPutObjectRequest::EventType && ev->Recipient == router) {
                ++uploadRequests;
            } else if (ev->GetTypeRewrite() == TEvPutObjectResponse::EventType && !uploadReply) {
                uploadReplyNode = nodeId;
                uploadReply = std::move(ev);
                return false;
            }
            return true;
        };

        PutToS3VirtualGroup(env, first, groupId, firstId, firstValue);
        WaitForS3Condition(env, [&] { return bool(uploadReply); });
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, second, groupId, firstId, 0, 8), firstValue.substr(0, 8));

        auto send = [&](TLogoBlobID id, TInstant deadline) {
            env.Runtime->WrapInActorContext(second, [&] {
                SendToBSProxy(second, groupId, new TEvBlobStorage::TEvPut(id, waitingValue, deadline));
            });
        };
        auto wait = [&](TDuration timeout) {
            return env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(second, false, env.Runtime->GetClock() + timeout);
        };
        auto expectTimeout = [&](TLogoBlobID id) {
            const auto result = wait(TDuration::Seconds(4));
            UNIT_ASSERT(result);
            UNIT_ASSERT_EQUAL(result->Get()->Id, id);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::TIMEOUT, result->Get()->ToString());
        };

        send(finiteHead, env.Runtime->GetClock() + TDuration::Seconds(2));
        UNIT_ASSERT(!wait(TDuration::MilliSeconds(200)));
        expectTimeout(finiteHead);

        send(unlimitedHead, TInstant::Max());
        send(finiteTail, env.Runtime->GetClock() + TDuration::Seconds(2));
        expectTimeout(finiteTail);

        // A slow successful PUT keeps its slot and hot-write charge until it can publish.
        UNIT_ASSERT(!wait(TDuration::Seconds(320)));
        UNIT_ASSERT_VALUES_EQUAL(uploadRequests, 1);
        env.Runtime->FilterFunction = {};
        env.Runtime->Send(uploadReply.release(), uploadReplyNode);

        const auto put = wait(TDuration::Seconds(10));
        UNIT_ASSERT(put);
        UNIT_ASSERT_EQUAL(put->Get()->Id, unlimitedHead);
        UNIT_ASSERT_VALUES_EQUAL_C(put->Get()->Status, NKikimrProto::OK, put->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, second, groupId, firstId, 0, 8), firstValue.substr(0, 8));
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, second, groupId, unlimitedHead), waitingValue);
    }

    Y_UNIT_TEST(AsyncS3TimedOutPutReleasesCreditAndCollectsLateObject) {
        TEnvironmentSetup env(MakeCoolingTestSettings());
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetMaxPendingBytes(MaxWriteCreditBytes);
        async.SetTargetObjectBytes(MaxBlobSize);
        const TString bucketName = "async-put-timeout";
        const auto group = CreateS3VirtualGroup(env, bucketName, &async);
        const auto tablet = WaitForS3VirtualGroup(env, group);
        const auto writer = env.Runtime->AllocateEdgeActor(1);
        const auto waiter = env.Runtime->AllocateEdgeActor(2);
        const TString body(MaxBlobSize, 'h');
        // The credit ceiling includes room for a footer, even when this data channel omits it.
        const TString waitingBody(MaxWriteCreditBytes - body.size() + 1, 'w');
        const TLogoBlobID hot(100500, 1, 1, 0, body.size(), 0);
        const TLogoBlobID next(100500, 1, 2, 0, waitingBody.size(), 0);
        const auto delayed = DelayCoolingPut(env, writer, group, hot, body);
        env.Runtime->WrapInActorContext(waiter, [&] {
            SendToBSProxy(waiter, group, new TEvBlobStorage::TEvPut(next, waitingBody, TInstant::Max()));
        });
        auto wait = [&](TDuration duration) {
            return env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(waiter, false,
                env.Runtime->GetClock() + duration);
        };
        UNIT_ASSERT(!wait(TDuration::Seconds(1)));
        const auto reply = wait(TDuration::Minutes(11));
        UNIT_ASSERT(reply);
        UNIT_ASSERT_EQUAL(reply->Get()->Id, next);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Status, NKikimrProto::OK, reply->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(GetUnfinishedPutWatchCount(env, tablet), 1);

        auto& bucket = Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->MutableBucket(bucketName);
        bucket.PutObject(delayed.Key, delayed.Body);
        WaitForS3Condition(env, [&] { return !bucket.GetObject(delayed.Key); });
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, writer, group, hot, 0, 4), "hhhh");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, waiter, group, next), waitingBody);
    }

    Y_UNIT_TEST(AsyncS3ReplayDuringRecoveryPreservesSibling) {
        using namespace NWrappers::NExternalStorage;

        TEnvironmentSetup env(MakeCoolingTestSettings());
        auto& runtime = *env.Runtime;
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(16);
        async.SetRepackBelowLivePercent(0);
        const TString bucketName = "async-replay-during-recovery";
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, &async);
        const auto tabletId = TryGetBlobDepotTabletId(env, groupId);
        UNIT_ASSERT(tabletId);
        const auto& bucket = Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket(bucketName);
        const TLogoBlobID first(100500, 1, 1, 0, 8, 0);
        const TLogoBlobID second(100500, 1, 2, 0, 8, 0);
        const auto writer = runtime.AllocateEdgeActor(1);
        const auto replayer = runtime.AllocateEdgeActor(2);
        const auto reader = runtime.AllocateEdgeActor(3);

        PutToS3VirtualGroup(env, writer, groupId, first, "abcdefgh");
        PutToS3VirtualGroup(env, writer, groupId, second, "ijklmnop");
        WaitForS3Condition(env, [&] { return bucket.GetSize() == 1; });

        const auto packedResolve = WaitForPublishedS3Range(env, reader, groupId, first, second);
        UNIT_ASSERT_VALUES_EQUAL(packedResolve.ResolvedKeysSize(), 2);
        const auto& firstLink = packedResolve.GetResolvedKeys(0).GetValueChain(0);
        const auto& secondLink = packedResolve.GetResolvedKeys(1).GetValueChain(0);
        const auto locator = TS3Locator::FromProto(firstLink.GetS3Locator());
        UNIT_ASSERT(locator == TS3Locator::FromProto(secondLink.GetS3Locator()));
        UNIT_ASSERT_VALUES_EQUAL(locator.Len, 16);
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 1);
        const auto packed = bucket.GetObjectKeys().front();
        UNIT_ASSERT(packed.EndsWith("/" + locator.MakeObjectName({})));
        const auto object = bucket.GetObject(packed);
        UNIT_ASSERT(object);
        UNIT_ASSERT_VALUES_EQUAL(*object, "abcdefghijklmnop");

        const TLogoBlobID single(first.TabletID(), 1, 3, 0, 16, 0);
        const TString singleBody(16, 's');
        PutToS3VirtualGroup(env, writer, groupId, single, singleBody);
        WaitForPublishedS3Range(env, reader, groupId, single, single);
        const auto keys = bucket.GetObjectKeys();
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2);
        const TString singleObject = keys.front() == packed ? keys.back() : keys.front();

        // Compact a staged cooling PUT. Recovery needs this page before scanning Data, while
        // foreground point lookups remain free to fault in exactly the rows touched by the replays.
        bool putInFlight = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPutObjectRequest::EventType) {
                putInFlight = true;
                return false;
            }
            return true;
        };
        const TLogoBlobID staged(first.TabletID(), 1, 4, 0, 16, 0);
        PutToS3VirtualGroup(env, writer, groupId, staged, TString(16, 'x'));
        WaitForS3Condition(env, [&] { return putInFlight; });
        const auto collection = CompactBlobDepotTable(env, *tabletId, NBlobDepot::Schema::CoolingS3Puts::TableId);
        SetDataCoolingEnabled(runtime, false);
        std::unique_ptr<IEventHandle> dataLoad;
        ui32 loadNode = 0;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (!dataLoad && IsBlobDepotTablePage(*ev, collection)) {
                loadNode = nodeId;
                dataLoad = std::move(ev);
                return false;
            }
            return true;
        };
        RebootBlobDepotTablet(env, *tabletId);
        WaitForS3Condition(env, [&] { return bool(dataLoad); });

        // Both objects lose their only loaded reference. The shared pack still has an unloaded sibling.
        PutToS3VirtualGroup(env, replayer, groupId, first, "abcdefgh");
        PutToS3VirtualGroup(env, replayer, groupId, single, singleBody);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, replayer, groupId, first), "abcdefgh");
        UNIT_ASSERT(bucket.GetObject(packed));
        UNIT_ASSERT(bucket.GetObject(singleObject));

        // Crash before deferred reference checks can complete. Block the scanner on restart so
        // collection of the single-owner object must come from the durable candidate.
        runtime.FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
            return ev->GetTypeRewrite() != TEvListObjectsRequest::EventType;
        };
        RebootBlobDepotTablet(env, *tabletId);
        runtime.Send(std::move(dataLoad), loadNode);
        WaitForS3Condition(env, [&] { return !bucket.GetObject(singleObject); });
        const auto newReader = runtime.AllocateEdgeActor(4);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, newReader, groupId, second), "ijklmnop");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, newReader, groupId, single), singleBody);
        UNIT_ASSERT(bucket.GetObject(packed));
        runtime.FilterFunction = {};
    }

    Y_UNIT_TEST(AsyncS3RestartsBeforePublishing) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings());
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        const TString bucketName = "async-restart";
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, &async);
        const auto tabletId = TryGetBlobDepotTabletId(env, groupId);
        UNIT_ASSERT(tabletId);
        const auto& bucket = Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket(bucketName);
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);

        Aws::Http::ContinueRequestHandler canContinue;
        bool held = false;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            // Observe the uploader's cancellation at router ingress. The forwarded SDK callback
            // owns the physical slot and is released when the fake storage finishes its PUT.
            if (ev->GetTypeRewrite() == TEvPutObjectRequest::EventType && ev->Recipient == MakeBlobDepotS3RouterID(*tabletId)) {
                canContinue = ev->Get<TEvPutObjectRequest>()->GetRequest().GetContinueRequestHandler();
            } else if (ev->GetTypeRewrite() == TEvPutObjectResponse::EventType) {
                held = true;
                return false;
            }
            return true;
        };
        PutToS3VirtualGroup(env, edge, groupId, id, "abcdefgh");
        WaitForS3Condition(env, [&] { return held; });
        UNIT_ASSERT(canContinue && canContinue(nullptr));
        const auto unpublished = bucket.GetObjectKeys();
        UNIT_ASSERT_VALUES_EQUAL(unpublished.size(), 1);

        env.Runtime->FilterFunction = {};
        RebootBlobDepotTablet(env, *tabletId);
        UNIT_ASSERT(!canContinue(nullptr));
        WaitForS3Condition(env, [&] { return bucket.GetSize() == 1 && !bucket.GetObject(unpublished.front()); });

        UNIT_ASSERT(!bucket.GetObject(unpublished.front()));
        const auto objects = bucket.GetObjectKeys();
        UNIT_ASSERT_VALUES_EQUAL(objects.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, id), "abcdefgh");
    }

    Y_UNIT_TEST(AsyncS3RecoversStagedPutBeforeColdDataScan) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings());
        auto& runtime = *env.Runtime;
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        const TString bucketName = "async-staged-put-cold-recovery";
        const auto group = CreateS3VirtualGroup(env, bucketName, &async);
        const auto tablet = WaitForS3VirtualGroup(env, group);
        const auto edge = runtime.AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        auto& bucket = Singleton<TFakeExternalStorage>()->MutableBucket(bucketName);

        const auto [lateKey, lateBody] = DelayCoolingPut(env, edge, group, id, "abcdefgh");
        // The staged PUT fits in the last migration batch. Data precharge in the
        // next phase must fault on a real page and restart its transaction.
        const auto collection = CompactBlobDepotTable(env, tablet, NBlobDepot::Schema::Data::TableId);
        SetDataCoolingEnabled(runtime, false);
        bool dataPageRead = false;
        bool probed = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            dataPageRead |= IsBlobDepotTablePage(*ev, collection);
            if (ev->GetTypeRewrite() == TEvHeadObjectRequest::EventType) {
                const auto& request = ev->Get<TEvHeadObjectRequest>()->GetRequest();
                probed |= TStringBuf(request.GetKey().c_str()) == lateKey;
            }
            // Only the recovered watch can discover the late object.
            return ev->GetTypeRewrite() != TEvListObjectsRequest::EventType;
        };
        RebootBlobDepotTablet(env, tablet);
        WaitForS3Condition(env, [&] { return dataPageRead && probed; });
        UNIT_ASSERT_VALUES_EQUAL(GetUnfinishedPutWatchCount(env, tablet), 1);

        RebootBlobDepotTablet(env, tablet);
        UNIT_ASSERT_VALUES_EQUAL(GetUnfinishedPutWatchCount(env, tablet), 1);
        bucket.PutObject(lateKey, lateBody);
        WaitForS3Condition(env, [&] { return !bucket.GetObject(lateKey); });
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, group, id), "abcdefgh");
        runtime.FilterFunction = {};
    }

    Y_UNIT_TEST(AsyncS3CollectsLatePutByExactKey) {
        using namespace NWrappers::NExternalStorage;

        TEnvironmentSetup env(MakeCoolingTestSettings());
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        const TString bucketName = "async-late-put";
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, &async);
        const auto tabletId = TryGetBlobDepotTabletId(env, groupId);
        UNIT_ASSERT(tabletId);
        auto& bucket = Singleton<TFakeExternalStorage>()->MutableBucket(bucketName);
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const TLogoBlobID first(100500, 1, 1, 0, 8, 0);
        const TLogoBlobID second(100500, 1, 2, 0, 8, 0);

        PutToS3VirtualGroup(env, edge, groupId, first, "abcdefgh");
        WaitForS3Condition(env, [&] { return bucket.GetSize() == 1; });
        const TString liveKey = bucket.GetObjectKeys().front();
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, first), "abcdefgh");
        RebootBlobDepotTablet(env, *tabletId);

        TString lateKey;
        TString lateBody;
        bool recoveryProbed = false;
        bool changingMode = false;
        bool modeChangeApplied = false;
        TActorId startupScanner;
        bool startupScanCompleted = false;
        bool sawSecondPage = false;
        ui32 postStartupLists = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvPutObjectRequest::EventType:
                    if (lateKey.empty()) {
                        const auto& put = *ev->Get<TEvPutObjectRequest>();
                        lateKey = put.GetRequest().GetKey();
                        lateBody = put.Body;
                        return false;
                    }
                    break;
                case TEvHeadObjectRequest::EventType: {
                    const auto& head = ev->Get<TEvHeadObjectRequest>()->GetRequest();
                    recoveryProbed |= !lateKey.empty() && TStringBuf(head.GetKey().c_str()) == lateKey;
                    break;
                }
                case TEvBlobDepot::EvApplyConfigResult:
                    modeChangeApplied |= changingMode &&
                        ev->Get<TEvBlobDepot::TEvApplyConfigResult>()->Record.GetTabletId() == *tabletId;
                    break;
                case TEvListObjectsRequest::EventType: {
                    auto& list = ev->Get<TEvListObjectsRequest>()->MutableRequest();
                    if (changingMode && list.GetBucket() == bucketName) {
                        if (ev->Recipient == MakeBlobDepotS3RouterID(*tabletId)) {
                            startupScanner = ev->Sender;
                        }
                        list.SetMaxKeys(1);
                        sawSecondPage |= list.GetMarker() == liveKey;
                    }
                    if (startupScanCompleted) {
                        ++postStartupLists;
                    }
                    break;
                }
                case TEvListObjectsResponse::EventType: {
                    const auto& list = *ev->Get<TEvListObjectsResponse>();
                    startupScanCompleted |= startupScanner && ev->GetRecipientRewrite() == startupScanner &&
                        list.IsSuccess() && !list.Result.GetResult().GetIsTruncated();
                    break;
                }
            }
            return true;
        };

        PutToS3VirtualGroup(env, edge, groupId, second, "ijklmnop");
        WaitForS3Condition(env, [&] { return !lateKey.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(lateBody, "ijklmnop");
        UNIT_ASSERT(liveKey < lateKey);
        UNIT_ASSERT(!bucket.GetObject(lateKey));

        RebootBlobDepotTablet(env, *tabletId);
        WaitForS3Condition(env, [&] { return recoveryProbed && bucket.GetSize() == 2; });
        UNIT_ASSERT(bucket.GetObject(liveKey));
        UNIT_ASSERT(!bucket.GetObject(lateKey));

        changingMode = true;
        ReconfigureS3Group(env, MakeS3Settings(bucketName));
        WaitForS3Condition(env, [&] { return modeChangeApplied && startupScanCompleted; });
        UNIT_ASSERT(sawSecondPage);

        // An already issued PUT and a later retry can both land after the sync-mode startup scan.
        for (ui32 attempt = 0; attempt < 2; ++attempt) {
            bucket.PutObject(lateKey, lateBody);
            WaitForS3Condition(env, [&] { return !bucket.GetObject(lateKey); }, TDuration::Minutes(2));
        }
        env.Sim(TDuration::Minutes(16));
        UNIT_ASSERT_VALUES_EQUAL(postStartupLists, 0);
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, first), "abcdefgh");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, second), "ijklmnop");
    }

    Y_UNIT_TEST(AsyncS3GivesEachIORequestItsOwnDeadline) {
        using namespace NWrappers::NExternalStorage;

        TEnvironmentSetup env(MakeCoolingTestSettings());
        constexpr ui32 numBlobs = 17;
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(numBlobs);
        async.SetHotTargetBytes(numBlobs);
        const TString bucketName = "async-io-deadlines";
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, &async);
        const auto tabletId = TryGetBlobDepotTabletId(env, groupId);
        UNIT_ASSERT(tabletId);
        const auto& bucket = Singleton<TFakeExternalStorage>()->GetBucket(bucketName);
        auto& runtime = *env.Runtime;
        const auto writer = runtime.AllocateEdgeActor(1);

        TActorId uploader;
        THashSet<ui64> delayedReads;
        bool delayedPut = false;
        bool receivedPut = false;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            const auto delay = TDuration::Seconds(70);
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvGet: {
                    const auto& get = *ev->Get<TEvBlobStorage::TEvGet>();
                    if (get.GetHandleClass == NKikimrBlobStorage::EGetHandleClass::LowRead && get.QuerySize == 1 &&
                            get.Queries[0].Id.TabletID() == *tabletId) {
                        if (!uploader) {
                            uploader = ev->Sender;
                        }
                        UNIT_ASSERT_VALUES_EQUAL(ev->Sender, uploader);
                    }
                    break;
                }
                case TEvBlobStorage::EvGetResult:
                    if (uploader && ev->GetRecipientRewrite() == uploader && delayedReads.insert(ev->Cookie).second) {
                        runtime.Schedule(delay, ev.release(), nullptr, nodeId);
                        return false;
                    }
                    break;
                case TEvPutObjectResponse::EventType:
                    if (uploader && ev->GetRecipientRewrite() == uploader) {
                        if (!std::exchange(delayedPut, true)) {
                            runtime.Schedule(delay, ev.release(), nullptr, nodeId);
                            return false;
                        }
                        receivedPut = true;
                    }
                    break;
            }
            return true;
        };

        for (ui32 step = 1; step <= numBlobs; ++step) {
            PutToS3VirtualGroup(env, writer, groupId, TLogoBlobID(100500, 1, step, 0, 1, 0), "x");
        }
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 0);

        async.SetHotTargetBytes(0);
        ReconfigureS3Group(env, MakeS3Settings(bucketName, &async));

        WaitForS3Condition(env, [&] { return receivedPut; }, TDuration::Minutes(4));
        UNIT_ASSERT_VALUES_EQUAL(delayedReads.size(), numBlobs);
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetSize(), 1);
        const auto object = bucket.GetObject(bucket.GetObjectKeys().front());
        UNIT_ASSERT(object);
        UNIT_ASSERT_VALUES_EQUAL(*object, TString(numBlobs, 'x'));
    }

    Y_UNIT_TEST(AsyncS3KeepsSourceBackoffThroughKeepUpdates) {
        using namespace NWrappers::NExternalStorage;

        TEnvironmentSetup env(MakeCoolingTestSettings());
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        const TString bucketName = "async-keep-backoff";
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, &async);
        const auto tabletId = TryGetBlobDepotTabletId(env, groupId);
        UNIT_ASSERT(tabletId);
        const auto& bucket = Singleton<TFakeExternalStorage>()->GetBucket(bucketName);
        auto& runtime = *env.Runtime;
        const auto edge = runtime.AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);

        std::vector<TInstant> attempts;
        TActorId uploader;
        bool injectedError = false;
        bool deleteIssued = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet) {
                const auto& get = *ev->Get<TEvBlobStorage::TEvGet>();
                if (get.GetHandleClass == NKikimrBlobStorage::EGetHandleClass::LowRead && get.QuerySize == 1 &&
                        get.Queries[0].Id.TabletID() == *tabletId && ev->Sender != uploader) {
                    uploader = ev->Sender;
                    attempts.push_back(runtime.GetClock());
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && uploader &&
                    ev->GetRecipientRewrite() == uploader && !injectedError) {
                ev->Get<TEvBlobStorage::TEvGetResult>()->Status = NKikimrProto::ERROR;
                injectedError = true;
            } else if (ev->GetTypeRewrite() == TEvDeleteObjectRequest::EventType ||
                    ev->GetTypeRewrite() == TEvDeleteObjectsRequest::EventType) {
                deleteIssued = true;
            }
            return true;
        };

        PutToS3VirtualGroup(env, edge, groupId, id, "abcdefgh");
        WaitForS3Condition(env, [&] { return injectedError; });
        for (ui32 counter = 1; counter <= 2; ++counter) {
            SendTEvCollectGarbage(env, edge, groupId, id.TabletID(), 1, counter, id.Channel(), false, 1, 1,
                new TVector<TLogoBlobID>{id}, nullptr, false, false);
            auto result = CaptureTEvCollectGarbageResult(env, edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::OK, result->Get()->ToString());
        }

        WaitForS3Condition(env, [&] { return bucket.GetSize() == 1; });
        UNIT_ASSERT_VALUES_EQUAL(attempts.size(), 2);
        UNIT_ASSERT(attempts[1] - attempts[0] >= TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, id), "abcdefgh");
        UNIT_ASSERT_C(!deleteIssued, "a failed source read requested deletion of an object it never uploaded");
    }

    void RetryFirstCoolingPut(IEventHandle& ev, TString& key) {
        using namespace NWrappers::NExternalStorage;
        if (ev.GetTypeRewrite() == TEvPutObjectRequest::EventType && key.empty()) {
            // The SDK copies the request before invoking its retry handler on the worker thread.
            const auto request = ev.Get<TEvPutObjectRequest>()->GetRequest();
            key = request.GetKey();
            UNIT_ASSERT(request.GetRequestRetryHandler());
            request.GetRequestRetryHandler()(request);
        }
    }

    Y_UNIT_TEST_TWIN(AsyncS3CleansUpAnAmbiguousSingleAttempt, requestTimeout) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings());
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        const TString bucketName = requestTimeout ? "async-put-408" : "async-put-403";
        const ui32 group = CreateS3VirtualGroup(env, bucketName, &async);
        const auto router = MakeBlobDepotS3RouterID(WaitForS3VirtualGroup(env, group));
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        bool refused = false;
        bool cleanup = false;
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            const auto type = ev->GetTypeRewrite();
            if (!refused && type == TEvPutObjectRequest::EventType && ev->Recipient == router) {
                refused = true;
                Aws::Client::AWSError<Aws::S3::S3Errors> error(requestTimeout ? Aws::S3::S3Errors::REQUEST_TIMEOUT
                    : Aws::S3::S3Errors::ACCESS_DENIED, requestTimeout ? "RequestTimeout" : "AccessDenied", "failed", false);
                error.SetResponseCode(requestTimeout ? Aws::Http::HttpResponseCode::REQUEST_TIMEOUT : Aws::Http::HttpResponseCode::FORBIDDEN);
                const auto& key = ev->Get<TEvPutObjectRequest>()->GetRequest().GetKey();
                auto* reply = new TEvPutObjectResponse(TString(key.data(), key.size()), Aws::S3::S3Error(std::move(error)));
                env.Runtime->Send(new IEventHandle(ev->Sender, router, reply, 0, ev->Cookie), node);
                return false;
            }
            cleanup |= type == TEvHeadObjectRequest::EventType || type == TEvDeleteObjectRequest::EventType ||
                type == TEvDeleteObjectsRequest::EventType;
            return true;
        };
        PutToS3VirtualGroup(env, edge, group, id, "abcdefgh");
        const auto& bucket = Singleton<TFakeExternalStorage>()->GetBucket(bucketName);
        WaitForS3Condition(env, [&] { return refused && bucket.GetSize() == 1 && (!requestTimeout || cleanup); });
        UNIT_ASSERT_VALUES_EQUAL(cleanup, requestTimeout);
        env.Runtime->FilterFunction = {};
        WaitForPublishedS3Range(env, edge, group, id, id);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, group, id), "abcdefgh");
    }

    Y_UNIT_TEST(AsyncS3WatchesFailedPutAfterSdkRetry) {
        using namespace NWrappers::NExternalStorage;

        TEnvironmentSetup env(MakeCoolingTestSettings());
        const TString bucketName = "async-retried-put";
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, &async);
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        auto& bucket = Singleton<TFakeExternalStorage>()->MutableBucket(bucketName);

        TString failedKey;
        bool rejected = false;
        bool probed = false;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            RetryFirstCoolingPut(*ev, failedKey);
            if (ev->GetTypeRewrite() == TEvPutObjectResponse::EventType && !rejected) {
                auto& put = *ev->Get<TEvPutObjectResponse>();
                if (put.Key && *put.Key == failedKey) {
                    Aws::Client::AWSError<Aws::S3::S3Errors> error(
                        Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied", "denied", false);
                    error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);
                    put.Result = TEvPutObjectResponse::TResult(Aws::S3::S3Error(std::move(error)));
                    rejected = true;
                }
            } else if (ev->GetTypeRewrite() == TEvHeadObjectRequest::EventType) {
                probed |= ev->Get<TEvHeadObjectRequest>()->GetRequest().GetKey() == failedKey;
            }
            return true;
        };
        PutToS3VirtualGroup(env, edge, groupId, id, "abcdefgh");
        WaitForS3Condition(env, [&] { return rejected && probed && !bucket.GetObject(failedKey) && bucket.GetSize() == 1; });
        // An attempt preceding the authorization failure lands after the first cleanup.
        bucket.PutObject(failedKey, "abcdefgh");
        WaitForS3Condition(env, [&] { return !bucket.GetObject(failedKey); });
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, id), "abcdefgh");
        env.Runtime->FilterFunction = {};
    }

    struct TRetriedCoolingPut {
        TEnvironmentSetup Env{MakeCoolingTestSettings()};
        const TString BucketName = "async-retried-success";
        const TLogoBlobID Id{100500, 1, 1, 0, 8, 0};
        ui32 Group;
        TActorId Edge;
        TString Key;
        std::unique_ptr<IEventHandle> Reply;
        ui32 Node = 0;

        TRetriedCoolingPut() {
            using namespace NWrappers::NExternalStorage;
            NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
            async.SetTargetObjectBytes(8);
            Group = CreateS3VirtualGroup(Env, BucketName, &async);
            Edge = Env.Runtime->AllocateEdgeActor(1);
            CaptureRetriedPut(Id);
        }

        void CaptureRetriedPut(TLogoBlobID id) {
            using namespace NWrappers::NExternalStorage;
            Key.clear();
            Env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
                RetryFirstCoolingPut(*ev, Key);
                if (ev->GetTypeRewrite() == TEvPutObjectResponse::EventType) {
                    const auto& result = *ev->Get<TEvPutObjectResponse>();
                    if (result.Key && *result.Key == Key) {
                        UNIT_ASSERT(result.IsSuccess());
                        Node = node;
                        Reply = std::move(ev);
                        return false;
                    }
                }
                return true;
            };
            PutToS3VirtualGroup(Env, Edge, Group, id, "abcdefgh");
            WaitForS3Condition(Env, [&] { return bool(Reply); });
            Env.Runtime->FilterFunction = {};
        }

        void DeliverReply() {
            Env.Runtime->Send(std::move(Reply), Node);
        }

        void CheckLatePutCollected() {
            auto& bucket = Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->MutableBucket(BucketName);
            WaitForS3Condition(Env, [&] { return !bucket.GetObject(Key); });
            bucket.PutObject(Key, "abcdefgh");
            WaitForS3Condition(Env, [&] { return !bucket.GetObject(Key); });
        }
    };

    Y_UNIT_TEST(AsyncS3WatchesRetriedPutAfterFailedPublication) {
        TRetriedCoolingPut f;
        // Invalidate the pack's compare-and-swap while the successful reply is still in flight.
        PutToS3VirtualGroup(f.Env, f.Edge, f.Group, f.Id, "abcdefgh");
        f.DeliverReply();
        f.CheckLatePutCollected();
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(f.Env, f.Edge, f.Group, f.Id), "abcdefgh");
    }

    Y_UNIT_TEST(AsyncS3WatchesRetriedPutAfterLastReferenceIsCollected) {
        using namespace NWrappers::NExternalStorage;
        TRetriedCoolingPut f;
        f.DeliverReply();
        WaitForPublishedS3Range(f.Env, f.Edge, f.Group, f.Id, f.Id);

        bool probedLive = false;
        f.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvHeadObjectRequest::EventType) {
                probedLive |= ev->Get<TEvHeadObjectRequest>()->GetRequest().GetKey() == f.Key;
            }
            return true;
        };
        const auto tablet = TryGetBlobDepotTabletId(f.Env, f.Group);
        UNIT_ASSERT(tablet);
        RebootBlobDepotTablet(f.Env, *tablet);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(f.Env, f.Edge, f.Group, f.Id), "abcdefgh");
        UNIT_ASSERT(!probedLive);
        f.Env.Runtime->FilterFunction = {};
        SendTEvCollectGarbage(f.Env, f.Edge, f.Group, f.Id.TabletID(), 1, 1, f.Id.Channel(), true, 1, 1,
            nullptr, nullptr, false, true);
        const auto gc = CaptureTEvCollectGarbageResult(f.Env, f.Edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(gc->Get()->Status, NKikimrProto::OK, gc->Get()->ToString());
        f.CheckLatePutCollected();
    }

    void RebootAndCheckPutWatchBackoff(TRetriedCoolingPut& f, ui64 tablet, THashMap<TString, TInstant> lastHeads,
            const TString& lastProbedKey) {
        using namespace NWrappers::NExternalStorage;
        auto& runtime = *f.Env.Runtime;
        const auto count = lastHeads.size();
        // The serial probe worker may still be committing the last HEAD's next due time when we reboot.
        lastHeads.erase(lastProbedKey);
        const auto router = MakeBlobDepotS3RouterID(tablet);
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvHeadObjectRequest::EventType && ev->Recipient == router) {
                const auto& key = ev->Get<TEvHeadObjectRequest>()->GetRequest().GetKey();
                if (auto it = lastHeads.find(TString(key.c_str())); it != lastHeads.end()) {
                    UNIT_ASSERT(runtime.GetClock() >= it->second + TDuration::Seconds(8));
                    lastHeads.erase(it);
                }
            }
            return true;
        };
        SetDataCoolingEnabled(runtime, false);
        RebootBlobDepotTablet(f.Env, tablet);
        UNIT_ASSERT_VALUES_EQUAL(GetUnfinishedPutWatchCount(f.Env, tablet), count);
        WaitForS3Condition(f.Env, [&] { return lastHeads.empty(); }, TDuration::Minutes(2));
        runtime.FilterFunction = {};
    }

    void CheckLatePutsAfterWatchRetirement(TRetriedCoolingPut& f, ui64 tablet, const std::array<TString, 2>& lateKeys) {
        using namespace NWrappers::NExternalStorage;
        auto& runtime = *f.Env.Runtime;
        const auto router = MakeBlobDepotS3RouterID(tablet);
        auto& bucket = Singleton<TFakeExternalStorage>()->MutableBucket(f.BucketName);
        std::unique_ptr<IEventHandle> heldList;
        ui32 listNode = 0;
        runtime.FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvListObjectsRequest::EventType && ev->Recipient == router) {
                heldList = std::move(ev);
                listNode = node;
                return false;
            }
            return true;
        };
        // Put the old bytes back after the final HEADs but before the next LIST. No restart triggers collection.
        WaitForS3Condition(f.Env, [&] { return bool(heldList); }, TDuration::Minutes(65));
        UNIT_ASSERT_VALUES_EQUAL(GetUnfinishedPutWatchCount(f.Env, tablet), 0);
        for (const auto& key : lateKeys) {
            bucket.PutObject(key, "abcdefgh");
        }
        std::vector<TInstant> listPages;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvListObjectsRequest::EventType && ev->Recipient == router) {
                listPages.push_back(runtime.GetClock());
                ev->Get<TEvListObjectsRequest>()->MutableRequest().SetMaxKeys(1);
            }
            return true;
        };
        runtime.Send(std::move(heldList), listNode);
        WaitForS3Condition(f.Env, [&] {
            return listPages.size() >= 3 && std::ranges::none_of(lateKeys, [&](const TString& key) {
                return bucket.GetObject(key).has_value();
            });
        }, TDuration::Minutes(5));
        for (size_t i = 1; i < listPages.size(); ++i) {
            UNIT_ASSERT(listPages[i] >= listPages[i - 1] + TDuration::Minutes(1));
        }
        runtime.FilterFunction = {};
    }

    Y_UNIT_TEST(AsyncS3RetiresHistoricalPutWatchesWithoutDelayingGC) {
        using namespace NWrappers::NExternalStorage;
        TRetriedCoolingPut f;
        auto& runtime = *f.Env.Runtime;
        const auto tablet = WaitForS3VirtualGroup(f.Env, f.Group);
        const auto router = MakeBlobDepotS3RouterID(tablet);
        auto& bucket = Singleton<TFakeExternalStorage>()->MutableBucket(f.BucketName);
        auto collect = [&](ui64 tabletId, ui32 lastStep) {
            SendTEvCollectGarbage(f.Env, f.Edge, f.Group, tabletId, 1, 1, f.Id.Channel(), true, 1, lastStep,
                nullptr, nullptr, false, true);
            const auto gc = CaptureTEvCollectGarbageResult(f.Env, f.Edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(gc->Get()->Status, NKikimrProto::OK, gc->Get()->ToString());
        };

        const TString freshKey = f.Key;
        f.DeliverReply();
        WaitForPublishedS3Range(f.Env, f.Edge, f.Group, f.Id, f.Id);

        // Produce a backlog larger than one watch page using real retried PUTs and final-reference GC.
        constexpr ui32 count = 33;
        const auto historicalTablet = f.Id.TabletID() + 1;
        for (ui32 i = 1; i <= count; ++i) {
            const TLogoBlobID id(historicalTablet, 1, i, 0, 8, 0);
            f.CaptureRetriedPut(id);
            f.DeliverReply();
            WaitForPublishedS3Range(f.Env, f.Edge, f.Group, id, id);
        }
        const auto historicalKey = f.Key;
        const TLogoBlobID live(historicalTablet + 1, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(f.Env, f.Edge, f.Group, live, "survives");
        WaitForPublishedS3Range(f.Env, f.Edge, f.Group, live, live);
        UNIT_ASSERT_VALUES_EQUAL(GetUnfinishedPutWatchCount(f.Env, tablet), 0);

        bool stall = true;
        std::unique_ptr<IEventHandle> heldHead;
        ui32 heldNode = 0;
        THashMap<TString, ui32> attempts;
        THashMap<TString, TInstant> lastHead;
        TString lastProbedKey;
        runtime.FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvHeadObjectRequest::EventType && ev->Recipient == router) {
                const TString key(ev->Get<TEvHeadObjectRequest>()->GetRequest().GetKey().c_str());
                ++attempts[key];
                lastHead[key] = runtime.GetClock();
                lastProbedKey = key;
            } else if (stall && !heldHead && ev->GetTypeRewrite() == TEvHeadObjectResponse::EventType) {
                heldNode = node;
                heldHead = std::move(ev);
                return false;
            }
            return true;
        };

        collect(historicalTablet, count);
        WaitForS3Condition(f.Env, [&] { return heldHead && bucket.GetSize() == 2; });
        UNIT_ASSERT_VALUES_EQUAL(GetUnfinishedPutWatchCount(f.Env, tablet), count);
        collect(f.Id.TabletID(), f.Id.Step());
        WaitForS3Condition(f.Env, [&] { return !bucket.GetObject(freshKey); });
        UNIT_ASSERT_VALUES_EQUAL(attempts.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetUnfinishedPutWatchCount(f.Env, tablet), count + 1);

        stall = false;
        runtime.Send(std::move(heldHead), heldNode);
        WaitForS3Condition(f.Env, [&] {
            return attempts.size() == count + 1 && std::ranges::all_of(attempts, [](const auto& entry) {
                return entry.second >= 5;
            });
        }, TDuration::Minutes(2));

        RebootAndCheckPutWatchBackoff(f, tablet, std::move(lastHead), lastProbedKey);
        CheckLatePutsAfterWatchRetirement(f, tablet, {freshKey, historicalKey});
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(f.Env, f.Edge, f.Group, live), "survives");
    }

    Y_UNIT_TEST(AsyncS3BacksOffFailedDeletes) {
        using namespace NWrappers::NExternalStorage;

        TEnvironmentSetup env(MakeCoolingTestSettings());
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        async.SetTargetObjectBytes(8);
        const TString bucketName = "async-delete-backoff";
        const ui32 groupId = CreateS3VirtualGroup(env, bucketName, &async);
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        const TLogoBlobID live(100500, 1, 2, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, groupId, id, "abcdefgh");
        WaitForPublishedS3Range(env, edge, groupId, id, id);
        const auto& bucket = Singleton<TFakeExternalStorage>()->GetBucket(bucketName);
        const TString garbageKey = bucket.GetObjectKeys().at(0);
        PutToS3VirtualGroup(env, edge, groupId, live, "survives");
        WaitForPublishedS3Range(env, edge, groupId, live, live);

        Aws::Client::AWSError<Aws::S3::S3Errors> error(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied", "denied", false);
        error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);
        const Aws::S3::S3Error denied(std::move(error));
        std::vector<TInstant> deleteReplies;
        std::unique_ptr<IEventHandle> lateReply;
        ui32 lateNode = 0;
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDeleteObjectResponse::EventType) {
                auto& deletion = *ev->Get<TEvDeleteObjectResponse>();
                if (deletion.IsSuccess() && deletion.Key && *deletion.Key == garbageKey) {
                    deleteReplies.push_back(env.Runtime->GetClock());
                    if (deleteReplies.size() <= 2) {
                        deletion.Result = TEvDeleteObjectResponse::TResult(denied);
                    } else if (deleteReplies.size() == 3) {
                        lateReply = std::move(ev);
                        lateNode = node;
                        return false;
                    }
                }
            }
            return true;
        };

        SendTEvCollectGarbage(env, edge, groupId, id.TabletID(), 1, 1, id.Channel(), true, 1, 1,
            nullptr, nullptr, false, true);
        const auto collected = CaptureTEvCollectGarbageResult(env, edge, false);
        UNIT_ASSERT_VALUES_EQUAL(collected->Get()->Status, NKikimrProto::OK);
        WaitForS3Condition(env, [&] { return deleteReplies.size() >= 4; }, TDuration::Minutes(3));
        UNIT_ASSERT(deleteReplies[1] - deleteReplies[0] >= TDuration::MilliSeconds(100));
        UNIT_ASSERT(deleteReplies[2] - deleteReplies[1] >= TDuration::MilliSeconds(100));
        UNIT_ASSERT(deleteReplies[3] - deleteReplies[2] >= TDuration::Minutes(2));
        env.Runtime->FilterFunction = {};
        env.Runtime->Send(std::move(lateReply), lateNode);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, groupId, live), "survives");
    }

    Y_UNIT_TEST_TWIN(S3GetResolvesOnlyAnExplicitMissingKey, noSuchKey) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings());
        const ui32 group = CreateS3VirtualGroup(env, "get-missing-key");
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, group, id, "abcdefgh");
        bool failed = false;
        ui32 resolvesAfterFailure = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvGetObjectResponse::EventType) {
                Aws::Client::AWSError<Aws::S3::S3Errors> error(
                    noSuchKey ? Aws::S3::S3Errors::NO_SUCH_KEY : Aws::S3::S3Errors::RESOURCE_NOT_FOUND,
                    noSuchKey ? "NoSuchKey" : "", "not found", false);
                error.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
                ev->Get<TEvGetObjectResponse>()->Result = TEvGetObjectResponse::TResult(Aws::S3::S3Error(std::move(error)));
                failed = true;
            } else if (failed && ev->GetTypeRewrite() == TEvBlobDepot::EvResolve) {
                ++resolvesAfterFailure;
            }
            return true;
        };
        SendTEvGet(env, edge, group, id);
        const auto result = CaptureTEvGetResult(env, edge, false);
        UNIT_ASSERT(result);
        UNIT_ASSERT(failed);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(bool(resolvesAfterFailure), noSuchKey);
        env.Runtime->FilterFunction = {};
    }

    // the value written to S3 has no blob parts; its integrity is checked by reading it from S3, and the check gets its
    // answer
    Y_UNIT_TEST(CheckIntegrityOfS3Value) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
            .SetupHive = true,
        });
        const TString bucket = "check-integrity";
        const ui32 groupId = CreateS3VirtualGroup(env, bucket);
        auto& storage = *Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>();

        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString data = "stored in S3";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToS3VirtualGroup(env, edge, groupId, id, data);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetBucket(bucket).GetSize(), 1);

        UNIT_ASSERT_VALUES_EQUAL(CheckS3VirtualGroupIntegrity(env, edge, groupId, id), NKikimrProto::OK);

        storage.Clear(); // the object disappears from S3
        UNIT_ASSERT_VALUES_EQUAL(CheckS3VirtualGroupIntegrity(env, edge, groupId, id), NKikimrProto::ERROR);
    }

    // the range query ends with error as soon as one of its S3 reads fails; the other reads complete after that, and
    // their results are dropped instead of going to the query that is gone
    Y_UNIT_TEST(S3ReadsCompletingAfterQueryEnd) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
            .SetupHive = true,
        });
        const ui32 groupId = CreateS3VirtualGroup(env, "query-end");

        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString data = "stored in S3";
        for (ui32 step = 1; step <= 5; ++step) {
            PutToS3VirtualGroup(env, edge, groupId, TLogoBlobID(100500, 1, step, 0, data.size(), 0), data);
        }

        // the first S3 read of the range fails, as if the wrapper was gone; the rest are held until the query ends with
        // error and is gone
        bool failed = false;
        bool holdReads = true;
        std::vector<std::unique_ptr<IEventHandle>> held;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != NWrappers::NExternalStorage::TEvGetObjectResponse::EventType || !holdReads) {
                return true;
            } else if (failed) {
                held.push_back(std::move(ev));
                return false;
            }
            failed = true;
            const TActorId recipient = ev->GetRecipientRewrite();
            env.Runtime->Send(new IEventHandle(recipient, ev->Sender, new TEvents::TEvUndelivered(
                NWrappers::NExternalStorage::EvGetObjectRequest, TEvents::TEvUndelivered::ReasonActorUnknown)),
                recipient.NodeId());
            return false;
        };

        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvRange(100500, TLogoBlobID(100500, 0, 0, 0, 0, 0),
                TLogoBlobID(100500, Max<ui32>(), Max<ui32>(), 0, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie),
                false, TInstant::Max()));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::ERROR, res->Get()->ToString());
        UNIT_ASSERT(failed);
        env.Sim(TDuration::Seconds(1)); // let the agent finish with the query
        UNIT_ASSERT(!held.empty());

        // the reads complete after the query is gone
        holdReads = false;
        for (auto& ev : held) {
            const ui32 nodeId = ev->GetRecipientRewrite().NodeId();
            env.Runtime->Send(ev.release(), nodeId);
        }
        env.Sim(TDuration::Seconds(10));
        env.Runtime->FilterFunction = {};

        // the agent goes on serving queries
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(TLogoBlobID(100500, 1, 1, 0, data.size(), 0), 0, 0,
                TInstant::Max(), NKikimrBlobStorage::FastRead));
        });
        auto getResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Get()->Responses[0].Buffer.ConvertToString(), data);
    }
#endif
}
