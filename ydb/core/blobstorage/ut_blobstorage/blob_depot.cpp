#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blob_depot/events.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/testlib/helpers.h>

#ifndef KIKIMR_DISABLE_S3_OPS
#include <ydb/core/base/hive.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blob_depot/s3_router_events.h>
#include <ydb/core/blob_depot/types.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <util/generic/scope.h>
#include <util/string/hex.h>
#include <util/string/printf.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/aws_init/aws.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/datetime/base.h>
#endif

#include "blob_depot_test_functions.h"

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
            NKikimrBlobDepot::TS3BackendSettings settings, const TString& name = "vg") {
        auto *cmd = request.AddCommand()->MutableAllocateVirtualGroup();
        cmd->SetName(name);
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

    template<class TResult>
    TAutoPtr<TEventHandle<TResult>> WaitForS3GroupReply(TEnvironmentSetup& env, TActorId edge,
            TInstant deadline, bool waitForHttp) {
        for (;;) {
            // Real HTTP callbacks run on an SDK thread outside the simulated actor clock.
            const auto until = waitForHttp ? Min(deadline, env.Runtime->GetClock() + TDuration::MilliSeconds(5)) : deadline;
            auto reply = env.WaitForEdgeActorEvent<TResult>(edge, false, until);
            if (reply || until == deadline) {
                return reply;
            }
            Sleep(TDuration::MilliSeconds(5));
        }
    }

    void PutToS3VirtualGroup(TEnvironmentSetup& env, const TActorId& edge, ui32 groupId, const TLogoBlobID& id,
            const TString& data, bool waitForHttp = false) {
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, deadline));
        });
        auto result = WaitForS3GroupReply<TEvBlobStorage::TEvPutResult>(env, edge, deadline + TDuration::Seconds(1), waitForHttp);
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
            ui32 shift = 0, ui32 size = 0, bool waitForHttp = false) {
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(10);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, shift, size, deadline, NKikimrBlobStorage::FastRead));
        });
        auto result = WaitForS3GroupReply<TEvBlobStorage::TEvGetResult>(env, edge, deadline + TDuration::Seconds(1), waitForHttp);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::OK, result->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Responses[0].Status, NKikimrProto::OK, result->Get()->ToString());
        return result->Get()->Responses[0].Buffer.ConvertToString();
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

        const size_t maxPrefix = TS3Locator::MaxBasePathBytes() - storageName.size() - 1;
        auto s3 = MakeS3Settings("key-length", nullptr, TString(maxPrefix + 1, 'p'));
        const auto tooLong = TryReconfigureS3Group(env, s3, "new");
        UNIT_ASSERT(!tooLong.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(tooLong.GetErrorDescription(), "1024 bytes");
        s3.MutableSettings()->SetObjectKeyPattern(TString(maxPrefix, 'p'));
        ReconfigureS3Group(env, s3, "new");

        const TString longName(TS3Locator::MaxBasePathBytes() + 1, 'g');
        group->SetName(longName);
        const auto localOnly = env.Invoke(allocate);
        UNIT_ASSERT_C(localOnly.GetSuccess(), localOnly.GetErrorDescription());
        WaitForS3VirtualGroup(env, localOnly.GetStatus(0).GetGroupId(0));
        const auto destination = MakeS3Settings("first-s3-assignment");
        const auto unshortened = TryReconfigureS3Group(env, destination, longName);
        UNIT_ASSERT(!unshortened.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(unshortened.GetErrorDescription(), "1024 bytes");
        const auto attached = TryReconfigureS3Group(env, destination, longName,
            {.ConfirmNamespaceMigration = true, .MigratedS3Name = "short-namespace"});
        UNIT_ASSERT_C(attached.GetSuccess(), attached.GetErrorDescription());
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

    Y_UNIT_TEST_TWIN(LegacyS3OwnersCannotCollectPeerData, asyncPeer) {
        using namespace NWrappers::NExternalStorage;
        const auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        runtime.GetNode(env.Settings.ControllerNodeId)->AppData->FunctionRegistry = registry.Get();

        const TString bucketName = "shared-legacy-namespace";
        const TString peerName = "team/vg";
        THashMap<ui32, NKikimrBlobDepot::TBlobDepotConfig> legacy;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                auto& config = *ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.MutableConfig();
                const bool peer = TStringBuf(config.GetName()).StartsWith("team/");
                config.SetName(peer ? peerName : "vg");
                config.MutableS3BackendSettings()->MutableSettings()->SetObjectKeyPattern(peer ? "archive" : "archive/team");
                config.ClearS3NamespaceValidated();
                config.ClearS3NamespacePinned();
                config.ClearEverAuthorizedDataCooling();
                legacy[config.GetVirtualGroupId()] = config;
            }
            return true;
        };
        const ui32 writer = CreateS3VirtualGroup(env, bucketName, nullptr, "archive/team");
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        NKikimrBlobStorage::TConfigRequest createPeer;
        AddS3VirtualGroup(env, createPeer, MakeS3Settings(bucketName, asyncPeer ? &async : nullptr, "temporary"), peerName);
        const auto created = env.Invoke(createPeer);
        UNIT_ASSERT_C(created.GetSuccess(), created.GetErrorDescription());
        const ui32 peer = created.GetStatus(0).GetGroupId(0);
        const ui64 peerTablet = WaitForS3VirtualGroup(env, peer);
        WaitForS3Condition(env, [&] { return legacy.contains(writer) && legacy.contains(peer); });
        runtime.FilterFunction = {};
        StoreLegacyS3Config(env, legacy.at(writer), "vg");
        StoreLegacyS3Config(env, legacy.at(peer), peerName);
        env.RebootTablet(MakeBSControllerID());
        for (const TString& name : {TString("vg"), peerName}) {
            const auto validation = TryReconfigureS3Group(env, {}, name);
            UNIT_ASSERT(!validation.GetSuccess());
            UNIT_ASSERT_STRING_CONTAINS(validation.GetErrorDescription(), "effective key prefix");
        }

        const auto edge = runtime.AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, writer, id, "retained");
        const auto& bucket = Singleton<TFakeExternalStorage>()->GetBucket(bucketName);
        const auto keys = bucket.GetObjectKeys();
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        UNIT_ASSERT(TStringBuf(keys.front()).StartsWith("archive/team/vg/"));
        RebootBlobDepotTablet(env, peerTablet);
        ConnectToVirtualGroupAgent(env, edge, peer);
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetObject(keys.front()).value_or(""), "retained");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, runtime.AllocateEdgeActor(2), writer, id), "retained");

        // The peer never wrote S3 objects, so its namespace can be migrated to an unused prefix.
        const auto destination = MakeS3Settings(bucketName, asyncPeer ? &async : nullptr, "dedicated");
        const auto migrated = TryReconfigureS3Group(env, destination, peerName, {.ConfirmNamespaceMigration = true});
        UNIT_ASSERT_C(migrated.GetSuccess(), migrated.GetErrorDescription());
        NKikimrBlobDepot::TBlobDepotConfig validated;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                const auto& config = ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.GetConfig();
                if (config.GetVirtualGroupId() == writer) {
                    validated = config;
                }
            }
            return true;
        };
        ReconfigureS3Group(env, {}, "vg");
        WaitForS3Condition(env, [&] { return validated.GetS3NamespaceValidated(); });
        UNIT_ASSERT(!validated.GetEverAuthorizedDataCooling());
        runtime.FilterFunction = {};
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

    Y_UNIT_TEST(S3RetainsAmbiguousRetiredBucketAcrossRestart) {
        const auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        runtime.GetNode(env.Settings.ControllerNodeId)->AppData->FunctionRegistry = registry.Get();
        NKikimrBlobDepot::TBlobDepotConfig retired;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                retired = ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.GetConfig();
            }
            return true;
        };
        const TString bucket = "legacy-dotted-retirement";
        const auto retiredId = CreateS3VirtualGroup(env, bucket);
        WaitForS3Condition(env, [&] { return retired.GetVirtualGroupId() == retiredId; });
        runtime.FilterFunction = {};
        NKikimrBlobStorage::TConfigRequest createPeer;
        AddS3VirtualGroup(env, createPeer, MakeS3Settings("current-bucket"), "peer");
        const auto peer = env.Invoke(createPeer);
        UNIT_ASSERT_C(peer.GetSuccess(), peer.GetErrorDescription());
        WaitForS3VirtualGroup(env, peer.GetStatus(0).GetGroupId(0));
        retired.MutableS3BackendSettings()->MutableSettings()->SetObjectKeyPattern("archive/../shared");
        StoreLegacyS3Config(env, retired, "vg");
        env.RebootTablet(MakeBSControllerID());

        NKikimrBlobStorage::TConfigRequest remove;
        remove.AddCommand()->MutableDeleteSpecificGroups()->AddGroupIds(retiredId);
        const auto deleted = env.Invoke(remove);
        UNIT_ASSERT_C(deleted.GetSuccess(), deleted.GetErrorDescription());
        const auto settings = MakeS3Settings(bucket, nullptr, "shared");
        NKikimrBlobStorage::TConfigRequest claim;
        AddS3VirtualGroup(env, claim, settings, "candidate");
        const auto checkReservation = [&] {
            const auto allocated = env.Invoke(claim);
            UNIT_ASSERT(!allocated.GetSuccess());
            UNIT_ASSERT_STRING_CONTAINS(allocated.GetErrorDescription(), "ambiguous key prefix");
            const auto migrated = TryReconfigureS3Group(env, settings, "peer", {.ConfirmNamespaceMigration = true});
            UNIT_ASSERT(!migrated.GetSuccess());
            UNIT_ASSERT_STRING_CONTAINS(migrated.GetErrorDescription(), "ambiguous key prefix");
        };
        checkReservation();
        env.RebootTablet(MakeBSControllerID());
        checkReservation();
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

    Y_UNIT_TEST(S3ReservesRetiredNamespaceAcrossRestart) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings());
        const TString bucketName = "retired-namespace";
        const TString prefix = "archive";
        TString retiredStorageName;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                retiredStorageName = ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.GetConfig().GetName();
            }
            return true;
        };
        const ui32 retired = CreateS3VirtualGroup(env, bucketName, nullptr, prefix);
        const auto tabletId = TryGetBlobDepotTabletId(env, retired);
        UNIT_ASSERT(tabletId && !retiredStorageName.empty());
        env.Runtime->FilterFunction = {};
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, retired, id, "old-data");
        auto& bucket = Singleton<TFakeExternalStorage>()->MutableBucket(bucketName);
        const auto oldKeys = bucket.GetObjectKeys();
        UNIT_ASSERT_VALUES_EQUAL(oldKeys.size(), 1);

        NKikimrBlobStorage::TConfigRequest replace;
        replace.AddCommand()->MutableDeleteSpecificGroups()->AddGroupIds(retired);
        AddS3VirtualGroup(env, replace, MakeS3Settings(bucketName, nullptr, prefix));
        auto claim = replace;
        auto* claimant = claim.MutableCommand(1)->MutableAllocateVirtualGroup();
        claimant->SetName(retiredStorageName);
        claimant->SetBlobDepotId(*tabletId);
        const auto collision = env.Invoke(claim);
        UNIT_ASSERT(!collision.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(collision.GetErrorDescription(), "reserved for retired BlobDepot cleanup");

        const auto response = env.Invoke(replace);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        const ui32 replacement = response.GetStatus(1).GetGroupId(0);
        UNIT_ASSERT_UNEQUAL(WaitForS3VirtualGroup(env, replacement), *tabletId);
        PutToS3VirtualGroup(env, edge, replacement, id, "new-data");
        bucket.PutObject(oldKeys.front(), "old-data");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, replacement, id), "new-data");

        env.RebootTablet(MakeBSControllerID());
        claim.MutableCommand()->DeleteSubrange(0, 1);
        const auto recovered = env.Invoke(claim);
        UNIT_ASSERT(!recovered.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(recovered.GetErrorDescription(), "reserved for retired BlobDepot cleanup");
    }

    Y_UNIT_TEST(S3RetirementRequiresConfirmedHiveDeletion) {
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        const ui32 group = CreateS3VirtualGroup(env, "retire-after-hive");
        std::vector<TInstant> replies;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvHive::TEvDeleteTabletReply::EventType) {
                auto& reply = ev->Get<TEvHive::TEvDeleteTabletReply>()->Record;
                if (reply.ShardLocalIdxSize() == 1 && reply.GetShardLocalIdx(0) == group) {
                    replies.push_back(env.Runtime->GetClock());
                    if (replies.size() == 1) {
                        reply.SetStatus(NKikimrProto::INVALID_OWNER);
                        reply.MutableForwardRequest()->SetHiveTabletId(reply.GetOrigin());
                    } else if (replies.size() <= 3) {
                        reply.SetStatus(NKikimrProto::ERROR);
                    }
                }
            }
            return true;
        };

        NKikimrBlobStorage::TConfigRequest remove;
        remove.AddCommand()->MutableDeleteSpecificGroups()->AddGroupIds(group);
        const auto result = env.Invoke(remove);
        UNIT_ASSERT_C(result.GetSuccess(), result.GetErrorDescription());
        // Follow Hive's redirect immediately, then retry the transient failure on its timer.
        WaitForS3Condition(env, [&] { return replies.size() == 2; });
        WaitForS3Condition(env, [&] { return replies.size() == 3; }, TDuration::Minutes(2));
        UNIT_ASSERT(replies[2] >= replies[1] + TDuration::Minutes(1));
        // A rejected delete must also survive BSC recovery, which retries the durable queue.
        env.RebootTablet(MakeBSControllerID());
        WaitForS3Condition(env, [&] { return replies.size() == 4; });
        env.Runtime->FilterFunction = {};
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

    Y_UNIT_TEST(S3RetirementDropsCredentialsForMalformedPrefixWithoutSweeping) {
        using namespace NWrappers::NExternalStorage;
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
        const TString bucket = "legacy-malformed-retirement";
        const ui32 group = CreateS3VirtualGroup(env, bucket);
        WaitForS3Condition(env, [&] { return legacy.GetVirtualGroupId() == group; });
        runtime.FilterFunction = {};
        legacy.MutableS3BackendSettings()->MutableSettings()->SetObjectKeyPattern("archive/../shared");
        legacy.MutableS3BackendSettings()->MutableSettings()->SetAccessKey("retired-access-key");
        StoreLegacyS3Config(env, legacy, "vg");
        env.RebootTablet(MakeBSControllerID());

        bool hiveDeleted = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            const ui32 type = ev->GetTypeRewrite();
            if (type == TEvHive::TEvDeleteTabletReply::EventType) {
                const auto& reply = ev->Get<TEvHive::TEvDeleteTabletReply>()->Record;
                hiveDeleted |= reply.ShardLocalIdxSize() == 1 && reply.GetShardLocalIdx(0) == group &&
                    reply.GetStatus() == NKikimrProto::OK;
            } else if (hiveDeleted) {
                UNIT_ASSERT(type != TEvListObjectsRequest::EventType && type != TEvDeleteObjectRequest::EventType &&
                    type != TEvDeleteObjectsRequest::EventType);
            }
            return true;
        };
        NKikimrBlobStorage::TConfigRequest retire;
        retire.AddCommand()->MutableDeleteSpecificGroups()->AddGroupIds(group);
        UNIT_ASSERT(env.Invoke(retire).GetSuccess());
        WaitForS3Condition(env, [&] { return hiveDeleted; });

        NKikimrBlobDepot::TBlobDepotConfig expected;
        expected.SetName("archive/../shared/vg");
        expected.MutableS3BackendSettings()->MutableSettings()->SetBucket(bucket);
        const auto deadline = runtime.GetClock() + TDuration::Seconds(30);
        while (ReadRetiredS3Config(env, group).SerializeAsString() != expected.SerializeAsString()) {
            UNIT_ASSERT_C(runtime.GetClock() < deadline, "retired namespace still retains credentials");
            env.Sim(TDuration::MilliSeconds(100));
        }
        env.RebootTablet(MakeBSControllerID());
        UNIT_ASSERT_VALUES_EQUAL(ReadRetiredS3Config(env, group).SerializeAsString(), expected.SerializeAsString());
        runtime.FilterFunction = {};
    }

    Y_UNIT_TEST(S3RetirementHonorsCoolingRollout) {
        using namespace NWrappers::NExternalStorage;
        for (bool previouslyAuthorized : {false, true}) {
            TEnvironmentSetup env(MakeCoolingTestSettings(previouslyAuthorized));
            auto& runtime = *env.Runtime;
            const TString bucketName = "retired-s3-rollout";
            NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
            const ui32 group = CreateS3VirtualGroup(env, bucketName);
            const auto edge = runtime.AllocateEdgeActor(1);
            const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
            PutToS3VirtualGroup(env, edge, group, id, "old-data");
            const auto& bucket = Singleton<TFakeExternalStorage>()->GetBucket(bucketName);
            WaitForS3Condition(env, [&] { return bucket.GetSize() == 1; });
            if (previouslyAuthorized) {
                ReconfigureS3Group(env, MakeS3Settings(bucketName, &async));
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
            auto reply = env.WaitForEdgeActorEvent<NStorage::TEvNodeWardenBlobDepotS3RouterReady>(sender, false,
                runtime.GetClock() + TDuration::Seconds(2));
            UNIT_ASSERT(reply);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Generation, generation);
            return reply->Get()->Router;
        };
        auto release = [&](TActorId sender, ui32 generation) {
            runtime.Send(new IEventHandle(warden, sender, new NStorage::TEvNodeWardenReleaseBlobDepotS3Router(
                tablet, generation)), node);
        };

        const TString source = "router-migration-source";
        const TString destination = "router-migration-destination";
        const auto old = acquire(first, 1, source);
        UNIT_ASSERT(old && getRouter() == old);
        const auto current = acquire(second, 2, destination);
        UNIT_ASSERT(current && current != old && getRouter() == current);

        ui32 releases = 0;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            releases += ev->GetTypeRewrite() == TEvBlobStorage::EvNodeWardenReleaseBlobDepotS3Router;
            return true;
        };
        UNIT_ASSERT(!acquire(second, 1, source)); // delayed acquire from the agent's older registration
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

    Y_UNIT_TEST(S3TabletRestartReusesConfirmedEndpointOnlyForTheSameRoute) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env({.NodeCount = 1, .SetupHive = true});
        auto& runtime = *env.Runtime;
        bool balancerAvailable = true;
        bool tabletReleased = false;
        bool reacquiredAfterLastRelease = false;
        const TString bucket = "restart-handoff";
        auto settings = MakeS3Settings(bucket);
        settings.SetBalancerHost("handoff.test");
        runtime.FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NHttp::TEvHttpProxy::EvHttpOutgoingRequest) {
                const auto request = ev->Get<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>()->Request;
                if (request->Host == "handoff.test") {
                    if (balancerAvailable) {
                        auto incoming = request->Reverse();
                        incoming->Endpoint = std::make_shared<NHttp::THttpEndpointInfo>();
                        auto reply = incoming->CreateResponseOK("proxy.test:3128", "text/plain");
                        runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                            new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(request, reply->Reverse(request))), node);
                    }
                    return false;
                }
            } else if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenReleaseBlobDepotS3Router::EventType) {
                tabletReleased |= ev->Get<NStorage::TEvNodeWardenReleaseBlobDepotS3Router>()->RestartingForConfigChange;
            } else if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenAcquireBlobDepotS3Router::EventType && tabletReleased) {
                const auto id = ev->Get<NStorage::TEvNodeWardenAcquireBlobDepotS3Router>()->TabletId;
                reacquiredAfterLastRelease |= !runtime.GetNode(node)->ActorSystem->LookupLocalService(MakeBlobDepotS3RouterID(id));
            }
            return true;
        };
        Y_DEFER { runtime.FilterFunction = {}; };
        const ui64 tablet = WaitForS3VirtualGroup(env, CreateS3VirtualGroup(env, settings));
        const auto router = [&] { return runtime.GetNode(1)->ActorSystem->LookupLocalService(MakeBlobDepotS3RouterID(tablet)); };
        const auto edge = runtime.AllocateEdgeActor(1);
        const auto put = [&](const TString& data) {
            runtime.WrapInActorContext(edge, [&] {
                auto req = Aws::S3::Model::PutObjectRequest().WithBucket(bucket).WithKey("probe");
                auto ctx = std::make_shared<TS3PutContext>(TActivationContext::Monotonic() + TDuration::Seconds(1),
                    TS3PutContext::ESource::Agent);
                TActivationContext::Send(new IEventHandle(router(), edge, new TEvPutObjectRequest(req, TString(data), std::move(ctx))));
            });
            auto reply = env.WaitForEdgeActorEvent<TEvPutObjectResponse>(edge, false, runtime.GetClock() + TDuration::Seconds(2));
            UNIT_ASSERT(reply);
            return reply->Get()->IsSuccess();
        };
        WaitForS3Condition(env, [&] { return bool(router()); });
        const auto original = router();
        UNIT_ASSERT(put("initial"));

        balancerAvailable = false;
        settings.SetMetricsPushIntervalMs(100);
        settings.MutableSettings()->SetAccessKey("rotated");
        ReconfigureS3Group(env, settings);
        WaitForS3Condition(env, [&] { return router() && router() != original; });
        UNIT_ASSERT(reacquiredAfterLastRelease);
        UNIT_ASSERT(put("same-route"));

        const auto previous = router();
        settings.MutableSettings()->SetRegion("new-region");
        const auto result = TryReconfigureS3Group(env, settings, "vg", {.ConfirmRouteChange = true});
        UNIT_ASSERT_C(result.GetSuccess(), result.GetErrorDescription());
        WaitForS3Condition(env, [&] { return router() && router() != previous; });
        UNIT_ASSERT(!put("different-route"));
        UNIT_ASSERT_VALUES_EQUAL(Singleton<TFakeExternalStorage>()->GetBucket(bucket).GetObject("probe").value(), "same-route");
    }

    Y_UNIT_TEST(ForegroundPutWaitsForItsS3Router) {
        NKikimr::InitAwsAPI();
        Y_DEFER { NKikimr::ShutdownAwsAPI(); };
        TPortManager ports;
        const ui16 port = ports.GetPort();
        using TS3Mock = NWrappers::NTestHelpers::TS3Mock;
        TS3Mock destination{TS3Mock::TSettings(port)};
        UNIT_ASSERT_C(destination.Start(), destination.GetError());
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        const TString bucket = "acquire-race";
        const auto oldSettings = MakeS3Settings(bucket);
        auto settings = oldSettings;
        settings.MutableSettings()->SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
        settings.MutableSettings()->SetUseVirtualAddressing(false);
        settings.MutableSettings()->SetEndpoint(TStringBuilder() << "127.0.0.1:" << port);
        const auto group = CreateS3VirtualGroup(env, settings);
        const auto tablet = WaitForS3VirtualGroup(env, group);
        const auto service = MakeBlobDepotS3RouterID(tablet);

        // Leave an older generation's router registered at a node receiving its first foreground request.
        ui32 node = 0;
        for (const auto candidate : runtime.GetNodes()) {
            if (!runtime.GetNode(candidate)->ActorSystem->LookupLocalService(service)) {
                node = candidate;
                break;
            }
        }
        UNIT_ASSERT(node);
        const auto oldOwner = runtime.AllocateEdgeActor(node);
        runtime.Send(new IEventHandle(MakeBlobStorageNodeWardenID(node), oldOwner,
            new NStorage::TEvNodeWardenAcquireBlobDepotS3Router(tablet, 0, oldSettings)), node);
        const auto oldRoute = env.WaitForEdgeActorEvent<NStorage::TEvNodeWardenBlobDepotS3RouterReady>(oldOwner, false,
            runtime.GetClock() + TDuration::Seconds(2));
        UNIT_ASSERT(oldRoute && oldRoute->Get()->Router);

        std::unique_ptr<IEventHandle> acquire;
        runtime.FilterFunction = [&](ui32 recipientNode, std::unique_ptr<IEventHandle>& ev) {
            if (recipientNode == node && ev->GetTypeRewrite() == NStorage::TEvNodeWardenAcquireBlobDepotS3Router::EventType &&
                    ev->Get<NStorage::TEvNodeWardenAcquireBlobDepotS3Router>()->TabletId == tablet) {
                acquire = std::move(ev);
                return false;
            }
            return true;
        };
        const auto edge = runtime.AllocateEdgeActor(node);
        const auto deadline = runtime.GetClock() + TDuration::Seconds(30);
        runtime.WrapInActorContext(edge, [&] { SendToBSProxy(edge, group, new TEvBlobStorage::TEvStatus(deadline)); });
        WaitForS3Condition(env, [&] { return bool(acquire); });
        const TString data = "acknowledged";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        runtime.WrapInActorContext(edge, [&] { SendToBSProxy(edge, group, new TEvBlobStorage::TEvPut(id, data, deadline)); });
        const auto pending = GetServiceCounters(runtime.GetNode(node)->AppData->Counters, "blob_depot_agent")
            ->GetSubgroup("group", ToString(group))->GetSubgroup("subsystem", "pendingEventQueue")->GetCounter("Items");
        const auto& source = Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket(bucket);
        WaitForS3Condition(env, [&] { return pending->Val() == 2 || source.GetSize(); });
        UNIT_ASSERT_VALUES_EQUAL(source.GetSize(), 0);
        UNIT_ASSERT(destination.GetData().empty());
        runtime.FilterFunction = {};
        runtime.Send(acquire.release(), node);
        const auto put = WaitForS3GroupReply<TEvBlobStorage::TEvPutResult>(env, edge, deadline, true);
        UNIT_ASSERT(put);
        UNIT_ASSERT_VALUES_EQUAL_C(put->Get()->Status, NKikimrProto::OK, put->Get()->ErrorReason);
        UNIT_ASSERT_VALUES_EQUAL(source.GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(destination.GetData().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(destination.GetData().begin()->second, data);
    }

    Y_UNIT_TEST(MissingHotReadResolvesWhileWaitingForS3Router) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        const TString bucket = "reconnect-missing-hot-read";
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        const ui32 group = CreateS3VirtualGroup(env, bucket, &async);
        const auto reader = runtime.AllocateEdgeActor(1);
        const auto agent = ConnectToVirtualGroupAgent(env, reader, group);
        const TString data = "immutable-value";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToS3VirtualGroup(env, reader, group, id, data);

        std::unique_ptr<IEventHandle> hotReply;
        std::unique_ptr<IEventHandle> ready;
        ui32 s3Reads = 0;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Recipient == agent && ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && !ready) {
                auto& reply = *ev->Get<TEvBlobStorage::TEvGetResult>();
                UNIT_ASSERT_VALUES_EQUAL(reply.ResponseSz, 1);
                reply.Responses[0].Status = NKikimrProto::NODATA;
                hotReply = std::move(ev);
                return false;
            } else if (ev->Recipient == agent && ev->GetTypeRewrite() == NStorage::TEvNodeWardenBlobDepotS3RouterReady::EventType) {
                ready = std::move(ev);
                return false;
            } else if (ev->GetTypeRewrite() == TEvGetObjectRequest::EventType) {
                ++s3Reads;
            }
            return true;
        };
        const auto deadline = runtime.GetClock() + TDuration::Minutes(2);
        runtime.WrapInActorContext(reader, [&] {
            SendToBSProxy(reader, group, new TEvBlobStorage::TEvGet(id, 0, 0, deadline, NKikimrBlobStorage::FastRead));
        });
        WaitForS3Condition(env, [&] { return bool(hotReply); });
        ReconfigureS3Group(env, MakeS3Settings(bucket));
        WaitForS3Condition(env, [&] { return bool(ready); });

        // Replay the immutable value on the new sync route while the original read is still pending.
        PutToS3VirtualGroup(env, runtime.AllocateEdgeActor(2), group, id, data);
        const auto queued = GetServiceCounters(runtime.GetNode(agent.NodeId())->AppData->Counters, "blob_depot_agent")
            ->GetSubgroup("group", ToString(group))->GetSubgroup("subsystem", "s3")->GetCounter("GetsPendingQueueSize");
        runtime.Send(hotReply.release(), agent.NodeId());
        WaitForS3Condition(env, [&] { return queued->Val() == 1 || s3Reads; });
        UNIT_ASSERT_VALUES_EQUAL(s3Reads, 0);
        runtime.FilterFunction = {};
        runtime.Send(ready.release(), agent.NodeId());
        auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(reader, false, deadline);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Responses[0].Status, NKikimrProto::OK, result->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
    }

    Y_UNIT_TEST(SynchronousS3EndpointRotationRequiresConfirmation) {
        NKikimr::InitAwsAPI();
        Y_DEFER { NKikimr::ShutdownAwsAPI(); };
        TPortManager ports;
        const ui16 port = ports.GetPort();
        using TS3Mock = NWrappers::NTestHelpers::TS3Mock;
        TS3Mock server{TS3Mock::TSettings(port)};
        UNIT_ASSERT_C(server.Start(), server.GetError());
        auto settings = MakeS3Settings("sync-route");
        settings.MutableSettings()->SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
        settings.MutableSettings()->SetUseVirtualAddressing(false);
        settings.MutableSettings()->SetEndpoint(TStringBuilder() << "localhost:" << port);
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        bool validatedOnCreation = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobDepot::EvApplyConfig) {
                const auto& config = ev->Get<TEvBlobDepot::TEvApplyConfig>()->Record.GetConfig();
                validatedOnCreation = config.GetS3NamespaceValidated() && !config.GetEverAuthorizedDataCooling();
            }
            return true;
        };
        const ui32 group = CreateS3VirtualGroup(env, settings);
        WaitForS3Condition(env, [&] { return validatedOnCreation; });
        runtime.FilterFunction = {};
        const auto edge = runtime.AllocateEdgeActor(1);
        const auto agent = ConnectToVirtualGroupAgent(env, edge, group);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, group, id, "ack-data", true);

        // Both endpoints address the same HTTP server. BSC still requires an explicit acknowledgement.
        const TString endpoint = TStringBuilder() << "127.0.0.1:" << port;
        settings.MutableSettings()->SetEndpoint(endpoint);
        const auto denied = TryReconfigureS3Group(env, settings);
        UNIT_ASSERT(!denied.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(denied.GetErrorDescription(), "ConfirmS3RouteChange");
        bool reconnected = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == agent && ev->GetTypeRewrite() == TEvBlobDepot::EvRegisterAgentResult) {
                const auto& applied = ev->Get<TEvBlobDepot::TEvRegisterAgentResult>()->Record.GetS3BackendSettings();
                reconnected = applied.GetSettings().GetEndpoint() == endpoint;
            }
            return true;
        };
        const auto accepted = TryReconfigureS3Group(env, settings, "vg", {.ConfirmRouteChange = true});
        UNIT_ASSERT_C(accepted.GetSuccess(), accepted.GetErrorDescription());
        WaitForS3Condition(env, [&] { return reconnected; });
        runtime.FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, group, id, 0, 0, true), "ack-data");
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

    Y_UNIT_TEST(RemovingS3RetiresThrottledReadsAndCachedLocators) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        const TString bucketName = "removed-read-route";
        const ui32 group = CreateS3VirtualGroup(env, bucketName);
        const auto edge = runtime.AllocateEdgeActor(1);
        const auto agent = ConnectToVirtualGroupAgent(env, edge, group);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, group, id, "old-data");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, edge, group, id), "old-data");
        auto s3 = GetServiceCounters(runtime.GetNode(agent.NodeId())->AppData->Counters, "blob_depot_agent")
            ->GetSubgroup("group", ToString(group))->GetSubgroup("subsystem", "s3");
        const auto queued = s3->GetCounter("GetsPendingQueueSize", false);
        const auto slowDown = Aws::S3::S3Error(Aws::Client::AWSError<Aws::S3::S3Errors>(
            Aws::S3::S3Errors::SLOW_DOWN, "SlowDown", "retry later", true));
        std::unique_ptr<IEventHandle> held;
        bool parked = false;
        bool deleted = false;
        bool removed = false;
        ui32 unexpectedS3Reads = 0;
        ui32 freshResolves = 0;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (!parked && ev->GetTypeRewrite() == TEvGetObjectResponse::EventType) {
                auto& reply = *ev->Get<TEvGetObjectResponse>();
                reply.Result = TEvGetObjectResponse::TResult(slowDown);
                if (reply.GetReadInterval().first == 0) {
                    held = std::move(ev);
                    return false;
                }
            } else if (parked && ev->GetTypeRewrite() == TEvGetObjectRequest::EventType) {
                ++unexpectedS3Reads;
            } else if (ev->GetTypeRewrite() == TEvDeleteObjectResponse::EventType) {
                deleted = ev->Get<TEvDeleteObjectResponse>()->IsSuccess();
            } else if (ev->GetTypeRewrite() == TEvDeleteObjectsResponse::EventType) {
                deleted = ev->Get<TEvDeleteObjectsResponse>()->IsSuccess();
            } else if (ev->GetTypeRewrite() == TEvBlobDepot::EvRegisterAgentResult && ev->GetRecipientRewrite() == agent) {
                removed = !ev->Get<TEvBlobDepot::TEvRegisterAgentResult>()->Record.HasS3BackendSettings();
            } else if (removed && ev->GetTypeRewrite() == TEvBlobDepot::EvResolve) {
                const auto& record = ev->Get<TEvBlobDepot::TEvResolve>()->Record;
                freshResolves += record.ItemsSize() == 1 && record.GetItems(0).GetExactKey() == id.AsBinaryString();
            }
            return true;
        };

        auto read = [&](TActorId sender, ui32 shift) {
            runtime.WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, group, new TEvBlobStorage::TEvGet(id, shift, 1,
                    runtime.GetClock() + TDuration::Minutes(2), NKikimrBlobStorage::FastRead));
            });
        };
        auto expect = [&](TActorId sender, NKikimrProto::EReplyStatus status) {
            auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false,
                runtime.GetClock() + TDuration::Seconds(10));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Responses[0].Status, status, result->Get()->ToString());
        };
        read(edge, 0);
        WaitForS3Condition(env, [&] { return bool(held); });
        const auto queuedEdge = runtime.AllocateEdgeActor(1);
        read(queuedEdge, 1);
        // SlowDown reduces concurrency to one; the held GET occupies that slot throughout reconfiguration.
        WaitForS3Condition(env, [&] { return queued->Val() == 1; });
        parked = true;
        const auto gcEdge = runtime.AllocateEdgeActor(2);
        SendTEvCollectGarbage(env, gcEdge, group, id.TabletID(), 2, 1, id.Channel(), true,
            id.Generation(), id.Step(), nullptr, nullptr, false, true);
        const auto gc = CaptureTEvCollectGarbageResult(env, gcEdge);
        UNIT_ASSERT_VALUES_EQUAL_C(gc->Get()->Status, NKikimrProto::OK, gc->Get()->ToString());
        WaitForS3Condition(env, [&] { return deleted; });
        UNIT_ASSERT_VALUES_EQUAL(Singleton<TFakeExternalStorage>()->GetBucket(bucketName).GetSize(), 0);

        const auto disable = TryReconfigureS3Group(env, NKikimrBlobDepot::TS3BackendSettings{}, "vg", {.ConfirmEmpty = true});
        UNIT_ASSERT_C(disable.GetSuccess(), disable.GetErrorDescription());
        expect(queuedEdge, NKikimrProto::ERROR);
        UNIT_ASSERT(removed);
        UNIT_ASSERT_VALUES_EQUAL(queued->Val(), 0);
        const auto node = held->GetRecipientRewrite().NodeId();
        runtime.Send(held.release(), node);
        expect(edge, NKikimrProto::ERROR);

        read(edge, 0);
        expect(edge, NKikimrProto::NODATA);
        UNIT_ASSERT(freshResolves);
        UNIT_ASSERT_VALUES_EQUAL(unexpectedS3Reads, 0);
        runtime.FilterFunction = {};
    }

    Y_UNIT_TEST(ReconnectingAgentRefreshesS3MappingAfterMissedRemoval) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env(MakeCoolingTestSettings(false));
        auto& runtime = *env.Runtime;
        const TString oldBucket = "missed-removal-source";
        const TString newBucket = "missed-removal-destination";
        const ui32 group = CreateS3VirtualGroup(env, oldBucket);
        const auto reader = runtime.AllocateEdgeActor(1);
        const auto agent = ConnectToVirtualGroupAgent(env, reader, group);
        const TString data = "available-locally";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToS3VirtualGroup(env, reader, group, id, data);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, reader, group, id), data);
        const auto& source = Singleton<TFakeExternalStorage>()->GetBucket(oldBucket);
        const auto keys = source.GetObjectKeys();
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);

        bool switchedToHotWrites = false;
        bool removed = false;
        bool reassigned = false;
        ui32 s3Reads = 0;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Recipient == agent && ev->GetTypeRewrite() == TEvBlobDepot::EvRegisterAgentResult) {
                const auto& config = ev->Get<TEvBlobDepot::TEvRegisterAgentResult>()->Record;
                const auto& s3 = config.GetS3BackendSettings();
                // This agent cannot observe either intermediate configuration on its disconnected pipe.
                if (!config.HasS3BackendSettings()) {
                    removed = true;
                    return false;
                } else if (s3.GetSettings().GetBucket() == oldBucket && s3.HasAsyncMode()) {
                    switchedToHotWrites = true;
                    return false;
                }
                reassigned = s3.GetSettings().GetBucket() == newBucket;
            } else if (ev->GetTypeRewrite() == TEvGetObjectRequest::EventType) {
                ++s3Reads;
            }
            return true;
        };
        NKikimrBlobDepot::TS3BackendSettings::TAsyncMode async;
        ReconfigureS3Group(env, MakeS3Settings(oldBucket, &async));
        WaitForS3Condition(env, [&] { return switchedToHotWrites; });
        PutToS3VirtualGroup(env, runtime.AllocateEdgeActor(2), group, id, data);
        WaitForS3Condition(env, [&] { return !source.GetObject(keys.front()); });
        const auto disable = TryReconfigureS3Group(env, NKikimrBlobDepot::TS3BackendSettings{}, "vg", {.ConfirmEmpty = true});
        UNIT_ASSERT_C(disable.GetSuccess(), disable.GetErrorDescription());
        WaitForS3Condition(env, [&] { return removed; });
        ReconfigureS3Group(env, MakeS3Settings(newBucket, &async));
        WaitForS3Condition(env, [&] { return reassigned; });
        UNIT_ASSERT_VALUES_EQUAL(ReadFromS3VirtualGroup(env, reader, group, id), data);
        UNIT_ASSERT_VALUES_EQUAL(s3Reads, 0);
        runtime.FilterFunction = {};
    }

    Y_UNIT_TEST_TWIN(S3GetResolvesOnlyAnExplicitMissingKey, noSuchKey) {
        using namespace NWrappers::NExternalStorage;
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
            .SetupHive = true,
        });
        const ui32 group = CreateS3VirtualGroup(env, "get-missing-key");
        const auto edge = env.Runtime->AllocateEdgeActor(1);
        const TLogoBlobID id(100500, 1, 1, 0, 8, 0);
        PutToS3VirtualGroup(env, edge, group, id, "abcdefgh");
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
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
                const auto& resolve = ev->Get<TEvBlobDepot::TEvResolve>()->Record;
                if (resolve.ItemsSize() == 1 && resolve.GetItems(0).GetExactKey() == id.AsBinaryString()) {
                    UNIT_ASSERT_EQUAL(resolve.GetDataKind(), NKikimrBlobStorage::TDataKind::SYSTEM);
                    UNIT_ASSERT_VALUES_EQUAL(resolve.GetDeadlineUs(), deadline.MicroSeconds());
                    UNIT_ASSERT(resolve.GetSkipBridgeAccountingForSubrequests());
                    ++resolvesAfterFailure;
                }
            }
            return true;
        };
        env.Runtime->WrapInActorContext(edge, [&] {
            auto get = std::make_unique<TEvBlobStorage::TEvGet>(id, 0, 0, deadline, NKikimrBlobStorage::FastRead);
            get->DataKind = NKikimrBlobStorage::TDataKind::SYSTEM;
            get->IsProxySubrequest = true;
            SendToBSProxy(edge, group, get.release());
        });
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

        auto check = [&] {
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvCheckIntegrity(id, TInstant::Max(),
                    NKikimrBlobStorage::FastRead));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCheckIntegrityResult>(edge, false,
                env.Runtime->GetClock() + TDuration::Minutes(1));
            UNIT_ASSERT_C(res, "no answer to CheckIntegrity");
            Cerr << "*** " << res->Get()->ToString() << Endl;
            return res->Get()->Status;
        };

        UNIT_ASSERT_VALUES_EQUAL(check(), NKikimrProto::OK);

        storage.Clear(); // the object disappears from S3
        UNIT_ASSERT_VALUES_EQUAL(check(), NKikimrProto::ERROR);
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
            env.Runtime->Send(new IEventHandle(TEvents::TSystem::Undelivered, 0, recipient, ev->Sender, nullptr, 0),
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
