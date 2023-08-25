#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blob_depot/events.h>

#include "blob_depot_event_managers.h"

// #define LOG_PUT
// #define LOG_GET
// #define LOG_MULTIGET
// #define LOG_RANGE
// #define LOG_DISCOVER
// #define LOG_BLOCK
// #define LOG_COLLECT_GARBAGE

bool CheckBarrier(const TLogoBlobID& id, ui32 collectGen, ui32 collectStep) {
    return (id.Generation() < collectGen) || (id.Generation() == collectGen && id.Step() <= collectStep);
}

bool IsCollected(const TBlobInfo& blob, ui32 softCollectGen, ui32 softCollectStep, ui32 hardCollectGen, ui32 hardCollectStep) {
    bool keep = !blob.DoNotKeep && blob.Keep;
    return CheckBarrier(blob.Id, hardCollectGen, hardCollectStep) || (!keep && CheckBarrier(blob.Id, softCollectGen, softCollectStep));
}


TInstant MakeDeadline(TEnvironmentSetup& env, bool withDeadline, TDuration deadline = TDuration::Seconds(10)) {
    return withDeadline ? env.Runtime->GetClock() +deadline : TInstant::Max();
}

std::unique_ptr<IEventHandle> CaptureAnyResult(TEnvironmentSetup& env, TActorId sender) {
    std::set<TActorId> ids{sender};

    TActorId wakeup;
    TInstant deadline;

    env.Runtime->WrapInActorContext(sender, [&] {
        deadline = TActivationContext::Now() + TDuration::Seconds(1);
    });

    wakeup = env.Runtime->AllocateEdgeActor(sender.NodeId(), __FILE__, __LINE__);
    env.Runtime->Schedule(deadline, new IEventHandle(TEvents::TSystem::Wakeup, 0, wakeup, {}, nullptr, 0), nullptr,
        wakeup.NodeId());
    ids.insert(wakeup);

    for (;;) {
        auto ev = env.Runtime->WaitForEdgeActorEvent(ids);
        if (ev->GetRecipientRewrite() == wakeup && ev->GetTypeRewrite() == TEvents::TSystem::Wakeup) {
            env.Runtime->DestroyActor(wakeup);
            return nullptr;
        } else {
            if (wakeup) {
                env.Runtime->DestroyActor(wakeup);
            }
            return std::unique_ptr<IEventHandle>(ev.release());
        }
    }
}

void SendTEvPut(TEnvironmentSetup& env, TActorId sender, ui32 groupId, TLogoBlobID id, TString data, ui64 cookie) {
    auto ev = new TEvBlobStorage::TEvPut(id, data, TInstant::Max());

#ifdef LOG_PUT
    Cerr << "Request# " << ev->Print(false) << Endl;
#endif

    env.Runtime->WrapInActorContext(sender, [&] {
        SendToBSProxy(sender, groupId, ev, cookie);
    });
}

TAutoPtr<TEventHandle<TEvBlobStorage::TEvPutResult>> CaptureTEvPutResult(TEnvironmentSetup& env,
        TActorId sender, bool termOnCapture, bool withDeadline) {
    const TInstant deadline = MakeDeadline(env, withDeadline);
    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, termOnCapture, deadline);
    UNIT_ASSERT(res);

#ifdef LOG_PUT
    Cerr << "Response# " << res->Get()->ToString() << Endl;
#endif

    return res.Release();
}

void VerifyTEvPutResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvPutResult>> res, TBlobInfo& blob, TBSState& state) {
    ui32 blockedGen = state[blob.Id.TabletID()].BlockedGen;
    ui32 softCollectGen = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].SoftCollectGen;
    ui32 softCollectStep = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].SoftCollectStep;
    ui32 hardCollectGen = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].HardCollectGen;
    ui32 hardCollectStep = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].HardCollectStep;

    NKikimrProto::EReplyStatus status = res->Get()->Status;

    if (status == NKikimrProto::OK) {
        blob.Status = TBlobInfo::EStatus::WRITTEN;
    } else if (status == NKikimrProto::ERROR) {
        blob.Status = TBlobInfo::EStatus::UNKNOWN;
        return;
    }

    if (blob.Id.Generation() <= blockedGen) {
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::BLOCKED, TStringBuilder() <<
            "Unblocked put over the barrier, blob id# " << blob.Id.ToString() << ", blocked generation# " << blockedGen);
    } else if (IsCollected(blob, softCollectGen, softCollectStep, hardCollectGen, hardCollectStep) ) {
        if (status == NKikimrProto::OK) {
            Cerr << "Put over the barrier, blob id# " << blob.Id.ToString() << Endl;
        } else if (status != NKikimrProto::NODATA) {
            UNIT_FAIL("Unexpected status: " << NKikimrProto::EReplyStatus_Name(status));
        }
    } else if (status != NKikimrProto::OK && status != NKikimrProto::ERROR) {
        UNIT_FAIL(TStringBuilder() << "Unexpected status: " << NKikimrProto::EReplyStatus_Name(status));
    }
}

void VerifiedPut(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, TBlobInfo& blob, TBSState& state, bool withDeadline) {
    auto sender = env.Runtime->AllocateEdgeActor(nodeId);
    SendTEvPut(env, sender, groupId, blob.Id, blob.Data);
    auto res = CaptureTEvPutResult(env, sender, true, withDeadline);
    VerifyTEvPutResult(res.Release(), blob, state);
}

void SendTEvGet(TEnvironmentSetup& env, TActorId sender, ui32 groupId, TLogoBlobID id,
        bool mustRestoreFirst, bool isIndexOnly, std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData, ui64 cookie) {
    auto ev = new TEvBlobStorage::TEvGet(id, 0, id.BlobSize(), TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead,
            mustRestoreFirst, isIndexOnly, forceBlockTabletData);

#ifdef LOG_GET
    Cerr << "Request# " << ev->Print(true) << Endl;
#endif

    env.Runtime->WrapInActorContext(sender, [&] {
        SendToBSProxy(sender, groupId, ev, cookie);
    });
}

TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> CaptureTEvGetResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture, bool withDeadline) {
    const TInstant deadline = MakeDeadline(env, withDeadline);
    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, termOnCapture, deadline);
    UNIT_ASSERT(res);

#ifdef LOG_GET
    Cerr << "Response# " << res->Get()->ToString() << Endl;
#endif

    return res.Release();
}


void VerifyTEvGetResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> res,
        TBlobInfo& blob, bool mustRestoreFirst, bool isIndexOnly, std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData,
        TBSState& state)
{
    Y_UNUSED(forceBlockTabletData);

    ui32 softCollectGen = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].SoftCollectGen;
    ui32 softCollectStep = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].SoftCollectStep;
    ui32 hardCollectGen = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].HardCollectGen;
    ui32 hardCollectStep = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].HardCollectStep;

    NKikimrProto::EReplyStatus status = res->Get()->Status;
    auto& responses = res->Get()->Responses;

    if (status == NKikimrProto::OK) {
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        if ((blob.Status == TBlobInfo::EStatus::COLLECTED) || IsCollected(blob, softCollectGen, softCollectStep, hardCollectGen, hardCollectStep)) {
            if (responses[0].Status == NKikimrProto::OK) {
                Cerr << "Read over the barrier, blob id# " << responses[0].Id.ToString() << Endl;
            }
            blob.Status = TBlobInfo::EStatus::COLLECTED;
        } else if (blob.Status == TBlobInfo::EStatus::WRITTEN) {
            if (!isIndexOnly) {
                UNIT_ASSERT_VALUES_UNEQUAL(responses[0].Status, NKikimrProto::NODATA);
            }
            if (responses[0].Status == NKikimrProto::OK && !isIndexOnly) {
                UNIT_ASSERT_VALUES_EQUAL(responses[0].Buffer.ConvertToString(), blob.Data);
            }
        } else if (blob.Status == TBlobInfo::EStatus::UNKNOWN) {
            if (mustRestoreFirst && responses[0].Status == NKikimrProto::OK) {
                blob.Status = TBlobInfo::EStatus::WRITTEN;
            }
        } else {
            if (responses[0].Status != NKikimrProto::NODATA) {
                Cerr << "Read non-put blob id# " << responses[0].Id.ToString() << Endl;
            }
        }
    }
}

void VerifiedGet(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId,
        TBlobInfo& blob, bool mustRestoreFirst, bool isIndexOnly, std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData,
        TBSState& state, bool withDeadline)
{
    auto sender = env.Runtime->AllocateEdgeActor(nodeId);
    SendTEvGet(env, sender, groupId, blob.Id, mustRestoreFirst, isIndexOnly, forceBlockTabletData);
    auto res = CaptureTEvGetResult(env, sender, true, withDeadline);

    VerifyTEvGetResult(res.Release(), blob, mustRestoreFirst, isIndexOnly, forceBlockTabletData, state);
}

void SendTEvGet(TEnvironmentSetup& env, TActorId sender, ui32 groupId, std::vector<TBlobInfo>& blobs,
        bool mustRestoreFirst, bool isIndexOnly, std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData, ui64 cookie)
{
    ui32 sz = blobs.size();

    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[sz]);
    for (ui32 i = 0; i < sz; ++i) {
        queries[i].Id = blobs[i].Id;
        queries[i].Shift = 0;
        queries[i].Size = blobs[i].Data.size();
    }

    auto ev = new TEvBlobStorage::TEvGet(queries, sz, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead,
            mustRestoreFirst, isIndexOnly, forceBlockTabletData);

#ifdef LOG_MULTIGET
    Cerr << "Request# " << ev->Print(true) << Endl;
#endif

    env.Runtime->WrapInActorContext(sender, [&] {
        SendToBSProxy(sender, groupId, ev, cookie);
    });
}

TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> CaptureMultiTEvGetResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture, bool withDeadline) {
    const TInstant deadline = MakeDeadline(env, withDeadline);
    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, termOnCapture, deadline);
    UNIT_ASSERT(res);

#ifdef LOG_MULTIGET
    Cerr << "Response# " << res->Get()->ToString() << Endl;
#endif

    return res.Release();
}


void VerifyTEvGetResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> res,
        std::vector<TBlobInfo>& blobs, bool mustRestoreFirst, bool isIndexOnly, std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData,
        TBSState& state)
{
    Y_UNUSED(mustRestoreFirst);
    Y_UNUSED(forceBlockTabletData);
    NKikimrProto::EReplyStatus status = res->Get()->Status;
    auto& responses = res->Get()->Responses;

    if (status == NKikimrProto::OK) {
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, blobs.size());
        for (ui32 i = 0; i < blobs.size(); ++i) {
            ui32 softCollectGen = state[blobs[i].Id.TabletID()].Channels[blobs[i].Id.Channel()].SoftCollectGen;
            ui32 softCollectStep = state[blobs[i].Id.TabletID()].Channels[blobs[i].Id.Channel()].SoftCollectStep;
            ui32 hardCollectGen = state[blobs[i].Id.TabletID()].Channels[blobs[i].Id.Channel()].HardCollectGen;
            ui32 hardCollectStep = state[blobs[i].Id.TabletID()].Channels[blobs[i].Id.Channel()].HardCollectStep;
            if ((blobs[i].Status == TBlobInfo::EStatus::COLLECTED) || IsCollected(blobs[i], softCollectGen, softCollectStep, hardCollectGen, hardCollectStep)) {
                if (responses[i].Status == NKikimrProto::OK) {
                    Cerr << "Read over the barrier, blob id# " << responses[i].Id.ToString() << Endl;
                }
                blobs[i].Status = TBlobInfo::EStatus::COLLECTED;
            } else if (blobs[i].Status == TBlobInfo::EStatus::WRITTEN) {
                UNIT_ASSERT_VALUES_UNEQUAL(responses[i].Status, NKikimrProto::NODATA);
                if (responses[i].Status == NKikimrProto::OK && !isIndexOnly) {
                    UNIT_ASSERT_VALUES_EQUAL(responses[i].Buffer.ConvertToString(), blobs[i].Data);
                }
            } else if (blobs[i].Status == TBlobInfo::EStatus::UNKNOWN) {
                if (mustRestoreFirst && responses[i].Status == NKikimrProto::OK) {
                    blobs[i].Status = TBlobInfo::EStatus::WRITTEN;
                }
            } else {
                if (responses[i].Status != NKikimrProto::NODATA) {
                    Cerr << "Read over the barrier, blob id# " << responses[i].Id.ToString() << Endl;
                }
            }
        }
    }
}

void VerifiedGet(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, std::vector<TBlobInfo>& blobs, bool mustRestoreFirst, bool isIndexOnly, std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData,
        TBSState& state, bool withDeadline)
{
    auto sender = env.Runtime->AllocateEdgeActor(nodeId);
    SendTEvGet(env, sender, groupId, blobs, mustRestoreFirst, isIndexOnly, forceBlockTabletData);

    auto res = CaptureMultiTEvGetResult(env, sender, true, withDeadline);

    VerifyTEvGetResult(res.Release(), blobs, mustRestoreFirst, isIndexOnly, forceBlockTabletData, state);
}

void SendTEvRange(TEnvironmentSetup& env, TActorId sender, ui32 groupId, ui64 tabletId,
        TLogoBlobID from, TLogoBlobID to, bool mustRestoreFirst, bool indexOnly, ui64 cookie) {
    auto ev = new TEvBlobStorage::TEvRange(tabletId, from, to, mustRestoreFirst, TInstant::Max(), indexOnly);

#ifdef LOG_RANGE
    Cerr << "Request# " << ev->ToString() << Endl;
#endif

    env.Runtime->WrapInActorContext(sender, [&] {
        SendToBSProxy(sender, groupId, ev, cookie);
    });
}

TAutoPtr<TEventHandle<TEvBlobStorage::TEvRangeResult>> CaptureTEvRangeResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture, bool withDeadline) {
    const TInstant deadline = MakeDeadline(env, withDeadline);
    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(sender, termOnCapture, deadline);
    UNIT_ASSERT(res);

#ifdef LOG_RANGE
    Cerr << "Response# " << res->Get()->ToString() << Endl;
#endif

    UNIT_ASSERT(res);

    return res.Release();
}

void VerifyTEvRangeResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvRangeResult>> res, ui64 tabletId, TLogoBlobID from, TLogoBlobID to, bool mustRestoreFirst, bool indexOnly,
        std::vector<TBlobInfo>& blobs, TBSState& state)
{
    NKikimrProto::EReplyStatus status = res->Get()->Status;
    auto& responses = res->Get()->Responses;

    if (status == NKikimrProto::OK) {
        std::map<TLogoBlobID, TBlobInfo*> expected;
        for (auto& blob : blobs) {
            if (blob.Id >= from && blob.Id <= to && blob.Status != TBlobInfo::NONEXISTENT) {
                expected.insert({blob.Id, &blob});
            }
        }

        std::set<TLogoBlobID> found;
        for (auto& response : responses) {
            found.insert(response.Id);
            auto it = expected.find(response.Id);
            if (it == expected.end()) {
                UNIT_FAIL(TStringBuilder() << "TEvRange returned nonexistent blob with id# " << response.Id.ToString());
            }
            auto blob = it->second;
            UNIT_ASSERT_VALUES_EQUAL(tabletId, blob->Id.TabletID());
            UNIT_ASSERT_GE(blob->Id, from);
            UNIT_ASSERT_LE(blob->Id, to);
            ui32 softCollectGen = state[blob->Id.TabletID()].Channels[blob->Id.Channel()].SoftCollectGen;
            ui32 softCollectStep = state[blob->Id.TabletID()].Channels[blob->Id.Channel()].SoftCollectStep;
            ui32 hardCollectGen = state[blob->Id.TabletID()].Channels[blob->Id.Channel()].HardCollectGen;
            ui32 hardCollectStep = state[blob->Id.TabletID()].Channels[blob->Id.Channel()].HardCollectStep;
            if (blob->Status == TBlobInfo::EStatus::COLLECTED || IsCollected(*blob, softCollectGen, softCollectStep, hardCollectGen, hardCollectStep)) {
                Cerr << "TEvRange returned collected blob with id# " << it->first.ToString() << Endl;
                blob->Status = TBlobInfo::EStatus::COLLECTED;
            } else if (blob->Status == TBlobInfo::EStatus::UNKNOWN) {
                if (mustRestoreFirst) {
                    blob->Status = TBlobInfo::EStatus::WRITTEN;
                }
            }
            if (blob->Status == TBlobInfo::EStatus::WRITTEN) {
                if (indexOnly) {
                    UNIT_ASSERT_VALUES_EQUAL(response.Buffer, TString());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(response.Buffer, blob->Data);
                }
            }
        }

        for (auto& blob : blobs) {
            ui32 softCollectGen = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].SoftCollectGen;
            ui32 softCollectStep = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].SoftCollectStep;
            ui32 hardCollectGen = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].HardCollectGen;
            ui32 hardCollectStep = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].HardCollectStep;
            if ((blob.Status == TBlobInfo::EStatus::WRITTEN) &&  !IsCollected(blob, softCollectGen, softCollectStep, hardCollectGen, hardCollectStep) &&
                blob.Id >= from && blob.Id <= to && expected.find(blob.Id) == expected.end()) {
                UNIT_FAIL(TStringBuilder() << "TEvRange didn't find blob " << blob.Id.ToString());
            }
        }
    }
}

void VerifiedRange(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, ui64 tabletId, TLogoBlobID from, TLogoBlobID to,
        bool mustRestoreFirst, bool indexOnly, std::vector<TBlobInfo>& blobs, TBSState& state, bool withDeadline)
{
    auto sender = env.Runtime->AllocateEdgeActor(nodeId);
    SendTEvRange(env, sender, groupId, tabletId, from, to, mustRestoreFirst, indexOnly);
    auto res = CaptureTEvRangeResult(env, sender, true, withDeadline);
    VerifyTEvRangeResult(res.Release(), tabletId, from, to, mustRestoreFirst, indexOnly, blobs, state);
}

void SendTEvDiscover(TEnvironmentSetup& env, TActorId sender, ui32 groupId, ui64 tabletId, ui32 minGeneration, bool readBody,
        bool discoverBlockedGeneration, ui32 forceBlockedGeneration, bool fromLeader, ui64 cookie) {
    auto ev = new TEvBlobStorage::TEvDiscover(tabletId, minGeneration, readBody, discoverBlockedGeneration,
            TInstant::Max(), forceBlockedGeneration, fromLeader);

#ifdef LOG_DISCOVER
    Cerr << "Request# " << ev->Print(true) << Endl;
#endif

    env.Runtime->WrapInActorContext(sender, [&] {
        SendToBSProxy(sender, groupId, ev, cookie);
    });
}

TAutoPtr<TEventHandle<TEvBlobStorage::TEvDiscoverResult>> CaptureTEvDiscoverResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture, bool withDeadline) {
    const TInstant deadline = MakeDeadline(env, withDeadline);
    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvDiscoverResult>(sender, termOnCapture, deadline);
    UNIT_ASSERT(res);

#ifdef LOG_DISCOVER
    Cerr << "Response# " << res->Get()->ToString() << Endl;
#endif

    return res.Release();
}

void VerifyTEvDiscoverResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvDiscoverResult>> res, ui64 tabletId, ui32 minGeneration, bool readBody,
        bool discoverBlockedGeneration, ui32 forceBlockedGeneration, bool fromLeader, std::vector<TBlobInfo>& blobs, TBSState& state)
{
    ui32 blockedGen = state[tabletId].BlockedGen;
    Y_UNUSED(blockedGen);
    Y_UNUSED(discoverBlockedGeneration);
    Y_UNUSED(forceBlockedGeneration);
    Y_UNUSED(fromLeader);

    UNIT_ASSERT(res);
    auto status = res->Get()->Status;

    if (status != NKikimrProto::ERROR) {
        TBlobInfo* maxBlob = nullptr;
        TBlobInfo* discoveredBlob = nullptr;

        for (auto& blob : blobs) {
            if ((status == NKikimrProto::OK) && (blob.Id == res->Get()->Id)) {
                discoveredBlob = &blob;
                if (readBody && discoveredBlob->Status == TBlobInfo::EStatus::UNKNOWN) {
                    discoveredBlob->Status = TBlobInfo::EStatus::WRITTEN;
                }
            }
            ui32 softCollectGen = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].SoftCollectGen;
            ui32 softCollectStep = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].SoftCollectStep;
            ui32 hardCollectGen = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].HardCollectGen;
            ui32 hardCollectStep = state[blob.Id.TabletID()].Channels[blob.Id.Channel()].HardCollectStep;
            if (blob.Id.TabletID() == tabletId && blob.Id.Channel() == 0 && blob.Id.Generation() >= minGeneration) {
                if (blob.Status == TBlobInfo::WRITTEN && !IsCollected(blob, softCollectGen, softCollectStep, hardCollectGen, hardCollectStep)) {
                    if (!maxBlob || blob.Id > maxBlob->Id) {
                        maxBlob = &blob;
                    }
                }
            }
        }

        if (maxBlob) {
            UNIT_ASSERT_VALUES_UNEQUAL_C(status, NKikimrProto::NODATA, TStringBuilder() << "TEvDiscover didn't find existing blob " << maxBlob->Id);
            UNIT_ASSERT_VALUES_UNEQUAL_C(discoveredBlob, nullptr, TStringBuilder() << "Found nonexistent blob with id " << res->Get()->Id.ToString());
        }

        if (status == NKikimrProto::OK) {
            UNIT_ASSERT_VALUES_UNEQUAL_C(discoveredBlob, nullptr, TStringBuilder() << "Found nonexistent blob with id " << res->Get()->Id.ToString());
            ui32 softCollectGen = state[discoveredBlob->Id.TabletID()].Channels[discoveredBlob->Id.Channel()].SoftCollectGen;
            ui32 softCollectStep = state[discoveredBlob->Id.TabletID()].Channels[discoveredBlob->Id.Channel()].SoftCollectStep;
            ui32 hardCollectGen = state[discoveredBlob->Id.TabletID()].Channels[discoveredBlob->Id.Channel()].HardCollectGen;
            ui32 hardCollectStep = state[discoveredBlob->Id.TabletID()].Channels[discoveredBlob->Id.Channel()].HardCollectStep;
            if (IsCollected(*discoveredBlob, softCollectGen, softCollectStep, hardCollectGen, hardCollectStep)) {
                Cerr << "TEvDiscover found collected blob with id " << discoveredBlob->Id;
            } else if (discoveredBlob->Status == TBlobInfo::EStatus::UNKNOWN) {
                Cerr << "TEvDiscover found blob which was put with error" << Endl;
            } else if (discoveredBlob->Status == TBlobInfo::EStatus::WRITTEN) {
                UNIT_ASSERT(maxBlob);
                UNIT_ASSERT_VALUES_EQUAL(discoveredBlob->Id, maxBlob->Id);
                if (maxBlob->Id && readBody) {
                    UNIT_ASSERT_VALUES_EQUAL(discoveredBlob->Data, maxBlob->Data);
                }
            }
        }
    }
}

void VerifiedDiscover(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, ui64 tabletId, ui32 minGeneration, bool readBody,
        bool discoverBlockedGeneration, ui32 forceBlockedGeneration, bool fromLeader, std::vector<TBlobInfo>& blobs, TBSState& state, bool withDeadline) {
    auto sender = env.Runtime->AllocateEdgeActor(nodeId);
    SendTEvDiscover(env, sender, groupId, tabletId, minGeneration, readBody, discoverBlockedGeneration, forceBlockedGeneration, fromLeader);
    auto res = CaptureTEvDiscoverResult(env, sender, true, withDeadline);
    VerifyTEvDiscoverResult(res.Release(), tabletId, minGeneration, readBody, discoverBlockedGeneration, forceBlockedGeneration, fromLeader, blobs, state);
}

void SendTEvCollectGarbage(TEnvironmentSetup& env, TActorId sender, ui32 groupId,
    ui64 tabletId, ui32 recordGeneration, ui32 perGenerationCounter, ui32 channel,
    bool collect, ui32 collectGeneration,
    ui32 collectStep, TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep,
    bool isMultiCollectAllowed, bool hard, ui64 cookie)
{
    auto ev = new TEvBlobStorage::TEvCollectGarbage(tabletId, recordGeneration, perGenerationCounter, channel, collect, collectGeneration, collectStep,
                keep, doNotKeep, TInstant::Max(), isMultiCollectAllowed, hard);

#ifdef LOG_COLLECT_GARBAGE
    Cerr << "Request# " << ev->Print(false) << Endl;
#endif

    env.Runtime->WrapInActorContext(sender, [&] {
        SendToBSProxy(sender, groupId, ev, cookie);
    });
}

TAutoPtr<TEventHandle<TEvBlobStorage::TEvCollectGarbageResult>> CaptureTEvCollectGarbageResult(TEnvironmentSetup& env, TActorId sender,
        bool termOnCapture, bool withDeadline) {
    const TInstant deadline = MakeDeadline(env, withDeadline);
    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(sender, termOnCapture, deadline);
    UNIT_ASSERT(res);

#ifdef LOG_COLLECT_GARBAGE
    Cerr << "Response# " << res->Get()->ToString() << Endl;
#endif

    return res.Release();
}

void VerifyTEvCollectGarbageResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvCollectGarbageResult>> res,
    ui64 tabletId, ui32 recordGeneration, ui32 perGenerationCounter, ui32 channel,
    bool collect, ui32 collectGeneration,
    ui32 collectStep, TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep,
    bool isMultiCollectAllowed, bool hard, std::vector<TBlobInfo>& blobs, TBSState& state)
{
    Y_UNUSED(perGenerationCounter);
    Y_UNUSED(isMultiCollectAllowed);

    std::set<TLogoBlobID> setKeep;
    if (keep) {
        for (auto blobid : *keep) {
            setKeep.insert(blobid);
        }
    }
    std::set<TLogoBlobID> setNotKeep;
    if (doNotKeep) {
        for (auto blobid : *doNotKeep) {
            setNotKeep.insert(blobid);
        }
    }

    ui32 blockedGen = state[tabletId].BlockedGen;
    ui32& softCollectGen = state[tabletId].Channels[channel].SoftCollectGen;
    ui32& softCollectStep = state[tabletId].Channels[channel].SoftCollectStep;
    ui32& hardCollectGen = state[tabletId].Channels[channel].HardCollectGen;
    ui32& hardCollectStep = state[tabletId].Channels[channel].HardCollectStep;

    NKikimrProto::EReplyStatus status = res->Get()->Status;

    if (blockedGen >= recordGeneration) {
        if (status != NKikimrProto::ERROR) {
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::BLOCKED);
        }
    } else {
        if (status == NKikimrProto::OK) {
            if (hard) {
                hardCollectGen = std::max(hardCollectGen, collectGeneration);
                hardCollectStep = std::max(hardCollectStep, collectStep);
            } else {
                softCollectGen = std::max(softCollectGen, collectGeneration);
                softCollectStep = std::max(softCollectStep, collectStep);
            }

            for (auto& blob : blobs) {
                if (keep) {
                    if (setKeep.find(blob.Id) != setKeep.end()) {
                        blob.Keep = true;
                    }
                }
                if (doNotKeep) {
                    if (setNotKeep.find(blob.Id) != setNotKeep.end()) {
                        blob.DoNotKeep = true;
                    }
                }

                if ((blob.Status == TBlobInfo::EStatus::WRITTEN) && (blob.Id.TabletID() == tabletId) && (blob.Id.Channel() == channel) &&
                        (hard || collect) && IsCollected(blob, softCollectGen, softCollectStep, hardCollectGen, hardCollectStep)) {
                    blob.Status = TBlobInfo::EStatus::COLLECTED;
                }
            }
        }
    }
}

void VerifiedCollectGarbage(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId,
    ui64 tabletId, ui32 recordGeneration, ui32 perGenerationCounter, ui32 channel,
    bool collect, ui32 collectGeneration,
    ui32 collectStep, TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep,
    bool isMultiCollectAllowed, bool hard, std::vector<TBlobInfo>& blobs, TBSState& state, bool withDeadline)
{
    auto sender = env.Runtime->AllocateEdgeActor(nodeId);

    TAutoPtr<TVector<TLogoBlobID>> copyKeep;
    if (keep) {
        copyKeep.Reset(new TVector(*keep));
    }
    TAutoPtr<TVector<TLogoBlobID>> copyDoNotKeep;
    if (doNotKeep) {
        copyDoNotKeep.Reset(new TVector(*doNotKeep));
    }

    SendTEvCollectGarbage(env, sender, groupId, tabletId, recordGeneration, perGenerationCounter, channel, collect,
            collectGeneration, collectStep, keep, doNotKeep, isMultiCollectAllowed, hard);

    auto res = CaptureTEvCollectGarbageResult(env, sender, true, withDeadline);
    VerifyTEvCollectGarbageResult(res.Release(), tabletId, recordGeneration, perGenerationCounter, channel, collect,
        collectGeneration, collectStep, copyKeep.Get(), copyDoNotKeep.Get(), isMultiCollectAllowed, hard, blobs, state);
}


void SendTEvBlock(TEnvironmentSetup& env, TActorId sender, ui32 groupId, ui64 tabletId, ui32 generation, ui64 cookie) {
    auto ev = new TEvBlobStorage::TEvBlock(tabletId, generation, TInstant::Max());

#ifdef LOG_BLOCK
    Cerr << "Request# " << ev->Print(true) << Endl;
#endif

    env.Runtime->WrapInActorContext(sender, [&] {
        SendToBSProxy(sender, groupId, ev, cookie);
    });
}

TAutoPtr<TEventHandle<TEvBlobStorage::TEvBlockResult>> CaptureTEvBlockResult(TEnvironmentSetup& env, TActorId sender,
        bool termOnCapture, bool withDeadline) {
    const TInstant deadline = MakeDeadline(env, withDeadline);
    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(sender, termOnCapture, deadline);
    UNIT_ASSERT(res);

#ifdef LOG_BLOCK
    Cerr << "Response# " << res->Get()->ToString() << Endl;
#endif

    return res.Release();
}

void VerifyTEvBlockResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvBlockResult>> res, ui64 tabletId, ui32 generation, TBSState& state) {
    ui32& blockedGen = state[tabletId].BlockedGen;
    NKikimrProto::EReplyStatus status = res->Get()->Status;
    if (generation < blockedGen) {
        UNIT_ASSERT_VALUES_UNEQUAL(status, NKikimrProto::OK);
        if (status == NKikimrProto::BLOCKED) {
            Cerr << "TEvBlock: Detect race" << Endl;
        } else if (status == NKikimrProto::ERROR) {
            Cerr << "Unexpected ERROR" << Endl;
        } else {
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::ALREADY);
        }
    }
    if (status == NKikimrProto::OK) {
        blockedGen = generation;
    }
}

void VerifiedBlock(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, ui64 tabletId, ui32 generation, TBSState& state, bool withDeadline) {
    auto sender = env.Runtime->AllocateEdgeActor(nodeId);

    SendTEvBlock(env, sender, groupId, tabletId, generation);
    auto res = CaptureTEvBlockResult(env, sender, true, withDeadline);
    VerifyTEvBlockResult(res.Release(), tabletId, generation, state);
}
