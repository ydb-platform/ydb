#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blob_depot/events.h>

#include <util/random/mersenne.h>

#include <algorithm>

using namespace NKikimr::NBlobDepot;

// #define LOG_PUT
// #define LOG_GET
// #define LOG_MULTIGET
// #define LOG_RANGE
// #define LOG_DISCOVER
// #define LOG_BLOCK
// #define LOG_COLLECT_GARBAGE

Y_UNIT_TEST_SUITE(BlobDepot) {
    TMersenne<ui32> mt(1337);
    TMersenne<ui64> mt64(0xdeadf00d);

    void ConfigureEnvironment(ui32 numGroups, std::unique_ptr<TEnvironmentSetup>& envPtr, std::vector<ui32>& regularGroups, ui32& blobDepot, ui32 nodeCount = 8) {
        envPtr = std::make_unique<TEnvironmentSetup>(TEnvironmentSetup::TSettings{
            .NodeCount = nodeCount,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
            .BlobDepotId = MakeTabletID(1, 0, 0x10000),
            .BlobDepotChannels = 4
        });

        envPtr->CreateBoxAndPool(1, numGroups);
        envPtr->Sim(TDuration::Seconds(20));

        regularGroups = envPtr->GetGroups();

        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableAllocateVirtualGroup();
        cmd->SetName("vg");
        cmd->SetHiveId(1);
        cmd->SetStoragePoolName(envPtr->StoragePoolName);
        cmd->SetBlobDepotId(envPtr->Settings.BlobDepotId);
        auto *prof = cmd->AddChannelProfiles();
        prof->SetStoragePoolKind("");
        prof->SetCount(2);
        prof = cmd->AddChannelProfiles();
        prof->SetStoragePoolKind("");
        prof->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
        prof->SetCount(2);

        auto response = envPtr->Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        {
            auto ev = std::make_unique<TEvBlobDepot::TEvApplyConfig>();
            auto *config = ev->Record.MutableConfig();
            config->SetOperationMode(NKikimrBlobDepot::VirtualGroup);
            config->MutableChannelProfiles()->CopyFrom(cmd->GetChannelProfiles());

            const TActorId edge = envPtr->Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            envPtr->Runtime->SendToPipe(envPtr->Settings.BlobDepotId, edge, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
            envPtr->WaitForEdgeActorEvent<TEvBlobDepot::TEvApplyConfigResult>(edge);
        }

        blobDepot = response.GetStatus(0).GetGroupId(0);
    }

    TString DataGen(ui32 len) {
        TString res = "";
        for (ui32 i = 0; i < len; ++i) {
            res += 'A' + mt.GenRand() % ('z' - 'A');
        }
        return res;
    }

    struct TBlobInfo {
        enum EStatus {
            NONEXISTENT,
            WRITTEN,
            COLLECTED,
        };

        TBlobInfo(const TBlobInfo& other) = default;
        TBlobInfo(TBlobInfo&& other) = default;
        TBlobInfo(TString data, ui64 tablet, ui32 cookie, ui32 gen = 1, ui32 step = 1, ui32 channel = 0)
            : Status(EStatus::NONEXISTENT) 
            , Id(tablet, gen, step, channel, data.size(), cookie)
            , Data(data)
            , KeepFlag(false)
        {
        }

        TString ToString() {
            TString status;
            if (Status == EStatus::NONEXISTENT) {
                status = "NONEXISTENT";
            } else if (Status == EStatus::WRITTEN) {
                status = "WRITTEN";
            } else {
                status = "COLLECTED";
            }
            return TStringBuilder() << "Status# " << status << " Id# {" << Id.ToString() << "} Data# " << Data << " KeepFlag# " << KeepFlag;
        }

        EStatus Status;
        const TLogoBlobID Id;
        TString Data;
        bool KeepFlag;

        static const TBlobInfo& Nothing() {
            static const TBlobInfo nothing(TString(), 0, 0, 0, 0, 0);
            return nothing;
        }
    };

    struct TTabletInfo {
        struct TChanelInfo {
            ui32 SoftCollectGen = 0;
            ui32 SoftCollectStep = 0;
            ui32 HardCollectGen = 0;
            ui32 HardCollectStep = 0;
        };

        ui32 BlockedGen;
        std::vector<TChanelInfo> Channels;

        TTabletInfo()
            : BlockedGen(0)
            , Channels(6)
        {
        }
    };

    using TBSState = std::map<ui64, TTabletInfo>;

    struct TIntervals {
        std::vector<ui32> Borders; // [0; x_1) [x_1; x_2) ... [x_n-1; x_n)

        TIntervals(std::vector<ui32> borders) {
            Borders = borders;
            for (ui32 i = 1; i < Borders.size(); ++i) {
                Borders[i] += Borders[i - 1];
            }
        }

        ui32 GetInterval(ui32 x) {
            for (ui32 i = 0; i < Borders.size(); ++i) {
                if (x < Borders[i]) {
                    return i;
                }
            }
            return Borders.size();
        }
        ui32 UpperLimit() {
            return Borders[Borders.size() - 1];
        }
    };

    ui32 Rand(ui32 a, ui32 b) {
        if (a >= b) {
            return a;
        }
        return mt.GenRand() % (b - a) + a;
    }

    ui32 Rand(ui32 b) {
        return Rand(0, b);
    }

    ui32 Rand() {
        return mt.GenRand();
    }

    template <class T>
    T& Rand(std::vector<T>& v) {
        return v[Rand(v.size())];
    } 

    template <class T>
    const T& Rand(const std::vector<T>& v) {
        return v[Rand(v.size())];
    } 

    bool IsCollected(const TLogoBlobID& id, ui32 collectGen, ui32 collectStep) {
        return (id.Generation() < collectGen) || (id.Generation() == collectGen && id.Step() <= collectStep);
    }

    bool IsCollected(const TBlobInfo& blob, ui32 softCollectGen, ui32 softCollectStep, ui32 hardCollectGen, ui32 hardCollectStep) {
        return IsCollected(blob.Id, hardCollectGen, hardCollectStep) || (blob.KeepFlag && IsCollected(blob.Id, softCollectGen, softCollectStep));
    }

    void SendTEvPut(TEnvironmentSetup& env, TActorId sender, ui32 groupId, TLogoBlobID id, TString data) {
        auto ev = new TEvBlobStorage::TEvPut(id, data, TInstant::Max());

#ifdef LOG_PUT        
        Cerr << "Request# " << ev->Print(false) << Endl;
#endif

        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev);
        });
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvPutResult>> CaptureTEvPutResult(TEnvironmentSetup& env, 
            TActorId sender, bool termOnCapture = true) {
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, termOnCapture);

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

        if (res->Get()->Status == NKikimrProto::OK) {
            blob.Status = TBlobInfo::EStatus::WRITTEN;
        }
        
        if (blob.Id.Generation() <= blockedGen) {
            if (status == NKikimrProto::ALREADY)
                Cerr << "TEvPut got ALREADY instead of BLOCKED" << Endl;
            else
                UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::BLOCKED, 
                    TStringBuilder() << "Successful put over the barrier, blob id# " << blob.Id.ToString() << ", blocked generation# " << blockedGen);
        } else if (IsCollected(blob, softCollectGen, softCollectStep, hardCollectGen, hardCollectStep) ) {
            if (status == NKikimrProto::OK) {
                Cerr << "Put over the barrier, blob id# " << blob.Id.ToString() << Endl;
            } else if (status == NKikimrProto::ERROR) {
                Cerr << "Unexpected Error" << Endl;
            } else if (status != NKikimrProto::NODATA) {
                UNIT_FAIL("Unexpected status");
            }
        } else if (status != NKikimrProto::OK && status != NKikimrProto::ERROR) {
            UNIT_FAIL("Unexpected status");
        }
    }

    void VerifiedPut(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, TBlobInfo& blob, TBSState& state) {
        auto sender = env.Runtime->AllocateEdgeActor(nodeId);
        SendTEvPut(env, sender, groupId, blob.Id, blob.Data);
        auto res = CaptureTEvPutResult(env, sender, true);
        VerifyTEvPutResult(res.Release(), blob, state);
    }

    void SendTEvGet(TEnvironmentSetup& env, TActorId sender, ui32 groupId, TLogoBlobID id,
            bool mustRestoreFirst = false, bool isIndexOnly = false, ui32 forceBlockedGeneration = 0) {
        auto ev = new TEvBlobStorage::TEvGet(id, 0, id.BlobSize(), TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead, 
                mustRestoreFirst, isIndexOnly, forceBlockedGeneration);
                
#ifdef LOG_GET
        Cerr << "Request# " << ev->Print(true) << Endl;
#endif
    
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev);
        });
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> CaptureTEvGetResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture = true) {
        TInstant deadline;
        env.Runtime->WrapInActorContext(sender, [&] {
            deadline = TActivationContext::Now() + TDuration::Seconds(1);
        });
        
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, termOnCapture, deadline);
        
        // if (res.Get() == nullptr) { Cerr << "TEvGet didn't return" << Endl; return nullptr; } // <- Temporary solution

        UNIT_ASSERT(!!res.Get());

#ifdef LOG_GET        
        Cerr << "Response# " << res->Get()->ToString() << Endl;
#endif

        return res.Release();
    }


    void VerifyTEvGetResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> res,
            TBlobInfo& blob, bool mustRestoreFirst, bool isIndexOnly, ui32 forceBlockedGeneration,
            TBSState& state) 
    {
        Y_UNUSED(mustRestoreFirst);
        Y_UNUSED(forceBlockedGeneration);
        
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
            } else if (blob.Status == TBlobInfo::EStatus::WRITTEN) {
                UNIT_ASSERT_VALUES_UNEQUAL(responses[0].Status, NKikimrProto::NODATA);
                if (responses[0].Status == NKikimrProto::OK && !isIndexOnly) {
                    UNIT_ASSERT_VALUES_EQUAL(responses[0].Buffer, blob.Data);
                }
            } else {
                if (responses[0].Status != NKikimrProto::NODATA) { 
                    Cerr << "Read over the barrier, blob id# " << responses[0].Id.ToString() << Endl; 
                }
            }
        }
    }
    
    void VerifiedGet(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId,
            TBlobInfo& blob, bool mustRestoreFirst, bool isIndexOnly, ui32 forceBlockedGeneration,
            TBSState& state) 
    {
        auto sender = env.Runtime->AllocateEdgeActor(nodeId);
        SendTEvGet(env, sender, groupId, blob.Id, mustRestoreFirst, isIndexOnly, forceBlockedGeneration);
        auto res = CaptureTEvGetResult(env, sender, true);
        // if (!res) { return; } // <- Temporary solution

        VerifyTEvGetResult(res.Release(), blob, mustRestoreFirst, isIndexOnly, forceBlockedGeneration, state);
    }

    void SendTEvGet(TEnvironmentSetup& env, TActorId sender, ui32 groupId, std::vector<TBlobInfo>& blobs,
            bool mustRestoreFirst = false, bool isIndexOnly = false, ui32 forceBlockedGeneration = 0) 
    {
        ui32 sz = blobs.size();

        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[sz]);
        for (ui32 i = 0; i < sz; ++i) {
            queries[i].Id = blobs[i].Id;
            queries[i].Shift = 0;
            queries[i].Size = blobs[i].Data.size();
        }

        auto ev = new TEvBlobStorage::TEvGet(queries, sz, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead,
                mustRestoreFirst, isIndexOnly, forceBlockedGeneration);

#ifdef LOG_MULTIGET
        Cerr << "Request# " << ev->Print(true) << Endl;
#endif

        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev);
        });
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> CaptureMultiTEvGetResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture = true) {
        TInstant deadline;
        env.Runtime->WrapInActorContext(sender, [&] {
            deadline = TActivationContext::Now() + TDuration::Seconds(1);
        });

        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, termOnCapture, deadline);

        // if (!res) { Cerr << "TEvDiscover didn't return" << Endl; return nullptr; } // <- Temporary Solution

        UNIT_ASSERT(!!res.Get());

#ifdef LOG_MULTIGET        
        Cerr << "Response# " << res->Get()->ToString() << Endl;
#endif

        return res.Release();
    }


    void VerifyTEvGetResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> res, 
            std::vector<TBlobInfo>& blobs, bool mustRestoreFirst, bool isIndexOnly, ui32 forceBlockedGeneration,
            TBSState& state) 
    {
        Y_UNUSED(mustRestoreFirst);
        Y_UNUSED(forceBlockedGeneration);
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
                } else if (blobs[i].Status == TBlobInfo::EStatus::WRITTEN) {
                    UNIT_ASSERT_VALUES_UNEQUAL(responses[i].Status, NKikimrProto::NODATA);
                    if (responses[i].Status == NKikimrProto::OK && !isIndexOnly) {
                        UNIT_ASSERT_VALUES_EQUAL(responses[i].Buffer, blobs[i].Data);
                    }
                } else {
                    if (responses[i].Status != NKikimrProto::NODATA) { 
                        Cerr << "Read over the barrier, blob id# " << responses[i].Id.ToString() << Endl; 
                    }
                }
            }
        }
    }

    void VerifiedGet(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, std::vector<TBlobInfo>& blobs, bool mustRestoreFirst, bool isIndexOnly, ui32 forceBlockedGeneration,
            TBSState& state) 
    {
        auto sender = env.Runtime->AllocateEdgeActor(nodeId);
        SendTEvGet(env, sender, groupId, blobs, mustRestoreFirst, isIndexOnly, forceBlockedGeneration);

        auto res = CaptureMultiTEvGetResult(env, sender, true);
        // if (!res) { return; } // <- Temporary solution

        VerifyTEvGetResult(res.Release(), blobs, mustRestoreFirst, isIndexOnly, forceBlockedGeneration, state);
        
    }

    void SendTEvRange(TEnvironmentSetup& env, TActorId sender, ui32 groupId, ui64 tabletId, 
            TLogoBlobID from, TLogoBlobID to, bool mustRestoreFirst, bool indexOnly) {
        auto ev = new TEvBlobStorage::TEvRange(tabletId, from, to, mustRestoreFirst, TInstant::Max(), indexOnly);

#ifdef LOG_RANGE        
        Cerr << "Request# " << ev->ToString() << Endl;
#endif

        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev);
        });
    }
        
    TAutoPtr<TEventHandle<TEvBlobStorage::TEvRangeResult>> CaptureTEvRangeResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture = true) {
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(sender, termOnCapture);

#ifdef LOG_RANGE
        Cerr << "Response# " << res->Get()->ToString() << Endl;
#endif

        return res.Release();
    }

    void VerifyTEvRangeResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvRangeResult>> res, ui64 tabletId, TLogoBlobID from, TLogoBlobID to, bool mustRestoreFirst, bool indexOnly,
            std::vector<TBlobInfo>& blobs, TBSState& state) 
    {
        Y_UNUSED(mustRestoreFirst);
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
            bool mustRestoreFirst, bool indexOnly, std::vector<TBlobInfo>& blobs, TBSState& state) 
    {
        auto sender = env.Runtime->AllocateEdgeActor(nodeId);
        SendTEvRange(env, sender, groupId, tabletId, from, to, mustRestoreFirst, indexOnly);
        auto res = CaptureTEvRangeResult(env, sender, true);
        VerifyTEvRangeResult(res.Release(), tabletId, from, to, mustRestoreFirst, indexOnly, blobs, state);
    }  

    void SendTEvDiscover(TEnvironmentSetup& env, TActorId sender, ui32 groupId, ui64 tabletId, ui32 minGeneration, bool readBody, 
            bool discoverBlockedGeneration, ui32 forceBlockedGeneration, bool fromLeader) {
        auto ev = new TEvBlobStorage::TEvDiscover(tabletId, minGeneration, readBody, discoverBlockedGeneration, 
                TInstant::Max(), forceBlockedGeneration, fromLeader);

#ifdef LOG_DISCOVER
        Cerr << "Request# " << ev->Print(true) << Endl;
#endif

        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev);
        });
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvDiscoverResult>> CaptureTEvDiscoverResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture = true) {
        TInstant deadline;
        env.Runtime->WrapInActorContext(sender, [&] {
            deadline = TActivationContext::Now() + TDuration::Seconds(1);
        });

        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvDiscoverResult>(sender, termOnCapture, deadline);

        // if (!res) { Cerr << "TEvDiscover didn't return" << Endl; return nullptr; } // <- Temporary Solution

        UNIT_ASSERT_C(!!res, "Timeout - no TEvGetResult received");

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

        // if (!res) { return; } // <- Temporary solution 
        auto status = res->Get()->Status;
        TBlobInfo* maxBlob = nullptr;
        TBlobInfo* discoveredBlob = nullptr;

        for (auto& blob : blobs) {
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
            if ((status == NKikimrProto::OK) && (blob.Id == res->Get()->Id)) {
                discoveredBlob = &blob;
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
            if (discoveredBlob->Status == TBlobInfo::NONEXISTENT) {;
                UNIT_FAIL(TStringBuilder() << "Found nonexistent blob with id " << res->Get()->Id.ToString());
            } else if (discoveredBlob->Status == TBlobInfo::COLLECTED || IsCollected(*discoveredBlob, softCollectGen, softCollectStep, hardCollectGen, hardCollectStep)) {
                Cerr << "TEvDiscover found collected blob with id " << discoveredBlob->Id;
            } else {
                UNIT_ASSERT(maxBlob);
                UNIT_ASSERT_VALUES_EQUAL(discoveredBlob->Id, maxBlob->Id);
                if (readBody) {
                    UNIT_ASSERT_VALUES_EQUAL(discoveredBlob->Data, maxBlob->Data);
                }
            }
        }
    }

    void VerifiedDiscover(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, ui64 tabletId, ui32 minGeneration, bool readBody, 
            bool discoverBlockedGeneration, ui32 forceBlockedGeneration, bool fromLeader, std::vector<TBlobInfo>& blobs, TBSState& state) {
        auto sender = env.Runtime->AllocateEdgeActor(nodeId);
        SendTEvDiscover(env, sender, groupId, tabletId, minGeneration, readBody, discoverBlockedGeneration, forceBlockedGeneration, fromLeader);
        auto res = CaptureTEvDiscoverResult(env, sender, true);
        VerifyTEvDiscoverResult(res.Release(), tabletId, minGeneration, readBody, discoverBlockedGeneration, forceBlockedGeneration, fromLeader, blobs, state);
    }

    void SendTEvCollectGarbage(TEnvironmentSetup& env, TActorId sender, ui32 groupId, 
        ui64 tabletId, ui32 recordGeneration, ui32 perGenerationCounter, ui32 channel,
        bool collect, ui32 collectGeneration,
        ui32 collectStep, TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep,
        bool isMultiCollectAllowed, bool hard) 
    {
        auto ev = new TEvBlobStorage::TEvCollectGarbage(tabletId, recordGeneration, perGenerationCounter, channel, collect, collectGeneration, collectStep,
                    keep, doNotKeep, TInstant::Max(), isMultiCollectAllowed, hard);
        
#ifdef LOG_COLLECT_GARBAGE
        Cerr << "Request# " << ev->Print(false) << Endl;
#endif
       
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev);
        });
    }  
        
    TAutoPtr<TEventHandle<TEvBlobStorage::TEvCollectGarbageResult>> CaptureTEvCollectGarbageResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture = true) {
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(sender, termOnCapture);

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
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::BLOCKED); // <- known bug in blob depot
            if (status == NKikimrProto::ALREADY) { 
                Cerr << "Race detected, expected status BLOCKED" << Endl;
            } else { 
                UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::BLOCKED);
            }
        } else {
            if (collect) {
                if (hard) {
                    if (collectGeneration < hardCollectGen || (collectGeneration == hardCollectGen && collectStep < hardCollectStep)) {
                        UNIT_ASSERT_VALUES_UNEQUAL(status, NKikimrProto::OK);
                    }
                } else {
                    if (collectGeneration < softCollectGen || (collectGeneration == softCollectGen && collectStep < softCollectStep)) {
                        UNIT_ASSERT_VALUES_UNEQUAL(status, NKikimrProto::OK);
                    }
                }
            }

            if (status == NKikimrProto::OK) {
                if (collect) {
                    if (hard) {
                        hardCollectGen = collectGeneration;
                        hardCollectStep = collectStep;
                    } else {
                        softCollectGen = collectGeneration;
                        softCollectStep = collectStep;
                    }
                }
                for (auto& blob : blobs) {
                    if (keep) {
                        if (setKeep.find(blob.Id) != setKeep.end()) {
                            if (blob.Status != TBlobInfo::EStatus::WRITTEN) {
                                UNIT_FAIL("Setting keep flag on nonexistent blob");
                            }
                            blob.KeepFlag = true;
                        }
                    }
                    if (doNotKeep) {
                        if (setNotKeep.find(blob.Id) != setNotKeep.end()) {
                            blob.KeepFlag = false;
                        }
                    }

                    if ((blob.Status == TBlobInfo::EStatus::WRITTEN) && (blob.Id.TabletID() == tabletId) && (blob.Id.Channel() == channel) &&
                            IsCollected(blob, softCollectGen, softCollectStep, hardCollectGen, hardCollectStep)) {
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
        bool isMultiCollectAllowed, bool hard, std::vector<TBlobInfo>& blobs, TBSState& state) 
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

        auto res = CaptureTEvCollectGarbageResult(env, sender, true);
        VerifyTEvCollectGarbageResult(res.Release(), tabletId, recordGeneration, perGenerationCounter, channel, collect, 
            collectGeneration, collectStep, copyKeep.Get(), copyDoNotKeep.Get(), isMultiCollectAllowed, hard, blobs, state);
    }


    void SendTEvBlock(TEnvironmentSetup& env, TActorId sender, ui32 groupId, ui64 tabletId, ui32 generation) {
        auto ev = new TEvBlobStorage::TEvBlock(tabletId, generation, TInstant::Max());
        
#ifdef LOG_BLOCK
        Cerr << "Request# " << ev->Print(true) << Endl;
#endif
        
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev);
        });
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvBlockResult>> CaptureTEvBlockResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture = true) {
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(sender, termOnCapture);
        
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
            if (status == NKikimrProto::ERROR) {
                Cerr << "TEvBlock: Unexpected error" << Endl;
            } else if (status == NKikimrProto::BLOCKED) {
                Cerr << "TEvBlock: Detect race" << Endl;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::ALREADY);
            }
        }
        if (status == NKikimrProto::OK) {
            blockedGen = generation;
        }
    }


    void VerifiedBlock(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, ui64 tabletId, ui32 generation, TBSState& state) {
        auto sender = env.Runtime->AllocateEdgeActor(nodeId);

        SendTEvBlock(env, sender, groupId, tabletId, generation);
        auto res = CaptureTEvBlockResult(env, sender, true);
        VerifyTEvBlockResult(res.Release(), tabletId, generation, state);
    }

    void TestBasicPutAndGet(TEnvironmentSetup& env, ui64 tabletId, ui32 groupId) {
        std::vector<TBlobInfo> blobs;
        TBSState state;
        state[tabletId];

        blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1));
        blobs.push_back(TBlobInfo(DataGen(100), tabletId, 2));
        blobs.push_back(TBlobInfo(DataGen(200), tabletId, 1));

        VerifiedGet(env, 1, groupId, blobs[0], true, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[1], true, false, 0, state);

        VerifiedPut(env, 1, groupId, blobs[0], state);
        VerifiedPut(env, 1, groupId, blobs[1], state);

        VerifiedGet(env, 1, groupId, blobs[0], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[1], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[2], false, false, 0, state);

        VerifiedGet(env, 1, groupId, blobs, false, false, 0, state);

        blobs.push_back(TBlobInfo(DataGen(1000), tabletId + (1 << 12), 1));
        VerifiedPut(env, 1, groupId, blobs[2], state);
        VerifiedPut(env, 1, groupId, blobs[3], state);
        
        VerifiedGet(env, 1, groupId, blobs[2], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[3], false, false, 0, state);
    }
    
    TLogoBlobID MinBlobID(ui64 tablet) {
        return TLogoBlobID(tablet, 0, 0, 0, 0, 0);
    }

    TLogoBlobID MaxBlobID(ui64 tablet) {
        return TLogoBlobID(tablet, Max<ui32>(), Max<ui32>(), NKikimr::TLogoBlobID::MaxChannel,
            NKikimr::TLogoBlobID::MaxBlobSize, NKikimr::TLogoBlobID::MaxCookie, NKikimr::TLogoBlobID::MaxPartId,
            NKikimr::TLogoBlobID::MaxCrcMode);
    }

    void TestBasicRange(TEnvironmentSetup& env, ui64 tabletId, ui32 groupId) { 
        std::vector<TBlobInfo> blobs;
        TBSState state;
        state[tabletId];
        blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1));
        blobs.push_back(TBlobInfo(DataGen(100), tabletId, 2));
        blobs.push_back(TBlobInfo(DataGen(200), tabletId, 1));

        VerifiedPut(env, 1, groupId, blobs[0], state);
        VerifiedPut(env, 1, groupId, blobs[1], state);

        VerifiedRange(env, 1, groupId, tabletId, MinBlobID(tabletId), MaxBlobID(tabletId), false, false, blobs, state);
        VerifiedRange(env, 1, groupId, tabletId, MinBlobID(tabletId), MaxBlobID(tabletId), false, true, blobs, state);

        ui32 n = 100;
        for (ui32 i = 0; i < n; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1000 + i));
            if (i % 2) {
                VerifiedPut(env, 1, groupId, blobs[i], state);
            }
        }

        VerifiedRange(env, 1, groupId, tabletId, blobs[0].Id, blobs[n/2 - 1].Id, false, false, blobs, state);
        VerifiedRange(env, 1, groupId, tabletId, blobs[0].Id, blobs[n/2 - 1].Id, false, true, blobs, state);
    }

    void TestBasicDiscover(TEnvironmentSetup& env, ui64 tabletId, ui32 groupId) {

        std::vector<TBlobInfo> blobs;
        ui64 tablet2 = tabletId + 1000;
        TBSState state;
        state[tabletId];
        state[tablet2];

        blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, 2));
        blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, 3));
        blobs.push_back(TBlobInfo(DataGen(200), tabletId, 1, 4));

        VerifiedDiscover(env, 1, groupId, tabletId, 0, false, false, 0, true, blobs, state);
        VerifiedDiscover(env, 1, groupId, tabletId, 1, false, false, 0, true, blobs, state);
        VerifiedDiscover(env, 1, groupId, tabletId, 0, true, false, 0, true, blobs, state);

        VerifiedPut(env, 1, groupId, blobs[0], state);
        VerifiedPut(env, 1, groupId, blobs[1], state);

        VerifiedDiscover(env, 1, groupId, tabletId, 0, false, false, 0, true, blobs, state);
        VerifiedDiscover(env, 1, groupId, tabletId, 0, true, false, 0, true, blobs, state);
        VerifiedDiscover(env, 1, groupId, tabletId, 0, true, false, 0, true, blobs, state);

        VerifiedDiscover(env, 1, groupId, tabletId, 100, true, false, 0, true, blobs, state);

        blobs.push_back(TBlobInfo(DataGen(1000), tablet2, 10, 2));
        VerifiedDiscover(env, 1, groupId, tablet2, 0, true, false, 0, true, blobs, state);

        VerifiedPut(env, 1, groupId, blobs[3], state);
        VerifiedDiscover(env, 1, groupId, tablet2, 0, false, false, 0, true, blobs, state);

        VerifiedDiscover(env, 1, groupId, tablet2, 42, true, false, 0, true, blobs, state);
    }

    void TestBasicBlock(TEnvironmentSetup& env, ui64 tabletId, ui32 groupId) {
        ui32 tablet2 = tabletId + 1;
        std::vector<TBlobInfo> blobs;
        TBSState state;
        state[tabletId];
        state[tablet2];

        ui32 lastGen = 0;

        blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, lastGen++));
        blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, lastGen++));
        blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, lastGen++));
        blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, lastGen++));
        blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, lastGen++)); // blobs[4]

        ui32 lastGen2 = 1;
        blobs.push_back(TBlobInfo(DataGen(100), tablet2, 1, lastGen2++, 1));
        blobs.push_back(TBlobInfo(DataGen(100), tablet2, 2, lastGen2++, 2));
        blobs.push_back(TBlobInfo(DataGen(100), tablet2, 3, lastGen2++, 3));

        VerifiedPut(env, 1, groupId, blobs[2], state);

        VerifiedBlock(env, 1, groupId, tabletId, 3, state);

        VerifiedPut(env, 1, groupId, blobs[1], state);
        VerifiedPut(env, 1, groupId, blobs[3], state);
        VerifiedGet(env, 1, groupId, blobs[3], false, false, 0, state);

        VerifiedPut(env, 1, groupId, blobs[4], state);
        VerifiedGet(env, 1, groupId, blobs[4], false, false, 0, state);

        VerifiedBlock(env, 1, groupId, tabletId, 2, state);
        VerifiedBlock(env, 1, groupId, tabletId, 3, state);

        VerifiedPut(env, 1, groupId, blobs[5], state);

        VerifiedBlock(env, 1, groupId, tablet2, 2, state);

        VerifiedPut(env, 1, groupId, blobs[6], state);
        VerifiedGet(env, 1, groupId, blobs[6], false, false, 0, state);

        VerifiedPut(env, 1, groupId, blobs[7], state);
        VerifiedGet(env, 1, groupId, blobs[7], false, false, 0, state);
    }

    void TestBasicCollectGarbage(TEnvironmentSetup& env, ui64 tabletId, ui32 groupId) {    
        std::vector<TBlobInfo> blobs;
        ui64 tablet2 = tabletId + 1;
        TBSState state;
        state[tabletId];
        state[tablet2];

        for (ui32 i = 0; i < 10; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, 1, i + 1, 0));
        }

        for (ui32 i = 10; i < 20; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, 1, i + 1, (i % 2)));
        }

        for (ui32 i = 0; i < 10; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, 2, i + 1, 0));
        }

        for (ui32 i = 0; i < 10; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, 3 + i, 1, 0));
        }

        for (ui32 i = 0; i < 5; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tablet2, 1, 1, 1 + i, 0));
        }

        for (ui32 i = 0; i < 5; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tablet2, 1, 2 + i, 1, 0));
        }

        // blobs[0]..blobs[39] - tabletId
        // blobs[40]..blobs[49] - tablet2

        for (auto& blob : blobs) {
            VerifiedPut(env, 1, groupId, blob, state);
        }

        ui32 gen = 2;
        ui32 perGenCtr = 1;

        VerifiedCollectGarbage(env, 1, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 2, nullptr, nullptr, false, false,
            blobs, state);
        VerifiedGet(env, 1, groupId, blobs[0], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[1], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[2], false, false, 0, state);
        
        VerifiedGet(env, 1, groupId, blobs[20], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[30], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[31], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[40], false, false, 0, state); 

        VerifiedCollectGarbage(env, 1, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 1, nullptr, nullptr, false, false, blobs, state);

        {
            TBlobInfo blob(DataGen(100), tabletId, 99, 1, 1, 0);
            VerifiedPut(env, 1, groupId, blob, state);
            blobs.push_back(blob);
        }

        VerifiedCollectGarbage(env, 1, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 3, nullptr, nullptr, false, true,
            blobs, state);

        {
            TBlobInfo blob(DataGen(100), tabletId, 99, 1, 3, 0);
            VerifiedPut(env, 1, groupId, blob, state);
            blobs.push_back(blob);
        } 
        VerifiedRange(env, 1, groupId, tabletId, blobs[1].Id, blobs[1].Id, false, false, blobs, state);

        VerifiedGet(env, 1, groupId, blobs[1], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[2], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[3], false, false, 0, state);
        
        VerifiedGet(env, 1, groupId, blobs[20], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[30], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[31], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[40], false, false, 0, state); 

        VerifiedCollectGarbage(env, 1, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 1, nullptr, nullptr, false, true, blobs, state);

        VerifiedCollectGarbage(env, 1, groupId, tabletId, gen, perGenCtr++, 0, false, 1, 5, 
                new TVector<TLogoBlobID>({blobs[4].Id, blobs[5].Id}), 
                nullptr, 
                false, false,
                blobs, state);

        VerifiedGet(env, 1, groupId, blobs[4], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[5], false, false, 0, state);

        VerifiedCollectGarbage(env, 1, groupId, tabletId, gen, perGenCtr++, 0, false, 1, 6, 
                nullptr, 
                new TVector<TLogoBlobID>({blobs[4].Id, blobs[5].Id}), 
                false, false,
                blobs, state);
        VerifiedGet(env, 1, groupId, blobs[4], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[5], false, false, 0, state);


        VerifiedCollectGarbage(env, 1, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 15, nullptr, nullptr, false, true, blobs, state);
        
        VerifiedRange(env, 1, groupId, tabletId, blobs[10].Id, blobs[19].Id, false, false, blobs, state);

        gen++;
        perGenCtr = 1;
        VerifiedCollectGarbage(env, 1, groupId, tabletId, gen + 1, perGenCtr++, 0, true, 2, 1, nullptr, nullptr, false, false, blobs, state);
        VerifiedGet(env, 1, groupId, blobs[18], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[19], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[20], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[21], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[30], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[31], false, false, 0, state);
        VerifiedGet(env, 1, groupId, blobs[40], false, false, 0, state);

        VerifiedCollectGarbage(env, 1, groupId, tabletId, 6, 1, 0, true, 2, 1, nullptr, nullptr, false, false, blobs, state);

        VerifiedRange(env, 1, groupId, tabletId, blobs[0].Id, blobs[39].Id, false, false, blobs, state);
        VerifiedRange(env, 1, groupId, tablet2, blobs[40].Id, blobs[49].Id, false, false, blobs, state);

        VerifiedCollectGarbage(env, 1, groupId, tabletId, 7, 2, 0, true, 3, 1, nullptr, nullptr, false, true, blobs, state);

        VerifiedRange(env, 1, groupId, tabletId, blobs[0].Id, blobs[39].Id, false, false, blobs, state);

        VerifiedBlock(env, 1, groupId, tabletId, 10, state);
        VerifiedCollectGarbage(env, 1, groupId, tabletId, 7, 1, 0, true, 100, 1, nullptr, nullptr, false, true, blobs, state);
        VerifiedGet(env, 1, groupId, blobs[39], false, false, 0, state);
    }

    Y_UNIT_TEST(BasicPutAndGet) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot);
        
        TestBasicPutAndGet(*envPtr, 1, regularGroups[0]);
        TestBasicPutAndGet(*envPtr, 11, blobDepot);

        envPtr->Sim(TDuration::Seconds(20));
    }

    Y_UNIT_TEST(BasicRange) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot);
        
        TestBasicRange(*envPtr, 1, regularGroups[0]);
        TestBasicRange(*envPtr, 100, blobDepot);

        envPtr->Sim(TDuration::Seconds(20));
    }

    Y_UNIT_TEST(BasicDiscover) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot);
        
        TestBasicDiscover(*envPtr, 1000, regularGroups[0]);
        TestBasicDiscover(*envPtr, 100, blobDepot);

        envPtr->Sim(TDuration::Seconds(20));
    }

    Y_UNIT_TEST(BasicBlock) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot);
        
        TestBasicBlock(*envPtr, 15, regularGroups[0]);
        TestBasicBlock(*envPtr, 100, blobDepot);

        envPtr->Sim(TDuration::Seconds(20));
    }

    Y_UNIT_TEST(BasicCollectGarbage) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot);
        
        TestBasicCollectGarbage(*envPtr, 15, regularGroups[0]);
        TestBasicCollectGarbage(*envPtr, 100, blobDepot);

        envPtr->Sim(TDuration::Seconds(20));
    }

    Y_UNIT_TEST(Random) {
        enum EActions {
            ALTER = 0,
            PUT,
            GET,
            MULTIGET,
            RANGE,
            BLOCK,
            DISCOVER,
            COLLECT_GARBAGE_HARD,
            COLLECT_GARBAGE_SOFT,
        };
        std::vector<ui32> probs = { 10, 10, 3, 3, 2, 1, 1, 3, 3 };
        TIntervals act(probs);

        ui32 n = 1000;
        ui32 nodeCount = 8;

        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot, nodeCount);
        TEnvironmentSetup& env = *envPtr;

        ui32 groupId = blobDepot;
        // ui32 groupId = regularGroups[0];

        std::vector<ui32> tablets = {10, 11, 12};
        std::vector<ui32> tabletGen = {1, 1, 1};
        std::vector<ui32> tabletStep = {1, 1, 1};
        std::vector<ui32> channels = {0, 1, 2};

        std::vector<TBlobInfo> blobs;

        blobs.push_back(TBlobInfo("junk", 999, 999, 1, 1, 0));

        TBSState state;
        for (ui32 i = 0; i < tablets.size(); ++i) {
            state[tablets[i]];
        }

        ui32 perGenCtr = 0;

        for (ui32 i = 0; i < n; ++i) {
            ui32 tablet = Rand(tablets.size());
            ui32 tabletId = tablets[tablet];
            ui32 channel = Rand(channels);
            ui32& gen = tabletGen[tablet];
            ui32& step = tabletStep[tablet];
            ui32 node = Rand(1, nodeCount);

            ui32 softCollectGen = state[tabletId].Channels[channel].SoftCollectGen;
            ui32 softCollectStep = state[tabletId].Channels[channel].SoftCollectStep;
            ui32 hardCollectGen = state[tabletId].Channels[channel].HardCollectGen;
            ui32 hardCollectStep = state[tabletId].Channels[channel].HardCollectStep;
            
            ui32 action = act.GetInterval(Rand(act.UpperLimit()));
            // Cerr << action << Endl;
            switch (action) {
            case EActions::ALTER:
                {
                    if (Rand(3) == 0) {
                        gen += Rand(1, 2);
                        perGenCtr = 0;
                    } else {
                        step += Rand(1, 2);
                    }
                }
                break;

            case EActions::PUT:
                {
                    ui32 cookie = Rand(NKikimr::TLogoBlobID::MaxCookie);
                    TBlobInfo blob(DataGen(Rand(50, 1000)), tabletId, cookie, gen, step, channel);
                    VerifiedPut(env, node, groupId, blob, state);
                    blobs.push_back(blob);
                }
                break;

            case EActions::GET:
                {
                    TBlobInfo& blob = Rand(blobs);
                    bool mustRestoreFirst = Rand(2);
                    bool indexOnly = Rand(2);
                    ui32 forceBlockedGeneration = 0;
                    VerifiedGet(env, node, groupId, blob, mustRestoreFirst, indexOnly, forceBlockedGeneration, state);
                }
                break;

            case EActions::MULTIGET:
                {
                    std::vector<TBlobInfo> getBlobs;
                    ui32 requestSize = Rand(50, 100);
                    for (ui32 i = 0; i < blobs.size() && i < requestSize; ++i) {
                        TBlobInfo& blob = Rand(blobs);
                        if (blob.Id.TabletID() == tabletId) {
                            getBlobs.push_back(blob);
                        }
                    }

                    if (getBlobs.empty()) {
                        getBlobs.push_back(blobs[0]);
                    }

                    bool mustRestoreFirst = Rand(2);
                    bool indexOnly = Rand(2);
                    ui32 forceBlockedGeneration = 0;
                    VerifiedGet(env, node, groupId, getBlobs, mustRestoreFirst, indexOnly, forceBlockedGeneration, state);
                }
                break;

            case EActions::RANGE:
                {
                    TLogoBlobID r1 = Rand(blobs).Id;
                    TLogoBlobID r2 = Rand(blobs).Id;

                    TLogoBlobID from(tabletId, r1.Generation(), r1.Step(), r1.Channel(), r1.BlobSize(), r1.Cookie());
                    TLogoBlobID to(tabletId, r2.Generation(), r2.Step(), r2.Channel(), r2.BlobSize(), r2.Cookie());

                    if (from > to) {
                        std::swap(from, to);
                    }

                    bool mustRestoreFirst = Rand(2);
                    bool indexOnly = Rand(2);
                    VerifiedRange(env, node, groupId, tabletId, from, to, mustRestoreFirst, indexOnly, blobs, state);
                }
                break;

            case EActions::BLOCK:
                {
                    ui32 prevBlockedGen = state[tabletId].BlockedGen;
                    ui32 tryBlock = prevBlockedGen + Rand(4);
                    if (tryBlock > 0) {
                        tryBlock -= 1;
                    }

                    VerifiedBlock(env, node, groupId, tabletId, tryBlock, state);
                }
                break;


            case EActions::DISCOVER:
                {
                    ui32 minGeneration = Rand(0, gen + 2);
                    bool readBody = Rand(2);
                    bool discoverBlockedGeneration = Rand(2);
                    ui32 forceBlockedGeneration = 0; 
                    bool fromLeader = Rand(2);

                    VerifiedDiscover(env, node, groupId, tabletId, minGeneration, readBody, discoverBlockedGeneration, forceBlockedGeneration,
                        fromLeader, blobs, state);
                }
                break;

            case EActions::COLLECT_GARBAGE_HARD:
                {
                    ui32 tryGen = hardCollectGen + Rand(2);
                    ui32 tryStep = 0;
                    if (tryGen > 0 && !Rand(3)) { tryGen -= 1; }
                    if (tryGen > hardCollectGen) {
                        tryStep = Rand(hardCollectStep / 2);
                    } else {
                        tryStep = hardCollectStep + Rand(2);
                        if (tryStep > 0 && !Rand(3)) { tryStep -= 1; }
                    }

                    bool collect = Rand(2);
                    bool isMultiCollectAllowed = Rand(2);

                    THolder<TVector<TLogoBlobID>> keep(new TVector<TLogoBlobID>());
                    THolder<TVector<TLogoBlobID>> doNotKeep(new TVector<TLogoBlobID>());

                    for (auto& blob : blobs) {
                        if (blob.Status == TBlobInfo::EStatus::WRITTEN) {
                            if (!Rand(5)) {
                                keep->push_back(blob.Id);
                            } else if (Rand(2)) {
                                doNotKeep->push_back(blob.Id);
                            }
                        }
                    }

                    if (keep->size() == 0 && doNotKeep->size() == 0) {
                        collect = true; 
                    }

                    VerifiedCollectGarbage(env, node, groupId, tabletId, gen, perGenCtr++, channel, collect, 
                        tryGen, tryStep, keep.Release(), doNotKeep.Release(), isMultiCollectAllowed, true, blobs, state);
                }
                break;

            case EActions::COLLECT_GARBAGE_SOFT:
                {
                    ui32 tryGen = softCollectGen + Rand(2);
                    ui32 tryStep = 0;
                    if (tryGen > 0 && !Rand(3)) { tryGen -= 1; }
                    if (tryGen > softCollectGen) {
                        tryStep = Rand(softCollectStep / 2);
                    } else {
                        tryStep = softCollectStep + Rand(2);
                        if (tryStep > 0 && !Rand(3)) { tryStep -= 1; }
                    }

                    bool collect = Rand(2);
                    bool isMultiCollectAllowed = Rand(2);

                    THolder<TVector<TLogoBlobID>> keep(new TVector<TLogoBlobID>());
                    THolder<TVector<TLogoBlobID>> doNotKeep(new TVector<TLogoBlobID>());

                    for (auto& blob : blobs) {
                        if (blob.Status == TBlobInfo::EStatus::WRITTEN) {
                            if (!Rand(5)) {
                                keep->push_back(blob.Id);
                            } else if (Rand(2)) {
                                doNotKeep->push_back(blob.Id);
                            }
                        }
                    }

                    if (keep->size() == 0 && doNotKeep->size() == 0) {
                        collect = true; 
                    }

                    VerifiedCollectGarbage(env, node, groupId, tabletId, gen, perGenCtr++, channel, collect, 
                        tryGen, tryStep, keep.Release(), doNotKeep.Release(), isMultiCollectAllowed, false, blobs, state);
                }
                break;

            default: 
                UNIT_FAIL("TIntervals failed");
            }
        }

        envPtr->Sim(TDuration::Seconds(20));
    }
}
