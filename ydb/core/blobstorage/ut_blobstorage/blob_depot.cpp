#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blob_depot/events.h>

#include <util/random/mersenne.h>
#include <util/random/random.h>

#include <algorithm>
#include <random>

#include <blob_depot_event_managers.h>
#include <blob_depot_auxiliary_structures.h>

using namespace NKikimr::NBlobDepot;

Y_UNIT_TEST_SUITE(BlobDepot) {
    TMersenne<ui32> mt(13371);
    TMersenne<ui64> mt64(0xdeadf00d);
    const ui64 BLOB_DEPOT_TABLET_ID = MakeTabletID(1, 0, 0x10000);

    void ConfigureEnvironment(ui32 numGroups, std::unique_ptr<TEnvironmentSetup>& envPtr, std::vector<ui32>& regularGroups, ui32& blobDepot, ui32& blobDepotGroup, ui32 nodeCount = 8, 
            TBlobStorageGroupType erasure = TBlobStorageGroupType::ErasureMirror3of4) {
        envPtr = std::make_unique<TEnvironmentSetup>(TEnvironmentSetup::TSettings{
            .NodeCount = nodeCount,
            .Erasure = erasure,
            .BlobDepotId = BLOB_DEPOT_TABLET_ID,
            .BlobDepotChannels = 4,
            .BlobDepotUseMockGroup = false,
        });

        envPtr->CreateBoxAndPool(1, numGroups + 1);
        envPtr->Sim(TDuration::Seconds(20));

        std::vector<ui32> allGroups = envPtr->GetGroups();

        envPtr->SetupBlobDepot(allGroups[numGroups]);
        blobDepotGroup = allGroups[numGroups];
        allGroups.pop_back();

        regularGroups = allGroups;

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

    ui32 Rand64() {
        return mt64.GenRand();
    }

    template <class T>
    T& Rand(std::vector<T>& v) {
        return v[Rand(v.size())];
    } 

    ui32 SeedRand(ui32 a, ui32 b, ui32 seed) {
        TMersenne<ui32> temp(seed);
        if (a >= b) {
            return a;
        }
        return temp.GenRand() % (b - a) + a;
    }

    ui32 SeedRand(ui32 b, ui32 seed) {
        return SeedRand(0, b, seed);
    }

    template <class T>
    const T& Rand(const std::vector<T>& v) {
        return v[Rand(v.size())];
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

    void TestRestoreGet(TEnvironmentSetup& env, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks) {
        std::vector<TBlobInfo> blobs;
        TBSState state;
        state[tabletId];

        std::vector<TActorId> allVdisks = *vdisks;
        std::mt19937 g;
        std::shuffle(allVdisks.begin(), allVdisks.end(), g);

        std::vector<TActorId> brokenVdisks = { allVdisks[0], allVdisks[1], allVdisks[2], allVdisks[3] };
        auto blockedEventType = TEvBlobStorage::TEvVPut::EventType;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == blockedEventType) {
                for (auto vdisk : brokenVdisks) {
                    if (ev->Recipient == vdisk) {
                        return false;
                    }
                }
            }
            return true;
        };
        
        for (ui32 i = 0; i < blobsNum; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1 + i, 1, 1, 0));
        }

        for (ui32 i = 0; i < blobsNum; ++i) {
            VerifiedPut(env, 1, groupId, blobs[i], state);
        }

        brokenVdisks = { allVdisks[0], allVdisks[1] };
        blockedEventType = TEvBlobStorage::TEvVGet::EventType;

        for (ui32 i = 0; i < blobsNum; ++i) {
            VerifiedGet(env, 1, groupId, blobs[i], true, false, 0, state, false);
        }

        blockedEventType = TEvBlobStorage::TEvVGet::EventType;
        brokenVdisks = { allVdisks[4], allVdisks[5] };

        for (ui32 i = 0; i < blobsNum; ++i) {
            if (blobs[i].Status == TBlobInfo::EStatus::WRITTEN) {
                VerifiedGet(env, 1, groupId, blobs[i], false, false, 0, state, false);
            }
        }
    }

    void TestRestoreDiscover(TEnvironmentSetup& env, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks) {
        std::vector<TBlobInfo> blobs;
        TBSState state;
        state[tabletId];

        std::vector<TActorId> allVdisks = *vdisks;
        std::mt19937 g;
        std::shuffle(allVdisks.begin(), allVdisks.end(), g);

        std::vector<TActorId> brokenVdisks = { allVdisks[0], allVdisks[1], allVdisks[2], allVdisks[3] };
        auto blockedEventType = TEvBlobStorage::TEvVPut::EventType;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == blockedEventType) {
                for (auto vdisk : brokenVdisks) {
                    if (ev->Recipient == vdisk) {
                        return false;
                    }
                }
            }
            return true;
        };

        for (ui32 i = 0; i < blobsNum; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, 1, 1 + i, 0));
            brokenVdisks = { allVdisks[0], allVdisks[1], allVdisks[2], allVdisks[3] };
            blockedEventType = TEvBlobStorage::TEvVPut::EventType;
            VerifiedPut(env, 1, groupId, blobs[i], state);
            brokenVdisks = { allVdisks[0], allVdisks[1] };
            blockedEventType = TEvBlobStorage::TEvVGet::EventType;
            VerifiedDiscover(env, 1, groupId, tabletId, 0, true, false, 0, false, blobs, state, false);
        }

        for (ui32 i = blobsNum; i < 2 * blobsNum; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tabletId, 1, 1, 1 + i, 0));
            brokenVdisks = { allVdisks[0], allVdisks[1], allVdisks[2], allVdisks[3] };
            blockedEventType = TEvBlobStorage::TEvVPut::EventType;
            VerifiedPut(env, 1, groupId, blobs[i], state);
            brokenVdisks = { allVdisks[0], allVdisks[1] };
            blockedEventType = TEvBlobStorage::TEvVGet::EventType;
            VerifiedDiscover(env, 1, groupId, tabletId, 0, false, false, 0, false, blobs, state, false);
        }

        blockedEventType = TEvBlobStorage::TEvVGet::EventType;
        brokenVdisks = { allVdisks[4], allVdisks[5] };

        for (ui32 i = 0; i < blobsNum * 2; ++i) {
            if (blobs[i].Status == TBlobInfo::EStatus::WRITTEN) {
                VerifiedGet(env, 1, groupId, blobs[i], false, false, 0, state, false);
            }
        }
    }

    void TestRestoreRange(TEnvironmentSetup& env, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks) {
        std::vector<TBlobInfo> blobs;
        TBSState state;
        state[tabletId];

        std::vector<TActorId> allVdisks = *vdisks;
        std::mt19937 g;
        std::shuffle(allVdisks.begin(), allVdisks.end(), g);

        std::vector<TActorId> brokenVdisks = { allVdisks[0], allVdisks[1], allVdisks[2], allVdisks[3] };
        auto blockedEventType = TEvBlobStorage::TEvVPut::EventType;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == blockedEventType) {
                for (auto vdisk : brokenVdisks) {
                    if (ev->Recipient == vdisk) {
                        return false;
                    }
                }
            }
            return true;
        };

        for (ui32 i = 0; i < blobsNum; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tabletId, Rand(NKikimr::TLogoBlobID::MaxCookie), 1, 1 + i, 0));
            VerifiedPut(env, 1, groupId, blobs[i], state);
        }

        for (ui32 i = blobsNum; i < 2 * blobsNum; ++i) {
            blobs.push_back(TBlobInfo(DataGen(100), tabletId, Rand(NKikimr::TLogoBlobID::MaxCookie), 1, 1 + i, 0));
            VerifiedPut(env, 1, groupId, blobs[i], state);
        }
        
        blockedEventType = TEvBlobStorage::TEvVGet::EventType;
        brokenVdisks = { allVdisks[0], allVdisks[1] };

        VerifiedRange(env, 1, groupId, tabletId, blobs[0].Id, blobs[blobsNum - 1].Id, true, false, blobs, state, false);
        VerifiedRange(env, 1, groupId, tabletId, blobs[blobsNum].Id, blobs[2 * blobsNum - 1].Id, true, true, blobs, state, false);

        blockedEventType = TEvBlobStorage::TEvVGet::EventType;
        brokenVdisks = { allVdisks[4], allVdisks[5] };

        for (ui32 i = 0; i < 2 * blobsNum; ++i) {
            if (blobs[i].Status == TBlobInfo::EStatus::WRITTEN) {
                VerifiedGet(env, 1, groupId, blobs[i], false, false, 0, state, false);
            }
        }
    }

    Y_UNIT_TEST(BasicPutAndGet) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepotGroup;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot, blobDepotGroup, 8, TBlobStorageGroupType::Erasure4Plus2Block);
        
        TestBasicPutAndGet(*envPtr, 1, regularGroups[0]);
        TestBasicPutAndGet(*envPtr, 11, blobDepot);
    }

    Y_UNIT_TEST(BasicRange) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepotGroup;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot, blobDepotGroup, 8, TBlobStorageGroupType::Erasure4Plus2Block);
        
        TestBasicRange(*envPtr, 1, regularGroups[0]);
        TestBasicRange(*envPtr, 100, blobDepot);
    }

    Y_UNIT_TEST(BasicDiscover) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepotGroup;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot, blobDepotGroup, 8, TBlobStorageGroupType::Erasure4Plus2Block);
        
        TestBasicDiscover(*envPtr, 1000, regularGroups[0]);
        TestBasicDiscover(*envPtr, 100, blobDepot);
    }

    Y_UNIT_TEST(BasicBlock) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepotGroup;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot, blobDepotGroup, 8, TBlobStorageGroupType::Erasure4Plus2Block);
        
        TestBasicBlock(*envPtr, 15, regularGroups[0]);
        TestBasicBlock(*envPtr, 100, blobDepot);
    }

    Y_UNIT_TEST(BasicCollectGarbage) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepotGroup;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot, blobDepotGroup, 8, TBlobStorageGroupType::Erasure4Plus2Block);
        
        TestBasicCollectGarbage(*envPtr, 15, regularGroups[0]);
        TestBasicCollectGarbage(*envPtr, 100, blobDepot);
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
            RESTART_BLOB_DEPOT,
        };
        std::vector<ui32> probs = { 10, 10, 3, 3, 2, 1, 1, 3, 3, 1};
        TIntervals act(probs);

        ui32 iterationsNum = 1000;
        ui32 nodeCount = 8;

        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepotGroup;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot, blobDepotGroup, 8, TBlobStorageGroupType::Erasure4Plus2Block);
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

        for (ui32 iteration = 0; iteration < iterationsNum; ++iteration) {
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
            case EActions::RESTART_BLOB_DEPOT:
                {
                    auto edge = env.Runtime->AllocateEdgeActor(node);
                    env.Runtime->WrapInActorContext(edge, [&] {
                        TActivationContext::Register(CreateTabletKiller(BLOB_DEPOT_TABLET_ID));
                    });
                    env.Runtime->DestroyActor(edge);
                }
                break;

            default: 
                UNIT_FAIL("TIntervals failed");
            }
        }
    }

    Y_UNIT_TEST(LoadPutAndRead) {
        enum EActions {
            GET,
            MULTIGET,
            RANGE,
            DISCOVER,
            CATCH_ALL,
            RESTART_BLOB_DEPOT,
        };
        std::vector<ui32> probs = { 5, 1, 5, 5, 1, 1};
        TIntervals act(probs);

        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepotGroup;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot, blobDepotGroup, 8, TBlobStorageGroupType::Erasure4Plus2Block);

        TEnvironmentSetup& env = *envPtr;

        ui32 groupId = blobDepot;
        // ui32 groupId = regularGroups[0];
        ui64 tabletId = 1;
        
        std::vector<TBlobInfo> blobs;
        std::map<TLogoBlobID, TBlobInfo*> mappedBlobs;
        TBSState state;
        state[tabletId];

        ui32 blobsNum = 1 << 10;
        ui32 maxBlobSize = 1 << 15;
        ui32 readsNum = 500;

        TActorId edge = env.Runtime->AllocateEdgeActor(1);

        blobs.reserve(blobsNum);

        std::map<ui64, std::shared_ptr<TEvArgs>> requests;

        ui32 getCtr = 0;
        ui32 rangeCtr = 0;
        ui32 discoverCtr = 0;

        for (ui32 i = 0; i < blobsNum; ++i) {
            blobs.push_back(TBlobInfo(DataGen(Rand(1, maxBlobSize)), tabletId, Rand(NKikimr::TLogoBlobID::MaxCookie), 1, 1, 0));
            mappedBlobs[blobs[i].Id] = &blobs[i];
        }

        for (ui32 i = 0; i < blobsNum; ++i) {
            SendTEvPut(env, edge, groupId, blobs[i].Id, blobs[i].Data);
        }

        for (ui32 i = 0; i < blobsNum; ++i) {
            auto res = CaptureTEvPutResult(env, edge, false);
            UNIT_ASSERT_C(res->Get(), TStringBuilder() << "Fail on iteration# " << i);
            auto it = mappedBlobs.find(res->Get()->Id);
            if (it == mappedBlobs.end()) {
                UNIT_FAIL("Put nonexistent blob");
            }
            VerifyTEvPutResult(res.Release(), *it->second, state);
        }

        for (ui32 iteration = 0; iteration < readsNum; ++iteration) {
            ui32 action = act.GetInterval(Rand(act.UpperLimit()));
            if (iteration == readsNum - 1) {
                action = 4;
            }
            ui64 cookie = Rand64();
            // Cerr << action << Endl;
            switch (action) {
            case EActions::GET:
                {
                    ui32 blobNum = Rand(1 , blobsNum);
                    bool mustRestoreFirst = Rand(2);
                    bool indexOnly = Rand(2);
                    ui32 forceBlockedGeneration = 0;
                    SendTEvGet(env, edge, groupId, blobs[blobNum].Id, mustRestoreFirst, indexOnly, forceBlockedGeneration, cookie);
                    getCtr++;
                    requests[cookie] = std::make_shared<TEvGetArgs>(mustRestoreFirst, indexOnly, forceBlockedGeneration);
                }
                break;

            case EActions::MULTIGET:
                {
                    ui32 blobsInRequest = Rand(1, 33);
                    std::vector<TBlobInfo> request;
                    for (ui32 i = 0; i < blobsInRequest; ++i) {
                        request.push_back(Rand(blobs));
                    }
                    
                    bool mustRestoreFirst = Rand(2);
                    bool indexOnly = Rand(2);
                    ui32 forceBlockedGeneration = 0;
                    SendTEvGet(env, edge, groupId, request, mustRestoreFirst, indexOnly, forceBlockedGeneration, cookie);
                    getCtr++;
                    requests[cookie] = std::make_shared<TEvGetArgs>(mustRestoreFirst, indexOnly, forceBlockedGeneration);
                }
                break;

            case EActions::RANGE:
                {
                    TLogoBlobID from = Rand(blobs).Id;
                    TLogoBlobID to = Rand(blobs).Id;
                    if (from > to) {
                        std::swap(from, to);
                    }

                    bool mustRestoreFirst = false;
                    bool indexOnly = Rand(2);
                    SendTEvRange(env, edge, groupId, tabletId, from, to, mustRestoreFirst, indexOnly, cookie);
                    rangeCtr++;
                    requests[cookie] = std::make_shared<TEvRangeArgs>(mustRestoreFirst, indexOnly);
                }
                break;

            case EActions::DISCOVER:
                {
                    ui32 minGeneration = 0;
                    bool readBody = Rand(2);
                    bool discoverBlockedGeneration = Rand(2);
                    ui32 forceBlockedGeneration = 0; 
                    bool fromLeader = Rand(2);

                    SendTEvDiscover(env, edge, groupId, tabletId, minGeneration, readBody, discoverBlockedGeneration, forceBlockedGeneration,
                        fromLeader, cookie);
                    discoverCtr++;
                    requests[cookie] = std::make_shared<TEvDiscoverArgs>(minGeneration, readBody, discoverBlockedGeneration, forceBlockedGeneration, fromLeader);
                }
                break;

            case EActions::CATCH_ALL: 
                {
                    // Cerr << getCtr << ' ' << rangeCtr << ' ' << discoverCtr << Endl;
                    while (getCtr + rangeCtr + discoverCtr) {
                        auto ev = CaptureAnyResult(env, edge);
                        UNIT_ASSERT_C(ev, TStringBuilder() << "Event lost, expected " << getCtr << " TEvGetResult's, " << rangeCtr << " TEvRangeResult's, " << discoverCtr << " TEvDiscoverResult's");
                        switch (ev->GetTypeRewrite()) {
                        case TEvBlobStorage::TEvGetResult::EventType:
                            {
                                std::unique_ptr<TEventHandle<TEvBlobStorage::TEvGetResult>> res(reinterpret_cast<TEventHandle<TEvBlobStorage::TEvGetResult>*>(ev.release()));
                                UNIT_ASSERT(res);
                                std::vector<TBlobInfo> response;
                                ui32 responseSz = res->Get()->ResponseSz;
                                for (ui32 i = 0; i < responseSz; ++i) {
                                    response.push_back(*mappedBlobs[res->Get()->Responses[i].Id]);
                                }
                                TEvGetArgs args = *requests[res->Cookie]->Get<TEvGetArgs>();
                                VerifyTEvGetResult(res.release(), response, args.MustRestoreFirst, args.IndexOnly, args.ForceBlockedGeneration, state);
                            }
                            --getCtr;
                            break;
                        case TEvBlobStorage::TEvRangeResult::EventType:
                            {
                                std::unique_ptr<TEventHandle<TEvBlobStorage::TEvRangeResult>> res(reinterpret_cast<TEventHandle<TEvBlobStorage::TEvRangeResult>*>(ev.release()));
                                UNIT_ASSERT(res);
                                TLogoBlobID from = res->Get()->From;
                                TLogoBlobID to = res->Get()->To;
                                TEvRangeArgs args = *requests[res->Cookie]->Get<TEvRangeArgs>();
                                VerifyTEvRangeResult(res.release(), tabletId, from, to, args.MustRestoreFirst, args.IndexOnly, blobs, state);
                            }
                            --rangeCtr;
                            break;
                        case TEvBlobStorage::TEvDiscoverResult::EventType:
                            {
                                std::unique_ptr<TEventHandle<TEvBlobStorage::TEvDiscoverResult>> res(reinterpret_cast<TEventHandle<TEvBlobStorage::TEvDiscoverResult>*>(ev.release()));
                                UNIT_ASSERT(res);
                                UNIT_ASSERT(res->Get());
                                TEvDiscoverArgs args = *requests[res->Cookie]->Get<TEvDiscoverArgs>();
                                VerifyTEvDiscoverResult(res.release(), tabletId, args.MinGeneration, args.ReadBody, args.DiscoverBlockedGeneration,
                                    args.ForceBlockedGeneration, args.FromLeader, blobs, state);
                            }
                            --discoverCtr;
                            break;
                        }
                    }
                }
                break;

            case EActions::RESTART_BLOB_DEPOT:
                // Cerr << "RESTART" << Endl;
                {
                    env.Runtime->WrapInActorContext(edge, [&] {
                        TActivationContext::Register(CreateTabletKiller(BLOB_DEPOT_TABLET_ID));
                    });
                }
                break;

            default: 
                UNIT_FAIL("TIntervals failed");
            }
        }
    }

    Y_UNIT_TEST(RestoreGet) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepotGroup;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot, blobDepotGroup, 8, TBlobStorageGroupType::Erasure4Plus2Block);
        TEnvironmentSetup& env = *envPtr;

        auto vdisksRegular = env.GetGroupInfo(regularGroups[0])->GetDynamicInfo().ServiceIdForOrderNumber;
        auto vdisksBlobDepot = env.GetGroupInfo(blobDepotGroup)->GetDynamicInfo().ServiceIdForOrderNumber;
        
        TestRestoreGet(*envPtr, 15, regularGroups[0], 10, &vdisksRegular);
        TestRestoreGet(*envPtr, 100, blobDepot, 10, &vdisksBlobDepot);
    }

    Y_UNIT_TEST(RestoreDiscover) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepotGroup;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot, blobDepotGroup, 8, TBlobStorageGroupType::Erasure4Plus2Block);
        TEnvironmentSetup& env = *envPtr;

        auto vdisksRegular = env.GetGroupInfo(regularGroups[0])->GetDynamicInfo().ServiceIdForOrderNumber;
        auto vdisksBlobDepot = env.GetGroupInfo(blobDepotGroup)->GetDynamicInfo().ServiceIdForOrderNumber;
        
        TestRestoreDiscover(*envPtr, 15, regularGroups[0], 10, &vdisksRegular);
        TestRestoreDiscover(*envPtr, 100, blobDepot, 10, &vdisksBlobDepot);
    }

    Y_UNIT_TEST(RestoreRange) {
        std::unique_ptr<TEnvironmentSetup> envPtr;
        std::vector<ui32> regularGroups;
        ui32 blobDepotGroup;
        ui32 blobDepot;
        ConfigureEnvironment(1, envPtr, regularGroups, blobDepot, blobDepotGroup, 8, TBlobStorageGroupType::Erasure4Plus2Block);
        TEnvironmentSetup& env = *envPtr;

        auto vdisksRegular = env.GetGroupInfo(regularGroups[0])->GetDynamicInfo().ServiceIdForOrderNumber;
        auto vdisksBlobDepot = env.GetGroupInfo(blobDepotGroup)->GetDynamicInfo().ServiceIdForOrderNumber;
        
        TestRestoreRange(*envPtr, 15, regularGroups[0], 5, &vdisksRegular);
        TestRestoreRange(*envPtr, 100, blobDepot, 10, &vdisksBlobDepot);
    }
}
