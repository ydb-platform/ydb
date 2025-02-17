#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blob_depot/events.h>

#include <algorithm>
#include <random>

#include "blob_depot_test_functions.h"

void DecommitGroup(TBlobDepotTestEnvironment& tenv, ui32 groupId) {
    TString blobDepotPool = "decommit_blob_depot_pool";
    ui32 blobDepotPoolId = 42;
    tenv.Env->CreatePoolInBox(1, blobDepotPoolId, blobDepotPool);
    NKikimrBlobStorage::TConfigRequest request;

    auto *cmd = request.AddCommand()->MutableDecommitGroups();
    cmd->AddGroupIds(groupId);
    cmd->SetHiveId(tenv.Env->Runtime->GetDomainsInfo()->GetHive());
    auto *prof = cmd->AddChannelProfiles();
    prof->SetStoragePoolName(blobDepotPool);
    prof->SetCount(2);
    prof = cmd->AddChannelProfiles();
    prof->SetStoragePoolName(blobDepotPool);
    prof->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
    prof->SetCount(2);

    auto response = tenv.Env->Invoke(request);
    UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
}

void TestBasicPutAndGet(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId) {
    std::vector<TBlobInfo> blobs;
    TBSState state;
    state[tabletId];

    blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1));
    blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 2));
    blobs.push_back(TBlobInfo(tenv.DataGen(200), tabletId, 1));

    VerifiedGet(*tenv.Env, 1, groupId, blobs[0], true, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[1], true, false, std::nullopt, state);

    VerifiedPut(*tenv.Env, 1, groupId, blobs[0], state);
    VerifiedPut(*tenv.Env, 1, groupId, blobs[1], state);

    VerifiedGet(*tenv.Env, 1, groupId, blobs[0], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[1], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[2], false, false, std::nullopt, state);

    VerifiedGet(*tenv.Env, 1, groupId, blobs, false, false, std::nullopt, state);

    blobs.push_back(TBlobInfo(tenv.DataGen(1000), tabletId + (1 << 12), 1));
    VerifiedPut(*tenv.Env, 1, groupId, blobs[2], state);
    VerifiedPut(*tenv.Env, 1, groupId, blobs[3], state);

    VerifiedGet(*tenv.Env, 1, groupId, blobs[2], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[3], false, false, std::nullopt, state);
}

TLogoBlobID MinBlobID(ui64 tablet) {
    return TLogoBlobID(tablet, 0, 0, 0, 0, 0);
}

TLogoBlobID MaxBlobID(ui64 tablet) {
    return TLogoBlobID(tablet, Max<ui32>(), Max<ui32>(), NKikimr::TLogoBlobID::MaxChannel,
        NKikimr::TLogoBlobID::MaxBlobSize, NKikimr::TLogoBlobID::MaxCookie, NKikimr::TLogoBlobID::MaxPartId,
        NKikimr::TLogoBlobID::MaxCrcMode);
}

void TestBasicRange(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId) {
    std::vector<TBlobInfo> blobs;
    TBSState state;
    state[tabletId];
    blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1));
    blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 2));
    blobs.push_back(TBlobInfo(tenv.DataGen(200), tabletId, 1));

    VerifiedPut(*tenv.Env, 1, groupId, blobs[0], state);
    VerifiedPut(*tenv.Env, 1, groupId, blobs[1], state);

    VerifiedRange(*tenv.Env, 1, groupId, tabletId, MinBlobID(tabletId), MaxBlobID(tabletId), false, false, blobs, state);
    VerifiedRange(*tenv.Env, 1, groupId, tabletId, MinBlobID(tabletId), MaxBlobID(tabletId), false, true, blobs, state);

    ui32 n = 100;
    for (ui32 i = 0; i < n; ++i) {
        blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1000 + i));
        if (i % 2) {
            VerifiedPut(*tenv.Env, 1, groupId, blobs[i], state);
        }
    }

    VerifiedRange(*tenv.Env, 1, groupId, tabletId, blobs[0].Id, blobs[n/2 - 1].Id, false, false, blobs, state);
    VerifiedRange(*tenv.Env, 1, groupId, tabletId, blobs[0].Id, blobs[n/2 - 1].Id, false, true, blobs, state);
}

void TestBasicDiscover(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId) {

    std::vector<TBlobInfo> blobs;
    ui64 tablet2 = tabletId + 1000;
    TBSState state;
    state[tabletId];
    state[tablet2];

    blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 2));
    blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 3));
    blobs.push_back(TBlobInfo(tenv.DataGen(200), tabletId, 1, 4));
    blobs.push_back(TBlobInfo(tenv.DataGen(200), tabletId, 2, 4, 1));

    VerifiedDiscover(*tenv.Env, 1, groupId, tabletId, 0, false, false, 0, true, blobs, state);
    VerifiedDiscover(*tenv.Env, 1, groupId, tabletId, 1, false, false, 0, true, blobs, state);
    VerifiedDiscover(*tenv.Env, 1, groupId, tabletId, 0, true, false, 0, true, blobs, state);

    VerifiedPut(*tenv.Env, 1, groupId, blobs[0], state);
    VerifiedPut(*tenv.Env, 1, groupId, blobs[1], state);

    VerifiedDiscover(*tenv.Env, 1, groupId, tabletId, 0, false, false, 0, true, blobs, state);
    VerifiedDiscover(*tenv.Env, 1, groupId, tabletId, 0, true, false, 0, true, blobs, state);
    VerifiedDiscover(*tenv.Env, 1, groupId, tabletId, 0, true, false, 0, true, blobs, state);

    VerifiedDiscover(*tenv.Env, 1, groupId, tabletId, 100, true, false, 0, true, blobs, state);

    blobs.push_back(TBlobInfo(tenv.DataGen(1000), tablet2, 10, 2));
    VerifiedDiscover(*tenv.Env, 1, groupId, tablet2, 0, true, false, 0, true, blobs, state);

    VerifiedPut(*tenv.Env, 1, groupId, blobs[3], state);
    VerifiedDiscover(*tenv.Env, 1, groupId, tablet2, 0, false, false, 0, true, blobs, state);

    VerifiedDiscover(*tenv.Env, 1, groupId, tablet2, 42, true, false, 0, true, blobs, state);
}

void TestBasicBlock(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId) {
    ui32 tablet2 = tabletId + 1;
    std::vector<TBlobInfo> blobs;
    TBSState state;
    state[tabletId];
    state[tablet2];

    ui32 lastGen = 0;

    blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, lastGen++));
    blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, lastGen++));
    blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, lastGen++));
    blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, lastGen++));
    blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, lastGen++)); // blobs[4]

    ui32 lastGen2 = 1;
    blobs.push_back(TBlobInfo(tenv.DataGen(100), tablet2, 1, lastGen2++, 1));
    blobs.push_back(TBlobInfo(tenv.DataGen(100), tablet2, 2, lastGen2++, 2));
    blobs.push_back(TBlobInfo(tenv.DataGen(100), tablet2, 3, lastGen2++, 3));

    VerifiedPut(*tenv.Env, 1, groupId, blobs[2], state);

    VerifiedBlock(*tenv.Env, 1, groupId, tabletId, 3, state);

    VerifiedPut(*tenv.Env, 1, groupId, blobs[1], state);
    VerifiedPut(*tenv.Env, 1, groupId, blobs[3], state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[3], false, false, std::nullopt, state);

    VerifiedPut(*tenv.Env, 1, groupId, blobs[4], state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[4], false, false, std::nullopt, state);

    VerifiedBlock(*tenv.Env, 1, groupId, tabletId, 2, state);
    VerifiedBlock(*tenv.Env, 1, groupId, tabletId, 3, state);

    VerifiedPut(*tenv.Env, 1, groupId, blobs[5], state);

    VerifiedBlock(*tenv.Env, 1, groupId, tablet2, 2, state);

    VerifiedPut(*tenv.Env, 1, groupId, blobs[6], state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[6], false, false, std::nullopt, state);

    VerifiedPut(*tenv.Env, 1, groupId, blobs[7], state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[7], false, false, std::nullopt, state);
}

void TestBasicCollectGarbage(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId) {
    std::vector<TBlobInfo> blobs;
    ui64 tablet2 = tabletId + 1;
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

    // blobs[0]..blobs[39] - tabletId
    // blobs[40]..blobs[49] - tablet2

    for (auto& blob : blobs) {
        VerifiedPut(*tenv.Env, 1, groupId, blob, state);
    }

    ui32 gen = 2;
    ui32 perGenCtr = 1;

    VerifiedCollectGarbage(*tenv.Env, 1, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 2, nullptr, nullptr, false, false,
        blobs, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[0], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[1], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[2], false, false, std::nullopt, state);

    VerifiedGet(*tenv.Env, 1, groupId, blobs[20], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[30], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[31], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[40], false, false, std::nullopt, state);

    VerifiedCollectGarbage(*tenv.Env, 1, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 1, nullptr, nullptr, false, false, blobs, state);

    {
        TBlobInfo blob(tenv.DataGen(100), tabletId, 99, 1, 1, 0);
        VerifiedPut(*tenv.Env, 1, groupId, blob, state);
        blobs.push_back(blob);
    }

    VerifiedCollectGarbage(*tenv.Env, 1, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 3, nullptr, nullptr, false, true,
        blobs, state);

    {
        TBlobInfo blob(tenv.DataGen(100), tabletId, 99, 1, 3, 0);
        VerifiedPut(*tenv.Env, 1, groupId, blob, state);
        blobs.push_back(blob);
    }
    VerifiedRange(*tenv.Env, 1, groupId, tabletId, blobs[1].Id, blobs[1].Id, false, false, blobs, state);

    VerifiedGet(*tenv.Env, 1, groupId, blobs[1], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[2], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[3], false, false, std::nullopt, state);

    VerifiedGet(*tenv.Env, 1, groupId, blobs[20], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[30], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[31], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[40], false, false, std::nullopt, state);

    VerifiedCollectGarbage(*tenv.Env, 1, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 1, nullptr, nullptr, false, true, blobs, state);

    VerifiedCollectGarbage(*tenv.Env, 1, groupId, tabletId, gen, perGenCtr++, 0, false, 1, 5,
            new TVector<TLogoBlobID>({blobs[4].Id, blobs[5].Id}),
            nullptr,
            false, false,
            blobs, state);

    VerifiedGet(*tenv.Env, 1, groupId, blobs[4], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[5], false, false, std::nullopt, state);

    VerifiedCollectGarbage(*tenv.Env, 1, groupId, tabletId, gen, perGenCtr++, 0, false, 1, 6,
            nullptr,
            new TVector<TLogoBlobID>({blobs[4].Id, blobs[5].Id}),
            false, false,
            blobs, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[4], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[5], false, false, std::nullopt, state);


    VerifiedCollectGarbage(*tenv.Env, 1, groupId, tabletId, gen, perGenCtr++, 0, true, 1, 15, nullptr, nullptr, false, true, blobs, state);

    VerifiedRange(*tenv.Env, 1, groupId, tabletId, blobs[10].Id, blobs[19].Id, false, false, blobs, state);

    gen++;
    perGenCtr = 1;
    VerifiedCollectGarbage(*tenv.Env, 1, groupId, tabletId, gen + 1, perGenCtr++, 0, true, 2, 1, nullptr, nullptr, false, false, blobs, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[18], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[19], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[20], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[21], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[30], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[31], false, false, std::nullopt, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[40], false, false, std::nullopt, state);

    VerifiedCollectGarbage(*tenv.Env, 1, groupId, tabletId, 6, 1, 0, true, 2, 1, nullptr, nullptr, false, false, blobs, state);

    VerifiedRange(*tenv.Env, 1, groupId, tabletId, blobs[0].Id, blobs[39].Id, false, false, blobs, state);
    VerifiedRange(*tenv.Env, 1, groupId, tablet2, blobs[40].Id, blobs[49].Id, false, false, blobs, state);

    VerifiedCollectGarbage(*tenv.Env, 1, groupId, tabletId, 7, 2, 0, true, 3, 1, nullptr, nullptr, false, true, blobs, state);

    VerifiedRange(*tenv.Env, 1, groupId, tabletId, blobs[0].Id, blobs[39].Id, false, false, blobs, state);

    VerifiedBlock(*tenv.Env, 1, groupId, tabletId, 10, state);
    VerifiedCollectGarbage(*tenv.Env, 1, groupId, tabletId, 7, 1, 0, true, 100, 1, nullptr, nullptr, false, true, blobs, state);
    VerifiedGet(*tenv.Env, 1, groupId, blobs[39], false, false, std::nullopt, state);
}

void TestRestoreGet(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks) {
    std::vector<TBlobInfo> blobs;
    TBSState state;
    state[tabletId];

    std::vector<TActorId> allVdisks = *vdisks;
    std::mt19937 g;
    std::shuffle(allVdisks.begin(), allVdisks.end(), g);

    std::vector<TActorId> brokenVdisks = { allVdisks[0], allVdisks[1], allVdisks[2], allVdisks[3] };
    auto blockedEventType = TEvBlobStorage::TEvVPut::EventType;
    tenv.Env->Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
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
        blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1 + i, 1, 1, 0));
    }

    for (ui32 i = 0; i < blobsNum; ++i) {
        VerifiedPut(*tenv.Env, 1, groupId, blobs[i], state);
    }

    brokenVdisks = { allVdisks[0], allVdisks[1] };
    blockedEventType = TEvBlobStorage::TEvVGet::EventType;

    for (ui32 i = 0; i < blobsNum; ++i) {
        VerifiedGet(*tenv.Env, 1, groupId, blobs[i], true, false, std::nullopt, state, false);
    }

    blockedEventType = TEvBlobStorage::TEvVGet::EventType;
    brokenVdisks = { allVdisks[4], allVdisks[5] };

    for (ui32 i = 0; i < blobsNum; ++i) {
        if (blobs[i].Status == TBlobInfo::EStatus::WRITTEN) {
            VerifiedGet(*tenv.Env, 1, groupId, blobs[i], false, false, std::nullopt, state, false);
        }
    }
}

void TestRestoreDiscover(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks) {
    std::vector<TBlobInfo> blobs;
    TBSState state;
    state[tabletId];

    std::vector<TActorId> allVdisks = *vdisks;
    std::mt19937 g;
    std::shuffle(allVdisks.begin(), allVdisks.end(), g);

    std::vector<TActorId> brokenVdisks = { allVdisks[0], allVdisks[1], allVdisks[2], allVdisks[3] };
    auto blockedEventType = TEvBlobStorage::TEvVPut::EventType;
    tenv.Env->Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
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
        blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 1, 1 + i, 0));
        brokenVdisks = { allVdisks[0], allVdisks[1], allVdisks[2], allVdisks[3] };
        blockedEventType = TEvBlobStorage::TEvVPut::EventType;
        VerifiedPut(*tenv.Env, 1, groupId, blobs[i], state);
        brokenVdisks = { allVdisks[0], allVdisks[1] };
        blockedEventType = TEvBlobStorage::TEvVGet::EventType;
        VerifiedDiscover(*tenv.Env, 1, groupId, tabletId, 0, true, false, 0, false, blobs, state, false);
    }

    for (ui32 i = blobsNum; i < 2 * blobsNum; ++i) {
        blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, 1, 1, 1 + i, 0));
        brokenVdisks = { allVdisks[0], allVdisks[1], allVdisks[2], allVdisks[3] };
        blockedEventType = TEvBlobStorage::TEvVPut::EventType;
        VerifiedPut(*tenv.Env, 1, groupId, blobs[i], state);
        brokenVdisks = { allVdisks[0], allVdisks[1] };
        blockedEventType = TEvBlobStorage::TEvVGet::EventType;
        VerifiedDiscover(*tenv.Env, 1, groupId, tabletId, 0, false, false, 0, false, blobs, state, false);
    }

    blockedEventType = TEvBlobStorage::TEvVGet::EventType;
    brokenVdisks = { allVdisks[4], allVdisks[5] };

    for (ui32 i = 0; i < blobsNum * 2; ++i) {
        if (blobs[i].Status == TBlobInfo::EStatus::WRITTEN) {
            VerifiedGet(*tenv.Env, 1, groupId, blobs[i], false, false, std::nullopt, state, false);
        }
    }
}

void TestRestoreRange(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks) {
    std::vector<TBlobInfo> blobs;
    TBSState state;
    state[tabletId];

    std::vector<TActorId> allVdisks = *vdisks;
    std::mt19937 g;
    std::shuffle(allVdisks.begin(), allVdisks.end(), g);

    std::vector<TActorId> brokenVdisks = { allVdisks[0], allVdisks[1], allVdisks[2], allVdisks[3] };
    auto blockedEventType = TEvBlobStorage::TEvVPut::EventType;
    tenv.Env->Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
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
        blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, tenv.Rand(NKikimr::TLogoBlobID::MaxCookie), 1, 1 + i, 0));
        VerifiedPut(*tenv.Env, 1, groupId, blobs[i], state);
    }

    for (ui32 i = blobsNum; i < 2 * blobsNum; ++i) {
        blobs.push_back(TBlobInfo(tenv.DataGen(100), tabletId, tenv.Rand(NKikimr::TLogoBlobID::MaxCookie), 1, 1 + i, 0));
        VerifiedPut(*tenv.Env, 1, groupId, blobs[i], state);
    }

    blockedEventType = TEvBlobStorage::TEvVGet::EventType;
    brokenVdisks = { allVdisks[0], allVdisks[1] };

    VerifiedRange(*tenv.Env, 1, groupId, tabletId, blobs[0].Id, blobs[blobsNum - 1].Id, true, false, blobs, state, false);
    VerifiedRange(*tenv.Env, 1, groupId, tabletId, blobs[blobsNum].Id, blobs[2 * blobsNum - 1].Id, true, true, blobs, state, false);

    blockedEventType = TEvBlobStorage::TEvVGet::EventType;
    brokenVdisks = { allVdisks[4], allVdisks[5] };

    for (ui32 i = 0; i < 2 * blobsNum; ++i) {
        if (blobs[i].Status == TBlobInfo::EStatus::WRITTEN) {
            VerifiedGet(*tenv.Env, 1, groupId, blobs[i], false, false, std::nullopt, state, false);
        }
    }
}

void TestVerifiedRandom(TBlobDepotTestEnvironment& tenv, ui32 nodeCount, ui64 tabletId0, ui32 groupId,
        ui32 iterationsNum, ui32 decommitStep, ui32 timeLimitSec, std::vector<ui32> probabilities) {
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
        __COUNT__,
    };

    std::vector<std::string> actionName = {
        "ALTER", "PUT", "GET", "MULTIGET", "RANGE", "BLOCK", "DISCOVER", "COLLECT_GARBAGE_HARD", "COLLECT_GARBAGE_SOFT", "RESTART_BLOB_DEPOT"
    };

    TWeightedRandom<ui32> act(tenv.RandomSeed + 0xABCD);
    Y_ABORT_UNLESS(probabilities.size() == EActions::__COUNT__);
    for (ui32 i = 0; i < probabilities.size(); ++i) {
        act.AddValue(i, probabilities[i]);
    }

    std::vector<ui64> tablets = {tabletId0, tabletId0 + 1, tabletId0 + 2};
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

    THPTimer timer;

    for (ui32 iteration = 0; iteration < iterationsNum; ++iteration) {
        if (iteration == decommitStep) {
            DecommitGroup(tenv, groupId);
            continue;
        }

        if (timeLimitSec && timer.Passed() > timeLimitSec) {
            break;
        }
        ui32 tablet = tenv.Rand(tablets.size());
        ui32 tabletId = tablets[tablet];
        ui32 channel = tenv.Rand(channels);
        ui32& gen = tabletGen[tablet];
        ui32& step = tabletStep[tablet];
        ui32 node = tenv.Rand(1, nodeCount);

        ui32 softCollectGen = state[tabletId].Channels[channel].SoftCollectGen;
        ui32 softCollectStep = state[tabletId].Channels[channel].SoftCollectStep;
        ui32 hardCollectGen = state[tabletId].Channels[channel].HardCollectGen;
        ui32 hardCollectStep = state[tabletId].Channels[channel].HardCollectStep;

        ui32 action = act.GetRandom();
        // Cerr << "iteration# " << iteration << " action# " << actionName[action] << " timer# " << timer.Passed() << Endl;
        switch (action) {
        case EActions::ALTER:
            {
                if (tenv.Rand(3) == 0) {
                    gen += tenv.Rand(1, 2);
                    perGenCtr = 0;
                } else {
                    step += tenv.Rand(1, 2);
                }
            }
            break;

        case EActions::PUT:
            {
                ui32 cookie = tenv.Rand(NKikimr::TLogoBlobID::MaxCookie);
                TBlobInfo blob(tenv.DataGen(tenv.Rand(50, 1000)), tabletId, cookie, gen, step, channel);
                VerifiedPut(*tenv.Env, node, groupId, blob, state);
                blobs.push_back(blob);
            }
            break;

        case EActions::GET:
            {
                TBlobInfo& blob = tenv.Rand(blobs);
                bool mustRestoreFirst = false;
                bool indexOnly = tenv.Rand(2);
                VerifiedGet(*tenv.Env, node, groupId, blob, mustRestoreFirst, indexOnly, std::nullopt, state);
            }
            break;

        case EActions::MULTIGET:
            {
                std::vector<TBlobInfo> getBlobs;
                ui32 requestSize = tenv.Rand(50, 100);
                for (ui32 i = 0; i < blobs.size() && i < requestSize; ++i) {
                    TBlobInfo& blob = tenv.Rand(blobs);
                    if (blob.Id.TabletID() == tabletId) {
                        getBlobs.push_back(blob);
                    }
                }

                if (getBlobs.empty()) {
                    getBlobs.push_back(blobs[0]);
                }

                bool mustRestoreFirst = false;
                bool indexOnly = tenv.Rand(2);
                VerifiedGet(*tenv.Env, node, groupId, getBlobs, mustRestoreFirst, indexOnly, std::nullopt, state);
            }
            break;

        case EActions::RANGE:
            {
                TLogoBlobID r1 = tenv.Rand(blobs).Id;
                TLogoBlobID r2 = tenv.Rand(blobs).Id;

                TLogoBlobID from(tabletId, r1.Generation(), r1.Step(), r1.Channel(), r1.BlobSize(), r1.Cookie());
                TLogoBlobID to(tabletId, r2.Generation(), r2.Step(), r2.Channel(), r2.BlobSize(), r2.Cookie());

                if (from > to) {
                    std::swap(from, to);
                }

                bool mustRestoreFirst = false;
                bool indexOnly = tenv.Rand(2);
                VerifiedRange(*tenv.Env, node, groupId, tabletId, from, to, mustRestoreFirst, indexOnly, blobs, state);
            }
            break;

        case EActions::BLOCK:
            {
                ui32 prevBlockedGen = state[tabletId].BlockedGen;
                ui32 tryBlock = prevBlockedGen + tenv.Rand(4);
                if (tryBlock > 0) {
                    tryBlock -= 1;
                }

                VerifiedBlock(*tenv.Env, node, groupId, tabletId, tryBlock, state);
            }
            break;


        case EActions::DISCOVER:
            {
                ui32 minGeneration = tenv.Rand(0, gen + 2);
                bool readBody = tenv.Rand(2);
                bool discoverBlockedGeneration = tenv.Rand(2);
                ui32 forceBlockedGeneration = 0;
                bool fromLeader = tenv.Rand(2);

                VerifiedDiscover(*tenv.Env, node, groupId, tabletId, minGeneration, readBody, discoverBlockedGeneration, forceBlockedGeneration,
                    fromLeader, blobs, state);
            }
            break;

        case EActions::COLLECT_GARBAGE_HARD:
            {
                ui32 tryGen = hardCollectGen + tenv.Rand(2);
                ui32 tryStep = 0;
                if (tryGen > 0 && !tenv.Rand(3)) { tryGen -= 1; }
                if (tryGen > hardCollectGen) {
                    tryStep = tenv.Rand(hardCollectStep / 2);
                } else {
                    tryStep = hardCollectStep + tenv.Rand(2);
                    if (tryStep > 0 && !tenv.Rand(3)) { tryStep -= 1; }
                }

                bool collect = tenv.Rand(2);
                bool isMultiCollectAllowed = tenv.Rand(2);

                THolder<TVector<TLogoBlobID>> keep(new TVector<TLogoBlobID>());
                THolder<TVector<TLogoBlobID>> doNotKeep(new TVector<TLogoBlobID>());

                for (auto& blob : blobs) {
                    if (blob.Status == TBlobInfo::EStatus::WRITTEN) {
                        if (!tenv.Rand(5)) {
                            keep->push_back(blob.Id);
                        } else if (tenv.Rand(2)) {
                            doNotKeep->push_back(blob.Id);
                        }
                    }
                }

                if (keep->size() == 0 && doNotKeep->size() == 0) {
                    collect = true;
                }

                VerifiedCollectGarbage(*tenv.Env, node, groupId, tabletId, gen, perGenCtr++, channel, collect,
                    tryGen, tryStep, keep.Release(), doNotKeep.Release(), isMultiCollectAllowed, true, blobs, state);
            }
            break;

        case EActions::COLLECT_GARBAGE_SOFT:
            {
                ui32 tryGen = softCollectGen + tenv.Rand(2);
                ui32 tryStep = 0;
                if (tryGen > 0 && !tenv.Rand(3)) { tryGen -= 1; }
                if (tryGen > softCollectGen) {
                    tryStep = tenv.Rand(softCollectStep / 2);
                } else {
                    tryStep = softCollectStep + tenv.Rand(2);
                    if (tryStep > 0 && !tenv.Rand(3)) { tryStep -= 1; }
                }

                bool collect = tenv.Rand(2);
                bool isMultiCollectAllowed = tenv.Rand(2);

                THolder<TVector<TLogoBlobID>> keep(new TVector<TLogoBlobID>());
                THolder<TVector<TLogoBlobID>> doNotKeep(new TVector<TLogoBlobID>());

                for (auto& blob : blobs) {
                    if (blob.Status == TBlobInfo::EStatus::WRITTEN) {
                        if (!tenv.Rand(5)) {
                            keep->push_back(blob.Id);
                        } else if (tenv.Rand(2)) {
                            doNotKeep->push_back(blob.Id);
                        }
                    }
                }

                if (keep->size() == 0 && doNotKeep->size() == 0) {
                    collect = true;
                }

                VerifiedCollectGarbage(*tenv.Env, node, groupId, tabletId, gen, perGenCtr++, channel, collect,
                    tryGen, tryStep, keep.Release(), doNotKeep.Release(), isMultiCollectAllowed, false, blobs, state);
            }
            break;
        case EActions::RESTART_BLOB_DEPOT:
            if (tenv.BlobDepotTabletId) {
                auto edge = tenv.Env->Runtime->AllocateEdgeActor(node);
                tenv.Env->Runtime->WrapInActorContext(edge, [&] {
                    TActivationContext::Register(CreateTabletKiller(tenv.BlobDepotTabletId));
                });
                tenv.Env->Runtime->DestroyActor(edge);
            }
            break;

        default:
            UNIT_FAIL("Unknown action# " << action);
        }
    }
}

void TestLoadPutAndGet(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, ui32 maxBlobSize,
        ui32 readsNum, bool decommit, ui32 timeLimitSec, std::vector<ui32> probabilities) {
    enum EActions {
        GET,
        MULTIGET,
        RANGE,
        DISCOVER,
        CATCH_ALL,
        RESTART_BLOB_DEPOT,
        __COUNT__,
    };
    
    TWeightedRandom<ui32> act(tenv.RandomSeed + 0xABCD);
    Y_ABORT_UNLESS(probabilities.size() == EActions::__COUNT__);
    for (ui32 i = 0; i < probabilities.size(); ++i) {
        act.AddValue(i, probabilities[i]);
    }

    std::vector<TBlobInfo> blobs;
    std::map<TLogoBlobID, TBlobInfo*> mappedBlobs;
    TBSState state;
    state[tabletId];

    TActorId edge = tenv.Env->Runtime->AllocateEdgeActor(1);

    blobs.reserve(blobsNum);

    std::map<ui64, std::shared_ptr<TEvArgs>> requests;

    ui32 getCtr = 0;
    ui32 rangeCtr = 0;
    ui32 discoverCtr = 0;

    for (ui32 i = 0; i < blobsNum; ++i) {
        blobs.push_back(TBlobInfo(tenv.DataGen(tenv.Rand(1, maxBlobSize)), tabletId, tenv.Rand(NKikimr::TLogoBlobID::MaxCookie), 1, 1, 0));
        mappedBlobs[blobs[i].Id] = &blobs[i];
    }

    for (ui32 i = 0; i < blobsNum; ++i) {
        SendTEvPut(*tenv.Env, edge, groupId, blobs[i].Id, blobs[i].Data);
    }

    for (ui32 i = 0; i < blobsNum; ++i) {
        auto res = CaptureTEvPutResult(*tenv.Env, edge, false);
        UNIT_ASSERT_C(res->Get(), TStringBuilder() << "Fail on iteration# " << i);
        auto it = mappedBlobs.find(res->Get()->Id);
        if (it == mappedBlobs.end()) {
            UNIT_FAIL("Put nonexistent blob");
        }
        VerifyTEvPutResult(res.Release(), *it->second, state);
    }

    if (decommit) {
        DecommitGroup(tenv, groupId);
    }

    THPTimer timer;

    for (ui32 iteration = 0; iteration < readsNum; ++iteration) {
        ui32 action = act.GetRandom();
        if (iteration == readsNum - 1) { // Catch all results on the last iteration
            action = EActions::CATCH_ALL;
        }
        if (timeLimitSec && timer.Passed() > timeLimitSec) {
            break;
        }

        ui64 cookie = tenv.Rand64();
        // Cerr << action << Endl;
        switch (action) {
        case EActions::GET:
            {
                ui32 blobNum = tenv.Rand(1 , blobsNum);
                bool mustRestoreFirst = tenv.Rand(2);
                bool indexOnly = tenv.Rand(2);
                SendTEvGet(*tenv.Env, edge, groupId, blobs[blobNum].Id, mustRestoreFirst, indexOnly, std::nullopt, cookie);
                getCtr++;
                requests[cookie] = std::make_shared<TEvGetArgs>(mustRestoreFirst, indexOnly);
            }
            break;

        case EActions::MULTIGET:
            {
                ui32 blobsInRequest = tenv.Rand(1, 33);
                std::vector<TBlobInfo> request;
                for (ui32 i = 0; i < blobsInRequest; ++i) {
                    request.push_back(tenv.Rand(blobs));
                }

                bool mustRestoreFirst = tenv.Rand(2);
                bool indexOnly = tenv.Rand(2);
                SendTEvGet(*tenv.Env, edge, groupId, request, mustRestoreFirst, indexOnly, std::nullopt, cookie);
                getCtr++;
                requests[cookie] = std::make_shared<TEvGetArgs>(mustRestoreFirst, indexOnly);
            }
            break;

        case EActions::RANGE:
            {
                TLogoBlobID from = tenv.Rand(blobs).Id;
                TLogoBlobID to = tenv.Rand(blobs).Id;
                if (from > to) {
                    std::swap(from, to);
                }

                bool mustRestoreFirst = false;
                bool indexOnly = tenv.Rand(2);
                SendTEvRange(*tenv.Env, edge, groupId, tabletId, from, to, mustRestoreFirst, indexOnly, cookie);
                rangeCtr++;
                requests[cookie] = std::make_shared<TEvRangeArgs>(mustRestoreFirst, indexOnly);
            }
            break;

        case EActions::DISCOVER:
            {
                ui32 minGeneration = 0;
                bool readBody = tenv.Rand(2);
                bool discoverBlockedGeneration = tenv.Rand(2);
                ui32 forceBlockedGeneration = 0;
                bool fromLeader = tenv.Rand(2);

                SendTEvDiscover(*tenv.Env, edge, groupId, tabletId, minGeneration, readBody, discoverBlockedGeneration, forceBlockedGeneration,
                    fromLeader, cookie);
                discoverCtr++;
                requests[cookie] = std::make_shared<TEvDiscoverArgs>(minGeneration, readBody, discoverBlockedGeneration, forceBlockedGeneration, fromLeader);
            }
            break;

        case EActions::CATCH_ALL:
            {
                // Cerr << getCtr << ' ' << rangeCtr << ' ' << discoverCtr << Endl;
                while (getCtr + rangeCtr + discoverCtr) {
                    auto ev = CaptureAnyResult(*tenv.Env, edge);
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
                            VerifyTEvGetResult(res.release(), response, args.MustRestoreFirst, args.IndexOnly, std::nullopt, state);
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
            if (tenv.BlobDepotTabletId) {
                tenv.Env->Runtime->WrapInActorContext(edge, [&] {
                    TActivationContext::Register(CreateTabletKiller(tenv.BlobDepotTabletId));
                });
            }
            break;

        default:
            UNIT_FAIL("Unknown action# " << action);
        }
    }
}
