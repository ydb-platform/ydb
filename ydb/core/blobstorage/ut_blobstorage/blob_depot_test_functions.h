#pragma once

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blob_depot/events.h>

#include <util/random/mersenne.h>
#include <util/random/random.h>

#include <algorithm>
#include <random>

#include <blob_depot_event_managers.h>
#include <blob_depot_auxiliary_structures.h>

void ConfigureEnvironment(ui32 numGroups, std::unique_ptr<TEnvironmentSetup>& envPtr, std::vector<ui32>& regularGroups, ui32& blobDepot, ui32 nodeCount = 8, 
        TBlobStorageGroupType erasure = TBlobStorageGroupType::ErasureMirror3of4);


struct TBlobDepotTestEnvironment {
    TMersenne<ui32> Mt;
    TMersenne<ui64> Mt64;
    
    std::unique_ptr<TEnvironmentSetup> Env;
    std::vector<ui32> RegularGroups;
    ui32 BlobDepot;
    ui32 BlobDepotTabletId;
    THPTimer Timer;
    ui32 TimeLimit;
    TBlobDepotTestEnvironment(ui32 seed = 0, ui32 numGroups = 1, ui32 nodeCount = 8, TBlobStorageGroupType erasure = TBlobStorageGroupType::ErasureMirror3of4, ui32 timeLimit = 0) 
        : Mt(seed)
        , Mt64(seed)
        , TimeLimit(timeLimit) {
        Cerr << "Mersenne random seed " << seed << Endl;
        ConfigureEnvironment(numGroups, Env, RegularGroups, BlobDepot, nodeCount, erasure);
        BlobDepotTabletId = 0;
    }

    TString DataGen(ui32 len) {
        TString res = "";
        for (ui32 i = 0; i < len; ++i) {
            res += 'A' + Mt.GenRand() % ('z' - 'A');
        }
        return res;
    }

    ui32 Rand(ui32 a, ui32 b) {
        if (a >= b) {
            return a;
        }
        return Mt.GenRand() % (b - a) + a;
    }

    ui32 Rand(ui32 b) {
        return Rand(0, b);
    }

    ui32 Rand() {
        return Mt.GenRand();
    }

    ui32 Rand64() {
        return Mt64.GenRand();
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

    bool IsFinished() {
        return TimeLimit && Timer.Passed() > TimeLimit;
    }
};

void DecommitGroup(TBlobDepotTestEnvironment& tenv, ui32 groupId);

void TestBasicPutAndGet(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId);

TLogoBlobID MinBlobID(ui64 tablet);
TLogoBlobID MaxBlobID(ui64 tablet);

void TestBasicRange(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId);

void TestBasicDiscover(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId);

void TestBasicBlock(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId);

void TestBasicCollectGarbage(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId);

void TestRestoreGet(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks);

void TestRestoreDiscover(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks);

void TestRestoreRange(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks);

void TestVerifiedRandom(TBlobDepotTestEnvironment& tenv, ui32 nodeCount, ui64 tabletId0, ui32 groupId, ui32 iterationsNum, ui32 decommitStep = 1e9, std::vector<ui32> probabilities = { 10, 10, 3, 3, 2, 1, 1, 3, 3, 1 });

void TestLoadPutAndGet(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, ui32 maxBlobSize, ui32 readsNum, bool decommit = false, std::vector<ui32> probabilities = { 5, 1, 5, 5, 1, 1 });