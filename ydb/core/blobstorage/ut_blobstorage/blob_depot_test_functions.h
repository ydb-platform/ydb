#pragma once

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blob_depot/events.h>

#include <util/random/mersenne.h>
#include <util/random/random.h>

#include <algorithm>
#include <random>

#include "blob_depot_event_managers.h"

void TestBasicPutAndGet(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId);

void TestBasicRange(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId);

void TestBasicDiscover(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId);

void TestBasicBlock(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId);

void TestBasicCollectGarbage(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId);

void TestRestoreGet(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks);

void TestRestoreDiscover(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks);

void TestRestoreRange(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, std::vector<TActorId>* vdisks);

void TestVerifiedRandom(TBlobDepotTestEnvironment& tenv, ui32 nodeCount, ui64 tabletId0, ui32 groupId, ui32 iterationsNum,
        ui32 decommitStep = 1e9, ui32 timeLimitSec = 0, std::vector<ui32> probabilities = { 10, 10, 3, 3, 2, 1, 1, 3, 3, 1 });

void TestLoadPutAndGet(TBlobDepotTestEnvironment& tenv, ui64 tabletId, ui32 groupId, ui32 blobsNum, ui32 maxBlobSize, ui32 readsNum,
        bool decommit = false, ui32 timeLimitSec = 0, std::vector<ui32> probabilities = { 5, 1, 5, 5, 1, 1 });