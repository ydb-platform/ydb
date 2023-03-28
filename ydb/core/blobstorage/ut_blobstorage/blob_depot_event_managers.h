#pragma once

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blob_depot/events.h>

#include "blob_depot_test_env.h"

#include <optional>

std::unique_ptr<IEventHandle> CaptureAnyResult(TEnvironmentSetup& env, TActorId sender);

/* --------------------------------- PUT --------------------------------- */
void SendTEvPut(TEnvironmentSetup& env, TActorId sender, ui32 groupId, TLogoBlobID id, TString data, ui64 cookie = 0);
TAutoPtr<TEventHandle<TEvBlobStorage::TEvPutResult>> CaptureTEvPutResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture = true, bool withDeadline = true);
void VerifyTEvPutResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvPutResult>> res, TBlobInfo& blob, TBSState& state);
void VerifiedPut(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, TBlobInfo& blob, TBSState& state, bool withDeadline = true);

/* --------------------------------- GET --------------------------------- */
void SendTEvGet(TEnvironmentSetup& env, TActorId sender, ui32 groupId, TLogoBlobID id,
        bool mustRestoreFirst = false, bool isIndexOnly = false,
        std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData = {}, ui64 cookie = 0);
TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> CaptureTEvGetResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture = true, bool withDeadline = true);
void VerifyTEvGetResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> res,
        TBlobInfo& blob, bool mustRestoreFirst, bool isIndexOnly,
        std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData, TBSState& state);
void VerifiedGet(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, TBlobInfo& blob, bool mustRestoreFirst, bool isIndexOnly,
        std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData, TBSState& state, bool withDeadline = true);

/* --------------------------------- MULTIGET --------------------------------- */
void SendTEvGet(TEnvironmentSetup& env, TActorId sender, ui32 groupId, std::vector<TBlobInfo>& blobs,
        bool mustRestoreFirst = false, bool isIndexOnly = false,
        std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData = {}, ui64 cookie = 0);
TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> CaptureMultiTEvGetResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture = true, bool withDeadline = true);
void VerifyTEvGetResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> res,
        std::vector<TBlobInfo>& blobs, bool mustRestoreFirst, bool isIndexOnly,
        std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData, TBSState& state);
void VerifiedGet(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, std::vector<TBlobInfo>& blobs, bool mustRestoreFirst, bool isIndexOnly,
        std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData, TBSState& state, bool withDeadline = true);

/* --------------------------------- RANGE --------------------------------- */
void SendTEvRange(TEnvironmentSetup& env, TActorId sender, ui32 groupId, ui64 tabletId,
        TLogoBlobID from, TLogoBlobID to, bool mustRestoreFirst, bool indexOnly, ui64 cookie = 0);
TAutoPtr<TEventHandle<TEvBlobStorage::TEvRangeResult>> CaptureTEvRangeResult(TEnvironmentSetup& env, TActorId sender, bool termOnCapture = true, bool withDeadline = true);
void VerifyTEvRangeResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvRangeResult>> res, ui64 tabletId, TLogoBlobID from, TLogoBlobID to, bool mustRestoreFirst, bool indexOnly,
        std::vector<TBlobInfo>& blobs, TBSState& state);
void VerifiedRange(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, ui64 tabletId, TLogoBlobID from, TLogoBlobID to,
        bool mustRestoreFirst, bool indexOnly, std::vector<TBlobInfo>& blobs, TBSState& state, bool withDeadline = true);

/* --------------------------------- DISCOVER --------------------------------- */
void SendTEvDiscover(TEnvironmentSetup& env, TActorId sender, ui32 groupId, ui64 tabletId, ui32 minGeneration, bool readBody,
        bool discoverBlockedGeneration, ui32 forceBlockedGeneration, bool fromLeader, ui64 cookie = 0);
TAutoPtr<TEventHandle<TEvBlobStorage::TEvDiscoverResult>> CaptureTEvDiscoverResult(TEnvironmentSetup& env, TActorId sender,
        bool termOnCapture = true, bool withDeadline = true);
void VerifyTEvDiscoverResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvDiscoverResult>> res, ui64 tabletId, ui32 minGeneration, bool readBody,
        bool discoverBlockedGeneration, ui32 forceBlockedGeneration, bool fromLeader, std::vector<TBlobInfo>& blobs, TBSState& state);
void VerifiedDiscover(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, ui64 tabletId, ui32 minGeneration, bool readBody,
        bool discoverBlockedGeneration, ui32 forceBlockedGeneration, bool fromLeader, std::vector<TBlobInfo>& blobs, TBSState& state, bool withDeadline = true);

/* --------------------------------- COLLECT GARBAGE --------------------------------- */
void SendTEvCollectGarbage(TEnvironmentSetup& env, TActorId sender, ui32 groupId,
    ui64 tabletId, ui32 recordGeneration, ui32 perGenerationCounter, ui32 channel,
    bool collect, ui32 collectGeneration,
    ui32 collectStep, TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep,
    bool isMultiCollectAllowed, bool hard, ui64 cookie = 0);
TAutoPtr<TEventHandle<TEvBlobStorage::TEvCollectGarbageResult>> CaptureTEvCollectGarbageResult(TEnvironmentSetup& env, TActorId sender,
        bool termOnCapture = true, bool withDeadline = true);
void VerifyTEvCollectGarbageResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvCollectGarbageResult>> res,
    ui64 tabletId, ui32 recordGeneration, ui32 perGenerationCounter, ui32 channel,
    bool collect, ui32 collectGeneration,
    ui32 collectStep, TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep,
    bool isMultiCollectAllowed, bool hard, std::vector<TBlobInfo>& blobs, TBSState& state);
void VerifiedCollectGarbage(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId,
    ui64 tabletId, ui32 recordGeneration, ui32 perGenerationCounter, ui32 channel,
    bool collect, ui32 collectGeneration,
    ui32 collectStep, TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep,
    bool isMultiCollectAllowed, bool hard, std::vector<TBlobInfo>& blobs, TBSState& state, bool withDeadline = true);

/* --------------------------------- BLOCK --------------------------------- */
void SendTEvBlock(TEnvironmentSetup& env, TActorId sender, ui32 groupId, ui64 tabletId, ui32 generation, ui64 cookie = 0);
TAutoPtr<TEventHandle<TEvBlobStorage::TEvBlockResult>> CaptureTEvBlockResult(TEnvironmentSetup& env, TActorId sender,
        bool termOnCapture = true, bool withDeadline = true);
void VerifyTEvBlockResult(TAutoPtr<TEventHandle<TEvBlobStorage::TEvBlockResult>> res, ui64 tabletId, ui32 generation, TBSState& state);
void VerifiedBlock(TEnvironmentSetup& env, ui32 nodeId, ui32 groupId, ui64 tabletId, ui32 generation, TBSState& state, bool withDeadline = true);
