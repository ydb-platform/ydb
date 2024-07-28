#include "defs.h"

#include "dsproxy_env_mock_ut.h"
#include "dsproxy_test_state_ut.h"
#include "dsproxy_vdisk_mock_ut.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/util/stlog.h>
#include <ydb/core/base/blobstorage_common.h>

#include <cstring>

namespace NKikimr {
namespace NDSProxyPatchTest {

struct TVDiskPointer {
    ui32 VDiskIdx;

    static TVDiskPointer GetVDiskIdx(ui32 vdiskIdx) {
        return {vdiskIdx};
    }

    std::pair<ui32, ui32> GetIndecies(const TDSProxyEnv &env, ui64 hash) const {
        TVDiskID vdisk = env.Info->GetVDiskId(VDiskIdx);
        ui32 idxInSubgroup = env.Info->GetIdxInSubgroup(vdisk, hash);
        return {VDiskIdx, idxInSubgroup};
    }
};

struct TTestArgs {
    TLogoBlobID OriginalId;
    TLogoBlobID PatchedId;
    ui64 OriginalGroupId;
    ui64 CurrentGroupId;
    TString Buffer;
    TVector<TDiff> Diffs;
    TBlobStorageGroupType GType;
    ui32 MaskForCookieBruteForcing;

    ui32 StatusFlags = 1;
    float ApproximateFreeSpaceShare = 0.1;
    TVector<TVector<ui64>> PartPlacement;
    TVector<bool> ErrorVDisks;

    void MakeDefaultPartPlacement() {
        for (TVector<ui64> &pl : PartPlacement) {
            pl.clear();
        }
        switch (GType.GetErasure()) {
        case TErasureType::Erasure4Plus2Block:
            for (ui64 idx = 0; idx < GType.TotalPartCount(); ++idx) {
                PartPlacement[idx].push_back(idx + 1);
            }
            break;
        case TErasureType::ErasureNone:
            PartPlacement[0].push_back(1);
            break;
        case TErasureType::ErasureMirror3:
            PartPlacement[0].push_back(1);
            PartPlacement[1].push_back(1);
            PartPlacement[2].push_back(1);
            break;
        case TErasureType::ErasureMirror3dc:
            PartPlacement[0].push_back(1);
            PartPlacement[3].push_back(2);
            PartPlacement[6].push_back(3);
            break;
        default:
            UNIT_ASSERT(false); // not implemented
        }
    }

    void ResetDefaultPlacement() {
        ErrorVDisks.assign(GType.BlobSubgroupSize(), false);
        PartPlacement.resize(GType.BlobSubgroupSize());
        MakeDefaultPartPlacement();
    }

    void MakeDefault(TBlobStorageGroupType type, bool sameGroupId = true, bool goodPatchedId = true) {
        constexpr ui32 bufferSize = 1000;
        OriginalId = TLogoBlobID(1, 2, 3, 4, bufferSize, 5);
        PatchedId = TLogoBlobID(1, 3, 3, 4, bufferSize, 6);
        OriginalGroupId = 0;
        CurrentGroupId = OriginalGroupId + !sameGroupId;
        Buffer = TString::Uninitialized(bufferSize);
        for (char &c : Buffer) {
            c = 'a';
        }
        for (ui32 idx = 0; idx < bufferSize; idx += 2) {
            Diffs.emplace_back("b", idx);
        }
        GType = type;

        ErrorVDisks.assign(type.BlobSubgroupSize(), false);
        PartPlacement.resize(type.BlobSubgroupSize());
        MakeDefaultPartPlacement();

        MaskForCookieBruteForcing = (Max<ui32>() << 17) & TLogoBlobID::MaxCookie;
        if (goodPatchedId) {
            UNIT_ASSERT(TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(OriginalId, &PatchedId,
                    MaskForCookieBruteForcing, OriginalGroupId, CurrentGroupId));
        }
    }
};

enum class ENaivePatchCase {
    Ok,
    ErrorOnGetItem,
    ErrorOnGet,
    ErrorOnPut,
};

#define CASE_TO_RETURN_STRING(cs) \
    case cs: return #cs \
// end CASE_TO_RETURN_STRING
TString ToString(ENaivePatchCase cs) {
    switch (cs) {
        CASE_TO_RETURN_STRING(ENaivePatchCase::Ok);
        CASE_TO_RETURN_STRING(ENaivePatchCase::ErrorOnGetItem);
        CASE_TO_RETURN_STRING(ENaivePatchCase::ErrorOnGet);
        CASE_TO_RETURN_STRING(ENaivePatchCase::ErrorOnPut);
    }
}

NKikimrProto::EReplyStatus GetPatchResultStatus(ENaivePatchCase naiveCase) {
    switch (naiveCase) {
    case ENaivePatchCase::Ok:
        return NKikimrProto::OK;
    case ENaivePatchCase::ErrorOnGetItem:
    case ENaivePatchCase::ErrorOnGet:
    case ENaivePatchCase::ErrorOnPut:
        return NKikimrProto::ERROR;
    }
}

NKikimrProto::EReplyStatus GetGetResultStatus(ENaivePatchCase naiveCase) {
    switch (naiveCase) {
    case ENaivePatchCase::Ok:
        return NKikimrProto::OK;
    case ENaivePatchCase::ErrorOnGetItem:
        return NKikimrProto::OK;
    case ENaivePatchCase::ErrorOnGet:
        return NKikimrProto::ERROR;
    case ENaivePatchCase::ErrorOnPut:
        return NKikimrProto::OK;
    }
}

NKikimrProto::EReplyStatus GetPutResultStatus(ENaivePatchCase naiveCase) {
    switch (naiveCase) {
    case ENaivePatchCase::Ok:
        return NKikimrProto::OK;
    case ENaivePatchCase::ErrorOnGetItem:
        return NKikimrProto::UNKNOWN;
    case ENaivePatchCase::ErrorOnGet:
        return NKikimrProto::UNKNOWN;
    case ENaivePatchCase::ErrorOnPut:
        return NKikimrProto::ERROR;
    }
}

enum class EVPatchCase {
    Ok,
    OneErrorAndAllPartExistInStart,
    OnePartLostInStart,
    DeadGroupInStart,
    ErrorDuringVPatchDiff,
    Custom,
};

TString ToString(EVPatchCase cs) {
    switch (cs) {
        CASE_TO_RETURN_STRING(EVPatchCase::Ok);
        CASE_TO_RETURN_STRING(EVPatchCase::OneErrorAndAllPartExistInStart);
        CASE_TO_RETURN_STRING(EVPatchCase::OnePartLostInStart);
        CASE_TO_RETURN_STRING(EVPatchCase::DeadGroupInStart);
        CASE_TO_RETURN_STRING(EVPatchCase::ErrorDuringVPatchDiff);
        CASE_TO_RETURN_STRING(EVPatchCase::Custom);
    }
}

NKikimrProto::EReplyStatus GetPatchResultStatus(EVPatchCase vpatchCase) {
    switch (vpatchCase) {
        case EVPatchCase::Ok:
        case EVPatchCase::OneErrorAndAllPartExistInStart:
            return NKikimrProto::OK;
        case EVPatchCase::OnePartLostInStart:
        case EVPatchCase::DeadGroupInStart:
        case EVPatchCase::ErrorDuringVPatchDiff:
        case EVPatchCase::Custom:
            return NKikimrProto::UNKNOWN;
    }
}

struct TFoundPartsResult {
    NKikimrProto::EReplyStatus Status;
    TVector<ui64> Parts;
};

TFoundPartsResult GetVPatchFoundPartsStatus(const TDSProxyEnv &env, const TTestArgs &args,
        EVPatchCase vpatchCase, const TVDiskPointer &vdisk) {

    TVector<ui64> foundPartsOk;
    auto [vdiskIdx, idxInSubgroup] = vdisk.GetIndecies(env, args.OriginalId.Hash());
    if (args.GType.GetErasure() == TErasureType::ErasureMirror3dc) {
        foundPartsOk.push_back(idxInSubgroup / 3);
    } else if (idxInSubgroup < args.GType.TotalPartCount()) {
        foundPartsOk.push_back(idxInSubgroup + 1);
    }
    TFoundPartsResult okResult{NKikimrProto::OK, foundPartsOk};
    TFoundPartsResult lostResult{NKikimrProto::OK, {}};
    TFoundPartsResult errorResult{NKikimrProto::ERROR, {}};
    TFoundPartsResult customOkResult{NKikimrProto::OK, args.PartPlacement[idxInSubgroup]};

    if (args.GType.GetErasure() == TErasureType::ErasureMirror3dc) {
        TFoundPartsResult correctResult = (idxInSubgroup % 3 == 0) ? okResult : lostResult;
        switch (vpatchCase) {
        case EVPatchCase::Ok:
        case EVPatchCase::ErrorDuringVPatchDiff:
            return correctResult;
        case EVPatchCase::OneErrorAndAllPartExistInStart:
            return (idxInSubgroup % 3 == 2 && idxInSubgroup / 3 == 2) ? errorResult : correctResult;
        case EVPatchCase::OnePartLostInStart:
            return (idxInSubgroup == 0) ? lostResult : correctResult;
        case EVPatchCase::DeadGroupInStart:
            return (idxInSubgroup / 3 < 2) ? errorResult : correctResult;
        case EVPatchCase::Custom:
            return args.ErrorVDisks[idxInSubgroup] ? errorResult : customOkResult;
        }
    } else {
        switch (vpatchCase) {
        case EVPatchCase::Ok:
        case EVPatchCase::ErrorDuringVPatchDiff:
            return okResult;
        case EVPatchCase::OneErrorAndAllPartExistInStart:
            return (idxInSubgroup == args.GType.TotalPartCount()) ? errorResult : okResult;
        case EVPatchCase::OnePartLostInStart:
            return (idxInSubgroup == 0) ? lostResult : okResult;
        case EVPatchCase::DeadGroupInStart:
            return (idxInSubgroup <= args.GType.Handoff()) ? errorResult : okResult;
        case EVPatchCase::Custom:
            return args.ErrorVDisks[vdiskIdx] ? errorResult : customOkResult;
        }
    }
}

NKikimrProto::EReplyStatus GetVPatchResultStatus(const TDSProxyEnv &env, const TTestArgs &args,
        EVPatchCase vpatchCase, const TVDiskPointer &vdisk)
{
    auto [prevStatus, parts] = GetVPatchFoundPartsStatus(env, args, vpatchCase, vdisk);
    if (!parts) {
        return NKikimrProto::UNKNOWN;
    }
    if (prevStatus != NKikimrProto::OK) {
        return NKikimrProto::UNKNOWN;
    }

    switch (vpatchCase) {
    case EVPatchCase::Ok:
    case EVPatchCase::OneErrorAndAllPartExistInStart:
    case EVPatchCase::OnePartLostInStart:
    case EVPatchCase::DeadGroupInStart:
    case EVPatchCase::Custom:
        return NKikimrProto::OK;
    case EVPatchCase::ErrorDuringVPatchDiff:
        return NKikimrProto::ERROR;
    }
}

enum class EMovedPatchCase {
    Ok,
    Error
};

TString ToString(EMovedPatchCase cs) {
    switch (cs) {
        CASE_TO_RETURN_STRING(EMovedPatchCase::Ok);
        CASE_TO_RETURN_STRING(EMovedPatchCase::Error);
    }
}

#undef CASE_TO_RETURN_STRING

NKikimrProto::EReplyStatus GetPatchResultStatus(EMovedPatchCase movedCase) {
    switch (movedCase) {
    case EMovedPatchCase::Ok:
        return NKikimrProto::OK;
    case EMovedPatchCase::Error:
        return NKikimrProto::UNKNOWN;
    }
}

NKikimrProto::EReplyStatus GetVMovedPatchResultStatus(EMovedPatchCase movedCase) {
    switch (movedCase) {
    case EMovedPatchCase::Ok:
        return NKikimrProto::OK;
    case EMovedPatchCase::Error:
        return NKikimrProto::ERROR;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename THandle, typename TEventHolder>
void SendByHandle(TTestBasicRuntime &runtime, const THandle &oldHandle, TEventHolder &&ev) {
    auto handle = std::make_unique<IEventHandle>(oldHandle->Sender, oldHandle->Recipient, ev.release(), oldHandle->Flags, oldHandle->Cookie);
    runtime.Send(handle.release());
}

void ReceivePatchResult(TTestBasicRuntime &runtime, const TTestArgs &args, NKikimrProto::EReplyStatus status) {
    CTEST << "ReceivePatchResult: Start\n";
    TAutoPtr<IEventHandle> handle;
    TEvBlobStorage::TEvPatchResult *result = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPatchResult>(handle);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, status);
    UNIT_ASSERT_VALUES_EQUAL(result->Id, args.PatchedId);
    if (status == NKikimrProto::OK) {
        UNIT_ASSERT_VALUES_EQUAL(result->StatusFlags.Raw, args.StatusFlags);
        UNIT_ASSERT_VALUES_EQUAL(result->ApproximateFreeSpaceShare, args.ApproximateFreeSpaceShare);
    }
    CTEST << "ReceivePatchResult: Finish\n";
}

void ConductGet(TTestBasicRuntime &runtime, const TTestArgs &args, ENaivePatchCase naiveCase) {
    CTEST << "ConductGet: Start NaiveCase: " << ToString(naiveCase) << "\n";
    NKikimrProto::EReplyStatus resultStatus = GetGetResultStatus(naiveCase);
    TAutoPtr<IEventHandle> handle;
    TEvBlobStorage::TEvGet *get = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGet>(handle);
    UNIT_ASSERT_VALUES_EQUAL(get->QuerySize, 1);
    UNIT_ASSERT_VALUES_EQUAL(get->Queries[0].Id, args.OriginalId);
    UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, args.PatchedId.Hash());

    std::unique_ptr<TEvBlobStorage::TEvGetResult> getResult;
    if (resultStatus == NKikimrProto::OK) {
        getResult = std::make_unique<TEvBlobStorage::TEvGetResult>(NKikimrProto::OK, 1, TGroupId::FromValue(args.CurrentGroupId));
        if (naiveCase == ENaivePatchCase::ErrorOnGetItem) {
            getResult->Responses[0].Id = args.OriginalId;
            getResult->Responses[0].Status = NKikimrProto::ERROR;
        } else {
            getResult->Responses[0].Buffer = TRope(args.Buffer);
            getResult->Responses[0].Id = args.OriginalId;
            getResult->Responses[0].Status = NKikimrProto::OK;
        }
    } else {
        getResult = std::make_unique<TEvBlobStorage::TEvGetResult>(NKikimrProto::ERROR, 0, TGroupId::FromValue(args.CurrentGroupId));
    }

    SendByHandle(runtime, handle, std::move(getResult));
    CTEST << "ConductGet: Finish\n";
}

TString MakePatchedBuffer(const TTestArgs &args) {
    TString buffer = TString::Uninitialized(args.Buffer.size());
    memcpy(buffer.begin(), args.Buffer.begin(), args.Buffer.size());
    for (auto &diff : args.Diffs) {
        memcpy(buffer.begin() + diff.Offset, diff.GetDataBegin(), diff.GetDiffLength());
    }
    return buffer;
}

void ConductPut(TTestBasicRuntime &runtime, const TTestArgs &args, ENaivePatchCase naiveCase) {
    NKikimrProto::EReplyStatus resultStatus = GetPutResultStatus(naiveCase);
    if (resultStatus == NKikimrProto::UNKNOWN) {
        CTEST << "ConductPut: Skip NaiveCase: " << ToString(naiveCase) << "\n";
        return;
    }
    CTEST << "ConductPut: Start NaiveCase: " << ToString(naiveCase) << "\n";
    TAutoPtr<IEventHandle> handle;
    TEvBlobStorage::TEvPut *put = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPut>(handle);
    UNIT_ASSERT_VALUES_EQUAL(put->Id, args.PatchedId);
    UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, args.OriginalId.Hash());
    TString patchedBuffer = MakePatchedBuffer(args);
    UNIT_ASSERT_VALUES_EQUAL(put->Buffer.ExtractUnderlyingContainerOrCopy<TString>(), patchedBuffer);

    std::unique_ptr<TEvBlobStorage::TEvPutResult> putResult = std::make_unique<TEvBlobStorage::TEvPutResult>(
            resultStatus, args.PatchedId, args.StatusFlags, TGroupId::FromValue(args.CurrentGroupId), args.ApproximateFreeSpaceShare);
    SendByHandle(runtime, handle, std::move(putResult));
    CTEST << "ConductPut: Finish\n";
}

void ConductNaivePatch(TTestBasicRuntime &runtime, const TTestArgs &args, ENaivePatchCase naiveCase) {
    CTEST << "ConductNaivePatch: Start NaiveCase: " << ToString(naiveCase) << Endl;
    ConductGet(runtime, args, naiveCase);
    ConductPut(runtime, args, naiveCase);
    NKikimrProto::EReplyStatus resultStatus = GetPatchResultStatus(naiveCase);
    ReceivePatchResult(runtime, args, resultStatus);
    CTEST << "ConductNaivePatch: Finish\n";
}

template <typename InnerType> 
TString ToString(const TVector<InnerType> &lst) {
    TStringBuilder bld;
    bld << '[';
    for (ui32 idx = 0; idx < lst.size(); ++idx) {
        if (idx) {
            bld << ", ";
        }
        bld << lst[idx];
    }
    bld << ']';
    return bld;
}

void ConductVPatchStart(TTestBasicRuntime &runtime, const TDSProxyEnv &env, const TTestArgs &args,
        EVPatchCase vpatchCase, TVDiskPointer vdiskPointer)
{
    auto [vdiskIdx, idxInSubgroup] = vdiskPointer.GetIndecies(env, args.OriginalId.Hash());
    CTEST << "ConductVPatchStart: Start vdiskIdx# " <<  vdiskIdx << " idxInSubgroup# " << idxInSubgroup << " VPatchCase: " << ToString(vpatchCase) << "\n";
    TVDiskID vdisk = env.Info->GetVDiskInSubgroup(idxInSubgroup, args.OriginalId.Hash());
    auto [status, parts] = GetVPatchFoundPartsStatus(env, args, vpatchCase, vdiskPointer);

    auto start = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPatchStart>({env.VDisks[vdiskIdx]});
    auto &startRecord = start->Get()->Record;
    UNIT_ASSERT(vdisk == VDiskIDFromVDiskID(startRecord.GetVDiskID()));
    UNIT_ASSERT(args.OriginalId == LogoBlobIDFromLogoBlobID(startRecord.GetOriginalBlobId()));

    TInstant now = runtime.GetCurrentTime();
    UNIT_ASSERT(startRecord.HasCookie());
    std::unique_ptr<TEvBlobStorage::TEvVPatchFoundParts> foundParts = std::make_unique<TEvBlobStorage::TEvVPatchFoundParts>(
            status, args.OriginalId, args.PatchedId, vdisk, startRecord.GetCookie(), now, "", &startRecord,
            nullptr, nullptr, nullptr, 0);
    for (auto partId : parts) {
        foundParts->AddPart(partId);
    }
    CTEST << "ConductVPatchStart: Send FoundParts vdiskIdx# " <<  vdiskIdx << " idxInSubgroup# " << idxInSubgroup << "parts# " << ToString(parts) << "\n";
    SendByHandle(runtime, start, std::move(foundParts));
    CTEST << "ConductVPatchStart: Finish vdiskIdx# " <<  vdiskIdx << " idxInSubgroup# " << idxInSubgroup << "\n";
}

void ConductVPatchDiff(TTestBasicRuntime &runtime, const TDSProxyEnv &env, const TTestArgs &args,
        EVPatchCase vpatchCase, TVDiskPointer vdiskPointer)
{
    auto [vdiskIdx, idxInSubgroup] = vdiskPointer.GetIndecies(env, args.PatchedId.Hash());
    TVDiskID vdisk = env.Info->GetVDiskInSubgroup(idxInSubgroup, args.PatchedId.Hash());
    NKikimrProto::EReplyStatus resultStatus = GetVPatchResultStatus(env, args, vpatchCase, vdiskPointer);
    if (resultStatus == NKikimrProto::UNKNOWN) {
        CTEST << "ConductVPatchDiff: Skip vdiskIdx# " <<  vdiskIdx << " idxInSubgroup# " << idxInSubgroup << " VPatchCase: " << ToString(vpatchCase) << "\n";
        return;
    }
    CTEST << "ConductVPatchDiff: Start vdiskIdx# " <<  vdiskIdx << " idxInSubgroup# " << idxInSubgroup << " VPatchCase: " << ToString(vpatchCase) << "\n";

    auto diffEv = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPatchDiff>({env.VDisks[vdiskIdx]});
    auto &diffRecord = diffEv->Get()->Record;
    UNIT_ASSERT(vdisk == VDiskIDFromVDiskID(diffRecord.GetVDiskID()));
    UNIT_ASSERT_VALUES_EQUAL(args.OriginalId, LogoBlobIDFromLogoBlobID(diffRecord.GetOriginalPartBlobId()).FullID());
    UNIT_ASSERT_VALUES_EQUAL(args.PatchedId, LogoBlobIDFromLogoBlobID(diffRecord.GetPatchedPartBlobId()).FullID());

    if (env.Info->Type.ErasureFamily() == TErasureType::ErasureMirror) {
        UNIT_ASSERT_C(!diffEv->Get()->IsXorReceiver(), "it can't be xorreceiver");
        UNIT_ASSERT_C(!diffRecord.XorReceiversSize(), "it can't has xorreceivers");
    }

    TInstant now = runtime.GetCurrentTime();
    UNIT_ASSERT(diffRecord.HasCookie());
    std::unique_ptr<TEvBlobStorage::TEvVPatchResult> result = std::make_unique<TEvBlobStorage::TEvVPatchResult>(
            resultStatus, args.OriginalId, args.PatchedId, vdisk, diffRecord.GetCookie(), now,
            &diffRecord, nullptr, nullptr, nullptr, 0);
    result->SetStatusFlagsAndFreeSpace(args.StatusFlags, args.ApproximateFreeSpaceShare);

    SendByHandle(runtime, diffEv, std::move(result));
    CTEST << "ConductVPatchDiff: Finish vdiskIdx# " <<  vdiskIdx << " idxInSubgroup# " << idxInSubgroup << "\n";
}

void ConductFailedVPatch(TTestBasicRuntime &runtime, const TDSProxyEnv &env, const TTestArgs &args) {
    return; // disabled vpatch
    CTEST << "ConductFailedVPatch: Start\n";
    for (ui32 idxInSubgroup = 0; idxInSubgroup < args.GType.BlobSubgroupSize(); ++idxInSubgroup) {
        TVDiskPointer vdisk = TVDiskPointer::GetVDiskIdx(idxInSubgroup);
        ConductVPatchStart(runtime, env, args, EVPatchCase::OnePartLostInStart, vdisk);
    }
    for (ui32 idxInSubgroup = 0; idxInSubgroup < args.GType.BlobSubgroupSize(); ++idxInSubgroup) {
        TVDiskPointer vdisk = TVDiskPointer::GetVDiskIdx(idxInSubgroup);
        ConductVPatchDiff(runtime, env, args, EVPatchCase::OnePartLostInStart, vdisk);
    }
    CTEST << "ConductFailedVPatch: Finish\n";
}


void ConductVMovedPatch(TTestBasicRuntime &runtime, const TTestArgs &args, EMovedPatchCase movedCase) {
    CTEST << "ConductVMovedPatch: Start MovedPatchCase: " << ToString(movedCase) << Endl;
    NKikimrProto::EReplyStatus resultStatus = GetVMovedPatchResultStatus(movedCase);
    TAutoPtr<IEventHandle> handle;
    TEvBlobStorage::TEvVMovedPatch *vPatch = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVMovedPatch>(handle);

    NKikimrBlobStorage::TEvVMovedPatch &vPatchRecord = vPatch->Record;
    UNIT_ASSERT(args.OriginalId == LogoBlobIDFromLogoBlobID(vPatchRecord.GetOriginalBlobId()));
    UNIT_ASSERT(args.PatchedId == LogoBlobIDFromLogoBlobID(vPatchRecord.GetPatchedBlobId()));
    UNIT_ASSERT_VALUES_EQUAL(vPatchRecord.DiffsSize(), args.Diffs.size());
    for (ui32 diffIdx = 0; diffIdx < args.Diffs.size(); ++diffIdx) {
        UNIT_ASSERT_VALUES_EQUAL(vPatchRecord.GetDiffs(diffIdx).GetOffset(), args.Diffs[diffIdx].Offset);
        UNIT_ASSERT_EQUAL(vPatchRecord.GetDiffs(diffIdx).GetBuffer(), args.Diffs[diffIdx].Buffer);
    }
    ui64 expectedCookie = ((ui64)args.OriginalId.Hash() << 32) | args.PatchedId.Hash();
    UNIT_ASSERT(vPatchRecord.HasCookie());
    UNIT_ASSERT(vPatchRecord.GetCookie() == expectedCookie);

    TVDiskID vDiskId = VDiskIDFromVDiskID(vPatchRecord.GetVDiskID());
    TOutOfSpaceStatus oos(args.StatusFlags, args.ApproximateFreeSpaceShare);
    std::unique_ptr<TEvBlobStorage::TEvVMovedPatchResult> vPatchResult = std::make_unique<TEvBlobStorage::TEvVMovedPatchResult>(
            resultStatus, args.OriginalId, args.PatchedId, vDiskId, expectedCookie, oos,
            TAppData::TimeProvider->Now(), 0, &vPatchRecord, nullptr, nullptr, nullptr, 0, TString());

    SendByHandle(runtime, handle, std::move(vPatchResult));
    CTEST << "ConductVMovedPatch: Finish\n";
}

void ConductMovedPatch(TTestBasicRuntime &runtime, const TDSProxyEnv &env, const TTestArgs &args,
        EMovedPatchCase movedCase)
{
    CTEST << "ConductMovedPatch: Start MovedPatchCase: " << ToString(movedCase) << Endl;
    ConductFailedVPatch(runtime, env, args);
    ConductVMovedPatch(runtime, args, movedCase);
    NKikimrProto::EReplyStatus resultStatus = GetPatchResultStatus(movedCase);
    if (resultStatus == NKikimrProto::UNKNOWN) {
        ConductNaivePatch(runtime, args, ENaivePatchCase::Ok);
    } else {
        ReceivePatchResult(runtime, args, resultStatus);
    }
    CTEST << "ConductMovedPatch: Finish\n";
}

void ConductFallbackPatch(TTestBasicRuntime &runtime, const TTestArgs &args) {
    CTEST << "ConductFallbackPatch: Start\n";
    ConductVMovedPatch(runtime, args, EMovedPatchCase::Ok);
    ReceivePatchResult(runtime, args, NKikimrProto::OK);
    CTEST << "ConductFallbackPatch: Finish\n";
}

void ConductVPatchEvents(TTestBasicRuntime &runtime, const TDSProxyEnv &env, const TTestArgs &args,
        EVPatchCase vpatchCase)
{
    return; // disabled vpatch
    CTEST << "ConductVPatchEvents: Start VPatchCase: " << ToString(vpatchCase) << Endl;
    for (ui32 idxInSubgroup = 0; idxInSubgroup < args.GType.BlobSubgroupSize(); ++idxInSubgroup) {
        TVDiskPointer vdisk = TVDiskPointer::GetVDiskIdx(idxInSubgroup);
        ConductVPatchStart(runtime, env, args, vpatchCase, vdisk);
    }
    for (ui32 idxInSubgroup = 0; idxInSubgroup < args.GType.BlobSubgroupSize(); ++idxInSubgroup) {
        TVDiskPointer vdisk = TVDiskPointer::GetVDiskIdx(idxInSubgroup);
        ConductVPatchDiff(runtime, env, args, vpatchCase, vdisk);
    }
    CTEST << "ConductVPatchEvents: Finish\n";
}

void ConductVPatch(TTestBasicRuntime &runtime, const TDSProxyEnv &env, const TTestArgs &args,
        EVPatchCase vpatchCase)
{
    CTEST << "ConductFallbackPatch: Start VPatchCase: " << ToString(vpatchCase) << Endl;
    ConductVPatchEvents(runtime, env, args, vpatchCase);
    NKikimrProto::EReplyStatus resultStatus = GetPatchResultStatus(vpatchCase);
    if (resultStatus == NKikimrProto::UNKNOWN) {
        ConductFallbackPatch(runtime, args);
    } else {
        ReceivePatchResult(runtime, args, resultStatus);
    }
    CTEST << "ConductFallbackPatch: Finish\n";
}

////////////////////////////////////////////////////////////////////////////////

TEvBlobStorage::TEvPatch::TPtr CreatePatch(TTestBasicRuntime &runtime, const TDSProxyEnv &env, const TTestArgs &args) {
    TArrayHolder<TEvBlobStorage::TEvPatch::TDiff> diffs(new TEvBlobStorage::TEvPatch::TDiff[args.Diffs.size()]);
    for (ui32 diffIdx = 0; diffIdx < args.Diffs.size(); ++diffIdx) {
        const TDiff &diff = args.Diffs[diffIdx];
        UNIT_ASSERT(!diff.IsXor && !diff.IsAligned);
        diffs[diffIdx].Offset = diff.Offset;
        diffs[diffIdx].Buffer = diff.Buffer;
    }
    TInstant deadline = runtime.GetCurrentTime() + TDuration::Seconds(5);
    std::unique_ptr<TEvBlobStorage::TEvPatch> patch = std::make_unique<TEvBlobStorage::TEvPatch>(
            args.OriginalGroupId, args.OriginalId, args.PatchedId, args.MaskForCookieBruteForcing, std::move(diffs),
            args.Diffs.size(), deadline);
    TEvBlobStorage::TEvPatch::TPtr ev(static_cast<TEventHandle<TEvBlobStorage::TEvPatch>*>(
            new IEventHandle(env.FakeProxyActorId, env.FakeProxyActorId, patch.release())));
    return ev;
}

void RunNaivePatchTest(TTestBasicRuntime &runtime, const TTestArgs &args, ENaivePatchCase naiveCase) {
    TDSProxyEnv env;
    env.Configure(runtime, args.GType, args.CurrentGroupId, 0, TBlobStorageGroupInfo::EEM_NONE);
    TEvBlobStorage::TEvPatch::TPtr patch = CreatePatch(runtime, env, args);
    std::unique_ptr<IActor> patchActor = env.CreatePatchRequestActor(patch, false);
    runtime.Register(patchActor.release(), 0, 0, TMailboxType::Simple, 0, env.FakeProxyActorId);
    ConductNaivePatch(runtime, args, naiveCase);
}

void RunMovedPatchTest(TTestBasicRuntime &runtime, const TTestArgs &args, EMovedPatchCase movedCase) {
    TDSProxyEnv env;
    env.Configure(runtime, args.GType, args.CurrentGroupId, 0, TBlobStorageGroupInfo::EEM_NONE);
    TEvBlobStorage::TEvPatch::TPtr patch = CreatePatch(runtime, env, args);
    std::unique_ptr<IActor> patchActor = env.CreatePatchRequestActor(patch, true);
    runtime.Register(patchActor.release(), 0, 0, TMailboxType::Simple, 0, env.FakeProxyActorId);
    ConductMovedPatch(runtime, env, args, movedCase);
}

void RunVPatchTest(TTestBasicRuntime &runtime, const TTestArgs &args, EVPatchCase vpatchCase) {
    TDSProxyEnv env;
    env.Configure(runtime, args.GType, args.CurrentGroupId, 0, TBlobStorageGroupInfo::EEM_NONE);
    TEvBlobStorage::TEvPatch::TPtr patch = CreatePatch(runtime, env, args);
    std::unique_ptr<IActor> patchActor = env.CreatePatchRequestActor(patch, true);
    runtime.Register(patchActor.release(), 0, 0, TMailboxType::Simple, 0, env.FakeProxyActorId);
    ConductVPatch(runtime, env, args, vpatchCase);
}

void RunSecuredPatchTest(TTestBasicRuntime &runtime, const TTestArgs &args, ENaivePatchCase naiveCase) {
    TDSProxyEnv env;
    env.Configure(runtime, args.GType, args.CurrentGroupId, 0);
    TEvBlobStorage::TEvPatch::TPtr patch = CreatePatch(runtime, env, args);
    std::unique_ptr<IActor> patchActor = env.CreatePatchRequestActor(patch, true);
    runtime.Register(patchActor.release(), 0, 0, TMailboxType::Simple, 0, env.FakeProxyActorId);
    ConductNaivePatch(runtime, args, naiveCase);
}

void SetLogPriorities(TTestBasicRuntime &runtime) {
    bool IsVerbose = false;
    if (IsVerbose) {
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_PATCH, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NActorsServices::TEST, NLog::PRI_DEBUG);
    }
    runtime.SetLogPriority(NKikimrServices::BS_PROXY, NLog::PRI_CRIT);
    runtime.SetLogPriority(NKikimrServices::BS_QUEUE, NLog::PRI_CRIT);
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDSProxyPatchTest) {

template <typename ECase>
void RunGeneralTest(void(*runner)(TTestBasicRuntime &runtime, const TTestArgs &args, ECase testCase),
        TErasureType::TErasureType::EErasureSpecies erasure,
        ECase testCase)
{
    TTestArgs args;
    args.MakeDefault(erasure, true);
    TTestBasicRuntime runtime(1, false);
    SetLogPriorities(runtime);
    SetupRuntime(runtime);
    runner(runtime, args, testCase);
}

#define Y_UNIT_TEST_NAIVE(naiveCase, erasure) \
    Y_UNIT_TEST(Naive ## naiveCase ## _ ## erasure) { \
        RunGeneralTest(RunNaivePatchTest, TErasureType::TErasureType:: erasure, ENaivePatchCase:: naiveCase); \
    } \
// end Y_UNIT_TEST_NAIVE

#define Y_UNIT_TEST_MOVED(movedCase, erasure) \
    Y_UNIT_TEST(Moved ## movedCase ## _ ## erasure) { \
        RunGeneralTest(RunMovedPatchTest, TErasureType::TErasureType:: erasure, EMovedPatchCase:: movedCase); \
    } \
// end Y_UNIT_TEST_MOVED

#define Y_UNIT_TEST_VPATCH(vpatchCase, erasure) \
    Y_UNIT_TEST(VPatch ## vpatchCase ## _ ## erasure) { \
        RunGeneralTest(RunVPatchTest, TErasureType::TErasureType:: erasure, EVPatchCase:: vpatchCase); \
    } \
// end Y_UNIT_TEST_VPATCH

#define Y_UNIT_TEST_SECURED(naiveCase, erasure) \
    Y_UNIT_TEST(Secured ## naiveCase ## _ ## erasure) { \
        RunGeneralTest(RunSecuredPatchTest, TErasureType::TErasureType:: erasure, ENaivePatchCase:: naiveCase); \
    } \
// end Y_UNIT_TEST_VPATCH

#define Y_UNIT_TEST_PATCH_PACK(erasure) \
    Y_UNIT_TEST_NAIVE(Ok, erasure) \
    Y_UNIT_TEST_NAIVE(ErrorOnGetItem, erasure) \
    Y_UNIT_TEST_NAIVE(ErrorOnGet, erasure) \
    Y_UNIT_TEST_NAIVE(ErrorOnPut, erasure) \
    Y_UNIT_TEST_MOVED(Ok, erasure) \
    Y_UNIT_TEST_MOVED(Error, erasure) \
    Y_UNIT_TEST_SECURED(Ok, erasure) \
    Y_UNIT_TEST_SECURED(ErrorOnGetItem, erasure) \
    Y_UNIT_TEST_SECURED(ErrorOnGet, erasure) \
    Y_UNIT_TEST_SECURED(ErrorOnPut, erasure) \
// end Y_UNIT_TEST_PATCH_PACK

//    Y_UNIT_TEST_VPATCH(Ok, erasure)
//    Y_UNIT_TEST_VPATCH(OneErrorAndAllPartExistInStart, erasure)
//    Y_UNIT_TEST_VPATCH(OnePartLostInStart, erasure)
//    Y_UNIT_TEST_VPATCH(DeadGroupInStart, erasure)
//    Y_UNIT_TEST_VPATCH(ErrorDuringVPatchDiff, erasure) 

    Y_UNIT_TEST_PATCH_PACK(ErasureNone)
    Y_UNIT_TEST_PATCH_PACK(Erasure4Plus2Block)
    Y_UNIT_TEST_PATCH_PACK(ErasureMirror3dc)

}

////////////////////////////////////////////////////////////////////////////////

void MakeFaultToleranceArgsForBlock4Plus2(TTestArgs &args, i64 wiped1, i64 wiped2,
        ui32 errorMask, i64 handoff1, i64 handoff2, i64 extraHandoff)
{
    args.ResetDefaultPlacement();
    if (wiped1 != -1) {
        args.PartPlacement[wiped1].clear();
    }
    if (wiped2 != -1) {
        args.PartPlacement[wiped2].clear();
    }
    if (handoff1 > 0) {
        args.PartPlacement[6].push_back(handoff1);
    }
    if (handoff2 > 0) {
        args.PartPlacement[7].push_back(handoff2);
    }
    if (extraHandoff < 0) {
        args.PartPlacement[6].push_back(-extraHandoff);
    }
    if (extraHandoff > 0) {
        args.PartPlacement[7].push_back(extraHandoff);
    }
    for (ui32 bit = 1, idx = 0; bit < errorMask; idx++, bit <<= 1) {
        args.ErrorVDisks[idx] = (errorMask & bit);
    }
}

void MakeFaultToleranceArgsForMirror3dc(TTestArgs &args, bool is2x2, ui32 errorMask, i64 wipedDc, i64 startIdxInDc[]) {
    args.ResetDefaultPlacement();
    for (auto &pl : args.PartPlacement) {
        pl.clear();
    }

    for (ui32 dcIdx = 0; dcIdx < 3; ++dcIdx) {
        ui32 replCnt = is2x2 ? 2 : 1;
        ui32 startIdx = dcIdx * 3;
        for (ui32 replIdx = 0; replIdx < replCnt; ++replIdx) {
            ui32 orderIdx = (startIdxInDc[dcIdx] + replIdx) % 3;
            ui32 diskIdx = startIdx + orderIdx;
            args.PartPlacement[diskIdx].push_back(dcIdx + 1);
        }
    }

    if (wipedDc >= 0) {
        ui32 startIdx = wipedDc * 3;
        for (ui32 orderIdx = 0; orderIdx < 3; ++orderIdx) {
            ui32 diskIdx = startIdx + orderIdx;
            args.PartPlacement[diskIdx].clear();
        }
    }

    for (ui32 bit = 1, idx = 0; bit < errorMask; idx++, bit <<= 1) {
        args.ErrorVDisks[idx] = (errorMask & bit);
    }
}

enum class EFaultToleranceCase {
    Ok,
    Fallback
};

EFaultToleranceCase GetFaultToleranceCaseForBlock4Plus2(const TDSProxyEnv &env, const TTestArgs &args) {
    UNIT_ASSERT_VALUES_EQUAL(args.PartPlacement.size(), args.ErrorVDisks.size());
    UNIT_ASSERT_VALUES_EQUAL(args.PartPlacement.size(), env.Info->Type.BlobSubgroupSize());
    TSubgroupPartLayout layout;
    for (ui32 vdiskIdx = 0; vdiskIdx <  env.Info->Type.BlobSubgroupSize(); ++vdiskIdx) {
        if (!args.ErrorVDisks[vdiskIdx]) {
            auto [_, idxInSubgroup] = TVDiskPointer::GetVDiskIdx(vdiskIdx).GetIndecies(env, args.OriginalId.Hash());
            for (ui64 partId : args.PartPlacement[idxInSubgroup]) {
                layout.AddItem(idxInSubgroup, partId - 1, env.Info->Type);
            }
        }
    }
    return EFaultToleranceCase::Fallback; // disabled vpatch
    if (layout.CountEffectiveReplicas(env.Info->Type) == env.Info->Type.TotalPartCount()) {
        return EFaultToleranceCase::Ok;
    } else {
        return EFaultToleranceCase::Fallback;
    }
}


EFaultToleranceCase GetFaultToleranceCaseForMirror3dc(const TDSProxyEnv &env, const TTestArgs &args) {
    UNIT_ASSERT_VALUES_EQUAL(args.PartPlacement.size(), args.ErrorVDisks.size());
    UNIT_ASSERT_VALUES_EQUAL(args.PartPlacement.size(), env.Info->Type.BlobSubgroupSize());
    TSubgroupPartLayout layout;
    constexpr ui32 dcCnt = 3;
    ui32 replInDc[dcCnt] = {0, 0, 0};
    for (ui32 vdiskIdx = 0; vdiskIdx < env.Info->Type.BlobSubgroupSize(); ++vdiskIdx) {
        if (!args.ErrorVDisks[vdiskIdx] && args.PartPlacement[vdiskIdx]) {
            ui32 dcIdx = vdiskIdx / 3;
            replInDc[dcIdx]++;
        }
    }
    ui32 x2cnt = 0;
    for (ui32 dcIdx = 0; dcIdx < dcCnt; ++dcIdx) {
        x2cnt += (replInDc[dcIdx] >= 2);
    }
    return EFaultToleranceCase::Fallback; // disabled vpatch
    if ((replInDc[0] && replInDc[1] && replInDc[2]) || x2cnt >= 2) {
        return EFaultToleranceCase::Ok;
    } else {
        return EFaultToleranceCase::Fallback;
    }
}

void ConductFaultTolerance(TTestBasicRuntime &runtime, const TDSProxyEnv &env, const TTestArgs &args,
        EFaultToleranceCase faultCase)
{
    CTEST << "ConductFallbackPatch: Start\n";
    ConductVPatchEvents(runtime, env, args, EVPatchCase::Custom);
    if (faultCase == EFaultToleranceCase::Fallback) {
        ConductFallbackPatch(runtime, args);
    } else {
        ReceivePatchResult(runtime, args, NKikimrProto::OK);
    }
    CTEST << "ConductFallbackPatch: Finish\n";
}

void RunFaultToleranceBlock4Plus2(TTestBasicRuntime &runtime, const TTestArgs &args) {
    TDSProxyEnv env;
    env.Configure(runtime, args.GType, args.CurrentGroupId, 0, TBlobStorageGroupInfo::EEM_NONE);
    TEvBlobStorage::TEvPatch::TPtr patch = CreatePatch(runtime, env, args);
    std::unique_ptr<IActor> patchActor = env.CreatePatchRequestActor(patch, true);
    runtime.Register(patchActor.release(), 0, 0, TMailboxType::Simple, 0, env.FakeProxyActorId);
    ConductFaultTolerance(runtime, env, args, GetFaultToleranceCaseForBlock4Plus2(env, args));
}

void RunFaultToleranceMirror3dc(TTestBasicRuntime &runtime, const TTestArgs &args) {
    TDSProxyEnv env;
    env.Configure(runtime, args.GType, args.CurrentGroupId, 0, TBlobStorageGroupInfo::EEM_NONE);
    TEvBlobStorage::TEvPatch::TPtr patch = CreatePatch(runtime, env, args);
    std::unique_ptr<IActor> patchActor = env.CreatePatchRequestActor(patch, true);
    runtime.Register(patchActor.release(), 0, 0, TMailboxType::Simple, 0, env.FakeProxyActorId);
    ConductFaultTolerance(runtime, env, args, GetFaultToleranceCaseForMirror3dc(env, args));
}


Y_UNIT_TEST_SUITE(TDSProxyFaultTolerancePatchTest) {
    Y_UNIT_TEST(mirror3dc) {
        TBlobStorageGroupType type = TBlobStorageGroupType::ErasureMirror3dc;
        ui64 errorMaskEnd = (1 << type.BlobSubgroupSize());

        TTestArgs args;
        args.MakeDefault(type);
        for (bool is2x2 : {false, true}) {
            for (i64 wipedDc = -1; wipedDc < 3; ++wipedDc) {
                //for (i64 i = 0; i < 27; ++i) { // don't use because this test alreade is very fat
                i64 startIdxInDC[3] = {0, 0, 0}; // {i % 3, i / 3 % 3, i / 9};
                TTestBasicRuntime runtime;
                SetupRuntime(runtime);
                for (ui32 errorMask = 0; errorMask < errorMaskEnd; ++errorMask) {
                    MakeFaultToleranceArgsForMirror3dc(args, is2x2, errorMask, wipedDc, startIdxInDC);
                    RunFaultToleranceMirror3dc(runtime, args);
                }
                //}
            }
        }
    }


    Y_UNIT_TEST(block42) {
        TBlobStorageGroupType type = TBlobStorageGroupType::Erasure4Plus2Block;
        ui64 errorMaskEnd = (1 << type.BlobSubgroupSize());

        TTestArgs args;
        args.MakeDefault(type);
        for (i64 wiped1 = -1; wiped1 < type.TotalPartCount(); ++wiped1) {
            for (i64 wiped2 = -1; wiped2 < type.TotalPartCount(); ++wiped2) {
                if (wiped1 != -1 && wiped1 >= wiped2) {
                    continue;
                }
                TTestBasicRuntime runtime;
                SetupRuntime(runtime);
                for (ui32 errorMask = 0; errorMask < errorMaskEnd; ++errorMask) {
                    for (i64 extra = -type.TotalPartCount(); extra <= type.TotalPartCount(); extra++) {
                        if (extra == wiped1 + 1 || extra == wiped2 + 1) {
                            continue;
                        }
                        if (-extra == wiped1 + 1 || -extra == wiped2 + 1) {
                            continue;
                        }
                        MakeFaultToleranceArgsForBlock4Plus2(args, wiped1, wiped2, errorMask, 0, 0, extra);
                        RunFaultToleranceBlock4Plus2(runtime, args);
                        MakeFaultToleranceArgsForBlock4Plus2(args, wiped1, wiped2, errorMask, wiped1 + 1, 0, extra);
                        RunFaultToleranceBlock4Plus2(runtime, args);
                        MakeFaultToleranceArgsForBlock4Plus2(args, wiped1, wiped2, errorMask, wiped2 + 1, 0, extra);
                        RunFaultToleranceBlock4Plus2(runtime, args);
                        MakeFaultToleranceArgsForBlock4Plus2(args, wiped1, wiped2, errorMask, 0, wiped1 + 1, extra);
                        RunFaultToleranceBlock4Plus2(runtime, args);
                        MakeFaultToleranceArgsForBlock4Plus2(args, wiped1, wiped2, errorMask, 0, wiped2 + 1, extra);
                        RunFaultToleranceBlock4Plus2(runtime, args);
                        MakeFaultToleranceArgsForBlock4Plus2(args, wiped1, wiped2, errorMask, wiped1 + 1, wiped2 + 1, extra);
                        RunFaultToleranceBlock4Plus2(runtime, args);
                        MakeFaultToleranceArgsForBlock4Plus2(args, wiped1, wiped2, errorMask, wiped2 + 1, wiped1 + 1, extra);
                        RunFaultToleranceBlock4Plus2(runtime, args);
                    }
                }
            }
        }
    }
}

} // NDSProxyPatchTest
} // NKikimr
