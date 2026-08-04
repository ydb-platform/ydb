#include "keyvalue_state.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/util/stlog.h>

namespace NKikimr {
namespace NKeyValue {

void TKeyValueState::ClearMoveDataBlobMovingStage() {
    MoveDataBlobMovingIsInProgress = false;
    MoveDataBlobMovingNeedsAnotherPass = false;
    MoveDataKey.clear();
    MoveDataBlobId = TLogoBlobID();
    MoveDataChainIndex = 0;
    MoveDataRecordTouched = false;
    MoveDataBlobIdToNewBlobId.clear();
}

void TKeyValueState::ClearMoveDataTrashCheckingStage() {
    MoveDataTrashCheckingVacuumGeneration = {};
    MoveDataTrashCheckingBlobId = TLogoBlobID();
    MoveDataTrashCheckingWaitingForGC = false;
}

void TKeyValueState::StartMoveData(TSet<ui32>&& moveDataGroups, const TActorId& moveDataRequestSender) {
    MoveDataGroups = std::move(moveDataGroups);
    MoveDataRequestSender = moveDataRequestSender;
    MoveDataIsInProgress = true;

    ClearMoveDataBlobMovingStage();
    MoveDataBlobMovingIsInProgress = true;
}

bool TKeyValueState::NeedMoveBlob(const TLogoBlobID& blobId) const {
    auto groupId = TabletInfo->GroupFor(blobId);
    Y_ABORT_UNLESS(groupId != Max<ui32>());
    return MoveDataGroups.contains(groupId);
}

std::unique_ptr<TEvKeyValue::TEvAdvanceMoveDataResult> TKeyValueState::AdvanceMoveData(ISimpleDb& db) {
    YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "AdvanceMoveData",
        {"keyValue", TabletId},
        {"marker", "KV93"});

    Y_ABORT_UNLESS(MoveDataBlobMovingIsInProgress);

    if (Index.empty()) {
        return TryCheckTrash();
    }

    if (MoveDataRecordTouched) {
        YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "AdvanceMoveData: MoveDataRecordTouched",
            {"keyValue", TabletId},
            {"marker", "KV102"});

        MoveDataRecordTouched = false;
        MoveDataChainIndex = 0;
    }

    TIndex::iterator itIndex;
    if (MoveDataKey.empty()) {
        itIndex = Index.begin();
        MoveDataChainIndex = 0;
    } else {
        itIndex = Index.find(MoveDataKey);
        if (itIndex == Index.end()) {
            itIndex = Index.upper_bound(MoveDataKey);
            MoveDataChainIndex = 0;
            if (itIndex == Index.end()) {
                return TryCheckTrash();
            }
        }
    }

    ui64 recordCount = 0;
    for (; itIndex != Index.end(); ++itIndex, ++recordCount) {
        MoveDataKey = itIndex->first;
        auto& record = itIndex->second;

        if (recordCount >= MaxMoveDataRecordsInOneTx) {
            return TEvKeyValue::TEvAdvanceMoveDataResult::Yield();
        }

        for (; MoveDataChainIndex < record.Chain.size(); ++MoveDataChainIndex) {
            auto& item = record.Chain[MoveDataChainIndex];
            if (item.IsInline()) {
                continue;
            }
            auto blobId = item.LogoBlobId;
            if (!NeedMoveBlob(blobId)) {
                continue;
            }

            YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "AdvanceMoveData: NeedMoveBlob",
                {"keyValue", TabletId},
                {"marker", "KV96"},
                {"blobId", blobId.ToString()},
                {"key", MoveDataKey});

            if (!MoveDataBlobIdToNewBlobId.contains(blobId)) {
                MoveDataBlobId = blobId;
                return TEvKeyValue::TEvAdvanceMoveDataResult::CopyBlob(blobId, NextRequestUid++);
            }

            auto newBlobId = MoveDataBlobIdToNewBlobId[blobId];
            if (RefCounts.find(newBlobId) == RefCounts.end()) {
                YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "AdvanceMoveData: new blob was deleted, copy again",
                    {"keyValue", TabletId},
                    {"marker", "KV101"});

                MoveDataBlobIdToNewBlobId.erase(blobId);
                MoveDataBlobId = blobId;
                return TEvKeyValue::TEvAdvanceMoveDataResult::CopyBlob(blobId, NextRequestUid++);
            }

            auto itRefCounts = RefCounts.find(blobId);
            Y_ABORT_UNLESS(itRefCounts != RefCounts.end());

            if (itRefCounts->second > 1) {
                --itRefCounts->second;
            } else {
                Dereference(blobId, db, false);
            }

            item.LogoBlobId = newBlobId;
            ++RefCounts[newBlobId];

            MoveDataBlobMovingIsInProgress = false;
            UpdateKeyValue(MoveDataKey, record, db);
            MoveDataBlobMovingIsInProgress = true;
        }

        MoveDataChainIndex = 0;
    }

    return TryCheckTrash();
}

std::unique_ptr<TEvKeyValue::TEvAdvanceMoveDataResult> TKeyValueState::BlobCopied(
        TEvKeyValue::TEvBlobCopied::EResult result,
        const TLogoBlobID& blobId,
        const TLogoBlobID& newBlobId,
        ISimpleDb& db)
{
    YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "OnBlobCopied",
        {"keyValue", TabletId},
        {"marker", "KV94"},
        {"blobId", blobId.ToString()},
        {"newBlobId", newBlobId.ToString()},
        {"moveDataKey", MoveDataKey},
        {"moveDataChainIndex", MoveDataChainIndex});

    Y_ABORT_UNLESS(MoveDataBlobMovingIsInProgress);
    Y_ABORT_UNLESS(!MoveDataKey.empty());
    Y_ABORT_UNLESS(blobId == MoveDataBlobId);

    MoveDataBlobId = TLogoBlobID();

    if (result == TEvKeyValue::TEvBlobCopied::EResult::NODATA) {
        YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "OnBlobCopied: NODATA",
            {"keyValue", TabletId},
            {"marker", "KV100"});
        Y_ABORT_UNLESS(MoveDataRecordTouched);
        Y_ABORT_UNLESS(RefCounts.find(blobId) == RefCounts.end());
    }

    if (MoveDataRecordTouched) {
        YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "OnBlobCopied: MoveDataRecordTouched",
            {"keyValue", TabletId},
            {"marker", "KV95"});
        MoveDataRecordTouched = false;
        MoveDataChainIndex = 0;
        return AdvanceMoveData(db);
    }

    auto itIndex = Index.find(MoveDataKey);
    Y_ABORT_UNLESS(itIndex != Index.end());
    auto& record = itIndex->second;
    Y_ABORT_UNLESS(MoveDataChainIndex < record.Chain.size());
    auto& item = record.Chain[MoveDataChainIndex];
    Y_ABORT_UNLESS(item.LogoBlobId == blobId);
    auto itRefCounts = RefCounts.find(blobId);
    Y_ABORT_UNLESS(itRefCounts != RefCounts.end());

    if (itRefCounts->second > 1) {
        --itRefCounts->second;
        MoveDataBlobIdToNewBlobId[blobId] = newBlobId;
    } else {
        Dereference(blobId, db, false);
    }

    item.LogoBlobId = newBlobId;
    RefCounts[newBlobId] = 1;

    MoveDataBlobMovingIsInProgress = false;
    UpdateKeyValue(MoveDataKey, record, db);
    MoveDataBlobMovingIsInProgress = true;

    ++MoveDataChainIndex;
    if (MoveDataChainIndex >= record.Chain.size()) {
        MoveDataChainIndex = 0;
        ++itIndex;
        if (itIndex != Index.end()) {
            MoveDataKey = itIndex->first;
            return AdvanceMoveData(db);
        } else {
            return TryCheckTrash();
        }
    }

    return AdvanceMoveData(db);
}

std::unique_ptr<TEvKeyValue::TEvAdvanceMoveDataResult> TKeyValueState::TryCheckTrash() {
    YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "TryCheckTrash",
        {"keyValue", TabletId},
        {"marker", "KV95"});

    if (MoveDataBlobMovingNeedsAnotherPass) {
        ClearMoveDataBlobMovingStage();
        MoveDataBlobMovingIsInProgress = true;
        return TEvKeyValue::TEvAdvanceMoveDataResult::Repeat();
    } else {
        MoveDataBlobMovingIsInProgress = false;
        ClearMoveDataTrashCheckingStage();
        return TEvKeyValue::TEvAdvanceMoveDataResult::CheckTrash();
    }
}

std::unique_ptr<TEvKeyValue::TEvAdvanceMoveDataResult> TKeyValueState::CheckTrash() {
    YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "CheckTrash",
        {"keyValue", TabletId},
        {"marker", "KV96"});

    TSet<TLogoBlobID>& trashBin = Trash;

    TMap<ui64, TSet<TLogoBlobID>>::iterator itTrashBin;
    bool finished = false;

    auto nextTrashBin = [&]() {
        if (!MoveDataTrashCheckingVacuumGeneration) {
            itTrashBin = TrashForVacuum.begin();
        } else {
            itTrashBin = TrashForVacuum.upper_bound(*MoveDataTrashCheckingVacuumGeneration);
        }
        if (itTrashBin == TrashForVacuum.end()) {
            finished = true;
            return;
        }
        trashBin = itTrashBin->second;
        MoveDataTrashCheckingVacuumGeneration = itTrashBin->first;
        MoveDataTrashCheckingBlobId = TLogoBlobID();
    };

    if (MoveDataTrashCheckingVacuumGeneration) {
        itTrashBin = TrashForVacuum.find(*MoveDataTrashCheckingVacuumGeneration);
        if (itTrashBin == TrashForVacuum.end()) {
            nextTrashBin();
            if (finished) {
                return TEvKeyValue::TEvAdvanceMoveDataResult::Finish();
            }
        } else {
            trashBin = itTrashBin->second;
        }
    }

    ui64 checkedBlobsCount = 0;
    while (!finished) {
        auto itTrash = trashBin.lower_bound(MoveDataTrashCheckingBlobId);
        if (itTrash == trashBin.end()) {
            nextTrashBin();
            if (finished) {
                break;
            }
            itTrash = trashBin.begin();
        }
        for (; itTrash != trashBin.end(); ++itTrash, ++checkedBlobsCount) {
            MoveDataTrashCheckingBlobId = *itTrash;
            if (checkedBlobsCount >= MaxMoveDataTrashCheckingBlobs) {
                return TEvKeyValue::TEvAdvanceMoveDataResult::CheckTrash();
            }
            if (NeedMoveBlob(MoveDataTrashCheckingBlobId)) {
                MoveDataTrashCheckingWaitingForGC = true;
                return TEvKeyValue::TEvAdvanceMoveDataResult::WaitForGC();
            }
        }
        nextTrashBin();
    }

    return TEvKeyValue::TEvAdvanceMoveDataResult::Finish();
}

void TKeyValueState::FinishMoveData(const TActorContext& ctx) {
    ctx.Send(MoveDataRequestSender, new TEvTablet::TEvMoveDataResponse(TabletId));

    MoveDataIsInProgress = false;
    MoveDataGroups.clear();
    MoveDataRequestSender = {};
}

} // NKeyValue
} // NKikimr
