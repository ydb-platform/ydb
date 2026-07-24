#include "keyvalue_state.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/util/stlog.h>

namespace NKikimr {
namespace NKeyValue {

void TKeyValueState::ClearMoveData() {
    MoveDataIsInProgress = false;
    MoveDataNeedsAnotherPass = false;
    MoveDataKey.clear();
    MoveDataBlobId = TLogoBlobID();
    MoveDataChainIndex = 0;
    MoveDataRecordTouched = false;
    MoveDataBlobIdToNewBlobId.clear();
}

void TKeyValueState::StartMoveData(TSet<ui32>&& moveDataGroups, const TActorId& moveDataRequestSender) {
    MoveDataGroups = std::move(moveDataGroups);
    MoveDataRequestSender = moveDataRequestSender;
    MoveDataIsInProgress = true;
}

bool TKeyValueState::NeedMoveBlob(const TLogoBlobID& blobId) const {
    auto groupId = TabletInfo->GroupFor(blobId);
    Y_ABORT_UNLESS(groupId != Max<ui32>());
    return MoveDataGroups.contains(groupId);
}

std::unique_ptr<TEvKeyValue::TEvAdvanceMoveDataResult> TKeyValueState::AdvanceMoveData(
        ISimpleDb& db, const TActorContext& ctx) {
    YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "AdvanceMoveData",
        {"keyValue", TabletId},
        {"marker", "KV93"});

    if (Index.empty()) {
        return TryFinishMoveData(ctx);
    }

    if (MoveDataRecordTouched) {
        MoveDataRecordTouched = false;
        MoveDataChainIndex = 0;
    }

    TIndex::iterator itIndex;
    if (MoveDataKey.empty()) {
        itIndex = Index.begin();
        MoveDataChainIndex = 0;
    } else {
        auto itIndex = Index.find(MoveDataKey);
        if (itIndex == Index.end()) {
            itIndex = Index.upper_bound(MoveDataKey);
            MoveDataChainIndex = 0;
            if (itIndex == Index.end()) {
                return TryFinishMoveData(ctx);
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
            auto& blobId = item.LogoBlobId;
            if (!NeedMoveBlob(blobId)) {
                continue;
            }

            if (!MoveDataBlobIdToNewBlobId.contains(blobId)) {
                return TEvKeyValue::TEvAdvanceMoveDataResult::CopyBlob(blobId);
            }

            auto itRefCounts = RefCounts.find(blobId);
            Y_ABORT_UNLESS(itRefCounts != RefCounts.end());
            if (itRefCounts->second > 1) {
                --itRefCounts->second;
            } else {
                RefCounts.erase(itRefCounts);
            }

            auto newBlobId = MoveDataBlobIdToNewBlobId[blobId];
            item.LogoBlobId = newBlobId;
            ++RefCounts[newBlobId];
            UpdateKeyValue(MoveDataKey, record, db);
        }

        MoveDataChainIndex = 0;
    }

    return TryFinishMoveData(ctx);
}

std::unique_ptr<TEvKeyValue::TEvAdvanceMoveDataResult> TKeyValueState::BlobCopied(
        const TLogoBlobID& blobId, const TLogoBlobID& newBlobId, ISimpleDb& db, const TActorContext& ctx) {
    YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "OnBlobCopied",
        {"keyValue", TabletId},
        {"marker", "KV94"},
        {"blobId", blobId.ToString()},
        {"newBlobId", newBlobId.ToString()});

    Y_ABORT_UNLESS(!MoveDataKey.empty());
    Y_ABORT_UNLESS(blobId == MoveDataBlobId);

    if (MoveDataRecordTouched) {
        MoveDataRecordTouched = false;
        MoveDataChainIndex = 0;
        return AdvanceMoveData(db, ctx);
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
        RefCounts.erase(itRefCounts);
    }

    item.LogoBlobId = newBlobId;
    RefCounts[newBlobId] = 1;
    UpdateKeyValue(MoveDataKey, record, db);

    MoveDataBlobId = TLogoBlobID();

    ++MoveDataChainIndex;
    if (MoveDataChainIndex >= record.Chain.size()) {
        MoveDataChainIndex = 0;
        ++itIndex;
        if (itIndex != Index.end()) {
            MoveDataKey = itIndex->first;
            return AdvanceMoveData(db, ctx);
        } else {
            return TryFinishMoveData(ctx);
        }
    }

    return AdvanceMoveData(db, ctx);
}

std::unique_ptr<TEvKeyValue::TEvAdvanceMoveDataResult> TKeyValueState::TryFinishMoveData(
        const TActorContext& ctx) {
    YDB_LOG_DEBUG_COMP(NKikimrServices::KEYVALUE, "TryFinishMoveData",
        {"keyValue", TabletId},
        {"marker", "KV95"});

    if (MoveDataNeedsAnotherPass) {
        ClearMoveData();

        MoveDataIsInProgress = true;
        return TEvKeyValue::TEvAdvanceMoveDataResult::Repeat();
    } else {
        ClearMoveData();

        ctx.Send(MoveDataRequestSender, new TEvTablet::TEvMoveDataResponse(TabletId));

        MoveDataGroups.clear();
        MoveDataRequestSender = {};
        return TEvKeyValue::TEvAdvanceMoveDataResult::Finish();
    }
}

} // NKeyValue
} // NKikimr
