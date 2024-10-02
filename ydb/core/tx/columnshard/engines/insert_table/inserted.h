#pragma once
#include "user_data.h"

#include <ydb/core/tx/columnshard/engines/defs.h>

namespace NKikimr::NOlap {

class TCommittedData;

class TInsertedData: public TUserDataContainer {
private:
    using TBase = TUserDataContainer;
    YDB_READONLY(TInsertWriteId, InsertWriteId, TInsertWriteId(0));
    YDB_READONLY_FLAG(NotAbortable, false);

public:
    void MarkAsNotAbortable() {
        NotAbortableFlag = true;
    }

    TInsertedData() = delete;   // avoid invalid TInsertedData anywhere

    TInsertedData(const TInsertWriteId writeId, const std::shared_ptr<TUserData>& userData)
        : TBase(userData)
        , InsertWriteId(writeId) {
    }

    /// We commit many writeIds in one txId. There could be several blobs with same WriteId and different DedupId.
    /// One of them wins and becomes committed. Original DedupId would be lost then.
    /// After commit we use original Initiator:WriteId as DedupId of inserted blob inside {PlanStep, TxId}.
    /// pathId, initiator, {writeId}, {dedupId} -> pathId, planStep, txId, {dedupId}
    [[nodiscard]] TCommittedData Commit(const ui64 planStep, const ui64 txId) const;
};

}   // namespace NKikimr::NOlap
