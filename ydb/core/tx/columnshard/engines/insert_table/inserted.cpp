#include "committed.h"
#include "inserted.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

TCommittedData TInsertedData::Commit(const ui64 planStep, const ui64 txId) const {
    return TCommittedData(UserData, planStep, txId, InsertWriteId);
}

}   // namespace NKikimr::NOlap
