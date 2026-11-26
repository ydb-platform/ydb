#include "write_tx_intervals_repository.h"
#include "schema.h"

namespace NKikimr::NReplication::NController {

using Schema = TControllerSchema;

bool TWriteTxIntervalsRepository::Load(NIceDb::TNiceDb& db, TWriteTxIntervals& intervals) {
    auto rowset = db.Table<Schema::TxIds>().Select();
    if (!rowset.IsReady()) {
        return false;
    }
    while (!rowset.EndOfSet()) {
        if (rowset.HaveValue<Schema::TxIds::FirstIntervalEndStep>()) {
            intervals.Start(rowset.GetValue<Schema::TxIds::WriteTxId>(),
                TRowVersion(rowset.GetValue<Schema::TxIds::FirstIntervalEndStep>(), 0));
        }
        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

void TWriteTxIntervalsRepository::SaveFirstIntervalEnd(NIceDb::TNiceDb& db, const TRowVersion& key, ui64 writeTxId,
        const TWriteTxIntervals& intervals)
{
    if (const auto first = intervals.FindFirstIntervalEnd(writeTxId)) {
        db.Table<Schema::TxIds>().Key(key.Step, key.TxId).Update(
            NIceDb::TUpdate<Schema::TxIds::FirstIntervalEndStep>(first->Step));
    }
}

}
