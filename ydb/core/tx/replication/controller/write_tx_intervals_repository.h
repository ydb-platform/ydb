#pragma once

#include "write_tx_intervals.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NReplication::NController {

class TWriteTxIntervalsRepository {
public:
    static bool Load(NIceDb::TNiceDb& db, TWriteTxIntervals& intervals);
    static void SaveFirstIntervalEnd(NIceDb::TNiceDb& db, const TRowVersion& key, ui64 writeTxId,
        const TWriteTxIntervals& intervals);
};

}
