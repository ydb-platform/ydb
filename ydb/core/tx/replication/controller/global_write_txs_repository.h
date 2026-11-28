#pragma once

#include "global_write_txs.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <optional>

namespace NKikimr::NReplication::NController {

class TGlobalWriteTxsRepository {
public:
    static std::optional<TMap<TRowVersion, ui64>> Load(NIceDb::TNiceDb& db);
    static void SaveChanges(NIceDb::TNiceDb& db, TGlobalWriteTxs& writeTxs);
};

}
