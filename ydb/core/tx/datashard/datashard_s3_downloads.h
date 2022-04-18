#pragma once

#include "datashard_s3_download.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/hash.h>

namespace NKikimr {
namespace NDataShard {

class TS3DownloadsManager {
public:
    bool Load(NIceDb::TNiceDb& db);
    void Reset();

    const TS3Download* Find(ui64 txId) const;
    const TS3Download& Store(NIceDb::TNiceDb& db, ui64 txId, const TS3Download& newInfo);

private:
    THashMap<ui64, TS3Download> Downloads;

}; // TS3DownloadsManager

} // namespace NDataShard
} // namespace NKikimr
