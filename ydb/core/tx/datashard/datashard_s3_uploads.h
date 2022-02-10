#pragma once

#include "datashard_s3_upload.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NDataShard {

class TS3UploadsManager {
public:
    bool Load(NIceDb::TNiceDb& db);
    void Reset();

    const TS3Upload* Find(ui64 txId) const;

    const TS3Upload& Add(NIceDb::TNiceDb& db, ui64 txId, const TString& uploadId);
    const TS3Upload& Add(NTable::TDatabase& db, ui64 txId, const TString& uploadId) {
        NIceDb::TNiceDb nicedb(db);
        return Add(nicedb, txId, uploadId);
    }

    const TS3Upload& ChangeStatus(NIceDb::TNiceDb& db, ui64 txId, TS3Upload::EStatus status,
        TMaybe<TString>&& error, TVector<TString>&& parts);

    const TS3Upload& ChangeStatus(NTable::TDatabase& db, ui64 txId, TS3Upload::EStatus status,
            TMaybe<TString>&& error, TVector<TString>&& parts)
    {
        NIceDb::TNiceDb nicedb(db);
        return ChangeStatus(nicedb, txId, status, std::move(error), std::move(parts));
    }

private:
    THashMap<ui64, TS3Upload> Uploads;

}; // TS3UploadsManager

} // namespace NDataShard
} // namespace NKikimr
