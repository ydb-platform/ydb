#pragma once

#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/library/actors/core/actorid.h>

#include <util/generic/hash.h>

namespace NKikimr::NDataShard {
    class TBuildIndexScanManager {
    public:
        struct TScanInfo {
            ui64 BuildId = 0;
            ui64 SeqNoGeneration = 0;
            ui64 SeqNoRound = 0;
            NActors::TActorId Sender;
            NKikimrTxDataShard::TEvBuildIndexCreateRequest Request;
        };

    public:
        bool Load(NIceDb::TNiceDb& db);

        void PersistAdd(NIceDb::TNiceDb& db, ui64 buildId, ui64 seqNoGeneration, ui64 seqNoRound,
                        const NActors::TActorId& sender, const NKikimrTxDataShard::TEvBuildIndexCreateRequest& request);

        void PersistRemove(NIceDb::TNiceDb& db, ui64 buildId, ui64 seqNoGeneration, ui64 seqNoRound);

        const THashMap<ui64, TScanInfo>& GetScans() const {
            return Scans;
        }

        void Reset();

    private:
        THashMap<ui64, TScanInfo> Scans;
    };

} // namespace NKikimr::NDataShard
