#include "build_index_scan_manager.h"

#include <ydb/core/tx/datashard/datashard_impl.h>

namespace NKikimr::NDataShard {
    bool TBuildIndexScanManager::Load(NIceDb::TNiceDb& db) {
        Scans.clear();

        auto rowset = db.Table<TDataShard::Schema::IndexBuildScans>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            TScanInfo info;
            info.BuildId = rowset.GetValue<TDataShard::Schema::IndexBuildScans::BuildId>();
            info.SeqNoGeneration = rowset.GetValue<TDataShard::Schema::IndexBuildScans::SeqNoGeneration>();
            info.SeqNoRound = rowset.GetValue<TDataShard::Schema::IndexBuildScans::SeqNoRound>();
            info.ResponseType = rowset.GetValue<TDataShard::Schema::IndexBuildScans::ResponseType>();
            info.FinalProgressRecordSerialized = rowset.GetValueOrDefault<TDataShard::Schema::IndexBuildScans::FinalProgressRecord>(TString());

            Scans[info.BuildId] = std::move(info);

            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

    void TBuildIndexScanManager::PersistAdd(NIceDb::TNiceDb& db, ui64 buildId,
                                            ui64 seqNoGeneration, ui64 seqNoRound,
                                            TStringBuf responseType)
    {
        db.Table<TDataShard::Schema::IndexBuildScans>()
            .Key(buildId, seqNoGeneration, seqNoRound)
            .Update(
                NIceDb::TUpdate<TDataShard::Schema::IndexBuildScans::ResponseType>(TString(responseType)),
                NIceDb::TUpdate<TDataShard::Schema::IndexBuildScans::FinalProgressRecord>(TString()));

        TScanInfo& info = Scans[buildId];
        info.BuildId = buildId;
        info.SeqNoGeneration = seqNoGeneration;
        info.SeqNoRound = seqNoRound;
        info.ResponseType = responseType;
        info.FinalProgressRecordSerialized.clear();
    }

    void TBuildIndexScanManager::PersistMarkFinalResponse(NIceDb::TNiceDb& db, ui64 buildId,
                                                        ui64 seqNoGeneration, ui64 seqNoRound,
                                                        const TString& serializedRecord)
    {
        db.Table<TDataShard::Schema::IndexBuildScans>()
            .Key(buildId, seqNoGeneration, seqNoRound)
            .Update(
                NIceDb::TUpdate<TDataShard::Schema::IndexBuildScans::ResponseType>(TString(IndexBuildScanResponseTypeFinal)),
                NIceDb::TUpdate<TDataShard::Schema::IndexBuildScans::FinalProgressRecord>(serializedRecord));

        TScanInfo& info = Scans[buildId];
        info.BuildId = buildId;
        info.SeqNoGeneration = seqNoGeneration;
        info.SeqNoRound = seqNoRound;
        info.ResponseType = TString(IndexBuildScanResponseTypeFinal);
        info.FinalProgressRecordSerialized = serializedRecord;
    }

    void TBuildIndexScanManager::PersistRemove(NIceDb::TNiceDb& db, ui64 buildId, ui64 seqNoGeneration, ui64 seqNoRound) {
        db.Table<TDataShard::Schema::IndexBuildScans>()
            .Key(buildId, seqNoGeneration, seqNoRound)
            .Delete();

        Scans.erase(buildId);
    }

    void TBuildIndexScanManager::Reset() {
        Scans.clear();
    }

} // namespace NKikimr::NDataShard
