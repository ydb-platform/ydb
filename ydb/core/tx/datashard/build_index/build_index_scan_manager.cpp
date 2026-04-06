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

            const TString senderStr = rowset.GetValue<TDataShard::Schema::IndexBuildScans::SenderActorId>();
            if (senderStr.size() == sizeof(NActors::TActorId)) {
                memcpy(&info.Sender, senderStr.data(), sizeof(NActors::TActorId));
            }

            const TString protoStr = rowset.GetValue<TDataShard::Schema::IndexBuildScans::RequestProto>();
            if (!info.Request.ParseFromString(protoStr)) {
                return false;
            }

            Scans[info.BuildId] = std::move(info);

            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

    void TBuildIndexScanManager::PersistAdd(NIceDb::TNiceDb& db, ui64 buildId, ui64 seqNoGeneration, ui64 seqNoRound,
                                            const NActors::TActorId& sender, const NKikimrTxDataShard::TEvBuildIndexCreateRequest& request)
    {
        TString senderStr(sizeof(NActors::TActorId), '\0');
        memcpy(&senderStr[0], &sender, sizeof(NActors::TActorId));

        TString protoStr;
        Y_ENSURE(request.SerializeToString(&protoStr));

        db.Table<TDataShard::Schema::IndexBuildScans>()
            .Key(buildId, seqNoGeneration, seqNoRound)
            .Update(
                NIceDb::TUpdate<TDataShard::Schema::IndexBuildScans::SenderActorId>(senderStr),
                NIceDb::TUpdate<TDataShard::Schema::IndexBuildScans::RequestProto>(protoStr));

        TScanInfo& info = Scans[buildId];
        info.BuildId = buildId;
        info.SeqNoGeneration = seqNoGeneration;
        info.SeqNoRound = seqNoRound;
        info.Sender = sender;
        info.Request = request;
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
