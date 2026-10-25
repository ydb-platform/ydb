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

            info.GeneratedFillContextSerialized = rowset.GetValueOrDefault<TDataShard::Schema::IndexBuildScans::GeneratedFillContext>(TString());

            Scans[info.BuildId] = std::move(info);

            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

    void TBuildIndexScanManager::PersistAdd(NIceDb::TNiceDb& db, ui64 buildId,
                                            ui64 seqNoGeneration, ui64 seqNoRound,
                                            ui32 responseType,
                                            const NKikimrTxDataShard::TGeneratedColumnFillContext* generatedContext)
    {
        const TString serialized = generatedContext ? generatedContext->SerializeAsString() : TString();
        db.Table<TDataShard::Schema::IndexBuildScans>()
            .Key(buildId, seqNoGeneration, seqNoRound)
            .Update(
                NIceDb::TUpdate<TDataShard::Schema::IndexBuildScans::ResponseType>(responseType),
                NIceDb::TUpdate<TDataShard::Schema::IndexBuildScans::FinalProgressRecord>(TString()),
                NIceDb::TUpdate<TDataShard::Schema::IndexBuildScans::GeneratedFillContext>(serialized));

        TScanInfo& info = Scans[buildId];
        info.BuildId = buildId;
        info.SeqNoGeneration = seqNoGeneration;
        info.SeqNoRound = seqNoRound;
        info.ResponseType = responseType;
        info.FinalProgressRecordSerialized.clear();
        info.GeneratedFillContextSerialized = serialized;
    }

    void TBuildIndexScanManager::PersistMarkFinalResponse(NIceDb::TNiceDb& db, ui64 buildId,
                                                        ui64 seqNoGeneration, ui64 seqNoRound,
                                                        const TString& serializedRecord)
    {
        const ui32 responseType = static_cast<ui32>(EBuildIndexEventType::SecondaryIndexResponseFinal);
        db.Table<TDataShard::Schema::IndexBuildScans>()
            .Key(buildId, seqNoGeneration, seqNoRound)
            .Update(
                NIceDb::TUpdate<TDataShard::Schema::IndexBuildScans::ResponseType>(responseType),
                NIceDb::TUpdate<TDataShard::Schema::IndexBuildScans::FinalProgressRecord>(serializedRecord));

        TScanInfo& info = Scans[buildId];
        info.BuildId = buildId;
        info.SeqNoGeneration = seqNoGeneration;
        info.SeqNoRound = seqNoRound;
        info.ResponseType = responseType;
        info.FinalProgressRecordSerialized = serializedRecord;
    }

    void TBuildIndexScanManager::PersistRemove(NIceDb::TNiceDb& db, ui64 buildId, ui64 seqNoGeneration, ui64 seqNoRound) {
        db.Table<TDataShard::Schema::IndexBuildScans>()
            .Key(buildId, seqNoGeneration, seqNoRound)
            .Delete();

        Scans.erase(buildId);
    }

    bool TBuildIndexScanManager::ValidateGeneratedFill(
        const NKikimrTxDataShard::TGeneratedColumnFillContext& context, TString& error) const
    {
        if (!context.HasBuildId() || !context.GetBuildId() ||
            !context.HasSeqNoGeneration() || !context.HasSeqNoRound() ||
            !context.HasOwnerId() || !context.GetOwnerId() ||
            !context.HasPathId() || !context.GetPathId() ||
            !context.HasSchemaVersion() || !context.GetSchemaVersion() ||
            !context.HasTargetColumnId() || !context.GetTargetColumnId() ||
            !context.HasSnapshotStep() || !context.GetSnapshotStep() ||
            !context.HasSnapshotTxId() || !context.GetSnapshotTxId() ||
            !context.HasProgramFingerprint() || context.GetProgramFingerprint().empty())
        {
            error = "Incomplete generated column fill binding";
            return false;
        }
        const auto* info = Scans.FindPtr(context.GetBuildId());
        if (!info || info->GeneratedFillContextSerialized.empty() ||
            info->SeqNoGeneration != context.GetSeqNoGeneration() ||
            info->SeqNoRound != context.GetSeqNoRound())
        {
            error = "Unknown or stale generated column fill attempt";
            return false;
        }
        NKikimrTxDataShard::TGeneratedColumnFillContext expected;
        if (!expected.ParseFromString(info->GeneratedFillContextSerialized) ||
            expected.GetOwnerId() != context.GetOwnerId() ||
            expected.GetPathId() != context.GetPathId() ||
            expected.GetSchemaVersion() != context.GetSchemaVersion() ||
            expected.GetTargetColumnId() != context.GetTargetColumnId() ||
            expected.GetSnapshotStep() != context.GetSnapshotStep() ||
            expected.GetSnapshotTxId() != context.GetSnapshotTxId() ||
            expected.GetProgramFingerprint() != context.GetProgramFingerprint())
        {
            error = "Generated column fill binding does not match the current build";
            return false;
        }
        return true;
    }

    void TBuildIndexScanManager::Reset() {
        Scans.clear();
    }

} // namespace NKikimr::NDataShard
