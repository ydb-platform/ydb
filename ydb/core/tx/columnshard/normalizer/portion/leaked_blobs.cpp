#include "leaked_blobs.h"

#include <ydb/core/keyvalue/keyvalue_const.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_accessor.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/columnshard/tables_manager.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/actor.h>

#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/vector.h>

#include <array>

namespace NKikimr::NOlap {

class TLeakedBlobsNormalizerChanges: public INormalizerChanges {
private:
    THashSet<TLogoBlobID> Leaks;
    const ui64 TabletId;
    const ui64 TablePathId;
    const TString TablePath;
    NColumnShard::TBlobGroupSelector DsGroupSelector;
    const NActors::NLog::EPriority LogLevel;
    ui64 LeakeadBlobsSize;

public:
    TLeakedBlobsNormalizerChanges(THashSet<TLogoBlobID>&& leaks, const ui64 tabletId, const ui64 tablePathId, TString tablePath, NColumnShard::TBlobGroupSelector dsGroupSelector,
        const NActors::NLog::EPriority logLevel)
        : Leaks(std::move(leaks))
        , TabletId(tabletId)
        , TablePathId(tablePathId)
        , TablePath(std::move(tablePath))
        , DsGroupSelector(dsGroupSelector)
        , LogLevel(logLevel) {
        LeakeadBlobsSize = 0;
        for (const auto& blob : Leaks) {
            LeakeadBlobsSize += blob.BlobSize();
        }
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /*normController*/) const override {
        NIceDb::TNiceDb db(txc.DB);
        for (auto&& i : Leaks) {
            TUnifiedBlobId blobId(DsGroupSelector.GetGroup(i), i);
            db.Table<NColumnShard::Schema::BlobsToDeleteWT>().Key(blobId.ToStringLegacy(), TabletId).Update();
        }

        return true;
    }

    void ApplyOnComplete(const TNormalizationController& /* normController */) const override {
        ACTORS_FORMATTED_LOG(LogLevel, NKikimrServices::TX_COLUMNSHARD)(
            "normalizer", TLeakedBlobsNormalizer::GetClassNameStatic())("tablet_id", TabletId)("table_path_id", TablePathId)("table_path", TablePath)("event", "apply_on_complete")("changes", DebugString());
    }

    ui64 GetSize() const override {
        return Leaks.size();
    }

    TString DebugString() const override {
        TStringBuilder sb;
        sb << "tablet=" << TabletId;
        sb << ";leaked_blob_count=" << Leaks.size();
        sb << ";leaked_blobs_size=" << LeakeadBlobsSize;
        return sb;
    }
};

class THiveHistoryCollector {
private:
    const ui64 TabletId;
    const ui64 TablePathId;
    const TString TablePath;
    const ui32 TabletGeneration;
    const bool Enabled;
    const NActors::NLog::EPriority LogLevel;
    static constexpr ui32 MaxRedirects = 8;
    bool Finished = false;
    TActorId PipeClient;
    ui64 HiveTabletId = TDomainsInfo::BadTabletId;
    ui32 RedirectsCount = 0;

    void FinishWithError(const TStringBuf reason) {
        if (Finished) {
            return;
        }
        Finished = true;
        ACTORS_FORMATTED_LOG(LogLevel, NKikimrServices::TX_COLUMNSHARD)("normalizer", TLeakedBlobsNormalizer::GetClassNameStatic())("tablet_id", TabletId)("table_path_id", TablePathId)("table_path", TablePath)("event", "hive_channel_history")("status", "failed")("reason", reason)("hive_tablet_id", HiveTabletId);
    }

    void SendRequestToHive(const TActorContext& ctx, const ui64 hiveTabletId) {
        HiveTabletId = hiveTabletId;
        PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, HiveTabletId, NTabletPipe::TClientRetryPolicy::WithRetries()));
        auto request = std::make_unique<TEvHive::TEvRequestHiveInfo>();
        request->Record.SetTabletID(TabletId);
        request->Record.SetReturnChannelHistory(true);
        NTabletPipe::SendData(ctx.SelfID, PipeClient, request.release());
    }

public:
    THiveHistoryCollector(const ui64 tabletId, const ui64 tablePathId, TString tablePath, const ui32 tabletGeneration, const bool enabled, const NActors::NLog::EPriority logLevel)
        : TabletId(tabletId)
        , TablePathId(tablePathId)
        , TablePath(std::move(tablePath))
        , TabletGeneration(tabletGeneration)
        , Enabled(enabled)
        , LogLevel(logLevel) {
        if (!Enabled) {
            Finished = true;
        }
    }

    bool IsEnabled() const {
        return Enabled;
    }

    bool IsFinished() const {
        return Finished;
    }

    void Bootstrap(const TActorContext& ctx) {
        if (!Enabled) {
            return;
        }
        HiveTabletId = AppData()->DomainsInfo->GetHive();
        if (HiveTabletId == TDomainsInfo::BadTabletId) {
            FinishWithError("hive_tablet_id_is_not_available");
            return;
        }

        SendRequestToHive(ctx, HiveTabletId);
    }

    void Handle(TEvHive::TEvResponseHiveInfo::TPtr& ev, const TActorContext& ctx) {
        if (!Enabled || Finished) {
            return;
        }
        auto* msg = ev->Get();
        if (msg->Record.HasForwardRequest()) {
            const ui64 redirectedHiveTabletId = msg->Record.GetForwardRequest().GetHiveTabletId();
            if (!redirectedHiveTabletId) {
                FinishWithError("forward_request_hive_tablet_id_is_empty");
                return;
            }
            if (redirectedHiveTabletId == HiveTabletId) {
                FinishWithError("forward_request_loop");
                return;
            }
            if (++RedirectsCount > MaxRedirects) {
                FinishWithError("too_many_forward_redirects");
                return;
            }
            ACTORS_FORMATTED_LOG(LogLevel, NKikimrServices::TX_COLUMNSHARD)("normalizer", TLeakedBlobsNormalizer::GetClassNameStatic())(
                "tablet_id", TabletId)("table_path_id", TablePathId)("table_path", TablePath)("event", "hive_channel_history_redirect")("from_hive_tablet_id", HiveTabletId)(
                "to_hive_tablet_id", redirectedHiveTabletId)("redirects_count", RedirectsCount);
            SendRequestToHive(ctx, redirectedHiveTabletId);
            return;
        }
        if (msg->Record.TabletsSize() == 0) {
            FinishWithError("empty_hive_response");
            return;
        }

        TVector<TString> historyEntries;
        for (const auto& tablet : msg->Record.GetTablets()) {
            for (ui32 channelIdx = 0; channelIdx < tablet.TabletChannelsSize(); ++channelIdx) {
                const auto& channel = tablet.GetTabletChannels(channelIdx);
                for (const auto& entry : channel.GetHistory()) {
                    TStringBuilder historyEntry;
                    historyEntry << "{channel=" << channelIdx << ",from_generation=" << entry.GetGeneration() << ",group_id=" << entry.GetGroup()
                                 << ",timestamp_ms=" << entry.GetTimestamp() << "}";
                    historyEntries.emplace_back(historyEntry);
                }
            }
        }
        const ui64 nowMs = TAppData::TimeProvider->Now().MilliSeconds();
        const ui64 recordsCount = historyEntries.size();
        static constexpr ui64 HistoryChunkSize = 50;
        const ui64 chunksTotal = recordsCount ? (recordsCount + HistoryChunkSize - 1) / HistoryChunkSize : 1;
        for (ui64 chunkIdx = 0; chunkIdx < chunksTotal; ++chunkIdx) {
            const ui64 beginIdx = chunkIdx * HistoryChunkSize;
            const ui64 endIdx = Min<ui64>(beginIdx + HistoryChunkSize, recordsCount);
            TStringBuilder chunkHistory;
            for (ui64 idx = beginIdx; idx < endIdx; ++idx) {
                if (idx > beginIdx) {
                    chunkHistory << ";";
                }
                chunkHistory << historyEntries[idx];
            }
            ACTORS_FORMATTED_LOG(LogLevel, NKikimrServices::TX_COLUMNSHARD)("normalizer", TLeakedBlobsNormalizer::GetClassNameStatic())("tablet_id", TabletId)("table_path_id", TablePathId)("table_path", TablePath)(
                "analyze_leaked_blobs", 1)("event", "hive_channel_history")("status", "ok")("hive_tablet_id", HiveTabletId)(
                "records_count", recordsCount)("chunk_idx", chunkIdx)("chunks_total", chunksTotal)(
                "now_ms", nowMs)("current_generation", TabletGeneration)(
                "chunk_records_count", endIdx - beginIdx)("history_chunk", chunkHistory);
        }

        Finished = true;
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (!Enabled || Finished) {
            return;
        }
        if (ev->Get()->ClientId != PipeClient) {
            return;
        }
        if (ev->Get()->Status != NKikimrProto::OK) {
            FinishWithError("pipe_connect_failed");
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (!Enabled || Finished) {
            return;
        }
        if (ev->Get()->ClientId == PipeClient) {
            FinishWithError("pipe_destroyed");
        }
    }

    void HandleTimeout() {
        if (!Enabled || Finished) {
            return;
        }
        FinishWithError("timeout");
    }
};

class TRemoveLeakedBlobsActor: public TActorBootstrapped<TRemoveLeakedBlobsActor> {
private:
    TVector<TTabletChannelInfo> Channels;
    THashSet<TLogoBlobID> CSBlobIds;
    THashSet<TLogoBlobID> BSBlobIds;
    ui64 TotalBlobsCount = 0;
    ui64 TotalBlobsSize = 0;
    ui64 DoNotKeepCount = 0;
    ui64 KeepCount = 0;
    TActorId CSActorId;
    ui64 CSTabletId;
    ui64 TablePathId = 0;
    TString TablePath;
    bool PrintLeakedBlobIds = false;
    NActors::NLog::EPriority LogLevel = NActors::NLog::PRI_WARN;
    i32 WaitingCount = 0;
    THashSet<ui32> WaitingRequests;
    NColumnShard::TBlobGroupSelector DsGroupSelector;
    THiveHistoryCollector HiveHistoryCollector;

    static ui64 CalculateBlobsSize(const THashSet<TLogoBlobID>& blobs) {
        ui64 size = 0;
        for (const auto& blobId : blobs) {
            size += blobId.BlobSize();
        }
        return size;
    }

    void PrintFoundLeakedBlobsStats() const {
        const ui64 leakedBlobsSize = CalculateBlobsSize(BSBlobIds);
        const ui64 csBlobsSize = CalculateBlobsSize(CSBlobIds);
        ACTORS_FORMATTED_LOG(LogLevel, NKikimrServices::TX_COLUMNSHARD)("normalizer", TLeakedBlobsNormalizer::GetClassNameStatic())("tablet_id", CSTabletId)("table_path_id", TablePathId)("table_path", TablePath)("event", "found_leaked_blobs_stats")("analyze_leaked_blobs", 1)("leaked_blobs_count", BSBlobIds.size())("leaked_blobs_size", leakedBlobsSize)("cs_blob_ids_count", CSBlobIds.size())("cs_blob_ids_size", csBlobsSize)("bs_total_blobs_count", TotalBlobsCount)("bs_total_blobs_size", TotalBlobsSize)("bs_do_not_keep_count", DoNotKeepCount)("bs_keep_count", KeepCount);
    }

    void PrintLeakedBlobIdsChunks() const {
        if (!PrintLeakedBlobIds) {
            ACTORS_FORMATTED_LOG(LogLevel, NKikimrServices::TX_COLUMNSHARD)("normalizer", TLeakedBlobsNormalizer::GetClassNameStatic())("analyze_leaked_blobs", 1)("tablet_id", CSTabletId)("table_path_id", TablePathId)("table_path", TablePath)("event", "found_leaked_blob_ids")("status", "not_requested");
            return;
        }

        static constexpr size_t ChunkSize = 100;
        const ui64 chunksTotal = (BSBlobIds.size() + ChunkSize - 1) / ChunkSize;
        ui64 chunkIdx = 0;
        TVector<TString> currentChunk;
        currentChunk.reserve(ChunkSize);

        auto printChunk = [&](const TVector<TString>& chunk) {
            ACTORS_FORMATTED_LOG(LogLevel, NKikimrServices::TX_COLUMNSHARD)("normalizer", TLeakedBlobsNormalizer::GetClassNameStatic())("tablet_id", CSTabletId)("table_path_id", TablePathId)("table_path", TablePath)("event", "found_leaked_blob_ids_chunk")("analyze_leaked_blobs", 1)("chunk_idx", chunkIdx)("chunks_total", chunksTotal)("chunk_size", chunk.size())("blob_ids", JoinStrings(chunk.begin(), chunk.end(), ","));
            ++chunkIdx;
        };

        for (const auto& blobId : BSBlobIds) {
            currentChunk.emplace_back(blobId.ToString());
            if (currentChunk.size() == ChunkSize) {
                printChunk(currentChunk);
                currentChunk.clear();
            }
        }
        if (!currentChunk.empty()) {
            printChunk(currentChunk);
        }
    }

    void CheckFinish() {
        if (WaitingCount || !HiveHistoryCollector.IsFinished()) {
            return;
        }
        AFL_VERIFY(CSBlobIds.size() <= BSBlobIds.size())("cs", CSBlobIds.size())("bs", BSBlobIds.size())("error", "have to use broken blobs repair");
        for (auto&& i : CSBlobIds) {
            AFL_VERIFY(BSBlobIds.erase(i))("error", "have to use broken blobs repair")("blob_id", i);
        }
        PrintFoundLeakedBlobsStats();
        PrintLeakedBlobIdsChunks();
        TActorContext::AsActorContext().Send(
            CSActorId, 
            std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(
                std::make_shared<TLeakedBlobsNormalizerChanges>(
                    std::move(BSBlobIds), CSTabletId, TablePathId, TablePath, DsGroupSelector, LogLevel
                )
            )
        );
        PassAway();
    }

public:
    TRemoveLeakedBlobsActor(TVector<TTabletChannelInfo>&& channels, THashSet<TLogoBlobID>&& csBlobIDs, const ui64 tablePathId, TString tablePath, TActorId csActorId, ui64 csTabletId,
        const ui32 tabletGeneration, const NColumnShard::TBlobGroupSelector& dsGroupSelector, const bool printLeakedBlobIds,
        const NActors::NLog::EPriority logLevel)
        : Channels(std::move(channels))
        , CSBlobIds(std::move(csBlobIDs))
        , CSActorId(csActorId)
        , CSTabletId(csTabletId)
        , TablePathId(tablePathId)
        , TablePath(std::move(tablePath))
        , PrintLeakedBlobIds(printLeakedBlobIds)
        , LogLevel(logLevel)
        , DsGroupSelector(dsGroupSelector)
        , HiveHistoryCollector(csTabletId, tablePathId, TablePath, tabletGeneration, printLeakedBlobIds, logLevel) {
    }

    void Bootstrap(const TActorContext& ctx) {
        WaitingCount = 0;

        for (auto it = Channels.begin(); it != Channels.end(); ++it) {
            if (it->Channel < 2) {
                continue;
            }
            for (auto&& i : it->History) {
                TLogoBlobID from(CSTabletId, 0, 0, it->Channel, 0, 0);
                TLogoBlobID to(CSTabletId, Max<ui32>(), Max<ui32>(), it->Channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
                auto request = MakeHolder<TEvBlobStorage::TEvRange>(CSTabletId, from, to, false, TInstant::Max(), true);
                SendToBSProxy(SelfId(), i.GroupID, request.Release(), ++WaitingCount);
                WaitingRequests.emplace(WaitingCount);
            }
        }
        HiveHistoryCollector.Bootstrap(ctx);
        if (HiveHistoryCollector.IsEnabled()) {
            ctx.Schedule(TDuration::Seconds(30), new TEvents::TEvWakeup());
        }
        CheckFinish();

        Become(&TThis::StateWait);
    }

    void Handle(TEvBlobStorage::TEvRangeResult::TPtr& ev, const TActorContext& /*ctx*/) {
        TEvBlobStorage::TEvRangeResult* msg = ev->Get();
        AFL_VERIFY(msg->Status == NKikimrProto::OK)("status", msg->Status)("error", msg->ErrorReason);
        AFL_VERIFY(--WaitingCount >= 0);
        AFL_VERIFY(WaitingRequests.erase(ev->Cookie));
        for (auto& resp : msg->Responses) {
            AFL_VERIFY(!resp.Buffer);
            DoNotKeepCount += resp.DoNotKeep;
            KeepCount += resp.Keep;
            if (resp.DoNotKeep && !resp.Keep) {
                continue;
            }
            const bool inserted = BSBlobIds.emplace(resp.Id).second;
            if (inserted) {
                TotalBlobsCount++;
                TotalBlobsSize += resp.Id.BlobSize();
            }
        }
        CheckFinish();
    }

    void Handle(TEvHive::TEvResponseHiveInfo::TPtr& ev, const TActorContext& ctx) {
        HiveHistoryCollector.Handle(ev, ctx);
        CheckFinish();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& /*ctx*/) {
        HiveHistoryCollector.Handle(ev);
        CheckFinish();
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& /*ctx*/) {
        HiveHistoryCollector.Handle(ev);
        CheckFinish();
    }

    void Handle(TEvents::TEvWakeup::TPtr&, const TActorContext& /*ctx*/) {
        HiveHistoryCollector.HandleTimeout();
        CheckFinish();
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvRangeResult, Handle);
            HFunc(TEvHive::TEvResponseHiveInfo, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

class TRemoveLeakedBlobsTask: public INormalizerTask {
    TVector<TTabletChannelInfo> Channels;
    THashSet<TLogoBlobID> CSBlobIDs;
    ui64 TablePathId = 0;
    TString TablePath;
    ui64 TabletId;
    ui32 TabletGeneration;
    TActorId ActorId;
    NColumnShard::TBlobGroupSelector DsGroupSelector;
    bool PrintLeakedBlobIds = false;
    NActors::NLog::EPriority LogLevel = NActors::NLog::PRI_WARN;

public:
    TRemoveLeakedBlobsTask(TVector<TTabletChannelInfo>&& channels, THashSet<TLogoBlobID>&& csBlobIDs, const ui64 tablePathId, TString tablePath, ui64 tabletId, ui32 tabletGeneration,
        TActorId actorId,
        const NColumnShard::TBlobGroupSelector& dsGroupSelector, const bool printLeakedBlobIds, const NActors::NLog::EPriority logLevel)
        : Channels(std::move(channels))
        , CSBlobIDs(std::move(csBlobIDs))
        , TablePathId(tablePathId)
        , TablePath(std::move(tablePath))
        , TabletId(tabletId)
        , TabletGeneration(tabletGeneration)
        , ActorId(actorId)
        , DsGroupSelector(dsGroupSelector)
        , PrintLeakedBlobIds(printLeakedBlobIds)
        , LogLevel(logLevel) {
    }
    void Start(const TNormalizationController& /*controller*/, const TNormalizationContext& /*nCtx*/) override {
        NActors::TActivationContext::Register(
            new TRemoveLeakedBlobsActor(
                std::move(Channels), std::move(CSBlobIDs), TablePathId, std::move(TablePath), ActorId, TabletId, TabletGeneration, DsGroupSelector, PrintLeakedBlobIds, LogLevel));
    }
};

void TLeakedBlobsStats::PrintToLog() const {
    ACTORS_FORMATTED_LOG(LogLevel, NKikimrServices::TX_COLUMNSHARD)("normalizer", TLeakedBlobsNormalizer::GetClassNameStatic())("tablet_id", TabletId)("table_path_id", TablePathId)("table_path", TablePath)("event", "stats")("stopped_on_portions", StoppedOnPortions)("stopped_on_indices", StoppedOnIndices)("stopped_on_columns", StoppedOnColumns)("stopped_on_blobs_to_delete", StoppedOnBlobsToDelete)("portions_loaded", PortionsLoaded)("portions_skipped", PortionsSkipped)("portions_only_indices_in_bs", PortionsOnlyIndicesInBs)("portions_in_bs", PortionsInBs)("indices_loaded", IndicesLoaded)("indices_inplaced", IndicesInplaced)("indices_in_foreign_storage", IndicesInForeignStorage)("indices_need_column_v2", IndicesNeedColumnV2)("indices_have_its_own_blob", IndicesHaveItsOwnBlob)("columns_loaded", ColumnsLoaded)("blobs_to_delete_loaded", BlobsToDeleteLoaded)("completed", Completed);
}

TLeakedBlobsNormalizer::TLeakedBlobsNormalizer(const TNormalizationController::TInitContext& info)
    : TBase(info)
    , Channels(info.GetStorageInfo()->Channels)
    , DsGroupSelector(info.GetStorageInfo())
    , Stats(TabletId) {
}

TConclusion<std::vector<INormalizerTask::TPtr>> TLeakedBlobsNormalizer::DoInit(
    const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    AFL_VERIFY(AppDataVerified().FeatureFlags.GetEnableWritePortionsOnInsert());

    NIceDb::TNiceDb db(txc.DB);
    NColumnShard::TTablesManager tablesManager(
        controller.GetStoragesManager(), controller.GetDataAccessorsManager(), std::make_shared<TPortionIndexStats>(), TabletId);
    if (!tablesManager.InitFromDB(db, nullptr)) {
        ACTORS_FORMATTED_LOG(LogLevel, NKikimrServices::TX_COLUMNSHARD)("normalizer", TLeakedBlobsNormalizer::GetClassNameStatic())(
            "tablet_id", TabletId)("table_path_id", TablePathId)("table_path", TablePath)("error", "can't initialize tables manager");
        return TConclusionStatus::Fail("Can't load index");
    }
    if (!tablesManager.HasPrimaryIndex()) {
        return std::vector<INormalizerTask::TPtr>{};
    }

    LoadTableIdentity(db);
    ReadParamsFromDescription();
    if (Stats.WannaPrint()) {
        Stats.PrintToLog();
    }

    TDbWrapper wrapper(db.GetDatabase(), &DsGroupSelector);
    auto conclusion = LoadPortionBlobIds(wrapper, tablesManager.GetPrimaryIndexAsVerified<TColumnEngineForLogs>().GetVersionedIndex());
    if (conclusion.IsFail()) {
        return conclusion;
    }
    conclusion = LoadBlobsToDelete(db);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    Stats.OnCompleted();
    Stats.PrintToLog();
    return std::vector<INormalizerTask::TPtr>{ std::make_shared<TRemoveLeakedBlobsTask>(
        std::move(Channels), std::move(Result), TablePathId, TablePath, TabletId, txc.Generation, TabletActorId, DsGroupSelector, PrintLeakedBlobIds, LogLevel) };
}

void TLeakedBlobsNormalizer::LoadTableIdentity(NIceDb::TNiceDb& db) {
    if (!NColumnShard::Schema::GetSpecialValueOpt(db, NColumnShard::Schema::EValueIds::OwnerPathId, TablePathId)) {
        TablePathId = 0;
    }
    if (!NColumnShard::Schema::GetSpecialValueOpt(db, NColumnShard::Schema::EValueIds::OwnerPath, TablePath)) {
        TablePath = "<unknown>";
    }
    Stats.SetTableIdentity(TablePathId, TablePath);
}


TConclusionStatus TLeakedBlobsNormalizer::LoadPortionBlobIds(
    TDbWrapper& wrapper, 
    const TVersionedIndex& versionedIndex
) {
    while (!BatchCursor.IsFinished()) {
        TConclusionStatus conclusion = TConclusionStatus::Success();
        switch (BatchCursor.GetStep()) {
            case TProcessPortionsStep::Portions:
                conclusion = LoadPortions(wrapper, versionedIndex);
                break;
            case TProcessPortionsStep::Indices:
                conclusion = LoadIndices(wrapper);
                break;
            case TProcessPortionsStep::Columns:
                conclusion = LoadColumns(wrapper);
                break;
            case TProcessPortionsStep::Finished:
                AFL_VERIFY(false)("error", "finished step");
        }
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TLeakedBlobsNormalizer::LoadPortions(
    TDbWrapper& wrapper,
    const TVersionedIndex& versionedIndex
) {
    auto allProcessed = wrapper.LoadPortions(
        [&](std::unique_ptr<TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
            auto schema = portion->GetSchema(versionedIndex);
            auto portionTier = metaProto.GetTierName();
            auto howToProcessPortion = DefineHowToProcessPortion(portionTier, schema);
            if (howToProcessPortion != THowToProcessPortion::Skip) {
                BatchCursor.AddPortion(portion->GetPathId(), portion->GetPortionIdVerified(), schema, portionTier, howToProcessPortion == THowToProcessPortion::All);
            }

            BatchCursor.OnPortionLoaded(portion->GetPathId(), portion->GetPortionIdVerified());
            Stats.OnPortionLoaded(howToProcessPortion);
            return !BatchCursor.IsFull();
        },
        BatchCursor.GetNextLoadPortionKey().first, 
        BatchCursor.GetNextLoadPortionKey().second
    );
    if (allProcessed) {
        BatchCursor.NoMorePortions();
    }
    if (BatchCursor.IsFull() || allProcessed) {
        BatchCursor.NextStep();
        return TConclusionStatus::Success();
    } else {
        Stats.OnStoppedOnPortions();
        return TConclusionStatus::Fail("LoadPortions: Portions are not ready yet");
    }
}

TConclusionStatus TLeakedBlobsNormalizer::LoadIndices(TDbWrapper& wrapper) {
    auto allProcessed = wrapper.LoadIndexes(
        [&](const TInternalPathId pathId, const ui64 portionId, TIndexChunkLoadContext&& indexChunk) {
            Stats.OnIndexLoaded();
            BatchCursor.MoveCurrentPortionTo(pathId, portionId);
            if (BatchCursor.NeedToSkip(pathId, portionId)) {
                return;
            }
            TPortionToProcess& portion = BatchCursor.GetCurrentPortion();
            
            if (portion.IsInDefaultStorage(indexChunk.GetEntityId())) {
                if (indexChunk.GetBlobRangeAddress()) {
                    Result.emplace(indexChunk.GetBlobRangeAddress()->GetBlobId().GetLogoBlobId());
                    Stats.OnIndexHasItsOwnBlob();
                } else if (indexChunk.GetBlobRangeLink16()) {
                    // if the portion is in default storage, we will take all its column blobs anyway
                    // so no need to add index idxs for checking
                    if (!portion.IsInDefaultStorage()) {
                        portion.AddDeferredIndexBlobIdx(indexChunk.GetBlobRangeLink16()->GetBlobIdxVerified());
                    }
                    Stats.OnIndexNeedColumnV2();
                } else {
                    Stats.OnIndexInplaced();
                }
            } else {
                Stats.OnIndexInForeignStorage();
            }
            return;
        },
        BatchCursor.StartPathId(),
        BatchCursor.StartPortionId(),
        BatchCursor.EndPathId(),
        BatchCursor.EndPortionId()
    );

    if (allProcessed) {
        BatchCursor.NextStep();
        return TConclusionStatus::Success();
    } else {
        Stats.OnStoppedOnIndices();
        return TConclusionStatus::Fail("LoadIndexes: Indexes are not ready yet");
    }
}

TConclusionStatus TLeakedBlobsNormalizer::LoadColumns(TDbWrapper& wrapper) {
    auto allProcessed = wrapper.LoadColumns(
        [&](TColumnChunkLoadContextV2&& columnChunk) {
            Stats.OnColumnLoaded();
            const auto pathId = columnChunk.GetPathId();
            const auto portionId = columnChunk.GetPortionId();
            BatchCursor.MoveCurrentPortionTo(pathId, portionId);
            if (BatchCursor.NeedToSkip(pathId, portionId)) {
                return;
            }
            TPortionToProcess& portion = BatchCursor.GetCurrentPortion();
            const auto& blobIds = columnChunk.GetBlobIds();
            if (!portion.GetDeferredIndexBlobIdxs().empty()) {
                for (auto& idx : portion.GetDeferredIndexBlobIdxs()) {
                    AFL_VERIFY(idx < blobIds.size())("idx", idx)("blob_ids_size", blobIds.size())("path_id", portion.GetPathId())("portion_id", portion.GetPortionId());
                    Result.emplace(blobIds[idx].GetLogoBlobId());
                }
            } else {
                for (auto& blobId : blobIds) {
                    Result.emplace(blobId.GetLogoBlobId());
                }
            }
        },
        BatchCursor.StartPathId(),
        BatchCursor.StartPortionId(),
        BatchCursor.EndPathId(),
        BatchCursor.EndPortionId()
    );

    if (allProcessed) {
        BatchCursor.NextStep();
        return TConclusionStatus::Success();
    } else {
        Stats.OnStoppedOnColumns();
        return TConclusionStatus::Fail("LoadColumns: Columns are not ready yet");
    }
}

TConclusionStatus TLeakedBlobsNormalizer::LoadBlobsToDelete(NIceDb::TNiceDb& db) {
    auto rowset = db.Table<NColumnShard::Schema::BlobsToDeleteWT>().Select();
    if (!rowset.IsReady()) {
        Stats.OnStoppedOnBlobsToDelete();
        return TConclusionStatus::Fail("Not ready: BlobsToDeleteWT");
    }
    while (!rowset.EndOfSet()) {
        Stats.OnBlobsToDeleteLoaded();
        const TString& blobIdStr = rowset.GetValue<NColumnShard::Schema::BlobsToDeleteWT::BlobId>();
        TString error;
        TUnifiedBlobId blobId = TUnifiedBlobId::ParseFromString(blobIdStr, &DsGroupSelector, error);
        AFL_VERIFY(blobId.IsValid())("event", "cannot_parse_blob")("error", error)("original_string", blobIdStr);
        Result.emplace(blobId.GetLogoBlobId());
        if (!rowset.Next()) {
            Stats.OnStoppedOnBlobsToDelete();
            return TConclusionStatus::Fail("Local table is not loaded: BlobsToDeleteWT");
        }
    }
    return TConclusionStatus::Success();
}

THowToProcessPortion TLeakedBlobsNormalizer::DefineHowToProcessPortion(
    const TString& portionTier,
    const ISnapshotSchema::TPtr& schema
) const {
    const TString& effectiveTier = portionTier.empty() ? NBlobOperations::TGlobal::DefaultStorageId : portionTier;
    const TIndexInfo& indexInfo = schema->GetIndexInfo();
    if (effectiveTier == NBlobOperations::TGlobal::DefaultStorageId) {
        return THowToProcessPortion::All;
    }
    for (auto&& entityId : indexInfo.GetEntityIds()) {
        auto entityStorageId = indexInfo.GetEntityStorageId(entityId, effectiveTier);
        if (entityStorageId == NBlobOperations::TGlobal::DefaultStorageId) {
            return THowToProcessPortion::OnlyIndices;
        }
    }
    return THowToProcessPortion::Skip;
}

namespace {
bool TryParseLogPriority(const TStringBuf value, NActors::NLog::EPriority& out) {
    TString normalized(value);
    normalized.to_upper();
    static const std::array<NActors::NLog::EPriority, 9> priorities = {
        NActors::NLog::PRI_TRACE,
        NActors::NLog::PRI_DEBUG,
        NActors::NLog::PRI_INFO,
        NActors::NLog::PRI_NOTICE,
        NActors::NLog::PRI_WARN,
        NActors::NLog::PRI_ERROR,
        NActors::NLog::PRI_CRIT,
        NActors::NLog::PRI_ALERT,
        NActors::NLog::PRI_EMERG
    };
    for (const auto priority : priorities) {
        if (normalized == NActors::NLog::PriorityToString(NActors::NLog::EPrio(priority))) {
            out = priority;
            return true;
        }
    }
    return false;
}
} // namespace

void TLeakedBlobsNormalizer::ReadParamsFromDescription() {
    if (ParamsInitialized) {
        return;
    }
    ParamsInitialized = true;

    const TString& description = GetUniqueDescription();

    TVector<TStringBuf> tokens;
    StringSplitter(description).Split(';').SkipEmpty().Collect(&tokens);
    for (ui32 i = 1; i < tokens.size(); ++i) {
        const TStringBuf token = tokens[i];
        const size_t eqPos = token.find('=');
        AFL_VERIFY(eqPos != TStringBuf::npos)("error", "invalid param format, expected key=value")("token", token)("description", description);

        const TStringBuf key = token.SubStr(0, eqPos);
        const TStringBuf value = token.SubStr(eqPos + 1);
        if (key == "batch_size") {
            const auto parsed = TryFromString<size_t>(TString(value));
            AFL_VERIFY(parsed.Defined())("error", "cannot parse batch_size")("value", TString(value))("description", description);
            AFL_VERIFY(*parsed > 0)("error", "batch_size has to be > 0")("value", *parsed)("description", description);
            BatchSize = *parsed;
        } else if (key == "print_leaked_blob_ids") {
            if (value == "true") {
                PrintLeakedBlobIds = true;
            } else if (value == "false") {
                PrintLeakedBlobIds = false;
            } else {
                AFL_VERIFY(false)("error", "cannot parse print_leaked_blob_ids")("value", TString(value))("description", description);
            }
        } else if (key == "log_level") {
            NActors::NLog::EPriority parsedPriority = NActors::NLog::PRI_WARN;
            AFL_VERIFY(TryParseLogPriority(value, parsedPriority))(
                "error", "cannot parse log_level")("value", TString(value))("description", description);
            LogLevel = parsedPriority;
        } else {
            AFL_VERIFY(false)("error", "unknown param key")("key", key)("description", description);
        }
    }

    BatchCursor = TBatchCursor(BatchSize);
    Stats.SetLogLevel(LogLevel);
    ACTORS_FORMATTED_LOG(LogLevel, NKikimrServices::TX_COLUMNSHARD)("normalizer", TLeakedBlobsNormalizer::GetClassNameStatic())("tablet_id", TabletId)("table_path_id", TablePathId)("table_path", TablePath)("batch_size", BatchSize)("print_leaked_blob_ids", PrintLeakedBlobIds)("log_level", static_cast<ui32>(LogLevel));
}

}   // namespace NKikimr::NOlap

