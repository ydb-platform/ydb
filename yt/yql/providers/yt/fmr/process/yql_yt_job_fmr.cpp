#include "yql_yt_job_fmr.h"

#include <yt/yql/providers/yt/common/yql_configuration.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_reduce_reader.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_key_hash_block_iterator.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_sort_helper.h>
#include <yt/yql/providers/yt/fmr/request_options/proto_helpers/yql_yt_request_proto_helpers.h>
#include <yt/yql/providers/yt/fmr/tvm/impl/yql_yt_fmr_tvm_impl.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parse_records.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_input_streams.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_tds_block_iterator.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_sorted_writer.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/static/yql_yt_static_service_discovery.h>

#include <yql/essentials/utils/log/log.h>

#include <util/thread/pool.h>

namespace NYql::NFmr {

namespace {

// ITableDataService that captures Put() blobs into an in-memory vector.
// Used during the MapReduceMap mapper phase to avoid writing to the real TDS.
class TQueueWriteTableDataService final: public ITableDataService {
public:
    explicit TQueueWriteTableDataService(TVector<TString>& blobs)
        : Blobs_(blobs)
    {
    }

    NThreading::TFuture<bool> Put(const TString& /*group*/, const TString& /*chunkId*/, const TString& value) override {
        Blobs_.push_back(value);
        return NThreading::MakeFuture(true);
    }

    NThreading::TFuture<TMaybe<TString>> Get(const TString& /*group*/, const TString& /*chunkId*/) const override {
        ythrow yexception() << "TQueueWriteTableDataService: Get is not supported";
    }

    NThreading::TFuture<void> Delete(const TString& /*group*/, const TString& /*chunkId*/) override {
        ythrow yexception() << "TQueueWriteTableDataService: Delete is not supported";
    }

    NThreading::TFuture<void> RegisterDeletion(const std::vector<TString>& /*groupsToDelete*/) override {
        ythrow yexception() << "TQueueWriteTableDataService: RegisterDeletion is not supported";
    }

    NThreading::TFuture<void> Clear() override {
        ythrow yexception() << "TQueueWriteTableDataService: Clear is not supported";
    }

private:
    TVector<TString>& Blobs_;
};

// IBlockIterator over an in-memory blob vector produced by TQueueWriteTableDataService.
// Each blob is a binary YSON list-fragment chunk written by TFmrTableDataServiceWriter.
class TQueueBlobBlockIterator final: public IBlockIterator {
public:
    TQueueBlobBlockIterator(TVector<TString> blobs,
                            std::vector<TString> keyColumns,
                            std::vector<ESortOrder> sortOrders)
        : Blobs_(std::move(blobs))
        , KeyColumns_(std::move(keyColumns))
        , SortOrders_(std::move(sortOrders))
    {
    }

    bool NextBlock(TIndexedBlock& out) final {
        if (Pos_ >= Blobs_.size()) {
            return false;
        }
        // Move the blob out so the original buffer is freed before the hashed
        // block is accumulated, avoiding x2 peak memory.
        out.Data = std::move(Blobs_[Pos_++]);
        TParserFragmentListIndex parser(out.Data, KeyColumns_);
        parser.Parse();
        out.Rows = parser.GetRows();
        return true;
    }

    std::vector<ESortOrder> GetSortOrder() final {
        return SortOrders_;
    }

private:
    TVector<TString> Blobs_;
    size_t Pos_ = 0;
    std::vector<TString> KeyColumns_;
    std::vector<ESortOrder> SortOrders_;
};

} // namespace

TFmrUserJob::TFmrUserJob()
    : TYqlUserJobBase()
{
}

void TFmrUserJob::Save(IOutputStream& s) const {
    TYqlUserJobBase::Save(s);
    ::SaveMany(&s,
        InputTables_,
        OutputTables_,
        ClusterConnections_,
        TableDataServiceDiscoveryFilePath_,
        YtJobServiceType_,
        FmrJobType_,
        Settings_,
        TvmSettings_,
        VanillaInfo_,
        ReduceOperationSpec_,
        IsMapReduceReducer_,
        IsMapReduceMap_,
        SortByHasKeyHashPrefix_,
        ForceTableIndexMarking_
    );
}

void TFmrUserJob::Load(IInputStream& s) {
    TYqlUserJobBase::Load(s);
    ::LoadMany(&s,
        InputTables_,
        OutputTables_,
        ClusterConnections_,
        TableDataServiceDiscoveryFilePath_,
        YtJobServiceType_,
        FmrJobType_,
        Settings_,
        TvmSettings_,
        VanillaInfo_,
        ReduceOperationSpec_,
        IsMapReduceReducer_,
        IsMapReduceMap_,
        SortByHasKeyHashPrefix_,
        ForceTableIndexMarking_
    );
}

TIntrusivePtr<NYT::IReaderImplBase> TFmrUserJob::MakeMkqlJobReader() {
    return MakeIntrusive<TMkqlReaderImpl>(*QueueReader_, YQL_JOB_CODEC_BLOCK_COUNT, YQL_JOB_CODEC_BLOCK_SIZE);
}

TIntrusivePtr<TMkqlWriterImpl> TFmrUserJob::MakeMkqlJobWriter() {
    TVector<IOutputStream*> outputStreams;
    for (auto& writer: TableDataServiceWriters_) {
        outputStreams.emplace_back(writer.Get());
    }
    return MakeIntrusive<TMkqlWriterImpl>(outputStreams, YQL_JOB_CODEC_BLOCK_COUNT, YQL_JOB_CODEC_BLOCK_SIZE);
}

void TFmrUserJob::ChangeMkqlIOSpecIfNeeded() {
    // disable skiff and arrow formats for fmr job.
    MkqlIOSpecs->UseBlockInput_ = false;
    MkqlIOSpecs->UseBlockOutput_ = false;
    MkqlIOSpecs->UseSkiff_ = false;
}

void TFmrUserJob::PostInitMkqlIOSpec() {
    if (!IsMapReduceReducer_) {
        return;
    }
    // Register _yql_key_hash as a skip field in all input decoders so the
    // codec silently discards it when present (inserted by MapReduceMap for
    // n-way merge routing, irrelevant to the reducer's schema).
    const TString keyHashName(YqlKeyHashColumn);
    NKikimr::NMiniKQL::TDataType* uint64Type =
        NKikimr::NMiniKQL::TDataType::Create(NUdf::TDataType<ui64>::Id, *Env);
    for (auto& [tableName, decoder] : MkqlIOSpecs->Decoders) {
        decoder.Fields.emplace(
            keyHashName,
            TMkqlIOSpecs::TDecoderSpec::TDecodeField{
                .Name = keyHashName,
                .StructIndex = Max<ui32>(),
                .Type = uint64Type,
            }
        );
    }
}

std::vector<ui32> TFmrUserJob::GetTableIndicesForInput(const TTaskTableRef& tableRef) const {
    std::vector<ui32> tableIndices;
    if (auto* ytTableTaskRef = std::get_if<TYtTableTaskRef>(&tableRef)) {
        if (ytTableTaskRef->TableIndices.empty()) {
            // Callers that build a TYtTableTaskRef without setting TableIndices (e.g. tests
            // constructing one directly) fall back to 0 for all paths; harmless since it's only
            // ever wrong when several distinct original inputs are actually in play, and real
            // production partitioning always fills TableIndices in lockstep with RichPaths.
            tableIndices.assign(ytTableTaskRef->RichPaths.size(), 0);
        } else {
            YQL_ENSURE(ytTableTaskRef->TableIndices.size() == ytTableTaskRef->RichPaths.size());
            tableIndices = ytTableTaskRef->TableIndices;
        }
    } else {
        auto& fmrTableInputRef = std::get<TFmrTableInputRef>(tableRef);
        tableIndices.emplace_back(fmrTableInputRef.TableIndex);
    }
    return tableIndices;
}

bool TFmrUserJob::NeedsTableIndexMarking() const {
    return ForceTableIndexMarking_;
}

void TFmrUserJob::FillQueueFromSingleInputTable(ui64 curTableNum, bool needsTableIndexMarking) {
    auto inputTableRef = InputTables_.Inputs[curTableNum];
    auto tableIndices = GetTableIndicesForInput(inputTableRef);
    auto inputTableReaders = GetTableInputStreams(YtJobService_, TableDataService_, inputTableRef, ClusterConnections_);
    YQL_ENSURE(inputTableReaders.size() == tableIndices.size());
    auto queueTableWriter = MakeIntrusive<TFmrRawTableQueueWriter>(
        UnionInputTablesQueue_, tableIndices.empty() ? 0 : tableIndices[0], needsTableIndexMarking);
    for (ui64 i = 0; i < inputTableReaders.size(); ++i) {
        queueTableWriter->SetTableIndex(tableIndices[i]);
        ParseRecords(inputTableReaders[i], queueTableWriter, 1, 1000000, CancelFlag_);
    }
    queueTableWriter->Flush();
    UnionInputTablesQueue_->NotifyInputFinished(curTableNum);
}


void TFmrUserJob::FillQueueFromInputTablesOrdered() {
    ui64 inputTablesNum = InputTables_.Inputs.size();
    bool needsTableIndexMarking = NeedsTableIndexMarking();
    auto state = std::make_shared<TOrderedWriteState>();
    state->NextToEmit = 0;
    for (ui64 curTableNum = 0; curTableNum < inputTablesNum; ++curTableNum) {
        ThreadPool_->SafeAddFunc([this, state, curTableNum, needsTableIndexMarking]() mutable {
            try {
                auto inputTableRef = InputTables_.Inputs[curTableNum];
                auto tableIndices = GetTableIndicesForInput(inputTableRef);
                auto inputTableReaders = GetTableInputStreams(
                    YtJobService_,
                    TableDataService_,
                    inputTableRef,
                    ClusterConnections_
                );
                YQL_ENSURE(inputTableReaders.size() == tableIndices.size());
                TTableWriterSettings writerSettings;
                auto taskWriter = MakeIntrusive<TFmrRawTableQueueWriterWithLock>(
                    UnionInputTablesQueue_,
                    curTableNum,
                    state,
                    needsTableIndexMarking,
                    writerSettings
                );
                for (ui64 i = 0; i < inputTableReaders.size(); ++i) {
                    taskWriter->SetTableIndex(tableIndices[i]);
                    ParseRecords(inputTableReaders[i], taskWriter, 1, 1000000, CancelFlag_);
                }
                taskWriter->Flush();
                with_lock(state->Mutex) {
                    state->NextToEmit++;
                    state->CondVar.BroadCast();
                }
                UnionInputTablesQueue_->NotifyInputFinished(curTableNum);
            } catch (...) {
                TString error = CurrentExceptionMessage();
                with_lock(state->Mutex) {
                    state->NextToEmit++;
                    state->CondVar.BroadCast();
                }
                UnionInputTablesQueue_->SetException(error);
            }
        });
    }
}

void TFmrUserJob::FillQueueFromInputTablesUnordered() {
    ui64 inputTablesNum = InputTables_.Inputs.size();
    bool needsTableIndexMarking = NeedsTableIndexMarking();
    for (ui64 curTableNum = 0; curTableNum < inputTablesNum; ++curTableNum) {
        ThreadPool_->SafeAddFunc([this, curTableNum, needsTableIndexMarking]() mutable {
            try {
                FillQueueFromSingleInputTable(curTableNum, needsTableIndexMarking);
            } catch (...) {
                UnionInputTablesQueue_->SetException(CurrentExceptionMessage());
            }
        });
    }
}

void TFmrUserJob::FillQueueFromReduceInput() {
    std::vector<IBlockIterator::TPtr> blockIterators;
    YQL_ENSURE(ReduceOperationSpec_.Defined());
    auto reduceBy = ReduceOperationSpec_->ReduceBy;
    auto sortBy = ReduceOperationSpec_->SortBy;
    // sortBy is [...reduceBy, ...tiebreaker] (e.g. a join's "_yql_sort"), with a leading
    // _yql_key_hash only for MapReduce's hash-shuffled intermediate tables (SortByHasKeyHashPrefix_,
    // set by the coordinator - see TReduceTaskParams::SortByHasKeyHashPrefix) - a plain Reduce
    // reads its input pre-sorted by the real reduceBy columns, with no hash involved. Either
    // way, task boundaries must only constrain on the reduce-key prefix (hash-if-present +
    // reduceBy) - see the comment on CompareRowToBoundaryPrefix for why comparing the tiebreaker
    // too would silently split a reduce group across tasks whenever a task cut lands between
    // rows that share a reduce key but differ in tiebreaker.
    const size_t numBoundaryKeyColumns = (SortByHasKeyHashPrefix_ ? 1 : 0) + reduceBy.Columns.size();
    for (const auto& inputTableRef : InputTables_.Inputs) {
        if (auto fmrInput = std::get_if<TFmrTableInputRef>(&inputTableRef)) {
            blockIterators.push_back(MakeIntrusive<TTableDataServiceBlockIterator>(
                fmrInput->TableId,
                fmrInput->TableRanges,
                TableDataService_,
                sortBy.Columns,
                sortBy.SortOrders,
                fmrInput->Columns,
                fmrInput->SerializedColumnGroups,
                fmrInput->IsFirstRowInclusive,
                fmrInput->IsLastRowInclusive,
                fmrInput->FirstRowKeys,
                fmrInput->LastRowKeys,
                /*readAheadChunks*/ ui64{4},
                numBoundaryKeyColumns
            ));
        } else {
            ythrow TFmrNonRetryableJobException() << "YtTables unsupported inside Reduce task for now";
        }
    }
    ThreadPool_->SafeAddFunc([this, blockIterators, reduceBy]() mutable {
        try {
            NYT::TRawTableReaderPtr reduceReader = MakeIntrusive<TReduceReader>(blockIterators, reduceBy);
            auto queueTableWriter = MakeIntrusive<TFmrRawTableQueueWriter>(UnionInputTablesQueue_);
            ParseRecords(reduceReader, queueTableWriter, 1, 1000000, CancelFlag_);
            queueTableWriter->Flush();
            for (ui64 i = 0; i < InputTables_.Inputs.size(); ++i) {
                UnionInputTablesQueue_->NotifyInputFinished(i);
            }
        } catch (...) {
            UnionInputTablesQueue_->SetException(CurrentExceptionMessage());
        }
    });
}

void TFmrUserJob::InitializeFmrUserJob(TVector<TString>* mapperBlobs) {
    if (!YtJobService_) {
        YQL_ENSURE(YtJobServiceType_ == "native" || YtJobServiceType_ == "file");
        YtJobService_ = YtJobServiceType_ == "native" ? MakeYtJobSerivce() : MakeFileYtJobService();
    }

    ThreadPool_ = CreateThreadPool(
        Settings_.ThreadPoolSize,
        Settings_.QueueSizeLimit,
        TThreadPool::TParams().SetBlocking(true).SetCatching(true));

    ui64 inputTablesSize = InputTables_.Inputs.size();
    UnionInputTablesQueue_ = MakeIntrusive<TFmrRawTableQueue>(inputTablesSize);
    QueueReader_ = MakeIntrusive<TFmrRawTableQueueReader>(UnionInputTablesQueue_);

    if (DirectTableDataService_) {
        TableDataService_ = DirectTableDataService_;
    } else {
        ITableDataServiceDiscovery::TPtr tableDataServiceDiscovery;
        if (Discovery_) {
            tableDataServiceDiscovery = Discovery_;
        } else if (VanillaInfo_.Defined()) {
            VanillaPeerTracker_ = std::make_unique<TStaticVanillaPeerTracker>(VanillaInfo_->Tracker);
            TStaticTableDataServiceDiscoverySettings settings;
            for (ui32 i = VanillaInfo_->TdsMinIndex; i < VanillaInfo_->Tracker.PeerIps.size(); ++i) {
                settings.Hosts.emplace_back(VanillaInfo_->Tracker.PeerIps[i], VanillaInfo_->TdsPort);
            }
            tableDataServiceDiscovery = MakeStaticTableDataServiceDiscovery(settings);
        } else {
            tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path = TableDataServiceDiscoveryFilePath_});
        }

        TTvmId tableDataServiceTvmId = 0;
        IFmrTvmClient::TPtr tvmClient;
        if (TvmSettings_.Defined()) {
            tvmClient = MakeFmrTvmClient({
                .SourceTvmAlias = TvmSettings_->WorkerTvmAlias,
                .TvmPort = TvmSettings_->TvmPort,
                .TvmSecret = TvmSettings_->TvmSecret
            });
            tableDataServiceTvmId = TvmSettings_->TableDataServiceTvmId;
        }
        TableDataService_ = MakeTableDataServiceClient(tableDataServiceDiscovery, tvmClient, tableDataServiceTvmId);
    }

    for (size_t i = 0; i < OutputTables_.size(); ++i) {
        auto& fmrTable = OutputTables_[i];
        // Only output 0 (the shuffle-bound intermediate table) is captured in-memory for the
        // hash+sort+write pipeline in DoFmrJob(). Outputs 1..K are extra map-direct tables
        // (mapper Variant tag indices 1..K) written straight to the real TDS, same as plain Map.
        if (IsMapReduceMap_ && i == 0) {
            // Capture mapper output in memory; hash+sort+write happens after Do() in DoFmrJob().
            Y_ENSURE(mapperBlobs, "mapperBlobs must be provided when IsMapReduceMap_ is set");
            auto queueTds = MakeIntrusive<TQueueWriteTableDataService>(*mapperBlobs);
            TableDataServiceWriters_.emplace_back(MakeIntrusive<TFmrTableDataServiceWriter>(
                fmrTable.TableId,
                fmrTable.PartId,
                queueTds,
                fmrTable.SerializedColumnGroups,
                Settings_.WriterSettings
            ));
        } else if (!fmrTable.SortingColumns.Columns.empty()) {
            TableDataServiceWriters_.emplace_back(MakeIntrusive<TFmrTableDataServiceSortedWriter>(
                fmrTable.TableId,
                fmrTable.PartId,
                TableDataService_,
                fmrTable.SerializedColumnGroups,
                Settings_.WriterSettings,
                fmrTable.SortingColumns
            ));
        } else {
            TableDataServiceWriters_.emplace_back(MakeIntrusive<TFmrTableDataServiceWriter>(
                fmrTable.TableId,
                fmrTable.PartId,
                TableDataService_,
                fmrTable.SerializedColumnGroups,
                Settings_.WriterSettings
            ));
        }
    }
}

namespace {

// Writes stats.bin so a separate-process job's result can be read back by the launcher (see
// TFmrUserJobLauncher::LaunchJob, which discards DoFmrJob's in-process return value entirely and
// reads this file instead). Not needed when DoFmrJob() runs in the same process as the caller.
void WriteStatsToFileIfNeeded(const TStatistics& stats, const TFmrUserJobOptions& options) {
    if (!options.WriteStatsToFile) {
        return;
    }
    auto serializedProtoStats = StatisticsToProto(stats).SerializeAsStringOrThrow();
    TFileOutput statsOutput("stats.bin");
    statsOutput.Write(serializedProtoStats.data(), serializedProtoStats.size());
    statsOutput.Flush();
}

} // namespace

TStatistics TFmrUserJob::GetStatistics(const TFmrUserJobOptions& options) {
    YQL_ENSURE(OutputTables_.size() == TableDataServiceWriters_.size());
    TStatistics mapJobStats;
    for (ui64 i = 0; i < OutputTables_.size(); ++i) {
        auto stats = TableDataServiceWriters_[i]->GetStats();
        mapJobStats.OutputTables.emplace(OutputTables_[i], stats);
    }
    WriteStatsToFileIfNeeded(mapJobStats, options);
    return mapJobStats;
}

TStatistics TFmrUserJob::DoFmrJob(const TFmrUserJobOptions& options) {
    TVector<TString> mapperBlobs;
    InitializeFmrUserJob(IsMapReduceMap_ ? &mapperBlobs : nullptr);
    if (FmrJobType_ == EFmrJobType::OrderedMap) {
        FillQueueFromInputTablesOrdered();
    } else if (FmrJobType_ == EFmrJobType::Map) {
        FillQueueFromInputTablesUnordered();
    } else if (FmrJobType_ == EFmrJobType::Reduce) {
        FillQueueFromReduceInput();
    }
    TYqlUserJobBase::Do();

    if (IsMapReduceMap_) {
        Y_ENSURE(!OutputTables_.empty(), "MapReduceMap must have at least one output table");
        // OutputTables_[0].SortingColumns is [_yql_key_hash, ...reduceBy, ...extra tiebreaker
        // columns] (e.g. "_yql_sort" for joins, needed so the reducer's compiled lambda sees rows
        // within a group in the right order). Only the reduceBy portion feeds the hash - including
        // the tiebreaker columns in the hash would make it depend on data that varies within a
        // single reduce group, splitting rows that must stay together.
        const auto& sortColumns = OutputTables_[0].SortingColumns;
        Y_ENSURE(!sortColumns.Columns.empty() && sortColumns.Columns[0] == TString(YqlKeyHashColumn),
                 "_yql_key_hash must be the first sort column in MapReduceMap");
        YQL_ENSURE(ReduceOperationSpec_.Defined());
        const size_t numReduceKeyColumns = ReduceOperationSpec_->ReduceBy.Columns.size();
        Y_ENSURE(numReduceKeyColumns + 1 <= sortColumns.Columns.size(),
                 "reduceBy columns must fit within the sort columns after _yql_key_hash");

        std::vector<TString> reduceKeyColumns(sortColumns.Columns.begin() + 1, sortColumns.Columns.begin() + 1 + numReduceKeyColumns);
        std::vector<ESortOrder> reduceKeySortOrders(sortColumns.SortOrders.begin() + 1, sortColumns.SortOrders.begin() + 1 + numReduceKeyColumns);

        // Collect all hashed blocks from mapper blobs; blobs are moved out one by one
        // so the original buffer is freed before the hashed block is accumulated.
        std::vector<TIndexedBlock> allBlocks;
        {
            auto inner = MakeIntrusive<TQueueBlobBlockIterator>(std::move(mapperBlobs), reduceKeyColumns, reduceKeySortOrders);
            IBlockIterator::TPtr hashIterator = MakeIntrusive<TKeyHashAddingBlockIterator>(
                std::move(inner), sortColumns.Columns, sortColumns.SortOrders, numReduceKeyColumns
            );
            TIndexedBlock block;
            while (hashIterator->NextBlock(block)) {
                allBlocks.push_back(std::move(block));
            }
        }

        // Sort globally and write rows in order to the final sorted TDS partition.
        TSortHelper sortHelper(allBlocks, sortColumns.SortOrders);
        auto ordering = sortHelper.GetSortedRowOrdering();

        auto sortedWriter = MakeIntrusive<TFmrTableDataServiceSortedWriter>(
            OutputTables_[0].TableId, OutputTables_[0].PartId,
            TableDataService_, OutputTables_[0].SerializedColumnGroups,
            Settings_.WriterSettings, sortColumns
        );
        for (const auto& pos : ordering) {
            TStringBuf rowBytes = allBlocks[pos.BlockIndex].GetRowBytes(pos.RowIndex);
            sortedWriter->Write(rowBytes.data(), rowBytes.size());
            sortedWriter->NotifyRowEnd();
        }
        sortedWriter->Flush();
        auto sortedWriterStats = sortedWriter->GetStats();
        TStatistics result({{OutputTables_[0], sortedWriterStats}});

        // Direct (map-bypass) outputs at indices 1..K were already written to the real TDS via
        // their normal writers (see InitializeFmrUserJob) and flushed generically by
        // TYqlUserJobBase::Do() above.
        YQL_ENSURE(OutputTables_.size() == TableDataServiceWriters_.size());
        for (size_t i = 1; i < OutputTables_.size(); ++i) {
            result.OutputTables.emplace(OutputTables_[i], TableDataServiceWriters_[i]->GetStats());
        }
        WriteStatsToFileIfNeeded(result, options);
        return result;
    }

    return GetStatistics(options);
}

} // namespace NYql::NFmr
