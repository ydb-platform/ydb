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
        IsMapReduceMap_
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
        IsMapReduceMap_
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

void TFmrUserJob::FillQueueFromSingleInputTable(ui64 curTableNum) {
    auto inputTableRef = InputTables_.Inputs[curTableNum];
    auto queueTableWriter = MakeIntrusive<TFmrRawTableQueueWriter>(UnionInputTablesQueue_);
    auto inputTableReaders = GetTableInputStreams(YtJobService_, TableDataService_, inputTableRef, ClusterConnections_);
    for (auto tableReader: inputTableReaders) {
        ParseRecords(tableReader, queueTableWriter, 1, 1000000, CancelFlag_);
    }
    queueTableWriter->Flush();
    UnionInputTablesQueue_->NotifyInputFinished(curTableNum);
}


void TFmrUserJob::FillQueueFromInputTablesOrdered() {
    ui64 inputTablesNum = InputTables_.Inputs.size();
    auto state = std::make_shared<TOrderedWriteState>();
    state->NextToEmit = 0;
    for (ui64 curTableNum = 0; curTableNum < inputTablesNum; ++curTableNum) {
        ThreadPool_->SafeAddFunc([this, state, curTableNum]() mutable {
            try {
                auto inputTableRef = InputTables_.Inputs[curTableNum];
                auto inputTableReaders = GetTableInputStreams(
                    YtJobService_,
                    TableDataService_,
                    inputTableRef,
                    ClusterConnections_
                );
                TTableWriterSettings writerSettings;
                auto taskWriter = MakeIntrusive<TFmrRawTableQueueWriterWithLock>(
                    UnionInputTablesQueue_,
                    curTableNum,
                    state,
                    writerSettings
                );
                for (auto tableReader : inputTableReaders) {
                    ParseRecords(tableReader, taskWriter, 1, 1000000, CancelFlag_);
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
    for (ui64 curTableNum = 0; curTableNum < inputTablesNum; ++curTableNum) {
        ThreadPool_->SafeAddFunc([this, curTableNum]() mutable {
            try {
                FillQueueFromSingleInputTable(curTableNum);
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
                fmrInput->LastRowKeys
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

    for (auto& fmrTable: OutputTables_) {
        if (IsMapReduceMap_) {
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

TStatistics TFmrUserJob::GetStatistics(const TFmrUserJobOptions& options) {
    YQL_ENSURE(OutputTables_.size() == TableDataServiceWriters_.size());
    TStatistics mapJobStats;
    for (ui64 i = 0; i < OutputTables_.size(); ++i) {
        auto stats = TableDataServiceWriters_[i]->GetStats();
        mapJobStats.OutputTables.emplace(OutputTables_[i], stats);
    }
    auto serializedProtoMapJobStats = StatisticsToProto(mapJobStats).SerializeAsStringOrThrow();

    if (options.WriteStatsToFile) {
        // don't serialize stats in case DoFmrJob() is called in the same process.
        TFileOutput statsOutput("stats.bin");
        statsOutput.Write(serializedProtoMapJobStats.data(), serializedProtoMapJobStats.size());
        statsOutput.Flush();
    }
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
        Y_ENSURE(OutputTables_.size() == 1, "MapReduceMap must have exactly one output table");
        const auto& sortColumns = OutputTables_[0].SortingColumns;
        Y_ENSURE(!sortColumns.Columns.empty() && sortColumns.Columns[0] == TString(YqlKeyHashColumn),
                 "_yql_key_hash must be the first sort column in MapReduceMap");

        std::vector<TString> reduceKeyColumns(sortColumns.Columns.begin() + 1, sortColumns.Columns.end());
        std::vector<ESortOrder> reduceKeySortOrders(sortColumns.SortOrders.begin() + 1, sortColumns.SortOrders.end());

        // Collect all hashed blocks from mapper blobs; blobs are moved out one by one
        // so the original buffer is freed before the hashed block is accumulated.
        std::vector<TIndexedBlock> allBlocks;
        {
            auto inner = MakeIntrusive<TQueueBlobBlockIterator>(std::move(mapperBlobs), reduceKeyColumns, reduceKeySortOrders);
            IBlockIterator::TPtr hashIterator = MakeIntrusive<TKeyHashAddingBlockIterator>(
                std::move(inner), sortColumns.Columns, sortColumns.SortOrders
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
        return TStatistics({{OutputTables_[0], sortedWriter->GetStats()}});
    }

    return GetStatistics(options);
}

} // namespace NYql::NFmr
