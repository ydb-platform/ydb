#include "yql_yt_job_fmr.h"
#include <util/thread/pool.h>
#include <yt/yql/providers/yt/common/yql_configuration.h>
#include <yt/yql/providers/yt/fmr/request_options/proto_helpers/yql_yt_request_proto_helpers.h>
#include <yt/yql/providers/yt/fmr/tvm/impl/yql_yt_fmr_tvm_impl.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parse_records.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_input_streams.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_sorted_writer.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

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
        IsOrdered_,
        Settings_,
        TvmSettings_
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
        IsOrdered_,
        Settings_,
        TvmSettings_
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

void TFmrUserJob::InitializeFmrUserJob() {
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

    auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path = TableDataServiceDiscoveryFilePath_});
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

    for (auto& fmrTable: OutputTables_) {
        if (!fmrTable.SortingColumns.Columns.empty()) {
            TableDataServiceWriters_.emplace_back(MakeIntrusive<TFmrTableDataServiceSortedWriter>(
                fmrTable.TableId,
                fmrTable.PartId,
                TableDataService_,
                fmrTable.SerializedColumnGroups,
                TFmrWriterSettings(),
                fmrTable.SortingColumns
            )); // TODO - settings
        } else {
            TableDataServiceWriters_.emplace_back(MakeIntrusive<TFmrTableDataServiceWriter>(
                fmrTable.TableId,
                fmrTable.PartId,
                TableDataService_,
                fmrTable.SerializedColumnGroups
            )); // TODO - settings
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
    InitializeFmrUserJob();
    if (IsOrdered_) {
        FillQueueFromInputTablesOrdered();
    } else {
        FillQueueFromInputTablesUnordered();
    }
    TYqlUserJobBase::Do();
    return GetStatistics(options);
}

} // namespace NYql::NFmr
