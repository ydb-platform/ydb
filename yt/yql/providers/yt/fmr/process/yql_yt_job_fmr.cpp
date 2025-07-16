#include "yql_yt_job_fmr.h"
#include <util/thread/pool.h>
#include <yt/yql/providers/yt/common/yql_configuration.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parse_records.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_input_streams.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

void TFmrUserJob::Save(IOutputStream& s) const {
    TYqlUserJobBase::Save(s);
    ::SaveMany(&s,
        InputTables_,
        OutputTables_,
        ClusterConnections_,
        UseFileGateway_,
        TableDataServiceDiscoveryFilePath_
    );
}

void TFmrUserJob::Load(IInputStream& s) {
    TYqlUserJobBase::Load(s);
    ::LoadMany(&s,
        InputTables_,
        OutputTables_,
        ClusterConnections_,
        UseFileGateway_,
        TableDataServiceDiscoveryFilePath_
    );
}

TString TFmrUserJob::GetJobFactoryPrefix() const {
    return "Fmr";
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

void TFmrUserJob::FillQueueFromInputTables() {
    ui64 inputTablesNum = InputTables_.Inputs.size();
    for (ui64 curTableNum = 0; curTableNum < inputTablesNum; ++curTableNum) {
        ThreadPool_->SafeAddFunc([&, curTableNum] () mutable {
            try {
                auto inputTableRef = InputTables_.Inputs[curTableNum];
                auto queueTableWriter = MakeIntrusive<TFmrRawTableQueueWriter>(UnionInputTablesQueue_);
                auto inputTableReaders = GetTableInputStreams(YtJobService_, TableDataService_, inputTableRef, ClusterConnections_);
                for (auto tableReader: inputTableReaders) {
                    ParseRecords(tableReader, queueTableWriter, 1, 1000000, CancelFlag_); // TODO - settings
                    UnionInputTablesQueue_->NotifyInputFinished(curTableNum);
                }
                queueTableWriter->Flush();
            } catch (...) {
                UnionInputTablesQueue_->SetException(CurrentExceptionMessage());
            }
        });
    }
}

void TFmrUserJob::InitializeFmrUserJob() {
    ui64 inputTablesSize = InputTables_.Inputs.size();
    UnionInputTablesQueue_ = MakeIntrusive<TFmrRawTableQueue>(inputTablesSize);
    QueueReader_ = MakeIntrusive<TFmrRawTableQueueReader>(UnionInputTablesQueue_);

    YtJobService_ = UseFileGateway_ ? MakeFileYtJobSerivce() : MakeYtJobSerivce();

    auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path = TableDataServiceDiscoveryFilePath_});
    TableDataService_ = MakeTableDataServiceClient(tableDataServiceDiscovery);

    for (auto& fmrTable: OutputTables_) {
        TableDataServiceWriters_.emplace_back(MakeIntrusive<TFmrTableDataServiceWriter>(fmrTable.TableId, fmrTable.PartId, TableDataService_)); // TODO - settings
    }
}

void TFmrUserJob::DoFmrJob() {
    InitializeFmrUserJob();
    FillQueueFromInputTables();
    TYqlUserJobBase::Do();
}

} // namespace NYql
