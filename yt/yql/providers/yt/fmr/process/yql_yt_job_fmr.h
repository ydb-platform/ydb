#pragma once

#include <util/thread/pool.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_raw_table_queue_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_raw_table_queue_writer.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/table_data_service/client/impl/yql_yt_table_data_service_client_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/file/yql_yt_file_service_discovery.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>
#include <yt/yql/providers/yt/job/yql_job_user_base.h>

namespace NYql {

using namespace NYql::NFmr;

class TFmrUserJob: public TYqlUserJobBase {
public:
    TFmrUserJob()
        : TYqlUserJobBase()
    {
    }

    virtual ~TFmrUserJob() {
        CancelFlag_->store(true);
        ThreadPool_->Stop();
    }

    void SetTaskInputTables(const TTaskTableInputRef& taskInputTables) {
        InputTables_ = taskInputTables;
    }

    void SetTaskFmrOutputTables(const std::vector<TFmrTableOutputRef>& outputTables) {
        OutputTables_ = outputTables;
    }

    void SetClusterConnections(const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections) {
        ClusterConnections_ = clusterConnections;
    }

    void SetYtJobService(bool useFileGateway) {
        UseFileGateway_ = useFileGateway;
    }

    void SetTableDataService(const TString& tableDataServiceDiscoveryFilePath) {
        TableDataServiceDiscoveryFilePath_ = tableDataServiceDiscoveryFilePath;
    }

    void Save(IOutputStream& s) const override;
    void Load(IInputStream& s) override;

    void DoFmrJob();

protected:
    TIntrusivePtr<TMkqlWriterImpl> MakeMkqlJobWriter() override;

    TIntrusivePtr<NYT::IReaderImplBase> MakeMkqlJobReader() override;

    TString GetJobFactoryPrefix() const override;

private:
    void FillQueueFromInputTables();

    void InitializeFmrUserJob();

    // Serializable part (don't forget to add new members to Save/Load)
    TTaskTableInputRef InputTables_;
    std::vector<TFmrTableOutputRef> OutputTables_;
    std::unordered_map<TFmrTableId, TClusterConnection> ClusterConnections_;
    bool UseFileGateway_;
    TString TableDataServiceDiscoveryFilePath_;
    // End of serializable part

    TFmrRawTableQueue::TPtr UnionInputTablesQueue_; // Queue which represents union of all input streams
    TFmrRawTableQueueReader::TPtr QueueReader_;
    TVector<TFmrTableDataServiceWriter::TPtr> TableDataServiceWriters_;
    ITableDataService::TPtr TableDataService_;
    IYtJobService::TPtr YtJobService_;
    THolder<IThreadPool> ThreadPool_ = CreateThreadPool(3);
    std::shared_ptr<std::atomic<bool>> CancelFlag_ = std::make_shared<std::atomic<bool>>(false);
    // TODO - pass settings for various classes here.
};

} // namespace NYql
