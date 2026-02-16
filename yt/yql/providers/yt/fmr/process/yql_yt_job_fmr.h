#pragma once

#include <util/thread/pool.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_raw_table_queue_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_raw_table_queue_writer.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_base_writer.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/table_data_service/client/impl/yql_yt_table_data_service_client_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/file/yql_yt_file_service_discovery.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>
#include <yt/yql/providers/yt/job/yql_job_user_base.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_queue_writer_with_lock.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

struct TFmrUserJobOptions {
    bool WriteStatsToFile = false;
};


class TFmrUserJob: public TYqlUserJobBase {
public:
    TFmrUserJob();

    virtual ~TFmrUserJob() {
        CancelFlag_->store(true);
        if (ThreadPool_) {
            ThreadPool_->Stop();
        }
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

    void SetYtJobService(IYtJobService::TPtr jobService) {
        YtJobService_ = jobService;
    } // not for serialization, set when FmrJob is launched in the same process.

    void SetYtJobServiceType(const TString& ytJobServiceType) {
        YtJobServiceType_ = ytJobServiceType;
    }

    void SetTableDataService(const TString& tableDataServiceDiscoveryFilePath) {
        TableDataServiceDiscoveryFilePath_ = tableDataServiceDiscoveryFilePath;
    }

    void SetIsOrdered(bool isOrdered) {
        IsOrdered_ = isOrdered;
    }

    void SetSettings(const TFmrUserJobSettings& settings) {
        Settings_ = settings;
    }

    void Save(IOutputStream& s) const override;
    void Load(IInputStream& s) override;

    TStatistics DoFmrJob(const TFmrUserJobOptions& options);

protected:
    TIntrusivePtr<TMkqlWriterImpl> MakeMkqlJobWriter() override;

    TIntrusivePtr<NYT::IReaderImplBase> MakeMkqlJobReader() override;

private:
    void FillQueueFromSingleInputTable(ui64 tableIndex);
    void FillQueueFromInputTablesUnordered();
    void FillQueueFromInputTablesOrdered();

    void InitializeFmrUserJob();

    TStatistics GetStatistics(const TFmrUserJobOptions& options);

    // Serializable part (don't forget to add new members to Save/Load)
    TTaskTableInputRef InputTables_;
    std::vector<TFmrTableOutputRef> OutputTables_;
    std::unordered_map<TFmrTableId, TClusterConnection> ClusterConnections_;
    TString TableDataServiceDiscoveryFilePath_;
    TString YtJobServiceType_; // file or native
    bool IsOrdered_ = false;
    TFmrUserJobSettings Settings_ = TFmrUserJobSettings();
    // End of serializable part

    TFmrRawTableQueue::TPtr UnionInputTablesQueue_; // Queue which represents union of all input streams
    TFmrRawTableQueueReader::TPtr QueueReader_;
    TVector<TFmrTableDataServiceBaseWriter::TPtr> TableDataServiceWriters_;
    ITableDataService::TPtr TableDataService_;
    IYtJobService::TPtr YtJobService_;
    THolder<IThreadPool> ThreadPool_;
    std::shared_ptr<std::atomic<bool>> CancelFlag_ = std::make_shared<std::atomic<bool>>(false);
    // TODO - pass settings for various classes here.
};

} // namespace NYql::NFmr
