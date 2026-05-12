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
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/interface/yql_yt_service_discovery.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>
#include <yt/yql/providers/yt/job/yql_job_user_base.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_queue_writer_with_lock.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/vanilla/peer_tracker/yql_yt_vanilla_peer_tracker.h>

namespace NYql::NFmr {

// Parameters describing a vanilla YT operation; carried through FMR job state
// so that separate-process job binaries can resolve peers (e.g. TDS) via ListJobs.
struct TVanillaInfo {
    TStaticVanillaPeerTrackerSettings Tracker;
    ui16 TdsPort = 8002;

    void Save(IOutputStream* s) const {
        ::Save(s, Tracker);
        ::Save(s, TdsPort);
    }

    void Load(IInputStream* s) {
        ::Load(s, Tracker);
        ::Load(s, TdsPort);
    }
};

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

    // Set a live discovery object instead of a file path.
    // Not serialized — only valid for in-process job execution.
    void SetTableDataServiceDiscovery(ITableDataServiceDiscovery::TPtr discovery) {
        Discovery_ = std::move(discovery);
    }

    // Set vanilla TDS info for separate-process jobs: the binary resolves TDS
    // nodes via ListJobs instead of reading a local file.
    void SetVanillaInfo(TVanillaInfo info) {
        VanillaInfo_ = std::move(info);
    }

    void SetFmrJobType(EFmrJobType jobType) {
        FmrJobType_ = jobType;
    }

    void SetSettings(const TFmrUserJobSettings& settings) {
        Settings_ = settings;
    }

    void SetTvmSettings(const TMaybe<TFmrTvmJobSettings>& tvmSettings) {
        TvmSettings_ = tvmSettings;
    }

    void SetReduceOperationSpec(const TReduceOperationSpec& reduceOperationSpec) {
        ReduceOperationSpec_ = reduceOperationSpec;
    }

    void Save(IOutputStream& s) const override;
    void Load(IInputStream& s) override;

    TStatistics DoFmrJob(const TFmrUserJobOptions& options);

protected:
    TIntrusivePtr<TMkqlWriterImpl> MakeMkqlJobWriter() override;

    TIntrusivePtr<NYT::IReaderImplBase> MakeMkqlJobReader() override;

    void ChangeMkqlIOSpecIfNeeded() override;

private:
    void FillQueueFromSingleInputTable(ui64 tableIndex);
    void FillQueueFromInputTablesUnordered();
    void FillQueueFromInputTablesOrdered();
    void FillQueueFromReduceInput();

    void InitializeFmrUserJob();

    TStatistics GetStatistics(const TFmrUserJobOptions& options);

    // Serializable part (don't forget to add new members to Save/Load)
    TTaskTableInputRef InputTables_;
    std::vector<TFmrTableOutputRef> OutputTables_;
    std::unordered_map<TFmrTableId, TClusterConnection> ClusterConnections_;
    TString TableDataServiceDiscoveryFilePath_;
    TString YtJobServiceType_; // file or native
    EFmrJobType FmrJobType_ = EFmrJobType::Map;
    TFmrUserJobSettings Settings_ = TFmrUserJobSettings();
    TMaybe<TFmrTvmJobSettings> TvmSettings_ = Nothing();
    TMaybe<TVanillaInfo> VanillaInfo_ = Nothing();
    TMaybe<TReduceOperationSpec> ReduceOperationSpec_;
    // End of serializable part

    // Non-serialized: set only for in-process execution via SetTableDataServiceDiscovery.
    ITableDataServiceDiscovery::TPtr Discovery_;

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
