#include <library/cpp/threading/future/core/future.h>

#include <util/stream/file.h>

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parse_records.h>
#include <yt/yql/providers/yt/fmr/yt_service/impl/yql_yt_yt_service_impl.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

class TFmrJob: public IFmrJob {
public:

    TFmrJob(ITableDataService::TPtr tableDataService, IYtService::TPtr ytService, std::shared_ptr<std::atomic<bool>> cancelFlag, const TMaybe<TFmrJobSettings>& settings)
        : TableDataService_(tableDataService), YtService_(ytService), CancelFlag_(cancelFlag), Settings_(settings)
    {
    }

    virtual std::variant<TError, TStatistics> Download(const TDownloadTaskParams& params, const std::unordered_map<TString, TClusterConnection>& clusterConnections) override {
        try {
            const auto ytTable = params.Input;
            const auto cluster = params.Input.Cluster;
            const auto path = params.Input.Path;
            const auto output = params.Output;
            const auto tableId = output.TableId;
            const auto partId = output.PartId;

            YQL_CLOG(DEBUG, FastMapReduce) << "Downloading " << cluster << '.' << path;
            YQL_ENSURE(clusterConnections.size() == 1);
            auto ytTableReader = YtService_->MakeReader(ytTable, clusterConnections.begin()->second);  // TODO - pass YtReader settings from Gateway
            auto tableDataServiceWriter = TFmrTableDataServiceWriter(tableId, partId, TableDataService_, GetFmrTableDataServiceWriterSettings());

            ParseRecords(*ytTableReader, tableDataServiceWriter, GetParseRecordSettings().BlockCount, GetParseRecordSettings().BlockSize);
            tableDataServiceWriter.Flush();

            TTableStats stats = tableDataServiceWriter.GetStats();
            auto statistics = TStatistics({{output, stats}});
            return statistics;
        } catch (...) {
            return TError(CurrentExceptionMessage());
        }
    }

    virtual std::variant<TError, TStatistics> Upload(const TUploadTaskParams& params, const std::unordered_map<TString, TClusterConnection>& clusterConnections) override {
        try {
            const auto ytTable = params.Output;
            const auto cluster = params.Output.Cluster;
            const auto path = params.Output.Path;
            const auto tableId = params.Input.TableId;
            const auto tableRanges = params.Input.TableRanges;

            YQL_CLOG(DEBUG, FastMapReduce) << "Uploading " << cluster << '.' << path;

            auto tableDataServiceReader = TFmrTableDataServiceReader(tableId, tableRanges, TableDataService_, GetFmrTableDataServiceReaderSettings());
            YQL_ENSURE(clusterConnections.size() == 1);
            auto ytTableWriter = YtService_->MakeWriter(ytTable, clusterConnections.begin()->second);
            ParseRecords(tableDataServiceReader, *ytTableWriter, GetParseRecordSettings().BlockCount, GetParseRecordSettings().BlockSize);
            ytTableWriter->Flush();

            return TStatistics();
        } catch (...) {
            return TError(CurrentExceptionMessage());
        }
    }

    virtual std::variant<TError, TStatistics> Merge(const TMergeTaskParams& params, const std::unordered_map<TString, TClusterConnection>& clusterConnections) override {
        // расширить таск парамс. добавить туда мету
        try {
            const auto inputs = params.Input;
            const auto output = params.Output;

            YQL_CLOG(DEBUG, FastMapReduce) << "Merging " << inputs.size() << " inputs";

            auto tableDataServiceWriter = TFmrTableDataServiceWriter(output.TableId, output.PartId, TableDataService_, GetFmrTableDataServiceWriterSettings());
            for (const auto& inputTableRef : inputs) {
                if (CancelFlag_->load()) {
                    return TError("Canceled");
                }
                auto inputTableReader = GetTableInputStream(inputTableRef, clusterConnections);
                ParseRecords(*inputTableReader, tableDataServiceWriter, GetParseRecordSettings().BlockCount, GetParseRecordSettings().BlockSize);
            }
            tableDataServiceWriter.Flush();
            return TStatistics({{output, tableDataServiceWriter.GetStats()}});
        } catch (...) {
            return TError(CurrentExceptionMessage());
        }
        return TError{"not implemented yet"};
    }

private:
    NYT::TRawTableReaderPtr GetTableInputStream(const TTaskTableRef& tableRef, const std::unordered_map<TString, TClusterConnection>& clusterConnections) const {
        auto ytTable = std::get_if<TYtTableRef>(&tableRef);
        auto fmrTable = std::get_if<TFmrTableInputRef>(&tableRef);
        if (ytTable) {
            TString tableId = ytTable->Cluster + "." + ytTable->Path;
            auto clusterConnection = clusterConnections.at(tableId);
            return YtService_->MakeReader(*ytTable, clusterConnection); // TODO - pass YtReader settings from Gateway
        } else if (fmrTable) {
            return MakeIntrusive<TFmrTableDataServiceReader>(fmrTable->TableId, fmrTable->TableRanges, TableDataService_, GetFmrTableDataServiceReaderSettings());
        } else {
            ythrow yexception() << "Unsupported table type";
        }
    }

    TParseRecordSettings GetParseRecordSettings() const {
        return Settings_ ? Settings_->ParseRecordSettings : TParseRecordSettings();
    }

    TFmrTableDataServiceReaderSettings GetFmrTableDataServiceReaderSettings() const {
        return Settings_ ? Settings_->FmrTableDataServiceReaderSettings : TFmrTableDataServiceReaderSettings();
    }

    TFmrTableDataServiceWriterSettings GetFmrTableDataServiceWriterSettings() const {
        return Settings_ ? Settings_->FmrTableDataServiceWriterSettings : TFmrTableDataServiceWriterSettings();
    }

private:
    ITableDataService::TPtr TableDataService_;
    IYtService::TPtr YtService_;
    std::shared_ptr<std::atomic<bool>> CancelFlag_;
    TMaybe<TFmrJobSettings> Settings_;
};

IFmrJob::TPtr MakeFmrJob(
    ITableDataService::TPtr tableDataService,
    IYtService::TPtr ytService,
    std::shared_ptr<std::atomic<bool>> cancelFlag,
    const TFmrJobSettings& settings
) {
    return MakeIntrusive<TFmrJob>(tableDataService, ytService, cancelFlag, settings);
}

TJobResult RunJob(
    TTask::TPtr task,
    ITableDataService::TPtr tableDataService,
    IYtService::TPtr ytService,
    std::shared_ptr<std::atomic<bool>> cancelFlag,
    const TMaybe<TFmrJobSettings>& settings
) {
    TFmrJobSettings jobSettings = settings ? *settings : GetJobSettingsFromTask(task);
    IFmrJob::TPtr job = MakeFmrJob(tableDataService, ytService, cancelFlag, jobSettings);

    auto processTask = [job, task] (auto&& taskParams) {
        using T = std::decay_t<decltype(taskParams)>;

        if constexpr (std::is_same_v<T, TUploadTaskParams>) {
            return job->Upload(taskParams, task->ClusterConnections);
        } else if constexpr (std::is_same_v<T, TDownloadTaskParams>) {
            return job->Download(taskParams, task->ClusterConnections);
        } else if constexpr (std::is_same_v<T, TMergeTaskParams>) {
            return job->Merge(taskParams, task->ClusterConnections);
        } else {
            throw std::runtime_error{"Unsupported task type"};
        }
    };

    std::variant<TError, TStatistics> taskResult = std::visit(processTask, task->TaskParams);
    auto err = std::get_if<TError>(&taskResult);
    if (err) {
        ythrow yexception() << "Job failed with error: " << err->ErrorMessage;
    }

    auto statistics = std::get_if<TStatistics>(&taskResult);
    return {ETaskStatus::Completed, *statistics};
};

TFmrJobSettings GetJobSettingsFromTask(TTask::TPtr task) {
    if (!task->JobSettings) {
        return TFmrJobSettings();
    }
    auto jobSettings = *task->JobSettings;
    YQL_ENSURE(jobSettings.IsMap());
    TFmrJobSettings resultSettings{};
    if (jobSettings.HasKey("parse_record_settings")) {
        auto& parseRecordSettings = jobSettings["parse_record_settings"];
        if (parseRecordSettings.HasKey("block_count")) {
            resultSettings.ParseRecordSettings.BlockCount = parseRecordSettings["block_count"].AsInt64();
        }
        if (parseRecordSettings.HasKey("block_size")) {
            resultSettings.ParseRecordSettings.BlockSize = parseRecordSettings["block_size"].AsInt64();
            // TODO - support different formats (B, MB, ...)
        }
    }
    if (jobSettings.HasKey("fmr_reader_settings")) {
        auto& fmrReaderSettings = jobSettings["fmr_reader_settings"];
        if (fmrReaderSettings.HasKey("read_ahead_chunks")) {
            resultSettings.FmrTableDataServiceReaderSettings.ReadAheadChunks = fmrReaderSettings["read_ahead_chunks"].AsInt64();
        }
    }
    if (jobSettings.HasKey("fmr_writer_settings")) {
        auto& fmrWriterSettings = jobSettings["fmr_writer_settings"];
        if (fmrWriterSettings.HasKey("chunk_size")) {
            resultSettings.FmrTableDataServiceWriterSettings.ChunkSize = fmrWriterSettings["chunk_size"].AsInt64();
        }
    }
    return resultSettings;
}

} // namespace NYql
