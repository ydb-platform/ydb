#pragma once

#include <util/generic/buffer.h>
#include <util/stream/output.h>
#include <util/system/condvar.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>

namespace NYql::NFmr {

struct TFmrWriterSettings {
    ui64 ChunkSize = 1024 * 1024;
    ui64 MaxInflightChunks = 1;
    ui64 MaxRowWeight = 1024 * 1024 * 16;
};

class TFmrTableDataServiceWriter: public NYT::TRawTableWriter {
public:
    using TPtr = TIntrusivePtr<TFmrTableDataServiceWriter>;

    TFmrTableDataServiceWriter(
        const TString& tableId,
        const TString& partId,
        ITableDataService::TPtr tableDataService,
        const TFmrWriterSettings& settings = TFmrWriterSettings()
    );

    TTableChunkStats GetStats();

    void NotifyRowEnd() override;

protected:
    void DoWrite(const void* buf, size_t len) override;

    void DoFlush() override;

private:
    void PutRows();

private:
    const TString TableId_;
    const TString PartId_;
    ITableDataService::TPtr TableDataService_;
    ui64 DataWeight_ = 0;
    ui64 CurrentChunkRows_ = 0;

    TBuffer TableContent_;
    const ui64 ChunkSize_; // size at which we push to table data service
    const ui64 MaxInflightChunks_;
    const ui64 MaxRowWeight_;

    ui64 ChunkCount_ = 0;
    std::vector<TChunkStats> PartIdChunkStats_;

    struct TFmrWriterState {
        ui64 CurInflightChunks = 0;
        TMutex Mutex = TMutex();
        TCondVar CondVar;
        std::exception_ptr Exception;
    };
    std::shared_ptr<TFmrWriterState> State_ = std::make_shared<TFmrWriterState>();
};

} // namespace NYql::NFmr
