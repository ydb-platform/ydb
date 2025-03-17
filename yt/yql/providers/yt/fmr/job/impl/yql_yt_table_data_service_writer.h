#pragma once

#include <util/generic/buffer.h>
#include <util/stream/output.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/table_data_service.h>
#include <yt/yql/providers/yt/fmr/utils/table_data_service_key.h>

namespace NYql::NFmr {

struct TFmrTableDataServiceWriterSettings {
    ui64 ChunkSize = 1024 * 1024; // 1Mb
};

class TFmrTableDataServiceWriter: public NYT::TRawTableWriter {
public:
    TFmrTableDataServiceWriter(
        const TString& tableId,
        const TString& partId,
        ITableDataService::TPtr tableDataService,
        const TFmrTableDataServiceWriterSettings& settings = TFmrTableDataServiceWriterSettings()
    );

    TTableStats GetStats();

    void NotifyRowEnd() override;

protected:
    void DoWrite(const void* buf, size_t len) override;

    void DoFlush() override;

private:
    const TString TableId_;
    const TString PartId_;
    ITableDataService::TPtr TableDataService_;
    ui64 DataWeight_ = 0;
    ui64 Rows_ = 0;

    TBuffer TableContent_;
    const ui64 ChunkSize_; // size at which we push to table data service
    ui64 ChunkCount_ = 0;
};

} // namespace NYql::NFmr
