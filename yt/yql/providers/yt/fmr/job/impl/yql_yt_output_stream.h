#pragma once

#include <util/generic/buffer.h>
#include <util/stream/output.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/table_data_service.h>
#include <yt/yql/providers/yt/fmr/utils/table_data_service_key.h>

namespace NYql::NFmr {

struct TFmrOutputStreamSettings {
    const ui64 MaxChunkSize  = 1024 * 1024; // 1Mb
};

class TFmrOutputStream: public IOutputStream {
public:
    TFmrOutputStream(
        TString tableId,
        TString partId,
        ITableDataService::TPtr tableDataService,
        TFmrOutputStreamSettings = TFmrOutputStreamSettings()
    );

    TTableStats GetStats();

protected:
    void DoWrite(const void* buf, size_t len) override;

    void DoFlush() override;

private:
    TString TableId_;
    TString PartId_;
    ITableDataService::TPtr TableDataService_;
    const size_t MaxChunkSize_;
    TBuffer ChunkBuf_;
    ui32 ChunkCount_ = 0;
    size_t CurrentChunkSize_ = 0;
    ui64 DataWeight_ = 0;
};

} // namespace NYql::NFmr
