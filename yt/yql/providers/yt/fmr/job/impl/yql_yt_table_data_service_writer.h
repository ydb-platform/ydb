#pragma once

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_base_writer.h>

namespace NYql::NFmr {

class TFmrTableDataServiceWriter: public TFmrTableDataServiceBaseWriter {
public:
    using TPtr = TIntrusivePtr<TFmrTableDataServiceWriter>;

    TFmrTableDataServiceWriter(
        const TString& tableId,
        const TString& partId,
        ITableDataService::TPtr tableDataService,
        const TString& columnGroupSpec = TString(),
        const TFmrWriterSettings& settings = TFmrWriterSettings()
    );

protected:
    void PutRows() override;
};

} // namespace NYql::NFmr

