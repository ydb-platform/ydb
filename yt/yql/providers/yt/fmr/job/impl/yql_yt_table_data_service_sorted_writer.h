#pragma once

#include <util/generic/buffer.h>
#include <util/stream/output.h>
#include <util/system/condvar.h>
#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_column_group_helpers.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_index_serialisation.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_base_writer.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_comparator.h>

namespace NYql::NFmr {

class TFmrTableDataServiceSortedWriter: public TFmrTableDataServiceBaseWriter {
public:
    using TPtr = TIntrusivePtr<TFmrTableDataServiceSortedWriter>;

    TFmrTableDataServiceSortedWriter(
        const TString& tableId,
        const TString& partId,
        ITableDataService::TPtr tableDataService,
        const TString& columnGroupSpec = TString(),
        const TFmrWriterSettings& settings = TFmrWriterSettings(),
        TSortingColumns keyColumns = {}
    );

protected:
    void PutRows() override;

private:
    void CheckIsSorted(TStringBuf currentYsonContent, const std::vector<TRowIndexMarkup>& chunkIndexes) const;
    TSortedChunkStats GetSortedChunkStats(TStringBuf currentYsonContent, const std::vector<TRowIndexMarkup>& chunkIndexes) const;
    TString GetIndexValue(TStringBuf currentYsonContent, const TColumnOffsetRange& index) const;
    NYT::TNode GetKeyRowByIndexes(TStringBuf currentYsonContent, const std::vector<TColumnOffsetRange>& indexes) const;

private:
    TSortingColumns KeyColumns_;
};

} // namespace NYql::NFmr
