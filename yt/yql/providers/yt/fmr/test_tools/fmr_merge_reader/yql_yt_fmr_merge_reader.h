#pragma once

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_sorted_merge_reader.h>

#include <yt/yql/providers/yt/fmr/test_tools/yson/yql_yt_yson_helpers.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_text_yson.h>

#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_tds_block_iterator.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_yt_block_iterator.h>

#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>

namespace NYql::NFmr {

TTempFileHandle WriteTempFile(const TString& content);

IBlockIterator::TPtr MakeYtFileIterator(
    const TString& filePath,
    const TVector<TString>& keyColumns,
    ui64 targetBlockBytes
);

IBlockIterator::TPtr MakeTdsIterator(
    const TString& tableId,
    const TString& partId,
    const ITableDataService::TPtr& tds,
    const TVector<TString>& keyColumns,
    const TVector<TString>& neededColumns
);

enum class EMergeReaderSourceType {
    YT,
    TDS
};

struct TMergeTestTable {
    EMergeReaderSourceType SourceType;
    TVector<TString> KeyColumns;
    TVector<TString> NeededColumns;
    TString RawTableBody;
};

TVector<IBlockIterator::TPtr> MakeTestIterators(TVector<TMergeTestTable> rawTestTables, TMaybe<ILocalTableDataService::TPtr> tds, TYtBlockIteratorSettings settings);

} // namespace NYql::NFmr

