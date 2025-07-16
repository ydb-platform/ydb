#pragma once

#include <arrow/api.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <util/generic/fwd.h>

namespace NYql::NConnector::NApi {
    class TReadSplitsResponse;
    class TSchema;
} // namespace NYql::NConnector::NApi

namespace NYql::NConnector {

    std::shared_ptr<arrow::RecordBatch> ReadSplitsResponseToArrowRecordBatch(const NApi::TReadSplitsResponse& resp);
    Ydb::Type GetColumnTypeByName(const NApi::TSchema& schema, const TString& name);

} // namespace NYql::NConnector
