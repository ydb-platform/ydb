#pragma once

#include <arrow/api.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>

namespace NYql::NConnector {

    std::shared_ptr<arrow::RecordBatch> ReadSplitsResponseToArrowRecordBatch(const NApi::TReadSplitsResponse& resp);
    Ydb::Type GetColumnTypeByName(const NApi::TSchema& schema, const TString& name);

}
