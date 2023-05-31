#pragma once

#include <arrow/api.h>
#include <ydb/library/yql/providers/generic/connector/api/protos/connector.pb.h>

namespace NYql::Connector {

    std::shared_ptr<arrow::RecordBatch> APIReadSplitsResponseToArrowRecordBatch(const API::ReadSplitsResponse& resp);
    Ydb::Type GetColumnTypeByName(const API::Schema& schema, const TString& name);

}
