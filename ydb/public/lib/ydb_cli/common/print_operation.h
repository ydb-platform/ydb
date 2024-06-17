#pragma once

#include "formats.h"
#include <ydb/public/sdk/cpp/client/ydb_export/export.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_query/query.h>

namespace NYdb {
namespace NConsoleClient {

/// Common
void PrintOperation(const TOperation& operation, EOutputFormat format);

/// YT
void PrintOperation(const NExport::TExportToYtResponse& operation, EOutputFormat format);
void PrintOperationsList(const NOperation::TOperationsList<NExport::TExportToYtResponse>& operations, EOutputFormat format);

/// S3
// export
void PrintOperation(const NExport::TExportToS3Response& operation, EOutputFormat format);
void PrintOperationsList(const NOperation::TOperationsList<NExport::TExportToS3Response>& operations, EOutputFormat format);
// import
void PrintOperation(const NImport::TImportFromS3Response& operation, EOutputFormat format);
void PrintOperationsList(const NOperation::TOperationsList<NImport::TImportFromS3Response>& operations, EOutputFormat format);

/// Index build
void PrintOperation(const NYdb::NTable::TBuildIndexOperation& operation, EOutputFormat format);
void PrintOperationsList(const NOperation::TOperationsList<NYdb::NTable::TBuildIndexOperation>& operations, EOutputFormat format);

/// QueryService
void PrintOperation(const NYdb::NQuery::TScriptExecutionOperation& operation, EOutputFormat format);
void PrintOperationsList(const NOperation::TOperationsList<NYdb::NQuery::TScriptExecutionOperation>& operations, EOutputFormat format);

}
}
