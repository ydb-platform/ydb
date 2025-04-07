#pragma once

#include "formats.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>

namespace NYdb {
namespace NConsoleClient {

/// Common
void PrintOperation(const TOperation& operation, EDataFormat format);

/// YT
void PrintOperation(const NExport::TExportToYtResponse& operation, EDataFormat format);
void PrintOperationsList(const NOperation::TOperationsList<NExport::TExportToYtResponse>& operations, EDataFormat format);

/// S3
// export
void PrintOperation(const NExport::TExportToS3Response& operation, EDataFormat format);
void PrintOperationsList(const NOperation::TOperationsList<NExport::TExportToS3Response>& operations, EDataFormat format);
// import
void PrintOperation(const NImport::TImportFromS3Response& operation, EDataFormat format);
void PrintOperationsList(const NOperation::TOperationsList<NImport::TImportFromS3Response>& operations, EDataFormat format);

/// Index build
void PrintOperation(const NYdb::NTable::TBuildIndexOperation& operation, EDataFormat format);
void PrintOperationsList(const NOperation::TOperationsList<NYdb::NTable::TBuildIndexOperation>& operations, EDataFormat format);

/// QueryService
void PrintOperation(const NYdb::NQuery::TScriptExecutionOperation& operation, EDataFormat format);
void PrintOperationsList(const NOperation::TOperationsList<NYdb::NQuery::TScriptExecutionOperation>& operations, EDataFormat format);

}
}
