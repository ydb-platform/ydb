#pragma once

#include "rpc_operation_conv_base.h"

#include <ydb/core/protos/export.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NGRpcService {

struct TExportConv: public TOperationConv<NKikimrExport::TExport> {
    static Ydb::TOperationId MakeOperationId(const ui64 id, NKikimrExport::TExport::SettingsCase kind) {
        Ydb::TOperationId operationId;
        operationId.SetKind(Ydb::TOperationId::EXPORT);
        NOperationId::AddOptionalValue(operationId, "id", ToString(id));

        switch (kind) {
        case NKikimrExport::TExport::kExportToYtSettings:
            NOperationId::AddOptionalValue(operationId, "kind", "yt");
            break;
        case NKikimrExport::TExport::kExportToS3Settings:
            NOperationId::AddOptionalValue(operationId, "kind", "s3");
            break;
        default:
            Y_DEBUG_ABORT("Unknown export kind");
            break;
        }

        return operationId;
    }

    static Operation ToOperation(const NKikimrExport::TExport& in) {
        auto operation = TOperationConv::ToOperation(in);

        if (operation.status() == Ydb::StatusIds::SUCCESS) {
            operation.set_ready(in.GetProgress() == Ydb::Export::ExportProgress::PROGRESS_DONE);
        } else if (operation.status() != Ydb::StatusIds::CANCELLED) {
            return operation;
        }

        operation.set_id(NOperationId::ProtoToString(MakeOperationId(in.GetId(), in.GetSettingsCase())));

        using namespace Ydb::Export;
        switch (in.GetSettingsCase()) {
        case NKikimrExport::TExport::kExportToYtSettings:
            Fill<ExportToYtMetadata, ExportToYtResult>(operation, in, in.GetExportToYtSettings());
            break;
        case NKikimrExport::TExport::kExportToS3Settings:
            Fill<ExportToS3Metadata, ExportToS3Result>(operation, in, in.GetExportToS3Settings());
            break;
        default:
            Y_DEBUG_ABORT("Unknown export kind");
            break;
        }

        return operation;
    }

}; // TExportConv

} // namespace NGRpcService
} // namespace NKikimr
