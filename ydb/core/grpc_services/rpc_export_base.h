#pragma once

#include <ydb/core/protos/export.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NGRpcService {

struct TExportConv {
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

    static Ydb::Operations::Operation ToOperation(const NKikimrExport::TExport& exprt) {
        Ydb::Operations::Operation operation;

        operation.set_id(NOperationId::ProtoToString(MakeOperationId(exprt.GetId(), exprt.GetSettingsCase())));
        operation.set_status(exprt.GetStatus());
        if (operation.status() == Ydb::StatusIds::SUCCESS) {
            operation.set_ready(exprt.GetProgress() == Ydb::Export::ExportProgress::PROGRESS_DONE);
        } else {
            operation.set_ready(true);
        }
        if (exprt.IssuesSize()) {
            operation.mutable_issues()->CopyFrom(exprt.GetIssues());
        }

        if (exprt.HasStartTime()) {
            *operation.mutable_create_time() = exprt.GetStartTime();
        }
        if (exprt.HasEndTime()) {
            *operation.mutable_end_time() = exprt.GetEndTime();
        }

        if (exprt.HasUserSID()) {
            operation.set_created_by(exprt.GetUserSID());
        }

        using namespace Ydb::Export;
        switch (exprt.GetSettingsCase()) {
        case NKikimrExport::TExport::kExportToYtSettings:
            Fill<ExportToYtMetadata, ExportToYtResult>(operation, exprt, exprt.GetExportToYtSettings());
            break;
        case NKikimrExport::TExport::kExportToS3Settings:
            Fill<ExportToS3Metadata, ExportToS3Result>(operation, exprt, exprt.GetExportToS3Settings());
            break;
        default:
            Y_DEBUG_ABORT("Unknown export kind");
            break;
        }

        return operation;
    }

private:
    template <typename TMetadata, typename TResult, typename TSettings>
    static void Fill(
            Ydb::Operations::Operation& operation,
            const NKikimrExport::TExport& exprt,
            const TSettings& settings) {
        TMetadata metadata;
        metadata.mutable_settings()->CopyFrom(settings);
        metadata.set_progress(exprt.GetProgress());
        metadata.mutable_items_progress()->CopyFrom(exprt.GetItemsProgress());
        operation.mutable_metadata()->PackFrom(metadata);

        TResult result;
        operation.mutable_result()->PackFrom(result);
    }

}; // TExportConv

} // namespace NGRpcService
} // namespace NKikimr
