#pragma once

#include "rpc_operation_conv_base.h"

#include <ydb/core/protos/import.pb.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NGRpcService {

struct TImportConv: public TOperationConv<NKikimrImport::TImport> {
    static Ydb::TOperationId MakeOperationId(const ui64 id, NKikimrImport::TImport::SettingsCase kind) {
        Ydb::TOperationId operationId;
        operationId.SetKind(Ydb::TOperationId::IMPORT);
        NOperationId::AddOptionalValue(operationId, "id", ToString(id));

        switch (kind) {
        case NKikimrImport::TImport::kImportFromS3Settings:
            NOperationId::AddOptionalValue(operationId, "kind", "s3");
            break;
        default:
            Y_DEBUG_ABORT("Unknown import kind");
            break;
        }

        return operationId;
    }

    static Operation ToOperation(const NKikimrImport::TImport& in) {
        auto operation = TOperationConv::ToOperation(in);

        if (operation.status() == Ydb::StatusIds::SUCCESS) {
            operation.set_ready(in.GetProgress() == Ydb::Import::ImportProgress::PROGRESS_DONE);
        } else if (operation.status() != Ydb::StatusIds::CANCELLED) {
            return operation;
        }

        operation.set_id(NOperationId::ProtoToString(MakeOperationId(in.GetId(), in.GetSettingsCase())));

        using namespace Ydb::Import;
        switch (in.GetSettingsCase()) {
        case NKikimrImport::TImport::kImportFromS3Settings:
            Fill<ImportFromS3Metadata, ImportFromS3Result>(operation, in, in.GetImportFromS3Settings());
            break;
        default:
            Y_DEBUG_ABORT("Unknown import kind");
            break;
        }

        return operation;
    }

}; // TImportConv

} // namespace NGRpcService
} // namespace NKikimr
