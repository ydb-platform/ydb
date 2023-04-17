#include "service_operation.h"

#include "operation_helpers.h"
#include "rpc_export_base.h"
#include "rpc_import_base.h"
#include "rpc_operation_request_base.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NSchemeShard;
using namespace NKikimrIssues;
using namespace NOperationId;
using namespace Ydb;

using TEvListOperationsRequest = TGrpcRequestNoOperationCall<Ydb::Operations::ListOperationsRequest,
    Ydb::Operations::ListOperationsResponse>;

class TListOperationsRPC: public TRpcOperationRequestActor<TListOperationsRPC, TEvListOperationsRequest>,
                          public TExportConv {

    TStringBuf GetLogPrefix() const override {
        switch (ParseKind(GetProtoRequest()->kind())) {
        case TOperationId::EXPORT:
            return "[ListExports]";
        case TOperationId::IMPORT:
            return "[ListImports]";
        case TOperationId::BUILD_INDEX:
            return "[ListIndexBuilds]";
        default:
            return "[Untagged]";
        }
    }

    IEventBase* MakeRequest() override {
        const auto& request = *GetProtoRequest();

        switch (ParseKind(GetProtoRequest()->kind())) {
        case TOperationId::EXPORT:
            return new TEvExport::TEvListExportsRequest(DatabaseName, request.page_size(), request.page_token(), request.kind());
        case TOperationId::IMPORT:
            return new TEvImport::TEvListImportsRequest(DatabaseName, request.page_size(), request.page_token(), request.kind());
        case TOperationId::BUILD_INDEX:
            return new TEvIndexBuilder::TEvListRequest(DatabaseName, request.page_size(), request.page_token());
        default:
            Y_FAIL("unreachable");
        }
    }

    void Handle(TEvExport::TEvListExportsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvExport::TEvListExportsResponse"
            << ": record# " << record.ShortDebugString());

        TResponse response;
        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        for (const auto& entry : record.GetEntries()) {
            *response.add_operations() = TExportConv::ToOperation(entry);
        }
        response.set_next_page_token(record.GetNextPageToken());
        Reply(response);
    }

    void Handle(TEvImport::TEvListImportsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvImport::TEvListImportsResponse"
            << ": record# " << record.ShortDebugString());

        TResponse response;
        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        for (const auto& entry : record.GetEntries()) {
            *response.add_operations() = TImportConv::ToOperation(entry);
        }
        response.set_next_page_token(record.GetNextPageToken());
        Reply(response);
    }

    void Handle(TEvIndexBuilder::TEvListResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvIndexBuilder::TEvListResponse"
            << ": record# " << record.ShortDebugString());

        TResponse response;

        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        for (const auto& entry : record.GetEntries()) {
            auto operation = response.add_operations();
            ::NKikimr::NGRpcService::ToOperation(entry, operation);
        }
        response.set_next_page_token(record.GetNextPageToken());
        Reply(response);
    }

public:
    using TRpcOperationRequestActor::TRpcOperationRequestActor;

    void Bootstrap() {
        switch (ParseKind(GetProtoRequest()->kind())) {
        case TOperationId::EXPORT:
        case TOperationId::IMPORT:
        case TOperationId::BUILD_INDEX:
            break;

        default:
            return Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "Unknown operation kind");
        }

        ResolveDatabase();
        Become(&TListOperationsRPC::StateWait);
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExport::TEvListExportsResponse, Handle);
            hFunc(TEvImport::TEvListImportsResponse, Handle);
            hFunc(TEvIndexBuilder::TEvListResponse, Handle);
        default:
            return StateBase(ev);
        }
    }

}; // TListOperationsRPC

void DoListOperationsRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TListOperationsRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
