#include "service_export.h"
#include "grpc_request_proxy.h"
#include "rpc_export_base.h"
#include "rpc_calls.h"
#include "rpc_operation_request_base.h"

#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/ydb_convert/compression.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NSchemeShard;
using namespace NKikimrIssues;
using namespace Ydb;

using TEvExportToYtRequest = TGrpcRequestOperationCall<Ydb::Export::ExportToYtRequest,
    Ydb::Export::ExportToYtResponse>;
using TEvExportToS3Request = TGrpcRequestOperationCall<Ydb::Export::ExportToS3Request,
    Ydb::Export::ExportToS3Response>;

template <typename TDerived, typename TEvRequest>
class TExportRPC: public TRpcOperationRequestActor<TDerived, TEvRequest, true>, public TExportConv {
    TStringBuf GetLogPrefix() const override {
        return "[CreateExport]";
    }

    IEventBase* MakeRequest() override {
        const auto& request = *this->GetProtoRequest();

        auto ev = MakeHolder<TEvExport::TEvCreateExportRequest>();
        ev->Record.SetTxId(this->TxId);
        ev->Record.SetDatabaseName(this->GetDatabaseName());
        if (this->UserToken) {
            ev->Record.SetUserSID(this->UserToken->GetUserSID());
        }

        auto& createExport = *ev->Record.MutableRequest();
        *createExport.MutableOperationParams() = request.operation_params();
        if constexpr (std::is_same_v<TEvRequest, TEvExportToYtRequest>) {
            *createExport.MutableExportToYtSettings() = request.settings();
        } else if constexpr (std::is_same_v<TEvRequest, TEvExportToS3Request>) {
            *createExport.MutableExportToS3Settings() = request.settings();
        }

        return ev.Release();
    }

    template <typename TProtoSettings>
    static void ExtractPaths(TVector<TString>& paths, const TProtoSettings& settings) {
        paths.reserve(settings.items_size() + 1);
        for (const auto& item : settings.items()) {
            paths.emplace_back(item.source_path());
        }
    }

    TVector<TString> ExtractPaths() {
        TVector<TString> paths;

        paths.emplace_back(this->GetDatabaseName()); // first entry is database
        ExtractPaths(paths, this->GetProtoRequest()->settings());

        return paths;
    }

    void ResolvePaths(const TVector<TString>& paths) {
        Y_ABORT_UNLESS(!paths.empty());

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = this->GetDatabaseName();

        for (const auto& path : paths) {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.Path = NKikimr::SplitPath(path);
            if (entry.Path.empty()) {
                return this->Reply(StatusIds::SCHEME_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR, "Cannot resolve empty path");
            }
        }

        this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        this->Become(&TDerived::StateResolvePaths);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;

        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": request# " << (request ? request->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (request->ResultSet.empty()) {
            return this->Reply(StatusIds::SCHEME_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        if (request->ErrorCount > 0) {
            for (const auto& entry : request->ResultSet) {
                if (entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                    continue;
                }

                StatusIds::StatusCode status;
                TIssuesIds::EIssueCode code;

                switch (entry.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                    status = StatusIds::UNAUTHORIZED;
                    code = TIssuesIds::ACCESS_DENIED;
                    break;
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                    status = StatusIds::SCHEME_ERROR;
                    code = TIssuesIds::PATH_NOT_EXIST;
                    break;
                case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                    status = StatusIds::UNAVAILABLE;
                    code = TIssuesIds::RESOLVE_LOOKUP_ERROR;
                    break;
                default:
                    status = StatusIds::SCHEME_ERROR;
                    code = TIssuesIds::GENERIC_RESOLVE_ERROR;
                    break;
                }

                return this->Reply(status, code, TStringBuilder() << "Cannot resolve path"
                    << ": path# " << CanonizePath(entry.Path)
                    << ", status# " << entry.Status);
            }
        }

        TString error;

        if (this->UserToken) {
            bool isDatabase = true; // first entry is database

            for (const auto& entry : request->ResultSet) {
                const ui32 access = isDatabase ? NACLib::GenericRead | NACLib::GenericWrite : NACLib::SelectRow;
                if (!this->CheckAccess(CanonizePath(entry.Path), entry.SecurityObject, access)) {
                    return;
                }

                isDatabase = false;
            }
        }

        NSchemeCache::TDomainInfo::TPtr domainInfo;
        for (const auto& entry : request->ResultSet) {
            if (!entry.DomainInfo) {
                LOG_E("Got empty domain info");
                return this->Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR);
            }

            if (!domainInfo) {
                domainInfo = entry.DomainInfo;
                continue;
            }

            if (domainInfo->DomainKey != entry.DomainInfo->DomainKey) {
                return this->Reply(StatusIds::SCHEME_ERROR, TIssuesIds::DOMAIN_LOCALITY_ERROR,
                    TStringBuilder() << "Failed locality check"
                        << ": expected# " << domainInfo->DomainKey
                        << ", actual# " << entry.DomainInfo->DomainKey);
            }
        }

        this->AllocateTxId();
        this->Become(&TDerived::StateWait);
    }

    void Handle(TEvExport::TEvCreateExportResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvExport::TEvCreateExportResponse"
            << ": record# " << record.ShortDebugString());

        this->Reply(TExportConv::ToOperation(record.GetEntry()));
    }

public:
    using TRpcOperationRequestActor<TDerived, TEvRequest, true>::TRpcOperationRequestActor;

    void Bootstrap(const TActorContext&) {
        const auto& request = *(this->GetProtoRequest());
        if (request.operation_params().has_forget_after() && request.operation_params().operation_mode() != Ydb::Operations::OperationParams::SYNC) {
            return this->Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "forget_after is not supported for this type of operation");
        }

        const auto& settings = request.settings();
        if (settings.items().empty()) {
            return this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Items are not set");
        }

        if constexpr (std::is_same_v<TEvRequest, TEvExportToS3Request>) {
            if (settings.compression()) {
                StatusIds::StatusCode status;
                TString error;
                if (!CheckCompression(settings.compression(), status, error)) {
                    return this->Reply(status, TIssuesIds::DEFAULT_ERROR, error);
                }
            }
        }

        ResolvePaths(ExtractPaths());
    }

    STATEFN(StateResolvePaths) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        }
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExport::TEvCreateExportResponse, Handle);
        default:
            return this->StateBase(ev);
        }
    }

}; // TExportRPC

class TExportToYtRPC: public TExportRPC<TExportToYtRPC, TEvExportToYtRequest> {
public:
    using TExportRPC::TExportRPC;
};

class TExportToS3RPC: public TExportRPC<TExportToS3RPC, TEvExportToS3Request> {
public:
    using TExportRPC::TExportRPC;
};

void DoExportToYtRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TExportToYtRPC(p.release()));
}

void DoExportToS3Request(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TExportToS3RPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
