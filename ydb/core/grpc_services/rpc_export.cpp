#include "service_export.h"
#include "grpc_request_proxy.h"
#include "rpc_export_base.h"
#include "rpc_calls.h"
#include "rpc_operation_request_base.h"

#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/base/path.h>
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
    static constexpr bool IsS3Export = std::is_same_v<TEvRequest, TEvExportToS3Request>;
    static constexpr bool IsYtExport = std::is_same_v<TEvRequest, TEvExportToYtRequest>;

    struct TExportItemInfo {
        TString Destination;
        bool Resolved = false;
    };

    enum class EStage {
        ResolvePaths, // Resolve explicitly specified paths. Check that they are all supported in export
        ExpandDirectories, // Expand directories and subdirectories
        ResolveExpandedPaths, // Resolve expanded paths, ignore children that is not supported in export
        AllocateTxId,
    };

    TStringBuf GetLogPrefix() const override {
        return "[CreateExport]";
    }

    static bool IsItemSupportedInExport(NSchemeCache::TSchemeCacheNavigate::EKind kind) {
        switch (kind) {
            case NSchemeCache::TSchemeCacheNavigate::KindTable:
            case NSchemeCache::TSchemeCacheNavigate::KindTopic:
                return true;
            case NSchemeCache::TSchemeCacheNavigate::KindView:
                return AppData()->FeatureFlags.GetEnableViewExport();
            default:
                return false;
        }
    }

    static bool IsLikeDirectory(NSchemeCache::TSchemeCacheNavigate::EKind kind) {
        return kind == NSchemeCache::TSchemeCacheNavigate::KindPath
            || kind == NSchemeCache::TSchemeCacheNavigate::KindSubdomain
            || kind == NSchemeCache::TSchemeCacheNavigate::KindExtSubdomain;
    }

    IEventBase* MakeRequest() override {
        const auto& request = *this->GetProtoRequest();

        auto ev = MakeHolder<TEvExport::TEvCreateExportRequest>();
        ev->Record.SetTxId(this->TxId);
        ev->Record.SetDatabaseName(this->GetDatabaseName());
        if (this->UserToken) {
            ev->Record.SetUserSID(this->UserToken->GetUserSID());
            ev->Record.SetSanitizedToken(this->UserToken->GetSanitizedToken());
        }
        ev->Record.SetPeerName(this->Request->GetPeerName());

        auto& createExport = *ev->Record.MutableRequest();
        *createExport.MutableOperationParams() = request.operation_params();
        if constexpr (IsYtExport) {
            auto* exportSettings = createExport.MutableExportToYtSettings();
            *exportSettings = request.settings();
            exportSettings->clear_items();
            for (const auto& [sourcePath, info] : ExportItems) {
                auto* item = exportSettings->add_items();
                item->set_source_path(sourcePath);
                item->set_destination_path(info.Destination);
            }
        }
        if constexpr (IsS3Export) {
            auto* exportSettings = createExport.MutableExportToS3Settings();
            *exportSettings = request.settings();
            exportSettings->set_source_path(CommonSourcePath);
            exportSettings->clear_items();
            for (const auto& [sourcePath, info] : ExportItems) {
                auto* item = exportSettings->add_items();
                item->set_source_path(sourcePath.substr(CommonSourcePath.size() + 1));
                item->set_destination_prefix(info.Destination);
            }
        }

        return ev.Release();
    }

    bool ExtractSpecifiedPaths(TVector<TString>& paths) {
        const auto& settings = this->GetProtoRequest()->settings();
        paths.reserve(settings.items_size() + 3);

        paths.emplace_back(this->GetDatabaseName()); // first entry is database
        paths.emplace_back(CommonSourcePath); // second entry is common source path
        for (const auto& item : settings.items()) {
            TString path = CanonizePath(item.source_path());
            if (path.size() > CommonSourcePath.size() && path.StartsWith(CommonSourcePath) && path[CommonSourcePath.size()] == '/') {
                paths.emplace_back(path); // Full path
            } else {
                paths.emplace_back(CommonSourcePath + path); // Relative path
            }
            auto [it, inserted] = ExportItems.insert({paths.back(), TExportItemInfo{}});
            if (!inserted) {
                this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, TStringBuilder() << "Duplicate export item source path: \"" << item.source_path() << "\"");
                return false;
            }
            if constexpr (IsS3Export) {
                it->second.Destination = item.destination_prefix();
            }
            if constexpr (IsYtExport) {
                it->second.Destination = item.destination_path();
            }
        }

        if constexpr (IsS3Export) {
            if (settings.items_size() == 0) { // expand all source path by default
                paths.emplace_back(CommonSourcePath);
                ExportItems.insert({CommonSourcePath, TExportItemInfo{}});
            }
        }

        return true;
    }

    void ResolvePaths(const TVector<TString>& paths, NSchemeCache::TSchemeCacheNavigate::EOp op = NSchemeCache::TSchemeCacheNavigate::OpPath) {
        Y_ABORT_UNLESS(!paths.empty());

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = this->GetDatabaseName();

        for (const auto& path : paths) {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = op;
            entry.Path = NKikimr::SplitPath(path);
            if (entry.Path.empty()) {
                return this->Reply(StatusIds::SCHEME_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR, "Cannot resolve empty path");
            }
        }

        this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        this->Become(&TDerived::StateResolvePaths);
    }

    void NextStage() {
        if (Stage == EStage::ResolvePaths) {
            Stage = EStage::ExpandDirectories;
        }

        if (Stage == EStage::ExpandDirectories && DirectoryItems.empty()) {
            Stage = EStage::ResolveExpandedPaths;
        }

        if (Stage == EStage::ExpandDirectories) {
            TVector<TString> paths;
            for (const auto& [path, _] : DirectoryItems) {
                paths.emplace_back(path);
            }
            ResolvePaths(paths, NSchemeCache::TSchemeCacheNavigate::OpList);
            return;
        }
        if (Stage == EStage::ResolveExpandedPaths) {
            TVector<TString> paths;
            for (const auto& [path, info] : ExportItems) {
                if (!info.Resolved) {
                    paths.emplace_back(path);
                }
            }
            if (paths.empty()) {
                Stage = EStage::AllocateTxId;
            } else {
                ResolvePaths(paths, NSchemeCache::TSchemeCacheNavigate::OpPath);
                return;
            }
        }
        if (Stage == EStage::AllocateTxId) {
            if (ExportItems.empty()) {
                return this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Nothing to export");
            }

            this->AllocateTxId();
            this->Become(&TDerived::StateWait);
            return;
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;

        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": request# " << (request ? request->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (request->ResultSet.empty()) {
            return this->Reply(StatusIds::SCHEME_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        if (request->ErrorCount > 0) {
            for (const NSchemeCache::TSchemeCacheNavigate::TEntry& entry : request->ResultSet) {
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

        if (Stage == EStage::ResolvePaths) {
            // All explicitly specified items must be supported in export
            for (size_t i = 2; i < request->ResultSet.size(); ++i) {
                const NSchemeCache::TSchemeCacheNavigate::TEntry& entry = request->ResultSet[i];
                TString path = CanonizePath(entry.Path);
                const auto it = ExportItems.find(path);
                if (it == ExportItems.end()) {
                    return this->Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::UNEXPECTED, TStringBuilder() << "Item \""
                        << path << "\" not found in export items list");
                }
                it->second.Resolved = true;
                const NSchemeCache::TSchemeCacheNavigate::EKind kind = entry.Kind;
                if (IsLikeDirectory(kind)) {
                    DirectoryItems[path] = it->second;
                }
                if (!IsItemSupportedInExport(kind)) {
                    if (IsLikeDirectory(kind)) { // If directories/databases are not supported => it is OK, they are expanded and then thrown
                        ExportItems.erase(it);
                    } else {
                        return this->Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, TStringBuilder() << "Item \""
                            << path << "\" is not supported in export");
                    }
                }
            }
        } else if (Stage == EStage::ExpandDirectories) {
            const TString canonizedDatabasePath = CanonizePath(this->GetDatabaseName());
            for (size_t i = 0; i < request->ResultSet.size(); ++i) {
                const NSchemeCache::TSchemeCacheNavigate::TEntry& entry = request->ResultSet[i];
                TString path = CanonizePath(request->ResultSet[i].Path);
                const auto it = DirectoryItems.find(path);
                if (it == DirectoryItems.end()) {
                    return this->Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::UNEXPECTED, TStringBuilder() << "Item \""
                        << path << "\" not found in directories list");
                }
                if (!entry.ListNodeEntry) {
                    return this->Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::UNEXPECTED, TStringBuilder() << "Directory \""
                        << path << "\" does not have list entry");
                }
                const bool isRoot = path == canonizedDatabasePath;
                for (const NSchemeCache::TSchemeCacheNavigate::TListNodeEntry::TChild& child : entry.ListNodeEntry->Children) {
                    const NSchemeCache::TSchemeCacheNavigate::EKind kind = child.Kind;
                    if (isRoot && kind == NSchemeCache::TSchemeCacheNavigate::KindPath) {
                        // Skip children that we don't want to export
                        if (child.Name.StartsWith("~")
                            || child.Name.StartsWith(".sys")
                            || child.Name.StartsWith(".metadata")
                            || child.Name.StartsWith("export-"))
                        {
                            continue;
                        }
                    }
                    const TString childPath = CanonizePath(TStringBuilder() << path << "/" << child.Name);
                    TString destination;
                    if (it->second.Destination) {
                        destination = TStringBuilder() << it->second.Destination << "/" << child.Name;
                    }
                    if (IsItemSupportedInExport(kind)) {
                        ExportItems.insert({childPath, TExportItemInfo{.Destination = destination}});
                    }
                    if (IsLikeDirectory(kind)) {
                        DirectoryItems.insert({childPath, TExportItemInfo{.Destination = destination}});
                    }
                }
                DirectoryItems.erase(it);
            }
        }

        TString error;

        if (this->UserToken) {
            for (size_t i = 0; i < request->ResultSet.size(); ++i) {
                const auto& entry = request->ResultSet[i];
                ui32 access = NACLib::SelectRow;
                if (Stage == EStage::ResolvePaths) {
                    if (i == 0) { // database
                        access = NACLib::GenericRead | NACLib::GenericWrite;
                    } else if (i == 1) { // common source path => don't check, it is only a prefix
                        access = 0;
                    }
                }
                if (access && !this->CheckAccess(CanonizePath(entry.Path), entry.SecurityObject, access)) {
                    return;
                }
            }
        }

        for (const auto& entry : request->ResultSet) {
            TString path = CanonizePath(entry.Path);
            const auto it = ExportItems.find(path);
            if (it != ExportItems.end()) {
                it->second.Resolved = true;
            }

            if (!entry.DomainInfo) {
                LOG_E("Got empty domain info");
                return this->Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR);
            }

            if (!DomainInfo) {
                DomainInfo = entry.DomainInfo;
                continue;
            }

            if (DomainInfo->DomainKey != entry.DomainInfo->DomainKey) {
                return this->Reply(StatusIds::SCHEME_ERROR, TIssuesIds::DOMAIN_LOCALITY_ERROR,
                    TStringBuilder() << "Failed locality check"
                        << ": expected# " << DomainInfo->DomainKey
                        << ", actual# " << entry.DomainInfo->DomainKey);
            }
        }

        NextStage();
    }

    void Handle(TEvExport::TEvCreateExportResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvExport::TEvCreateExportResponse"
            << ": record# " << record.ShortDebugString());

        this->Reply(TExportConv::ToOperation(record.GetEntry()));
    }

    void InitCommonSourcePath() {
        const auto& settings = this->GetProtoRequest()->settings();
        if constexpr (IsS3Export) {
            CommonSourcePath = CanonizePath(settings.source_path()); // /Foo/Bar, but empty result for empty source_path
        }
        if (CommonSourcePath.empty()) {
            CommonSourcePath = CanonizePath(this->GetDatabaseName());
        }
    }

    bool ValidateEncryptionParameters() {
        const auto& settings = this->GetProtoRequest()->settings();
        try {
            NBackup::TEncryptionIV iv = NBackup::TEncryptionIV::Generate();
            NBackup::TEncryptionKey key(settings.encryption_settings().symmetric_key().key());
            NBackup::TEncryptedFileSerializer::EncryptFullFile(settings.encryption_settings().encryption_algorithm(), key, iv, {});
            return true;
        } catch (const std::exception& ex) {
            this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, TStringBuilder() << "Invalid encryption settings: " << ex.what());
            return false;
        }
    }

public:
    using TRpcOperationRequestActor<TDerived, TEvRequest, true>::TRpcOperationRequestActor;

    void Bootstrap(const TActorContext&) {
        const auto& request = *this->GetProtoRequest();
        if (request.operation_params().has_forget_after() && request.operation_params().operation_mode() != Ydb::Operations::OperationParams::SYNC) {
            return this->Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "forget_after is not supported for this type of operation");
        }

        const auto& settings = request.settings();
        InitCommonSourcePath();

        if constexpr (IsYtExport) {
            if (settings.items().empty()) {
                return this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Items are not set");
            }
        }
        if constexpr (IsS3Export) {
            const bool encryptedExportFeatureFlag = AppData()->FeatureFlags.GetEnableEncryptedExport();
            const bool commonDestPrefixSpecified = !settings.destination_prefix().empty();
            if (!encryptedExportFeatureFlag) {
                // Check that no new fields are specified
                if (commonDestPrefixSpecified) {
                    return this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Destination prefix is not supported in current configuration");
                }
                if (!settings.source_path().empty()) {
                    return this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Source path is not supported in current configuration");
                }
                if (settings.has_encryption_settings()) {
                    return this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Export encryption is not supported in current configuration");
                }
            }
            if (settings.items().empty() && !commonDestPrefixSpecified) {
                return this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "No destination prefix specified. Don't know where to export");
            }
            for (const auto& item : settings.items()) {
                if (item.destination_prefix().empty() && !commonDestPrefixSpecified) {
                    return this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, TStringBuilder() << "No destination prefix or common destination prefix specified for item \"" << item.source_path() << "\"");
                }
            }
            if (settings.has_encryption_settings()) { // Validate that it is possible to encrypt with these settings
                if (!commonDestPrefixSpecified) {
                    return this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, TStringBuilder() << "No destination prefix specified for encrypted export");
                }

                if (!ValidateEncryptionParameters()) {
                    return;
                }
            }
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

        TVector<TString> specifiedPaths;
        if (!ExtractSpecifiedPaths(specifiedPaths)) {
            return;
        }
        ResolvePaths(specifiedPaths);
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

private:
    EStage Stage = EStage::ResolvePaths;
    NSchemeCache::TDomainInfo::TPtr DomainInfo;
    TString CommonSourcePath; // Canonized source path
    THashMap<TString, TExportItemInfo> ExportItems;
    THashMap<TString, TExportItemInfo> DirectoryItems;
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
