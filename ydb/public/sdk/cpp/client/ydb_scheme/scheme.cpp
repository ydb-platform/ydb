#include "scheme.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h> 
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/table_helpers/helpers.h> 
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h> 
#include <ydb/public/api/protos/ydb_scheme.pb.h> 
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h> 

namespace NYdb {
namespace NScheme {

using namespace NThreading;
using namespace Ydb::Scheme;

static ESchemeEntryType ConvertProtoEntryType(::Ydb::Scheme::Entry::Type entry) {
    switch (entry) {
    case ::Ydb::Scheme::Entry::DIRECTORY:
        return ESchemeEntryType::Directory;
    case ::Ydb::Scheme::Entry::TABLE:
        return ESchemeEntryType::Table;
    case ::Ydb::Scheme::Entry::PERS_QUEUE_GROUP:
        return ESchemeEntryType::PqGroup;
    case ::Ydb::Scheme::Entry::DATABASE:
        return ESchemeEntryType::SubDomain;
    case ::Ydb::Scheme::Entry::RTMR_VOLUME:
        return ESchemeEntryType::RtmrVolume;
    case ::Ydb::Scheme::Entry::BLOCK_STORE_VOLUME:
        return ESchemeEntryType::BlockStoreVolume;
    case ::Ydb::Scheme::Entry::COORDINATION_NODE:
        return ESchemeEntryType::CoordinationNode;
    case ::Ydb::Scheme::Entry::SEQUENCE:
        return ESchemeEntryType::Sequence;
    case ::Ydb::Scheme::Entry::REPLICATION:
        return ESchemeEntryType::Replication;
    default:
        return ESchemeEntryType::Unknown;
    }
}

class TSchemeClient::TImpl : public TClientImplCommon<TSchemeClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings) {}

    TAsyncStatus MakeDirectory(const TString& path, const TMakeDirectorySettings& settings) {
        auto request = MakeOperationRequest<Ydb::Scheme::MakeDirectoryRequest>(settings);
        request.set_path(path);

        return RunSimple<Ydb::Scheme::V1::SchemeService, MakeDirectoryRequest, MakeDirectoryResponse>(
            std::move(request),
            &Ydb::Scheme::V1::SchemeService::Stub::AsyncMakeDirectory,
            TRpcRequestSettings::Make(settings),
            settings.ClientTimeout_);
    }

    TAsyncStatus RemoveDirectory(const TString& path, const TRemoveDirectorySettings& settings) {
        auto request = MakeOperationRequest<Ydb::Scheme::RemoveDirectoryRequest>(settings);
        request.set_path(path);

        return RunSimple<Ydb::Scheme::V1::SchemeService, RemoveDirectoryRequest, RemoveDirectoryResponse>(
            std::move(request),
            &Ydb::Scheme::V1::SchemeService::Stub::AsyncRemoveDirectory,
            TRpcRequestSettings::Make(settings),
            settings.ClientTimeout_);
    }

    TAsyncDescribePathResult DescribePath(const TString& path, const TDescribePathSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Scheme::DescribePathRequest>(settings);
        request.set_path(path);

        auto promise = NThreading::NewPromise<TDescribePathResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                TSchemeEntry entry;
                if (any) {
                    DescribePathResult result;
                    any->UnpackTo(&result);
                    entry.Name = result.self().name();
                    entry.Owner = result.self().owner();
                    entry.Type = ConvertProtoEntryType(result.self().type());
                    entry.SizeBytes = result.self().size_bytes();
                    PermissionToSchemeEntry(result.self().effective_permissions(), &entry.EffectivePermissions);
                    PermissionToSchemeEntry(result.self().permissions(), &entry.Permissions);
                }

                TDescribePathResult val(std::move(entry),
                    TStatus(std::move(status)));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Scheme::V1::SchemeService, DescribePathRequest, DescribePathResponse>(
            std::move(request),
            extractor,
            &Ydb::Scheme::V1::SchemeService::Stub::AsyncDescribePath,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings),
            settings.ClientTimeout_);

        return promise.GetFuture();
    }

    TAsyncListDirectoryResult ListDirectory(const TString& path, const TListDirectorySettings& settings) {
        auto request = MakeOperationRequest<Ydb::Scheme::ListDirectoryRequest>(settings);
        request.set_path(path);

        auto promise = NThreading::NewPromise<TListDirectoryResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                TSchemeEntry entry;
                TVector<TSchemeEntry> children;
                if (any) {
                    ListDirectoryResult result;
                    any->UnpackTo(&result);
                    entry.Name = result.self().name();
                    entry.Owner = result.self().owner();
                    entry.Type = ConvertProtoEntryType(result.self().type());

                    for (const auto& child : result.children()) {
                        TSchemeEntry tmp;
                        tmp.Name = child.name();
                        tmp.Owner = child.owner();
                        tmp.Type = ConvertProtoEntryType(child.type());
                        children.push_back(tmp);
                    }
                }

                TListDirectoryResult val(std::move(children), std::move(entry),
                    TStatus(std::move(status)));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Scheme::V1::SchemeService, ListDirectoryRequest, ListDirectoryResponse>(
            std::move(request),
            extractor,
            &Ydb::Scheme::V1::SchemeService::Stub::AsyncListDirectory,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings),
            settings.ClientTimeout_);

        return promise.GetFuture();

    }

    void PermissionsToRequest(const TPermissions& permissions, Permissions* to) {
        to->set_subject(permissions.Subject);
        for (const auto& perm : permissions.PermissionNames) {
            to->add_permission_names(perm);
        }
    }

    TAsyncStatus ModifyPermissions(const TString& path, const TModifyPermissionsSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Scheme::ModifyPermissionsRequest>(settings);
        request.set_path(path);
        if (settings.ClearAcl_) {
            request.set_clear_permissions(true);
        }

        for (const auto& action : settings.Actions_) {
            auto protoAction = request.add_actions();
            switch (action.first) {
                case EModifyPermissionsAction::Chown: {
                    protoAction->set_change_owner(action.second.Subject);
                }
                break;
                case EModifyPermissionsAction::Grant: {
                    PermissionsToRequest(action.second, protoAction->mutable_grant());
                }
                break;
                case EModifyPermissionsAction::Revoke: {
                    PermissionsToRequest(action.second, protoAction->mutable_revoke());
                }
                break;
                case EModifyPermissionsAction::Set: {
                    PermissionsToRequest(action.second, protoAction->mutable_set());
                }
                break;
            }
        }

        return RunSimple<Ydb::Scheme::V1::SchemeService, ModifyPermissionsRequest, ModifyPermissionsResponse>(
            std::move(request),
            &Ydb::Scheme::V1::SchemeService::Stub::AsyncModifyPermissions,
            TRpcRequestSettings::Make(settings),
            settings.ClientTimeout_);
    }

};

////////////////////////////////////////////////////////////////////////////////

TDescribePathResult::TDescribePathResult(TSchemeEntry&& entry, TStatus&& status)
    : TStatus(std::move(status))
    , Entry_(std::move(entry)) {}

TSchemeEntry TDescribePathResult::GetEntry() const {
    CheckStatusOk("TDescribePathResult::GetEntry");
    return Entry_;
}

////////////////////////////////////////////////////////////////////////////////

TListDirectoryResult::TListDirectoryResult(TVector<TSchemeEntry>&& children, TSchemeEntry&& self,
    TStatus&& status)
    : TDescribePathResult(std::move(self), std::move(status))
    , Children_(std::move(children)) {}

TVector<TSchemeEntry> TListDirectoryResult::GetChildren() const {
    CheckStatusOk("TListDirectoryResult::GetChildren");
    return Children_;
}

////////////////////////////////////////////////////////////////////////////////

TSchemeClient::TSchemeClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncStatus TSchemeClient::MakeDirectory(const TString& path, const TMakeDirectorySettings& settings) {
    return Impl_->MakeDirectory(path, settings);
}

TAsyncStatus TSchemeClient::RemoveDirectory(const TString &path, const TRemoveDirectorySettings& settings) {
    return Impl_->RemoveDirectory(path, settings);
}

TAsyncDescribePathResult TSchemeClient::DescribePath(const TString& path, const TDescribePathSettings& settings) {
    return Impl_->DescribePath(path, settings);
}

TAsyncListDirectoryResult TSchemeClient::ListDirectory(const TString& path,
    const TListDirectorySettings& settings)
{
    return Impl_->ListDirectory(path, settings);
}

TAsyncStatus TSchemeClient::ModifyPermissions(const TString& path,
    const TModifyPermissionsSettings& data)
{
    return Impl_->ModifyPermissions(path, data);
}

} // namespace NScheme
} // namespace NYdb
