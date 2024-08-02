#include "scheme.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/scheme_helpers/helpers.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

#include <util/string/join.h>

namespace NYdb {
namespace NScheme {

using namespace NThreading;
using namespace Ydb::Scheme;

void TPermissions::SerializeTo(::Ydb::Scheme::Permissions& proto) const {
    proto.set_subject(Subject);
    for (const auto& name : PermissionNames) {
        *proto.mutable_permission_names()->Add() = name;
    }
}

TVirtualTimestamp::TVirtualTimestamp(ui64 planStep, ui64 txId)
    : PlanStep(planStep)
    , TxId(txId)
{}

TVirtualTimestamp::TVirtualTimestamp(const ::Ydb::VirtualTimestamp& proto)
    : TVirtualTimestamp(proto.plan_step(), proto.tx_id())
{}

TString TVirtualTimestamp::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
}

void TVirtualTimestamp::Out(IOutputStream& out) const {
    out << "{ plan_step: " << PlanStep
        << ", tx_id: " << TxId
        << " }";
}

bool TVirtualTimestamp::operator<(const TVirtualTimestamp& rhs) const {
    return PlanStep < rhs.PlanStep && TxId < rhs.TxId;
}

bool TVirtualTimestamp::operator<=(const TVirtualTimestamp& rhs) const {
    return PlanStep <= rhs.PlanStep && TxId <= rhs.TxId;
}

bool TVirtualTimestamp::operator>(const TVirtualTimestamp& rhs) const {
    return PlanStep > rhs.PlanStep && TxId > rhs.TxId;
}

bool TVirtualTimestamp::operator>=(const TVirtualTimestamp& rhs) const {
    return PlanStep >= rhs.PlanStep && TxId >= rhs.TxId;
}

bool TVirtualTimestamp::operator==(const TVirtualTimestamp& rhs) const {
    return PlanStep == rhs.PlanStep && TxId == rhs.TxId;
}

bool TVirtualTimestamp::operator!=(const TVirtualTimestamp& rhs) const {
    return !(*this == rhs);
}

static ESchemeEntryType ConvertProtoEntryType(::Ydb::Scheme::Entry::Type entry) {
    switch (entry) {
    case ::Ydb::Scheme::Entry::DIRECTORY:
        return ESchemeEntryType::Directory;
    case ::Ydb::Scheme::Entry::TABLE:
        return ESchemeEntryType::Table;
    case ::Ydb::Scheme::Entry::COLUMN_TABLE:
        return ESchemeEntryType::ColumnTable;
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
    case ::Ydb::Scheme::Entry::TOPIC:
        return ESchemeEntryType::Topic;
    case ::Ydb::Scheme::Entry::COLUMN_STORE:
        return ESchemeEntryType::ColumnStore;
    case ::Ydb::Scheme::Entry::EXTERNAL_TABLE:
        return ESchemeEntryType::ExternalTable;
    case ::Ydb::Scheme::Entry::EXTERNAL_DATA_SOURCE:
        return ESchemeEntryType::ExternalDataSource;
    case ::Ydb::Scheme::Entry::VIEW:
        return ESchemeEntryType::View;
    case ::Ydb::Scheme::Entry::RESOURCE_POOL:
        return ESchemeEntryType::ResourcePool;
    default:
        return ESchemeEntryType::Unknown;
    }
}

TSchemeEntry::TSchemeEntry(const ::Ydb::Scheme::Entry& proto)
    : Name(proto.name())
    , Owner(proto.owner())
    , Type(ConvertProtoEntryType(proto.type()))
    , SizeBytes(proto.size_bytes())
    , CreatedAt(proto.created_at())
{
    PermissionToSchemeEntry(proto.effective_permissions(), &EffectivePermissions);
    PermissionToSchemeEntry(proto.permissions(), &Permissions);
}

void TSchemeEntry::Out(IOutputStream& out) const {
    out << "{ name: " << Name
        << ", owner: " << Owner
        << ", type: " << Type
        << ", size_bytes: " << SizeBytes
        << ", created_at: " << CreatedAt
        << " }";
}

void TSchemeEntry::SerializeTo(::Ydb::Scheme::ModifyPermissionsRequest& request) const {
    request.mutable_actions()->Add()->set_change_owner(Owner);
    for (const auto& permission : Permissions) {
        permission.SerializeTo(*request.mutable_actions()->Add()->mutable_set());
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
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus RemoveDirectory(const TString& path, const TRemoveDirectorySettings& settings) {
        auto request = MakeOperationRequest<Ydb::Scheme::RemoveDirectoryRequest>(settings);
        request.set_path(path);

        return RunSimple<Ydb::Scheme::V1::SchemeService, RemoveDirectoryRequest, RemoveDirectoryResponse>(
            std::move(request),
            &Ydb::Scheme::V1::SchemeService::Stub::AsyncRemoveDirectory,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncDescribePathResult DescribePath(const TString& path, const TDescribePathSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Scheme::DescribePathRequest>(settings);
        request.set_path(path);

        auto promise = NThreading::NewPromise<TDescribePathResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                DescribePathResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                promise.SetValue(TDescribePathResult(TStatus(std::move(status)), result.self()));
            };

        Connections_->RunDeferred<Ydb::Scheme::V1::SchemeService, DescribePathRequest, DescribePathResponse>(
            std::move(request),
            extractor,
            &Ydb::Scheme::V1::SchemeService::Stub::AsyncDescribePath,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncListDirectoryResult ListDirectory(const TString& path, const TListDirectorySettings& settings) {
        auto request = MakeOperationRequest<Ydb::Scheme::ListDirectoryRequest>(settings);
        request.set_path(path);

        auto promise = NThreading::NewPromise<TListDirectoryResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                ListDirectoryResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TVector<TSchemeEntry> children(Reserve(result.children().size()));
                for (const auto& child : result.children()) {
                    children.emplace_back(child);
                }

                promise.SetValue(TListDirectoryResult(TStatus(std::move(status)), result.self(), std::move(children)));
            };

        Connections_->RunDeferred<Ydb::Scheme::V1::SchemeService, ListDirectoryRequest, ListDirectoryResponse>(
            std::move(request),
            extractor,
            &Ydb::Scheme::V1::SchemeService::Stub::AsyncListDirectory,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

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
            TRpcRequestSettings::Make(settings));
    }

};

////////////////////////////////////////////////////////////////////////////////

TDescribePathResult::TDescribePathResult(TStatus&& status, const TSchemeEntry& entry)
    : TStatus(std::move(status))
    , Entry_(entry)
{}

const TSchemeEntry& TDescribePathResult::GetEntry() const {
    CheckStatusOk("TDescribePathResult::GetEntry");
    return Entry_;
}

void TDescribePathResult::Out(IOutputStream& out) const {
    if (IsSuccess()) {
        return Entry_.Out(out);
    } else {
        return TStatus::Out(out);
    }
}

////////////////////////////////////////////////////////////////////////////////

TListDirectoryResult::TListDirectoryResult(TStatus&& status, const TSchemeEntry& self, TVector<TSchemeEntry>&& children)
    : TDescribePathResult(std::move(status), self)
    , Children_(std::move(children))
{}

const TVector<TSchemeEntry>& TListDirectoryResult::GetChildren() const {
    CheckStatusOk("TListDirectoryResult::GetChildren");
    return Children_;
}

void TListDirectoryResult::Out(IOutputStream& out) const {
    if (IsSuccess()) {
        out << "{ children [" << JoinSeq(", ", Children_) << "] }";
    } else {
        return TStatus::Out(out);
    }
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
