#include "helpers.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include <ydb/public/api/protos/ydb_scheme.pb.h>

namespace NYdb::inline Dev {
namespace NScheme {

namespace {

::Ydb::Scheme::Entry::Type SchemeEntryTypeToProto(ESchemeEntryType type) {
    switch (type) {
    case ESchemeEntryType::Directory:
        return ::Ydb::Scheme::Entry::DIRECTORY;
    case ESchemeEntryType::Table:
        return ::Ydb::Scheme::Entry::TABLE;
    case ESchemeEntryType::ColumnTable:
        return ::Ydb::Scheme::Entry::COLUMN_TABLE;
    case ESchemeEntryType::PqGroup:
        return ::Ydb::Scheme::Entry::PERS_QUEUE_GROUP;
    case ESchemeEntryType::SubDomain:
        return ::Ydb::Scheme::Entry::DATABASE;
    case ESchemeEntryType::RtmrVolume:
        return ::Ydb::Scheme::Entry::RTMR_VOLUME;
    case ESchemeEntryType::BlockStoreVolume:
        return ::Ydb::Scheme::Entry::BLOCK_STORE_VOLUME;
    case ESchemeEntryType::CoordinationNode:
        return ::Ydb::Scheme::Entry::COORDINATION_NODE;
    case ESchemeEntryType::Sequence:
        return ::Ydb::Scheme::Entry::SEQUENCE;
    case ESchemeEntryType::Replication:
        return ::Ydb::Scheme::Entry::REPLICATION;
    case ESchemeEntryType::Topic:
        return ::Ydb::Scheme::Entry::TOPIC;
    case ESchemeEntryType::ColumnStore:
        return ::Ydb::Scheme::Entry::COLUMN_STORE;
    case ESchemeEntryType::ExternalTable:
        return ::Ydb::Scheme::Entry::EXTERNAL_TABLE;
    case ESchemeEntryType::ExternalDataSource:
        return ::Ydb::Scheme::Entry::EXTERNAL_DATA_SOURCE;
    case ESchemeEntryType::View:
        return ::Ydb::Scheme::Entry::VIEW;
    case ESchemeEntryType::ResourcePool:
        return ::Ydb::Scheme::Entry::RESOURCE_POOL;
    case ESchemeEntryType::BackupCollection:
        return ::Ydb::Scheme::Entry::BACKUP_COLLECTION;
    case ESchemeEntryType::SysView:
        return ::Ydb::Scheme::Entry::SYS_VIEW;
    case ESchemeEntryType::Transfer:
        return ::Ydb::Scheme::Entry::TRANSFER;
    case ESchemeEntryType::StreamingQuery:
        return ::Ydb::Scheme::Entry::STREAMING_QUERY;
    case ESchemeEntryType::Secret:
        return ::Ydb::Scheme::Entry::SECRET;
    case ESchemeEntryType::Unknown:
    default:
        return ::Ydb::Scheme::Entry::TYPE_UNSPECIFIED;
    }
}

} // namespace

void SchemeEntryToProto(const TSchemeEntry& entry, ::Ydb::Scheme::Entry* proto) {
    proto->set_name(TStringType{entry.Name});
    proto->set_owner(TStringType{entry.Owner});
    proto->set_type(SchemeEntryTypeToProto(entry.Type));
    proto->set_size_bytes(entry.SizeBytes);
    auto& timestamp = *proto->mutable_created_at();
    timestamp.set_plan_step(entry.CreatedAt.PlanStep);
    timestamp.set_tx_id(entry.CreatedAt.TxId);
    for (const auto& permission : entry.Permissions) {
        permission.SerializeTo(*proto->add_permissions());
    }
    for (const auto& permission : entry.EffectivePermissions) {
        permission.SerializeTo(*proto->add_effective_permissions());
    }
}

} // namespace NScheme
} // namespace NYdb
