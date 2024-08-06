#pragma once

#include "table_profiles.h"

#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr {

enum class EAlterOperationKind {
    // columns, column families, storage, ttl
    Common,
    // add indices
    AddIndex,
    // drop indices
    DropIndex,
    // add/alter/drop attributes
    Attribute,
    // add changefeeds
    AddChangefeed,
    // drop changefeeds
    DropChangefeed,
    // rename index
    RenameIndex,
};

struct TPathId;


THashSet<EAlterOperationKind> GetAlterOperationKinds(const Ydb::Table::AlterTableRequest* req);
bool BuildAlterTableModifyScheme(const TString& path, const Ydb::Table::AlterTableRequest* req, NKikimrSchemeOp::TModifyScheme* modifyScheme,
    const TTableProfiles& profiles, const TPathId& resolvedPathId,
    Ydb::StatusIds::StatusCode& status, TString& error);
bool BuildAlterTableModifyScheme(const Ydb::Table::AlterTableRequest* req, NKikimrSchemeOp::TModifyScheme* modifyScheme,
    const TTableProfiles& profiles, const TPathId& resolvedPathId,
    Ydb::StatusIds::StatusCode& status, TString& error);

bool FillAlterTableSettingsDesc(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::AlterTableRequest& in, const TTableProfiles& profiles,
    Ydb::StatusIds::StatusCode& code, TString& error, const TAppData* appData);

bool BuildAlterTableAddIndexRequest(const Ydb::Table::AlterTableRequest* req, NKikimrIndexBuilder::TIndexBuildSettings* settings,
    ui64 flags,
    Ydb::StatusIds::StatusCode& status, TString& error);

// out
void FillColumnDescription(Ydb::Table::DescribeTableResult& out,
    NKikimrMiniKQL::TType& splitKeyType, const NKikimrSchemeOp::TTableDescription& in);
void FillColumnDescription(Ydb::Table::CreateTableRequest& out,
    NKikimrMiniKQL::TType& splitKeyType, const NKikimrSchemeOp::TTableDescription& in);
void FillColumnDescription(Ydb::Table::DescribeTableResult& out, const NKikimrSchemeOp::TColumnTableDescription& in);
// in
bool FillColumnDescription(NKikimrSchemeOp::TTableDescription& out,
    const google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta>& in, Ydb::StatusIds::StatusCode& status, TString& error);
bool FillColumnDescription(NKikimrSchemeOp::TColumnTableDescription& out,
    const google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta>& in, Ydb::StatusIds::StatusCode& status, TString& error);
bool ExtractColumnTypeInfo(NScheme::TTypeInfo& outTypeInfo, TString& outTypeMod,
    const Ydb::Type& inType, Ydb::StatusIds::StatusCode& status, TString& error);

// out
void FillTableBoundary(Ydb::Table::DescribeTableResult& out,
    const NKikimrSchemeOp::TTableDescription& in, const NKikimrMiniKQL::TType& splitKeyType);
void FillTableBoundary(Ydb::Table::CreateTableRequest& out,
    const NKikimrSchemeOp::TTableDescription& in, const NKikimrMiniKQL::TType& splitKeyType);

// out
void FillIndexDescription(Ydb::Table::DescribeTableResult& out,
    const NKikimrSchemeOp::TTableDescription& in);
void FillIndexDescription(Ydb::Table::CreateTableRequest& out,
    const NKikimrSchemeOp::TTableDescription& in);
// in
bool FillIndexDescription(NKikimrSchemeOp::TIndexedTableCreationConfig& out,
    const Ydb::Table::CreateTableRequest& in, Ydb::StatusIds::StatusCode& status, TString& error);

// out
void FillChangefeedDescription(Ydb::Table::DescribeTableResult& out,
    const NKikimrSchemeOp::TTableDescription& in);
// in
bool FillChangefeedDescription(NKikimrSchemeOp::TCdcStreamDescription& out,
    const Ydb::Table::Changefeed& in, Ydb::StatusIds::StatusCode& status, TString& error);

// out
void FillTableStats(Ydb::Table::DescribeTableResult& out,
    const NKikimrSchemeOp::TPathDescription& in, bool withPartitionStatistic);

// out
void FillStorageSettings(Ydb::Table::DescribeTableResult& out,
    const NKikimrSchemeOp::TTableDescription& in);
void FillStorageSettings(Ydb::Table::CreateTableRequest& out,
    const NKikimrSchemeOp::TTableDescription& in);

// out
void FillColumnFamilies(Ydb::Table::DescribeTableResult& out,
    const NKikimrSchemeOp::TTableDescription& in);
void FillColumnFamilies(Ydb::Table::CreateTableRequest& out,
    const NKikimrSchemeOp::TTableDescription& in);

// out
void FillAttributes(Ydb::Table::DescribeTableResult& out,
    const NKikimrSchemeOp::TPathDescription& in);
void FillAttributes(Ydb::Table::CreateTableRequest& out,
    const NKikimrSchemeOp::TPathDescription& in);

// out
void FillPartitioningSettings(Ydb::Table::DescribeTableResult& out,
    const NKikimrSchemeOp::TTableDescription& in);
void FillPartitioningSettings(Ydb::Table::CreateTableRequest& out,
    const NKikimrSchemeOp::TTableDescription& in);

// in
bool CopyExplicitPartitions(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::ExplicitPartitions& in, Ydb::StatusIds::StatusCode& status, TString& error);

// out
void FillKeyBloomFilter(Ydb::Table::DescribeTableResult& out,
    const NKikimrSchemeOp::TTableDescription& in);
void FillKeyBloomFilter(Ydb::Table::CreateTableRequest& out,
    const NKikimrSchemeOp::TTableDescription& in);

// out
void FillReadReplicasSettings(Ydb::Table::DescribeTableResult& out,
    const NKikimrSchemeOp::TTableDescription& in);
void FillReadReplicasSettings(Ydb::Table::CreateTableRequest& out,
    const NKikimrSchemeOp::TTableDescription& in);

// in
bool FillTableDescription(NKikimrSchemeOp::TModifyScheme& out,
    const Ydb::Table::CreateTableRequest& in, const TTableProfiles& profiles,
    Ydb::StatusIds::StatusCode& status, TString& error, bool indexedTable = false);


// out
void FillSequenceDescription(Ydb::Table::CreateTableRequest& out,
    const NKikimrSchemeOp::TTableDescription& in);

// out
void FillSequenceDescription(Ydb::Table::CreateTableRequest& out,
    const NKikimrSchemeOp::TTableDescription& in);

} // namespace NKikimr
