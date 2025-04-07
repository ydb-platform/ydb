#pragma once

namespace NYdb::inline Dev::NTable {

class TKeyBound;
class TKeyRange;

struct TTableColumn;
struct TAlterTableColumn;

struct TPartitionStats;

struct TSequenceDescription;
class TChangefeedDescription;
class TIndexDescription;
class TColumnFamilyDescription;
class TTableDescription;

struct TExplicitPartitions;

struct TRenameIndex;

struct TGlobalIndexSettings;
struct TVectorIndexSettings;
struct TKMeansTreeSettings;
struct TCreateSessionSettings;
struct TSessionPoolSettings;
struct TClientSettings;
struct TBulkUpsertSettings;
struct TReadRowsSettings;
struct TStreamExecScanQuerySettings;
struct TTxOnlineSettings;
struct TCreateTableSettings;
struct TDropTableSettings;
struct TAlterTableSettings;
struct TCopyTableSettings;
struct TCopyTablesSettings;
struct TRenameTablesSettings;
struct TDescribeTableSettings;
struct TExplainDataQuerySettings;
struct TPrepareDataQuerySettings;
struct TExecDataQuerySettings;
struct TExecSchemeQuerySettings;
struct TBeginTxSettings;
struct TCommitTxSettings;
struct TRollbackTxSettings;
struct TCloseSessionSettings;
struct TKeepAliveSettings;
struct TReadTableSettings;

class TPartitioningSettings;
class TDateTypeColumnModeSettings;
class TValueSinceUnixEpochModeSettings;
class TTtlTierSettings;
class TTtlSettings;
class TAlterTtlSettings;
class TStorageSettings;
class TReadReplicasSettings;
class TTxSettings;

struct TColumnFamilyPolicy;
struct TStoragePolicy;
struct TPartitioningPolicy;
struct TReplicationPolicy;

class TBuildIndexOperation;

class TTtlDeleteAction;
class TTtlEvictToExternalStorageAction;

class TStorageSettingsBuilder;
class TPartitioningSettingsBuilder;
class TColumnFamilyBuilder;
class TTableStorageSettingsBuilder;
class TTableColumnFamilyBuilder;
class TTablePartitioningSettingsBuilder;
class TTableBuilder;
class TAlterStorageSettingsBuilder;
class TAlterColumnFamilyBuilder;
class TAlterTtlSettingsBuilder;
class TAlterAttributesBuilder;
class TAlterPartitioningSettingsBuilder;

class TPrepareQueryResult;
class TExplainQueryResult;
class TDescribeTableResult;
class TDataQueryResult;
class TBeginTransactionResult;
class TCommitTransactionResult;
class TCreateSessionResult;
class TKeepAliveResult;
class TBulkUpsertResult;
class TReadRowsResult;

template<typename TPart>
class TSimpleStreamPart;

class TScanQueryPart;

class TTablePartIterator;
class TScanQueryPartIterator;

class TReadTableSnapshot;

class TCopyItem;
class TRenameItem;

class TDataQuery;

class TTransaction;
class TTxControl;

class TSession;
class TTableClient;

}  // namespace NYdb
