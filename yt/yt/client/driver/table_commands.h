#pragma once

#include "command.h"

#include <yt/yt/client/formats/format.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadTableCommand
    : public TTypedCommand<NApi::TTableReaderOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TReadTableCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableReader;
    NFormats::TControlAttributesConfigPtr ControlAttributes;
    bool Unordered;
    bool StartRowIndexOnly;

    void DoExecute(ICommandContextPtr context) override;
    bool HasResponseParameters() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TReadBlobTableCommand
    : public TTypedCommand<NApi::TTableReaderOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TReadBlobTableCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableReader;

    std::optional<TString> PartIndexColumnName;
    std::optional<TString> DataColumnName;

    i64 StartPartIndex;
    i64 Offset;
    i64 PartSize;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TLocateSkynetShareCommand
    : public TTypedCommand<NApi::TLocateSkynetShareOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TLocateSkynetShareCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteTableCommand
    : public TTypedCommand<NApi::TTableWriterOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TWriteTableCommand);

    static void Register(TRegistrar registrar);

protected:
    virtual TFuture<NApi::ITableWriterPtr> CreateTableWriter(
        const ICommandContextPtr& context) const;

    void DoExecuteImpl(const ICommandContextPtr& context);

private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableWriter;
    i64 MaxRowBufferSize;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetTableColumnarStatisticsCommand
    : public TTypedCommand<NApi::TGetColumnarStatisticsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetTableColumnarStatisticsCommand);

    static void Register(TRegistrar registrar);

private:
    std::vector<NYPath::TRichYPath> Paths;
    NTableClient::EColumnarStatisticsFetcherMode FetcherMode;
    std::optional<int> MaxChunksPerNodeFetch;
    bool EnableEarlyFinish;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionTablesCommand
    : public TTypedCommand<NApi::TPartitionTablesOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TPartitionTablesCommand);

    static void Register(TRegistrar registrar);

private:
    std::vector<NYPath::TRichYPath> Paths;
    NTableClient::ETablePartitionMode PartitionMode;
    i64 DataWeightPerPartition;
    std::optional<int> MaxPartitionCount;
    bool EnableKeyGuarantee;

    //! Treat the #DataWeightPerPartition as a hint and not as a maximum limit.
    //! Consider the situation when the #MaxPartitionCount is given
    //! and the total data weight exceeds #MaxPartitionCount * #DataWeightPerPartition.
    //! If #AdjustDataWeightPerPartition is |true|
    //! the #partition_tables command will yield partitions exceeding the #DataWeightPerPartition.
    //! If #AdjustDataWeightPerPartition is |false|
    //! the #partition_tables command will throw an exception.
    bool AdjustDataWeightPerPartition;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
class TTabletCommandBase
    : public TTypedCommand<TOptions>
{
protected:
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT_LITE(TTabletCommandBase);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path);

        registrar.template ParameterWithUniversalAccessor<std::optional<int>>(
            "first_tablet_index",
            [] (TThis* command) -> auto& {
                return command->Options.FirstTabletIndex;
            })
            .Default();

        registrar.template ParameterWithUniversalAccessor<std::optional<int>>(
            "last_tablet_index",
            [] (TThis* command) -> auto& {
                return command->Options.LastTabletIndex;
            })
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMountTableCommand
    : public TTabletCommandBase<NApi::TMountTableOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TMountTableCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUnmountTableCommand
    : public TTabletCommandBase<NApi::TUnmountTableOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TUnmountTableCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemountTableCommand
    : public TTabletCommandBase<NApi::TRemountTableOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TRemountTableCommand);

    static void Register(TRegistrar /*registrar*/)
    { }

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TFreezeTableCommand
    : public TTabletCommandBase<NApi::TFreezeTableOptions>
{
    REGISTER_YSON_STRUCT_LITE(TFreezeTableCommand);

    static void Register(TRegistrar /*registrar*/)
    { }

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUnfreezeTableCommand
    : public TTabletCommandBase<NApi::TUnfreezeTableOptions>
{
    REGISTER_YSON_STRUCT_LITE(TUnfreezeTableCommand);

    static void Register(TRegistrar /*registrar*/)
    { }

public:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TReshardTableCommand
    : public TTabletCommandBase<NApi::TReshardTableOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TReshardTableCommand);

    static void Register(TRegistrar registrar);

private:
    std::optional<std::vector<NTableClient::TLegacyOwningKey>> PivotKeys;
    std::optional<int> TabletCount;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TReshardTableAutomaticCommand
    : public TTabletCommandBase<NApi::TReshardTableAutomaticOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TReshardTableAutomaticCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAlterTableCommand
    : public TTypedCommand<NApi::TAlterTableOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAlterTableCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TSelectRowsOptions
    : public NApi::TSelectRowsOptions
    , public TTabletTransactionOptions
{ };

class TSelectRowsCommand
    : public TTypedCommand<TSelectRowsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TSelectRowsCommand);

    static void Register(TRegistrar registrar);

private:
    TString Query;
    NYTree::IMapNodePtr PlaceholderValues;
    bool EnableStatistics = false;

    void DoExecute(ICommandContextPtr context) override;
    bool HasResponseParameters() const override;
};

////////////////////////////////////////////////////////////////////////////////

struct TExplainQueryOptions
    : public NApi::TExplainQueryOptions
    , public TTabletTransactionOptions
{ };

class TExplainQueryCommand
    : public TTypedCommand<TExplainQueryOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TExplainQueryCommand);

    static void Register(TRegistrar registrar);

private:
    TString Query;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TInsertRowsCommand
    : public TTypedCommand<TInsertRowsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TInsertRowsCommand);

    static void Register(TRegistrar registrar);

private:
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;
    bool Update;
    bool Aggregate;
    NTableClient::ELockType LockType;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TLookupRowsOptions
    : public NApi::TLookupRowsOptions
    , public TTabletTransactionOptions
{ };

class TLookupRowsCommand
    : public TTypedCommand<TLookupRowsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TLookupRowsCommand);

    static void Register(TRegistrar registrar);

private:
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;
    std::optional<std::vector<TString>> ColumnNames;
    bool Versioned;
    NTableClient::TRetentionConfigPtr RetentionConfig;

    void DoExecute(ICommandContextPtr context) override;
    bool HasResponseParameters() const override;
};

////////////////////////////////////////////////////////////////////////////////

struct TPullRowsOptions
    : public NApi::TPullRowsOptions
{ };

class TPullRowsCommand
    : public TTypedCommand<TPullRowsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TPullRowsCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;

    virtual void DoExecute(ICommandContextPtr context) override;
    bool HasResponseParameters() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetInSyncReplicasCommand
    : public TTypedCommand<NApi::TGetInSyncReplicasOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetInSyncReplicasCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    bool AllKeys;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TDeleteRowsCommand
    : public TTypedCommand<TDeleteRowsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TDeleteRowsCommand);

    static void Register(TRegistrar registrar);

private:
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TLockRowsCommand
    : public TTypedCommand<TLockRowsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TLockRowsCommand);

    static void Register(TRegistrar registrar);

private:
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;
    std::vector<TString> Locks;
    NTableClient::ELockType LockType;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TTrimRowsCommand
    : public TTypedCommand<NApi::TTrimTableOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TTrimRowsCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    int TabletIndex;
    i64 TrimmedRowCount;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TEnableTableReplicaCommand
    : public TTypedCommand<NApi::TAlterTableReplicaOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TEnableTableReplicaCommand);

    static void Register(TRegistrar registrar);

private:
    NTabletClient::TTableReplicaId ReplicaId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TDisableTableReplicaCommand
    : public TTypedCommand<NApi::TAlterTableReplicaOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TDisableTableReplicaCommand);

    static void Register(TRegistrar registrar);

private:
    NTabletClient::TTableReplicaId ReplicaId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAlterTableReplicaCommand
    : public TTypedCommand<NApi::TAlterTableReplicaOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAlterTableReplicaCommand);

    static void Register(TRegistrar registrar);

private:
    NTabletClient::TTableReplicaId ReplicaId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetTabletInfosCommand
    : public TTypedCommand<NApi::TGetTabletInfosOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetTabletInfosCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath Path;
    std::vector<int> TabletIndexes;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetTabletErrorsCommand
    : public TTypedCommand<NApi::TGetTabletErrorsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetTabletErrorsCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetTablePivotKeysCommand
    : public TTypedCommand<NApi::TGetTablePivotKeysOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetTablePivotKeysCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateTableBackupCommand
    : public TTypedCommand<NApi::TCreateTableBackupOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TCreateTableBackupCommand);

    static void Register(TRegistrar registrar);

private:
    NApi::TBackupManifestPtr Manifest;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRestoreTableBackupCommand
    : public TTypedCommand<NApi::TRestoreTableBackupOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TRestoreTableBackupCommand);

    static void Register(TRegistrar registrar);

private:
    NApi::TBackupManifestPtr Manifest;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
