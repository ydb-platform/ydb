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
    TReadTableCommand();

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
    TReadBlobTableCommand();

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
    TLocateSkynetShareCommand();

private:
    NYPath::TRichYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteTableCommand
    : public TTypedCommand<NApi::TTableWriterOptions>
{
public:
    TWriteTableCommand();

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
    TGetTableColumnarStatisticsCommand();

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
    TPartitionTablesCommand();

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

    TTabletCommandBase()
    {
        this->RegisterParameter("path", Path);
        this->RegisterParameter("first_tablet_index", this->Options.FirstTabletIndex)
            .Default();
        this->RegisterParameter("last_tablet_index", this->Options.LastTabletIndex)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMountTableCommand
    : public TTabletCommandBase<NApi::TMountTableOptions>
{
public:
    TMountTableCommand();

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUnmountTableCommand
    : public TTabletCommandBase<NApi::TUnmountTableOptions>
{
public:
    TUnmountTableCommand();

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemountTableCommand
    : public TTabletCommandBase<NApi::TRemountTableOptions>
{
public:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TFreezeTableCommand
    : public TTabletCommandBase<NApi::TFreezeTableOptions>
{
private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUnfreezeTableCommand
    : public TTabletCommandBase<NApi::TUnfreezeTableOptions>
{
public:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TReshardTableCommand
    : public TTabletCommandBase<NApi::TReshardTableOptions>
{
public:
    TReshardTableCommand();

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
    TReshardTableAutomaticCommand();

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAlterTableCommand
    : public TTypedCommand<NApi::TAlterTableOptions>
{
public:
    TAlterTableCommand();

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
    TSelectRowsCommand();

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
    TExplainQueryCommand();

private:
    TString Query;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TInsertRowsCommand
    : public TTypedCommand<TInsertRowsOptions>
{
public:
    TInsertRowsCommand();

private:
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;
    bool Update;
    bool Aggregate;

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
    TLookupRowsCommand();

private:
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;
    std::optional<std::vector<TString>> ColumnNames;
    bool Versioned;
    NTableClient::TRetentionConfigPtr RetentionConfig;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TPullRowsOptions
    : public NApi::TPullRowsOptions
{ };

class TPullRowsCommand
    : public TTypedCommand<TPullRowsOptions>
{
public:
    TPullRowsCommand();

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
    TGetInSyncReplicasCommand();

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
    TDeleteRowsCommand();

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
    TLockRowsCommand();

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
    TTrimRowsCommand();

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
    TEnableTableReplicaCommand();

private:
    NTabletClient::TTableReplicaId ReplicaId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TDisableTableReplicaCommand
    : public TTypedCommand<NApi::TAlterTableReplicaOptions>
{
public:
    TDisableTableReplicaCommand();

private:
    NTabletClient::TTableReplicaId ReplicaId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAlterTableReplicaCommand
    : public TTypedCommand<NApi::TAlterTableReplicaOptions>
{
public:
    TAlterTableReplicaCommand();

private:
    NTabletClient::TTableReplicaId ReplicaId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetTabletInfosCommand
    : public TTypedCommand<NApi::TGetTabletInfosOptions>
{
public:
    TGetTabletInfosCommand();

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
    TGetTabletErrorsCommand();

private:
    NYPath::TYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetTablePivotKeysCommand
    : public TTypedCommand<NApi::TGetTablePivotKeysOptions>
{
public:
    TGetTablePivotKeysCommand();

private:
    NYPath::TYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateTableBackupCommand
    : public TTypedCommand<NApi::TCreateTableBackupOptions>
{
public:
    TCreateTableBackupCommand();

private:
    NApi::TBackupManifestPtr Manifest;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRestoreTableBackupCommand
    : public TTypedCommand<NApi::TRestoreTableBackupOptions>
{
public:
    TRestoreTableBackupCommand();

private:
    NApi::TBackupManifestPtr Manifest;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
