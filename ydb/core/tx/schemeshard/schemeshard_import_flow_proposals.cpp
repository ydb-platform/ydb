#include "schemeshard_import_flow_proposals.h"
#include "schemeshard_path_describer.h"

#include <ydb/core/base/path.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr {
namespace NSchemeShard {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    TImportInfo::TPtr importInfo,
    ui32 itemIdx,
    TString& error
) {
    Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
    const auto& item = importInfo->Items.at(itemIdx);

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());
    auto& record = propose->Record;

    if (importInfo->UserSID) {
        record.SetOwner(*importInfo->UserSID);
    }

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
    modifyScheme.SetInternal(true);

    const TPath domainPath = TPath::Init(importInfo->DomainPathId, ss);

    std::pair<TString, TString> wdAndPath;
    if (!TrySplitPathByDb(item.DstPathName, domainPath.PathString(), wdAndPath, error)) {
        return nullptr;
    }

    modifyScheme.SetWorkingDir(wdAndPath.first);

    auto* indexedTable = modifyScheme.MutableCreateIndexedTable();
    auto& tableDesc = *(indexedTable->MutableTableDescription());
    tableDesc.SetName(wdAndPath.second);

    Y_ABORT_UNLESS(ss->TableProfilesLoaded);
    Ydb::StatusIds::StatusCode status;
    if (!FillTableDescription(modifyScheme, item.Scheme, ss->TableProfiles, status, error, true)) {
        return nullptr;
    }

    for(const auto& column: item.Scheme.columns()) {
        switch (column.default_value_case()) {
            case Ydb::Table::ColumnMeta::kFromSequence: {
                const auto& fromSequence = column.from_sequence();

                auto* seqDesc = indexedTable->MutableSequenceDescription()->Add();
                if (!FillSequenceDescription(*seqDesc, fromSequence, status, error)) {
                    return nullptr;
                }

                break;
            }
            case Ydb::Table::ColumnMeta::kFromLiteral: {
                break;
            }
            default: break;
        }
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    TImportInfo::TPtr importInfo,
    ui32 itemIdx
) {
    TString unused;
    return CreateTablePropose(ss, txId, importInfo, itemIdx, unused);
}

static NKikimrSchemeOp::TTableDescription GetTableDescription(TSchemeShard* ss, const TPathId& pathId) {
    auto desc = DescribePath(ss, TlsActivationContext->AsActorContext(), pathId);
    auto record = desc->GetRecord();

    Y_ABORT_UNLESS(record.HasPathDescription());
    Y_ABORT_UNLESS(record.GetPathDescription().HasTable());

    return record.GetPathDescription().GetTable();
}

static NKikimrSchemeOp::TTableDescription RebuildTableDescription(
    const NKikimrSchemeOp::TTableDescription& src,
    const Ydb::Table::CreateTableRequest& scheme
) {
    NKikimrSchemeOp::TTableDescription tableDesc;
    tableDesc.MutableKeyColumnNames()->CopyFrom(src.GetKeyColumnNames());

    THashMap<TString, ui32> columnNameToIdx;
    for (ui32 i = 0; i < src.ColumnsSize(); ++i) {
        Y_ABORT_UNLESS(columnNameToIdx.emplace(src.GetColumns(i).GetName(), i).second);
    }

    for (const auto& column : scheme.columns()) {
        auto it = columnNameToIdx.find(column.name());
        Y_ABORT_UNLESS(it != columnNameToIdx.end());

        Y_ABORT_UNLESS(it->second < src.ColumnsSize());
        tableDesc.MutableColumns()->Add()->CopyFrom(src.GetColumns(it->second));
    }

    return tableDesc;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> RestorePropose(
    TSchemeShard* ss,
    TTxId txId,
    TImportInfo::TPtr importInfo,
    ui32 itemIdx
) {
    Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
    const auto& item = importInfo->Items.at(itemIdx);

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());

    auto& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpRestore);
    modifyScheme.SetInternal(true);

    const TPath dstPath = TPath::Init(item.DstPathId, ss);
    Y_ABORT_UNLESS(dstPath.IsResolved());

    modifyScheme.SetWorkingDir(dstPath.Parent().PathString());

    auto& task = *modifyScheme.MutableRestore();
    task.SetTableName(dstPath.LeafName());
    *task.MutableTableDescription() = RebuildTableDescription(GetTableDescription(ss, item.DstPathId), item.Scheme);

    switch (importInfo->Kind) {
    case TImportInfo::EKind::S3:
        {
            task.SetNumberOfRetries(importInfo->Settings.number_of_retries());
            auto& restoreSettings = *task.MutableS3Settings();
            restoreSettings.SetEndpoint(importInfo->Settings.endpoint());
            restoreSettings.SetBucket(importInfo->Settings.bucket());
            restoreSettings.SetAccessKey(importInfo->Settings.access_key());
            restoreSettings.SetSecretKey(importInfo->Settings.secret_key());
            restoreSettings.SetObjectKeyPattern(importInfo->Settings.items(itemIdx).source_prefix());
            restoreSettings.SetUseVirtualAddressing(!importInfo->Settings.disable_virtual_addressing());

            switch (importInfo->Settings.scheme()) {
            case Ydb::Import::ImportFromS3Settings::HTTP:
                restoreSettings.SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
                break;
            case Ydb::Import::ImportFromS3Settings::HTTPS:
                restoreSettings.SetScheme(NKikimrSchemeOp::TS3Settings::HTTPS);
                break;
            default:
                Y_ABORT("Unknown scheme");
            }

            if (const auto region = importInfo->Settings.region()) {
                restoreSettings.SetRegion(region);
            }
        }
        break;
    }

    return propose;
}

THolder<TEvSchemeShard::TEvCancelTx> CancelRestorePropose(
    TImportInfo::TPtr importInfo,
    TTxId restoreTxId
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvCancelTx>();

    auto& record = propose->Record;
    record.SetTxId(importInfo->Id);
    record.SetTargetTxId(ui64(restoreTxId));

    return propose;
}

THolder<TEvIndexBuilder::TEvCreateRequest> BuildIndexPropose(
    TSchemeShard* ss,
    TTxId txId,
    TImportInfo::TPtr importInfo,
    ui32 itemIdx,
    const TString& uid
) {
    Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
    const auto& item = importInfo->Items.at(itemIdx);

    NKikimrIndexBuilder::TIndexBuildSettings settings;

    const TPath dstPath = TPath::Init(item.DstPathId, ss);
    settings.set_source_path(dstPath.PathString());

    Y_ABORT_UNLESS(item.NextIndexIdx < item.Scheme.indexes_size());
    settings.mutable_index()->CopyFrom(item.Scheme.indexes(item.NextIndexIdx));

    if (settings.mutable_index()->type_case() == Ydb::Table::TableIndex::TypeCase::TYPE_NOT_SET) {
        settings.mutable_index()->mutable_global_index();
    }

    const TPath domainPath = TPath::Init(importInfo->DomainPathId, ss);
    auto propose = MakeHolder<TEvIndexBuilder::TEvCreateRequest>(ui64(txId), domainPath.PathString(), std::move(settings));
    (*propose->Record.MutableOperationParams()->mutable_labels())["uid"] = uid;

    return propose;
}

THolder<TEvIndexBuilder::TEvCancelRequest> CancelIndexBuildPropose(
    TSchemeShard* ss,
    TImportInfo::TPtr importInfo,
    TTxId indexBuildId
) {
    const TPath domainPath = TPath::Init(importInfo->DomainPathId, ss);
    return MakeHolder<TEvIndexBuilder::TEvCancelRequest>(ui64(indexBuildId), domainPath.PathString(), ui64(indexBuildId));
}

} // NSchemeShard
} // NKikimr
