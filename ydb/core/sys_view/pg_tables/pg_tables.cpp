#include "pg_tables.h"
#include <util/string/builder.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/hfunc.h>


namespace NKikimr {
namespace NSysView {

using namespace NActors;

TStringBuf TPgTablesScanBase::GetColumnName(NTable::TTag tag) const {
    Y_ENSURE(tag > 0 && tag <= SchemaColumns_.size());
    return SchemaColumns_.at(tag - 1)._ColumnName;
}

TCell TPgTablesScanBase::MakePgCell(const Schema::PgColumn& column, const TString& value, TVector<TString>& cellData) {
    NYql::NUdf::TStringRef ref;
    auto convert = NPg::PgNativeBinaryFromNativeText(value, NPg::PgTypeIdFromTypeDesc(column._ColumnTypeInfo.GetTypeDesc()));
    if (convert.Error) {
        ConvertError_ = *convert.Error;
        return TCell();
    }
    cellData.emplace_back(convert.Str);
    ref = NYql::NUdf::TStringRef(cellData.back());
    Y_ENSURE(ref.Size() > 0);
    return TCell(ref.Data(), ref.Size());
}

TVector<TCell> TPgTablesScanBase::MakePgTablesRow(const TString& tableName, const TString& tableOwner, TVector<TString>& cellData) {
    TVector<TCell> res;
    res.reserve(Columns.size());
    for (const auto& column : Columns) {
        if (auto filler = Fillers_.FindPtr(GetColumnName(column.Tag))) {
            res.emplace_back(MakePgCell(SchemaColumns_.at(column.Tag - 1), (*filler)(tableName, tableOwner), cellData));
        } else {
            res.emplace_back();
        }
    }
    return res;
}

TVector<TCell> TPgTablesScanBase::MakePgTablesStaticRow(const NYql::NPg::TTableInfo& tableInfo, TVector<TString>& cellData) {
    TVector<TCell> res;
    res.reserve(Columns.size());
    for (const auto& column : Columns) {
        if (auto filler = StaticFillers_.FindPtr(GetColumnName(column.Tag))) {
            res.emplace_back(MakePgCell(SchemaColumns_.at(column.Tag - 1), (*filler)(tableInfo), cellData));
        } else {
            res.emplace_back();
        }
    }
    return res;
}

void TPgTablesScanBase::ExpandBatchWithStaticTables(const THolder<NKqp::TEvKqpCompute::TEvScanData>& batch) {
    for (const auto& tableDesc : NYql::NPg::GetStaticTables()) {
        TVector<TString> cellData;
        TVector<TCell> cells = MakePgTablesStaticRow(tableDesc, cellData);
        if (!ConvertError_.Empty()) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, ConvertError_);
            return;
        }
        TArrayRef<const TCell> ref(cells);
        batch->Rows.emplace_back(TOwnedCellVec::Make(ref));
    }
}

constexpr auto TPgTablesScanBase::ActorActivityType() {
    return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
}

void TPgTablesScanBase::ProceedToScan() {
    auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>();
    NKikimrSchemeOp::TDescribePath& record = request->Record;
    auto pathVec = SplitPath(TablePath_);
    auto sz = pathVec.size();
    Y_ENSURE(sz > 2 && pathVec[sz - 2] == ".sys");
    pathVec.pop_back(); pathVec.pop_back();
    record.SetPath(JoinPath(pathVec));
    record.MutableOptions()->SetReturnPartitioningInfo(false);
    record.MutableOptions()->SetReturnPartitionConfig(false);
    record.MutableOptions()->SetReturnChildren(true);
    auto PipeCache = MakePipePerNodeCacheID(true);
    Send(PipeCache, new TEvPipeCache::TEvForward(request.Release(), SchemeShardId, true), IEventHandle::FlagTrackDelivery);
    Become(&TPgTablesScan::StateWork);
}

void TPgTablesScanBase::Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    const auto& record = ev->Get()->GetRecord();
    const auto status = record.GetStatus();
    switch (status) {
        case NKikimrScheme::StatusSuccess: {
            const auto& pathDescription = record.GetPathDescription();
            Y_ENSURE(pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeDir);
            break;
        }
        case NKikimrScheme::StatusPathDoesNotExist:
        case NKikimrScheme::StatusSchemeError: {
            ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, record.GetReason());
            return;
        }
        case NKikimrScheme::StatusAccessDenied: {
            ReplyErrorAndDie(Ydb::StatusIds::UNAUTHORIZED, record.GetReason());
            return;
        }
        case NKikimrScheme::StatusNotAvailable: {
            ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, record.GetReason());
            return;
        }
        default: {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, record.GetReason());
            return;
        }
    }

    auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);

    ExpandBatchWithStaticTables(batch);

    for (size_t i = 0; i < record.GetPathDescription().ChildrenSize(); ++i) {
        TVector<TString> cellData;
        const auto& tableName = record.GetPathDescription().GetChildren(i).GetName();
        const auto& tableOwner = record.GetPathDescription().GetSelf().GetOwner();
        TVector<TCell> cells = MakePgTablesRow(tableName, tableOwner, cellData);
        if (!ConvertError_.Empty()) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, ConvertError_);
            return;
        }
        TArrayRef<const TCell> ref(cells);
        batch->Rows.emplace_back(TOwnedCellVec::Make(ref));
    }

    batch->Finished = true;

    SendBatch(std::move(batch));
}

void TPgTablesScanBase::StateWork(TAutoPtr<IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
        default:
            LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                "NSysView::TScanActorBase: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
    }
}

TPgTablesScanBase::TPgTablesScanBase(
    const NActors::TActorId &ownerId, ui32 scanId, const TTableId &tableId,
    const TString& tablePath,
    const TTableRange &tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn> &columns,
    const TVector<Schema::PgColumn>& schemaColumns,
    THashMap<TString, TRowFiller>&& fillers,
    THashMap<TString, TStaticRowFiller>&& staticFillers)
    : NKikimr::NSysView::TScanActorBase<TPgTablesScanBase>(ownerId, scanId, tableId, tableRange, columns),
      TablePath_(tablePath),
      SchemaColumns_(schemaColumns),
      Fillers_(std::move(fillers)),
      StaticFillers_(std::move(staticFillers)) {}


TPgTablesScan::TPgTablesScan(
    const NActors::TActorId &ownerId, ui32 scanId, const TTableId &tableId,
    const TString& tablePath,
    const TTableRange &tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn> &columns)
    : TPgTablesScanBase(
        ownerId,
        scanId,
        tableId,
        tablePath,
        tableRange,
        columns,
        Singleton<Schema::PgTablesSchemaProvider>()->GetColumns(PgTablesName),
        {   //fillers
            {"schemaname", [](const TString&, const TString&) {
                return "public";
            }},
            {"tablename", [](const TString& tableName, const TString&) {
                return tableName;
            }},
            {"tableowner", [](const TString&, const TString& tableOwner) {
                return tableOwner;
            }},
            {"hasindexes", [](const TString&, const TString&) {
                return "true";
            }},
            {"hasrules", [](const TString&, const TString&) {
                return "false";
            }},
            {"hastriggers", [](const TString&, const TString&) {
                return "false";
            }},
            {"rowsecurity", [](const TString&, const TString&) {
                return "false";
            }},
        },
        { //static fillers
            {"schemaname", [](const NYql::NPg::TTableInfo& tInfo) {
                return tInfo.Schema;
            }},
            {"tablename", [](const NYql::NPg::TTableInfo& tInfo) {
                return tInfo.Name;
            }}
        }) {}

TInformationSchemaTablesScan::TInformationSchemaTablesScan(
    const NActors::TActorId &ownerId, ui32 scanId, const TTableId &tableId,
    const TString& tablePath,
    const TTableRange &tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn> &columns)
    : TPgTablesScanBase(
        ownerId,
        scanId,
        tableId,
        tablePath,
        tableRange,
        columns,
        Singleton<Schema::PgTablesSchemaProvider>()->GetColumns(InformationSchemaTablesName),
        {   //fillers
            {"table_schema", [](const TString&, const TString&) {
                return "public";
            }},
            {"table_name", [](const TString& tableName, const TString&) {
                return tableName;
            }},
        },
        { //static fillers
            {"table_schema", [](const NYql::NPg::TTableInfo& tInfo) {
                return tInfo.Schema;
            }},
            {"table_name", [](const NYql::NPg::TTableInfo& tInfo) {
                return tInfo.Name;
            }}
        }) {}

TPgClassScan::TPgClassScan(
    const NActors::TActorId &ownerId, ui32 scanId, const TTableId &tableId,
    const TString& tablePath,
    const TTableRange &tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn> &columns)
    : TPgTablesScanBase(
        ownerId,
        scanId,
        tableId,
        tablePath,
        tableRange,
        columns,
        Singleton<Schema::PgTablesSchemaProvider>()->GetColumns(PgClassName),
        {   //fillers
            {"relnamespace", [](const TString&, const TString&) {
                return "2200"; //PgPublicNamespace
            }},
            {"relname", [](const TString& tableName, const TString&) {
                return tableName;
            }},
            {"relowner", [](const TString&, const TString&) {
                return "10";
            }},
            {"relkind", [](const TString&, const TString&) {
                return TString(1,  static_cast<char>(NYql::NPg::ERelKind::Relation));
            }},
            {"relispartition", [](const TString&, const TString&) {
                return "false";
            }},
        },
        { //static fillers
            {"oid", [](const NYql::NPg::TTableInfo& tInfo) {
                return ToString(tInfo.Oid);
            }},
            {"relispartition", [](const NYql::NPg::TTableInfo&) {
                return "false";
            }},
            {"relkind", [](const NYql::NPg::TTableInfo& tInfo) {
                return ToString(static_cast<char>(tInfo.Kind));
            }},
            {"relname", [](const NYql::NPg::TTableInfo& tInfo) {
                return tInfo.Name;
            }},
            {"relnamespace", [this](const NYql::NPg::TTableInfo& tInfo) {
                return ToString(namespaces[tInfo.Schema]);
            }},
            {"relowner", [](const NYql::NPg::TTableInfo&) {
                return "1";
            }},
            {"relam", [this](const NYql::NPg::TTableInfo& tInfo) {
                const ui32 amOid = (tInfo.Kind == NYql::NPg::ERelKind::Relation) ? btreeAmOid : 0;
                return ToString(amOid);
            }},
        })
{
    NYql::NPg::EnumNamespace([&](ui32 oid, const NYql::NPg::TNamespaceDesc &desc) {
        namespaces[desc.Name] = oid;
    });

    NYql::NPg::EnumAm([&](ui32 oid, const NYql::NPg::TAmDesc &desc) {
        if (desc.AmName == "btree") {
            btreeAmOid = oid;
        }
    });
}

THolder<NActors::IActor> CreatePgTablesScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId, const TString& tablePath,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TPgTablesScan>(ownerId, scanId, tableId, tablePath, tableRange, columns);
}

THolder<NActors::IActor> CreateInformationSchemaTablesScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId, const TString& tablePath,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TInformationSchemaTablesScan>(ownerId, scanId, tableId, tablePath, tableRange, columns);
}

THolder<NActors::IActor> CreatePgClassScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId, const TString& tablePath,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TPgClassScan>(ownerId, scanId, tableId, tablePath, tableRange, columns);
}

} // NSysView
} // NKikimr
