#pragma once

#include <ydb/core/kqp/runtime/kqp_compute.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>

namespace NKikimr {
namespace NSysView {

using TRowFiller = std::function<TString(const TString&, const TString&)>;
using TStaticRowFiller = std::function<TString(const NYql::NPg::TTableInfo&)>;


class TPgTablesScanBase : public NKikimr::NSysView::TScanActorBase<TPgTablesScanBase> {
private:
    TVector<TCell> MakePgTablesRow(const TString& tableName, const TString& tableOwner, TVector<TString>& cellData);
    TVector<TCell> MakePgTablesStaticRow(const NYql::NPg::TTableInfo& tableInfo, TVector<TString>& cellData);
    void ExpandBatchWithStaticTables(const THolder<NKqp::TEvKqpCompute::TEvScanData>& batch);
    TStringBuf GetColumnName(NTable::TTag tag) const;
protected:
    TCell MakePgCell(const Schema::PgColumn& column, const TString& value, TVector<TString>& cellData);
public:
    TPgTablesScanBase(
        const NActors::TActorId& ownerId,
        ui32 scanId,
        const TTableId& tableId,
        const TTableRange& tableRange,
        const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        const TVector<Schema::PgColumn>& schemaColumns,
        THashMap<TString, TRowFiller>&& fillers,
        THashMap<TString, TStaticRowFiller>&& staticFillers);

    static constexpr auto ActorActivityType();
    void ProceedToScan();
    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx);
    void StateWork(TAutoPtr<IEventHandle>& ev);
protected:
    TString ConvertError_;
    const TVector<Schema::PgColumn>& SchemaColumns_;
    const THashMap<TString, TRowFiller> Fillers_;
    const THashMap<TString, TStaticRowFiller> StaticFillers_;
};

class TPgTablesScan : public TPgTablesScanBase {
public:
    TPgTablesScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);
};
class TInformationSchemaTablesScan : public TPgTablesScanBase {
public:
    TInformationSchemaTablesScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);
};

class TPgClassScan : public TPgTablesScanBase {
public:
    TPgClassScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);
private:
    THashMap<TString, ui32> namespaces;
    ui32 btreeAmOid;
};

THolder<NActors::IActor> CreatePgTablesScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);
THolder<NActors::IActor> CreateInformationSchemaTablesScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);
THolder<NActors::IActor> CreatePgClassScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);


} // NSysView
} // NKikimr
