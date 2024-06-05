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

class TPgTablesScan : public NKikimr::NSysView::TScanActorBase<TPgTablesScan> {
private:
    TCell MakePgCell(const Schema::PgColumn& column, const TString& value, TVector<TString>& cellData) {
        NYql::NUdf::TStringRef ref;
        auto typeDesc = column._ColumnTypeInfo.GetTypeDesc();
        auto convert = NPg::PgNativeBinaryFromNativeText(value, NPg::PgTypeIdFromTypeDesc(typeDesc));
        if (convert.Error) {
            ConvertError_ = *convert.Error;
            return TCell();
        }
        cellData.emplace_back(convert.Str);
        ref = NYql::NUdf::TStringRef(cellData.back());
    
        return TCell(ref.Data(), ref.Size());
    }

    TVector<TCell> MakePgTablesRow(const TString& tableName, const TString& tableOwner, TVector<TString>& cellData) {
        const auto &Columns =  Schema::PgTables::Columns;
        return {
            MakePgCell(Columns[0], "true", cellData), //hasindexes
            MakePgCell(Columns[1], "false", cellData), //hasrules
            MakePgCell(Columns[2], "false", cellData), //hastriggers
            MakePgCell(Columns[3], "false", cellData), //rowsecurity
            MakePgCell(Columns[4], "public", cellData), //schemaname
            MakePgCell(Columns[5], tableName, cellData), //tablename
            MakePgCell(Columns[6], tableOwner, cellData), //tableowner
            TCell() //tablespace
        };
    }
public:
    using TBase = NKikimr::NSysView::TScanActorBase<TPgTablesScan>;
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TPgTablesScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
    : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
    };

    void ProceedToScan() {
        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>(AppData()->TenantName);
        NKikimrSchemeOp::TDescribePath& record = request->Record;
        record.SetPath(AppData()->TenantName);
        record.MutableOptions()->SetReturnPartitioningInfo(false);
        record.MutableOptions()->SetReturnPartitionConfig(false);
        record.MutableOptions()->SetReturnChildren(true);
        auto PipeCache = MakePipePerNodeCacheID(true);
        Send(PipeCache, new TEvPipeCache::TEvForward(request.Release(), SchemeShardId, true), IEventHandle::FlagTrackDelivery);
        Become(&TPgTablesScan::StateWork);
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
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

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default: 
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TScanActorBase: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }
private:
    TString ConvertError_;
};

THolder<NActors::IActor> CreatePgTablesScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TPgTablesScan>(ownerId, scanId, tableId, tableRange, columns);
}

} // NSysView
} // NKikimr
