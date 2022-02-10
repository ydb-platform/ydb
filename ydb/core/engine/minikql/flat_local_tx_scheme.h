#pragma once

#include "flat_local_minikql_host.h"
#include <ydb/core/tablet_flat/flat_dbase_apply.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/client/minikql_compile/yql_expr_minikql.h>
#include <ydb/core/client/minikql_compile/compile_context.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NMiniKQL {

class TFlatLocalSchemeTx : public NTabletFlatExecutor::ITransaction {
public:
    TFlatLocalSchemeTx(TActorId sender, TEvTablet::TEvLocalSchemeTx::TPtr &ev)
        : Sender(sender)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        Y_UNUSED(ctx);

        Response.Reset(new TEvTablet::TEvLocalSchemeTxResponse);

        auto &delta =  Ev->Get()->Record.GetSchemeChanges();

        auto currentScheme = txc.DB.GetScheme();
        NTable::TSchemeModifier(currentScheme).Apply(delta);
        // TODO: Validate scheme change

        if (!Ev->Get()->Record.GetDryRun())
            txc.DB.Alter().Merge(delta);

        auto schemeSnapshot = currentScheme.GetSnapshot();
        Response->Record.MutableFullScheme()->Swap(schemeSnapshot.Get());
        Response->Record.SetStatus(NKikimrProto::OK);
        Response->Record.SetOrigin(txc.Tablet);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        MakeResponse(ctx);
    }

    void MakeResponse(const TActorContext &ctx) {
        ctx.Send(Sender, Response.Release());
    }

private:
    const TActorId Sender;
    TEvTablet::TEvLocalSchemeTx::TPtr Ev;
    TAutoPtr<TEvTablet::TEvLocalSchemeTxResponse> Response;
};

}}
