#pragma once

#include "flat_local_minikql_host.h"
#include <ydb/core/base/localdb.h>
#include <ydb/core/tablet_flat/flat_dbase_apply.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/client/minikql_compile/yql_expr_minikql.h>
#include <ydb/core/client/minikql_compile/compile_context.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
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
        Response->Record.SetOrigin(txc.Tablet);

        const auto& delta = Ev->Get()->Record.GetSchemeChanges();
        const auto& currentScheme = txc.DB.GetScheme();
        auto proposedScheme = currentScheme;
        NTable::TSchemeModifier modifier(proposedScheme);
        modifier.Apply(delta);

        for (ui32 tableId : modifier.Affects) {
            const auto* before = currentScheme.GetTableInfo(tableId);
            const auto* after = proposedScheme.GetTableInfo(tableId);
            if (!before || !after || before->CompactionPolicy == after->CompactionPolicy) {
                continue;
            }

            TString error;
            if (!NLocalDb::ValidateCompactionPolicyChange(*before->CompactionPolicy, *after->CompactionPolicy, error)) {
                Response->Record.SetStatus(NKikimrProto::ERROR);
                Response->Record.SetErrorReason(error);
                return true;
            }
        }

        if (!Ev->Get()->Record.GetDryRun()) {
            txc.DB.Alter().Merge(delta);
        }

        auto schemeSnapshot = proposedScheme.GetSnapshot();
        Response->Record.MutableFullScheme()->Swap(schemeSnapshot.Get());
        Response->Record.SetStatus(NKikimrProto::OK);

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
