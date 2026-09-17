#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

#include <ydb/library/actors/core/log.h>

#include <util/stream/format.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxDescribeScheme : public TSchemeShard::TRwTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    TPathDescriber PathDescriber;

    THolder<TEvSchemeShard::TEvDescribeSchemeResultBuilder> Result;

    TTxDescribeScheme(TSelf *self, TEvSchemeShard::TEvDescribeScheme::TPtr &ev)
        : TRwTxBase(self)
        , Sender(ev->Sender)
        , Cookie(ev->Cookie)
        , PathDescriber(self, std::move(ev->Get()->Record))
    {}

    TTxType GetTxType() const override { return TXTYPE_DESCRIBE_SCHEME; }

    void DoExecute(TTransactionContext& /*txc*/, const TActorContext& ctx) override {
        YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::SCHEMESHARD_DESCRIBE, "TTxDescribeScheme DoExecute",
            {"record", PathDescriber.GetParams().ShortDebugString()},
            {"schemeshard", Self->TabletID()},
        );

        Result = PathDescriber.Describe(ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        const auto& params = PathDescriber.GetParams();

        if (params.HasPathId()) {
            YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::SCHEMESHARD_DESCRIBE, "Tablet describe pathId",
                {"tablet", Self->TabletID()},
                {"pathId", params.GetPathId()},
                {"took", HumanReadable(ExecuteDuration)},
                {"resultStatus", NKikimrScheme::EStatus_Name(Result->Record.GetStatus())},
            );
        } else {
            YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::SCHEMESHARD_DESCRIBE, "Tablet describe path",
                {"tablet", Self->TabletID()},
                {"path", params.GetPath()},
                {"took", HumanReadable(ExecuteDuration)},
                {"resultStatus", NKikimrScheme::EStatus_Name(Result->Record.GetStatus())},
            );
        }

        YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::SCHEMESHARD_DESCRIBE, "TTxDescribeScheme DoComplete",
            {"result", Result->GetRecord().ShortDebugString()},
            {"schemeshard", Self->TabletID()},
        );

        ctx.Send(Sender, std::move(Result), 0, Cookie);
    }

};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxDescribeScheme(TEvSchemeShard::TEvDescribeScheme::TPtr &ev) {
    return new TTxDescribeScheme(this, ev);
}

}}

#undef YDB_LOG_THIS_FILE_COMPONENT
