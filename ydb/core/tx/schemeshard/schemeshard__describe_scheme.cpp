#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

#include <util/stream/format.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT NKikimrServices::SCHEMESHARD_DESCRIBE

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
        YDBLOG_CTX_DEBUG(ctx, "TTxDescribeScheme DoExecute",
            {"record", PathDescriber.GetParams().ShortDebugString()},
            {"at_schemeshard", Self->TabletID()});

        Result = PathDescriber.Describe(ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        const auto& params = PathDescriber.GetParams();

        if (params.HasPathId()) {
            YDBLOG_CTX_INFO(ctx, "Tablet describe pathId took result status",
                {"#_Self->TabletID()", Self->TabletID()},
                {"#_params.GetPathId()", params.GetPathId()},
                {"#_HumanReadable(ExecuteDuration)", HumanReadable(ExecuteDuration)},
                {"#_NKikimrScheme::EStatus_Name(Result->Record.GetStatus())", NKikimrScheme::EStatus_Name(Result->Record.GetStatus())});
        } else {
            YDBLOG_CTX_INFO(ctx, "Tablet describe path \" \" took result status",
                {"#_Self->TabletID()", Self->TabletID()},
                {"#_params.GetPath()", params.GetPath()},
                {"#_HumanReadable(ExecuteDuration)", HumanReadable(ExecuteDuration)},
                {"#_NKikimrScheme::EStatus_Name(Result->Record.GetStatus())", NKikimrScheme::EStatus_Name(Result->Record.GetStatus())});
        }

        YDBLOG_CTX_DEBUG(ctx, "TTxDescribeScheme DoComplete",
            {"result", Result->GetRecord().ShortDebugString()},
            {"at_schemeshard", Self->TabletID()});

        ctx.Send(Sender, std::move(Result), 0, Cookie);
    }

};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxDescribeScheme(TEvSchemeShard::TEvDescribeScheme::TPtr &ev) {
    return new TTxDescribeScheme(this, ev);
}

}}
