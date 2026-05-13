#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

#include <util/stream/format.h>

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
        LOG_DEBUG_S(ctx, NKikimrServices::SCHEMESHARD_DESCRIBE,
                    "TTxDescribeScheme DoExecute"
                        << ", record: " << PathDescriber.GetParams().ShortDebugString()
                        << ", at schemeshard: " << Self->TabletID());

        Result = PathDescriber.Describe(ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        const auto& params = PathDescriber.GetParams();

        if (params.HasPathId()) {
            LOG_INFO_S(ctx, NKikimrServices::SCHEMESHARD_DESCRIBE,
                       "Tablet " << Self->TabletID()
                                 << " describe pathId " << params.GetPathId()
                                 << " took " << HumanReadable(ExecuteDuration)
                                 << " result status " <<NKikimrScheme::EStatus_Name(Result->Record.GetStatus()));
        } else {
            LOG_INFO_S(ctx, NKikimrServices::SCHEMESHARD_DESCRIBE,
                       "Tablet " << Self->TabletID()
                                 << " describe path \"" << params.GetPath() << "\""
                                 << " took " << HumanReadable(ExecuteDuration)
                                 << " result status " <<NKikimrScheme::EStatus_Name(Result->Record.GetStatus()));
        }

        LOG_DEBUG_S(ctx, NKikimrServices::SCHEMESHARD_DESCRIBE,
                    "TTxDescribeScheme DoComplete"
                        << ", result: " << Result->GetRecord().ShortDebugString()
                        << ", at schemeshard: " << Self->TabletID());

        ctx.Send(Sender, std::move(Result), 0, Cookie);
    }

};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxDescribeScheme(TEvSchemeShard::TEvDescribeScheme::TPtr &ev) {
    return new TTxDescribeScheme(this, ev);
}

}}
