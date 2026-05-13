#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TReject: public ISubOperation {
    const TOperationId OperationId;
    THolder<TProposeResponse> Response;

public:
    TReject(TOperationId id, THolder<TProposeResponse> response)
        : OperationId(id)
        , Response(std::move(response))
    {}

    TReject(TOperationId id, NKikimrScheme::EStatus status, const TString& explain)
        : OperationId(id)
        , Response(
            new TEvSchemeShard::TEvModifySchemeTransactionResult(
                NKikimrScheme::StatusAccepted, 0, 0))
    {
        Response->SetError(status, explain);
    }

    const TOperationId& GetOperationId() const override {
        return OperationId;
    }

    const TTxTransaction& GetTransaction() const override {
        static const TTxTransaction fake;
        return fake;
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        Y_ABORT_UNLESS(Response);

        const auto ssId = context.SS->SelfTabletId();

        YDB_LOG_CTX_NOTICE(context.Ctx, "TReject Propose",
            {"opId", OperationId},
            {"explain", Response->Record.GetReason()},
            {"at_schemeshard", ssId});

        Response->Record.SetTxId(ui64(OperationId.GetTxId()));
        Response->Record.SetSchemeshardId(ui64(ssId));
        return std::move(Response);
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TReject");
    }

    bool ProgressState(TOperationContext&) override {
        Y_ABORT("no ProgressState for TReject");
    }

    void AbortUnsafe(TTxId, TOperationContext&) override {
        Y_ABORT("no AbortUnsafe for TReject");
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateReject(TOperationId id, THolder<TProposeResponse> response) {
    return new TReject(id, std::move(response));
}

ISubOperation::TPtr CreateReject(TOperationId id, NKikimrScheme::EStatus status, const TString& message) {
    return new TReject(id, status, message);
}

}
