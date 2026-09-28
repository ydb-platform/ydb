#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TReject: public ISubOperation {
    const char* Name() const override final { return "TReject"; }
    const char* CurrentStateName() const override final { return "none"; }

    const TOperationId OperationId;
    THolder<TProposeResponse> Response;

public:
    TReject(TOperationId id, THolder<TProposeResponse> response)
        : OperationId(id)
        , Response(std::move(response))
    {}

    TReject(TOperationId id, NKikimrScheme::EStatus status, const TString& reason)
        : OperationId(id)
        , Response(new TEvSchemeShard::TEvModifySchemeTransactionResult(NKikimrScheme::StatusAccepted, 0, 0))
    {
        Response->SetError(status, reason);
    }

    const TOperationId GetId() const override {
        return OperationId;
    }

    const NKikimrSchemeOp::TModifyScheme& GetModifyScheme() const override {
        static const NKikimrSchemeOp::TModifyScheme fake;
        return fake;
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        Y_ABORT_UNLESS(Response);

        Response->Record.SetTxId(ui64(OperationId.GetTxId()));
        Response->Record.SetSchemeshardId(context.SS->TabletID());

        YDB_LOG_NOTICE_CTX(context.Ctx, "",
            {"reason", Response->Record.GetReason()},
        );

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

#undef YDB_LOG_THIS_FILE_COMPONENT
