#include "schemeshard_xxport__get.h"
#include "schemeshard_export.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TExport::TTxGet: public TSchemeShard::TXxport::TTxGet<
        TExportInfo,
        TEvExport::TEvGetExportRequest,
        TEvExport::TEvGetExportResponse
> {
    using TTxGetBase::TTxGetBase;

    TTxType GetTxType() const override {
        return TXTYPE_GET_EXPORT;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        return DoExecuteImpl(Self->Exports, txc, ctx);
    }

}; // TTxGet

ITransaction* TSchemeShard::CreateTxGetExport(TEvExport::TEvGetExportRequest::TPtr& ev) {
    return new TExport::TTxGet(this, ev);
}

} // NSchemeShard
} // NKikimr
