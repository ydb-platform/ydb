#include "schemeshard_import.h"
#include "schemeshard_xxport__get.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TImport::TTxGet: public TSchemeShard::TXxport::TTxGet<
        TImportInfo,
        TEvImport::TEvGetImportRequest,
        TEvImport::TEvGetImportResponse
> {
    using TTxGetBase::TTxGetBase;

    TTxType GetTxType() const override {
        return TXTYPE_GET_IMPORT;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        return DoExecuteImpl(Self->Imports, txc, ctx);
    }

}; // TTxGet

ITransaction* TSchemeShard::CreateTxGetImport(TEvImport::TEvGetImportRequest::TPtr& ev) {
    return new TImport::TTxGet(this, ev);
}

} // NSchemeShard
} // NKikimr
