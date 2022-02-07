#include "schemeshard_xxport__list.h"
#include "schemeshard_import.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TImport::TTxList: public TSchemeShard::TXxport::TTxList<
        TImportInfo,
        TEvImport::TEvListImportsRequest,
        TEvImport::TEvListImportsResponse,
        TSchemeShard::TImport::TTxList
> {
    using TTxListBase::TTxListBase;

    TTxType GetTxType() const override {
        return TXTYPE_LIST_IMPORTS;
    }

    static bool TryParseKind(const TString& str, TImportInfo::EKind& parsed) {
        if (str == "import/s3") {
            parsed = TImportInfo::EKind::S3;
            return true;
        }

        return false;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        return DoExecuteImpl(Self->Imports, txc, ctx);
    }

}; // TTxList

ITransaction* TSchemeShard::CreateTxListImports(TEvImport::TEvListImportsRequest::TPtr& ev) {
    return new TImport::TTxList(this, ev);
}

} // NSchemeShard
} // NKikimr
