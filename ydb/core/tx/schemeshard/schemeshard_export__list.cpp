#include "schemeshard_export.h"
#include "schemeshard_xxport__list.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TExport::TTxList: public TSchemeShard::TXxport::TTxList<
        TExportInfo,
        TEvExport::TEvListExportsRequest,
        TEvExport::TEvListExportsResponse,
        TSchemeShard::TExport::TTxList
> {
    using TTxListBase::TTxListBase;

    TTxType GetTxType() const override {
        return TXTYPE_LIST_EXPORTS;
    }

    static bool TryParseKind(const TString& str, TExportInfo::EKind& parsed) {
        if (str == "export/s3") {
            parsed = TExportInfo::EKind::S3;
        } else { // fallback to yt
            parsed = TExportInfo::EKind::YT;
        }

        return true;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        return DoExecuteImpl(Self->Exports, txc, ctx);
    }

}; // TTxList

ITransaction* TSchemeShard::CreateTxListExports(TEvExport::TEvListExportsRequest::TPtr& ev) {
    return new TExport::TTxList(this, ev);
}

} // NSchemeShard
} // NKikimr
