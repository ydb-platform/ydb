#include "schemeshard_import_scheme_getter.h"
#include "schemeshard_private.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr {
namespace NSchemeShard {

class TSchemeGetterFallback: public TActorBootstrapped<TSchemeGetterFallback> {
public:
    explicit TSchemeGetterFallback(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx)
        : ReplyTo(replyTo)
        , ImportInfo(importInfo)
        , ItemIdx(itemIdx)
    {
    }

    void Bootstrap() {
        Send(ReplyTo, new TEvPrivate::TEvImportSchemeReady(ImportInfo->Id, ItemIdx, false, "Imports from S3 are disabled"));
        PassAway();
    }

private:
    const TActorId ReplyTo;
    TImportInfo::TPtr ImportInfo;
    const ui32 ItemIdx;

}; // TSchemeGetterFallback

IActor* CreateSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx) {
    return new TSchemeGetterFallback(replyTo, importInfo, itemIdx);
}

} // NSchemeShard
} // NKikimr
