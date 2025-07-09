#include "schemeshard_import_getters.h"
#include "schemeshard_private.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
namespace NSchemeShard {

class TSchemeGetterFallback: public TActorBootstrapped<TSchemeGetterFallback> {
public:
    explicit TSchemeGetterFallback(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx)
        : ReplyTo(replyTo)
        , ImportInfo(std::move(importInfo))
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

class TSchemaMappingGetterFallback: public TActorBootstrapped<TSchemaMappingGetterFallback> {
public:
    explicit TSchemaMappingGetterFallback(const TActorId& replyTo, TImportInfo::TPtr importInfo)
        : ReplyTo(replyTo)
        , ImportInfo(std::move(importInfo))
    {
    }

    void Bootstrap() {
        Send(ReplyTo, new TEvPrivate::TEvImportSchemaMappingReady(ImportInfo->Id, false, "Imports from S3 are disabled"));
        PassAway();
    }

private:
    const TActorId ReplyTo;
    TImportInfo::TPtr ImportInfo;
}; // TSchemeGetterFallback

IActor* CreateSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx, TMaybe<NBackup::TEncryptionIV>) {
    return new TSchemeGetterFallback(replyTo, std::move(importInfo), itemIdx);
}

IActor* CreateSchemaMappingGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo) {
    return new TSchemaMappingGetterFallback(replyTo, std::move(importInfo));
}

} // NSchemeShard
} // NKikimr
