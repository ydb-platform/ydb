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

class TListObjectsInS3ExportGetterFallback: public TActorBootstrapped<TListObjectsInS3ExportGetterFallback> {
public:
    TListObjectsInS3ExportGetterFallback(TEvImport::TEvListObjectsInS3ExportRequest::TPtr&& ev)
        : Request(std::move(ev))
    {
    }

    void Bootstrap() {
        auto result = MakeHolder<TEvImport::TEvListObjectsInS3ExportResponse>();
        result->Record.set_status(Ydb::StatusIds::UNSUPPORTED);
        result->Record.add_issues()->set_message("S3 listings are disabled");
        Send(Request->Sender, std::move(result));
        PassAway();
    }

private:
    TEvImport::TEvListObjectsInS3ExportRequest::TPtr Request;
}; // TListObjectsInS3ExportGetterFallback

IActor* CreateSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx, TMaybe<NBackup::TEncryptionIV>) {
    return new TSchemeGetterFallback(replyTo, std::move(importInfo), itemIdx);
}

IActor* CreateSchemaMappingGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo) {
    return new TSchemaMappingGetterFallback(replyTo, std::move(importInfo));
}

IActor* CreateListObjectsInS3ExportGetter(TEvImport::TEvListObjectsInS3ExportRequest::TPtr&& ev) {
    return new TListObjectsInS3ExportGetterFallback(std::move(ev));
}

} // NSchemeShard
} // NKikimr
