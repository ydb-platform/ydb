#include "schemeshard_export_metadata_uploader.h"
#include "schemeshard_private.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSchemeShard {

class TMetadataUploaderFallback: public TActorBootstrapped<TMetadataUploaderFallback> {
public:
    explicit TMetadataUploaderFallback(const TActorId& replyTo, TExportInfo::TPtr exportInfo, ui32 itemIdx)
        : ReplyTo(replyTo)
        , ExportInfo(exportInfo)
        , ItemIdx(itemIdx)
    {
    }

    void Bootstrap() {
        Send(ReplyTo, new TEvPrivate::TEvExportMetadataUploaded(ExportInfo->Id, ItemIdx, false, "Exports to S3 are disabled"));
        PassAway();
    }

private:
    const TActorId ReplyTo;
    TExportInfo::TPtr ExportInfo;
    const ui32 ItemIdx;

}; // TMetadataUploaderFallback


IActor* CreateMetadataUploader(const TActorId& replyTo, TExportInfo::TPtr exportInfo, ui32 itemIdx) {
    return new TMetadataUploaderFallback(replyTo, exportInfo, itemIdx);
}

} // namespace NKikimr::NSchemeShard
