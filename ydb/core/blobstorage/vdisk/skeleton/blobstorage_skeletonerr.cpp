#include "blobstorage_skeletonerr.h"

namespace NKikimr::NErrBuilder {

    template<>
    std::unique_ptr<IEventBase> ErroneousResult<TEvBlobStorage::TEvVPhantomFlagStorageListChunks>(
            const TVDiskContextPtr&, NKikimrProto::EReplyStatus status, const TString&,
            TEvBlobStorage::TEvVPhantomFlagStorageListChunks::TPtr&, const TInstant&, const TActorIDPtr&,
            const TVDiskID& vdiskId, ui64, const TIntrusivePtr<TBlobStorageGroupInfo>&) {
        return std::make_unique<TEvBlobStorage::TEvVPhantomFlagStorageListChunksResult>(status, vdiskId);
    }

    template<>
    std::unique_ptr<IEventBase> ErroneousResult<TEvBlobStorage::TEvVPhantomFlagStorageReadChunk>(
            const TVDiskContextPtr&, NKikimrProto::EReplyStatus status, const TString&,
            TEvBlobStorage::TEvVPhantomFlagStorageReadChunk::TPtr& ev, const TInstant&, const TActorIDPtr&,
            const TVDiskID& vdiskId, ui64, const TIntrusivePtr<TBlobStorageGroupInfo>&) {
        return std::make_unique<TEvBlobStorage::TEvVPhantomFlagStorageReadChunkResult>(
            status, vdiskId, ev->Get()->GetChunkIndex());
    }

} // namespace NKikimr::NErrBuilder
