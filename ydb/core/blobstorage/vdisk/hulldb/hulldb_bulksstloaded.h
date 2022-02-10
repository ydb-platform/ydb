#pragma once

#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>

namespace NKikimr {

    struct TEvBulkSstsLoaded : public TEventLocal<TEvBulkSstsLoaded, TEvBlobStorage::EvBulkSstsLoaded> {
        TVector<TIntrusivePtr<TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>>> Segments;

        TEvBulkSstsLoaded(TVector<TIntrusivePtr<TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>>>&& segments)
            : Segments(std::move(segments))
        {}
    };

} // NKikimr
