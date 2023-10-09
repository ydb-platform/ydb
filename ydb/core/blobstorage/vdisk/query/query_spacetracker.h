#pragma once
#include "defs.h"
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TQueryResultSizeTracker
    // This class is used for tracking query result size to avoid memory or
    // transport (protobuf) overflow
    ////////////////////////////////////////////////////////////////////////////
    class TQueryResultSizeTracker {
    public:
        // add LogoBlob w/o data
        void AddLogoBlobIndex() {
            Size += LogoBlobIndexSize;
        }

        void AddLogoBlobData(ui64 blobSize, ui64 queryShift, ui64 querySize) {
            if (querySize) {
                Size += querySize;
            } else {
                Y_ABORT_UNLESS(blobSize >= queryShift, "blobSize# %" PRIu64 " queryShift# %" PRIu64, blobSize, queryShift);
                Size += blobSize - queryShift;
            }
        }

        void AddAllPartsOfLogoBlob(const TBlobStorageGroupType &type, TLogoBlobID blobId) {
            for (ui32 partIdx = 0; partIdx < type.TotalPartCount(); ++partIdx) {
                AddLogoBlobIndex();
                AddLogoBlobData(type.PartSize(TLogoBlobID(blobId, partIdx + 1)), 0, 0);
            }
        }

        bool IsOverflow() const {
            return Size > MaxProtobufSize;
        }

        ui64 GetSize() const {
            return Size;
        }

        void Init() {
            Size = VGetResultIndexSize;
        }

    private:
        // Result size we have counted
        ui64 Size = 0;

        // Max size of TQueryResult without Buffer data
        static constexpr ui64 LogoBlobIndexSize =
            1 + 2        // Status
            + 1 + 1 + 24 // BlobId
            + 1 + 10     // Shift
            + 1 + 10     // Size
            + 1 + 10     // Buffer filed byte and varint
            + 1 + 10     // Cookie
            + 1 + 10     // FullDataSize
            + 1 + 10;    // Ingress

        // Max size of TEvVGetResult without TQueryResult data
        static constexpr ui64 VGetResultIndexSize =
            1 + 2          // Status
            + 1 + 10       // TQueryResult field byte and varint
            + 1 + 1 + 30   // VDiskID
            + 1 + 10       // Cookier
            + 1 + 2 + 274  // MsgQoS
            + 1 + 5        // BlockedGeneration
            + 1 + 1 + 45;  // Timestamp=
        // total limit on result
    };

} // NKikimr
