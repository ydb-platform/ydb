#pragma once

#include "defs.h"
#include "ddisk.h"


namespace NKikimr::NDDisk {

    class TSegmentManager {
    public:
        using TSegment = std::tuple<ui32, ui32>; // [begin; end)
        
        struct TOutdatedRequest {
            ui64 SyncIndex;
            ui64 RequestId;
        };

    private:
        struct TRequestInFlight;
        using TRequestIt = THashMap<ui64, TRequestInFlight>::iterator;
        using TSegmentIt = TMap<std::tuple<ui64, ui32, ui32>, TRequestIt>::iterator;

        struct TRequestInFlight {
            ui64 VChunkIndex;
            ui64 SyncId;
            THashMap<TSegment, TSegmentIt> Segments;

            TString ToString() const;
        };

        ui64 NextRequestId = 1;
        THashMap<ui64, TRequestInFlight> RequestsInFlight; // request_id -> TRequestInFlight

        TMap<std::tuple<ui64, ui32, ui32>, TRequestIt> SegmentsInFlight; // (vchunk_id, offset, end) -> request_id


    private:
        void DropSegment(ui64 vchunkIndex, TSegment dropSegment, std::vector<TOutdatedRequest> *outdated);

    public:
        void PopRequest(ui64 requestId, std::vector<TSegment> *segments); // return set of actual subsegments
        void PushRequest(ui64 vchunkIndex, ui64 syncId, TSegment segment, ui64 *requestId, std::vector<TOutdatedRequest> *outdated);

    };

} // NKikimr::NDDisk
