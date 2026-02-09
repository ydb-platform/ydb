#include "segment_manager.h"

using namespace NKikimr::NDDisk;

TString TSegmentManager::TRequestInFlight::ToString() const {
    TStringBuilder builder;
    builder << "{\"VChunkIndex\": " << VChunkIndex
        << ", \"SyncId\": " << SyncId
        << ", \"Segments\": [";
    bool sep = false;
    for (auto &[segment, _] : Segments) {
        if (sep) {
            builder << ", ";
        }
        auto [begin, end] = segment;
        builder << "{\"begin\": " << begin << ", \"end\": " << end << "}";
    }    
    builder << "]}";
    return builder;
}


void TSegmentManager::DropSegment(ui64 vchunkIndex, TSegment dropSegment, std::vector<TOutdatedRequest> *outdated) {
    // O(log(Segments.count) + overlapped_segments)

    Y_VERIFY(outdated != nullptr);

    auto [dropBegin, dropEnd] = dropSegment;
    TSegmentIt segmentIt = SegmentsInFlight.lower_bound({vchunkIndex, dropBegin, ui32(0)});

    while (segmentIt != SegmentsInFlight.end()) {
        auto [segVChunkIndex, begin, end] = segmentIt->first;
        if (segVChunkIndex != vchunkIndex) {
            break;
        }

        if (dropEnd <= begin) {
            // [dropBegin; dropEnd)
            //                      [begin; end)
            // ---------------------------------
            //                      [begin; end)

            break;
        }

        TRequestIt requestIt = segmentIt->second;
        TRequestInFlight &request = requestIt->second;

        auto requestSegmentIt = request.Segments.find(TSegment{begin, end});
        Y_VERIFY_S(requestSegmentIt != request.Segments.end(), 
            "Broken invariant; Can't find segment in request; Request# "
            << request.ToString() << " begin# " << begin << " end# " << end
        );
        request.Segments.erase(requestSegmentIt);   
        
        if (dropEnd < end) {
            // [dropBegin;dropEnd)
            //             [begin;          end)
            // ---------------------------------
            //                    [newBegin;end)
            //
            // newBegin = dropEnd
            Y_VERIFY(dropBegin <= begin && dropEnd < end);
         
            auto tmpSegmentIt = SegmentsInFlight.emplace({vchunkIndex, dropEnd, end}, requestIt).second;
            request.Segments.emplace_back(TSegment{dropEnd, end}, tmpSegmentIt);
        } else {
            // [dropBegin;              dropEnd)
            //             [begin; end)
            // ---------------------------------
            //             *empty*
            Y_VERIFY(dropBegin <= begin && end <= dropEnd);

            if (request.Segments.empty()) {
                ui64 requestId = requestIt->first;
                outdated->emplace_back({request->SyncId, requestId});
            }
        }
        segmentIt = SegmentsInFlight.erase(segmentIt);
    }

    // look at backward segment; begin < dropBegin
    if (segmentIt != SegmentsInFlight.begin()) {
        --segmentIt;
        auto [segVChunkIndex, begin, end] = segmentIt->first;
        if (segVChunkIndex != vchunkIndex) {
            break;
        }

        if (end <= dropBegin) {
            //              [dropBegin; dropEnd)
            // [begin; end)
            // ---------------------------------
            // [begin; end)    

            return;
        }

        TRequestIt requestIt = segmentIt->second;
        TRequestInFlight &request = *requestIt;

        auto requestSegmentIt = request.Segments.find({begin, end});
        Y_VERIFY_S(requestSegmentIt != request.Segments.end(), 
            "Broken invariant; Can't find segment in request; Request# "
            << request.ToString() << " begin# " << begin << " end# " << end
        );
        request.Segments.erase(requestSegmentIt);  
        
        if (end <= dropEnd) {
            //               [dropBegin;dropEnd)
            // [begin;                 end)
            // ---------------------------------
            // [begin;newEnd)
            //
            // newEnd == dropBegin
            Y_VERIFY(begin < dropBegin && end <= dropEnd);
         
            auto tmpSegmentIt = SegmentsInFlight.emplace({vchunkIndex, begin, dropBegin}, requestIt).second;
            request.Segments.emplace_back(TSegment{begin, dropBegin}, tmpSegmentIt);
        } else {
            //               [dropBegin;dropEnd)
            // [begin;                                    end)
            // -----------------------------------------------
            // [begin;newEnd)                   [newBegin;end)
            //
            // newEnd == dropBegin
            // newBegint == dropEnd

            auto tmpSegmentIt = SegmentsInFlight.emplace({vchunkIndex, begin, dropBegin}, requestIt).second;
            request.Segments.emplace_back(TSegment{begin, dropBegin}, tmpSegmentIt);

            tmpSegmentIt = SegmentsInFlight.emplace({vchunkIndex, dropEnd, end}, requestIt).second;
            request.Segments.emplace_back(TSegment{dropEnd, end}, tmpSegmentIt);
        }
        SegmentsInFlight.erase(segmentIt);
    }
}


void TSegmentManager::PopRequest(ui64 requestIdx, std::vector<TSegment> *segments) {
    Y_VERIFY(segments != nullptr);

    auto requestIt = RequestsInFlight.find(requestIdx);
    if (requestIt == RequestsInFlight.end()) {
        return {};
    }

    ui64 vChunkIndex = requestIt->VChunkIndex;
    segments->clear();
    segments->reserve(requestIt->Segments.size());

    for (auto [segment, segmentIt] : requestIt->Segments) {
        auto &[begin, end] = segment;
        SegmentsInFlight.erase(segmentIt);
        segments->emplace_back(segment);
    }

    RequestsInFlight.erase(requestIt);
    return segments;
}

void TSegmentManager::PushRequest(ui64 vchunkIndex, ui64 syncId, TSegment segment, ui64 *requestId, std::vector<TOutdatedRequest> *outdated) {
    Y_VERIFY(requestId != nullptr);

    DropSegment(vchunkIndex, segment, outdated);
    *requestId = NextRequestId++;
    auto &[begin, end] = segment;
    TRequestIt requestIt = RequestsInFlight.emplace(*requestId, {vchunkIndex, syncId, {}});
    TSegmentIt segmentIt = SegmentsInFlight.emplace({vchunkIndex, begin, end}, requestIt);
    requestIt->second.Segments.emplace(segment, segmentIt);
}
