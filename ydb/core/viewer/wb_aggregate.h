#pragma once
#include <ydb/core/node_whiteboard/node_whiteboard.h>

namespace NKikimr::NViewer {

using namespace NNodeWhiteboard;
using namespace ::google::protobuf;

template<typename ResponseType>
struct TWhiteboardInfo;

void AggregateMessage(::google::protobuf::Message& protoTo, const ::google::protobuf::Message& protoFrom);

template<typename ResponseType>
class TWhiteboardAggregator {
public:
    using TResponseType = ResponseType;

    static THolder<TResponseType> AggregateResponses(TVector<THolder<TResponseType>>& responses) {
        THolder<TResponseType> result = MakeHolder<TResponseType>();
        for (const auto& response : responses) {
            AggregateMessage(result->Record, response->Record);
        }
        return result;
    }

    static THolder<TResponseType> AggregateResponses(TMap<TTabletId, THolder<TResponseType>>& responses) {
        THolder<TResponseType> result = MakeHolder<TResponseType>();
        for (const auto& response : responses) {
            AggregateMessage(result->Record, response.second->Record);
        }
        return result;
    }
};

template <typename ResponseType>
THolder<ResponseType> AggregateWhiteboardResponses(TVector<THolder<ResponseType>>& responses) {
    return TWhiteboardAggregator<ResponseType>::AggregateResponses(responses);
}

template <typename ResponseType>
THolder<ResponseType> AggregateWhiteboardResponses(TMap<TTabletId, THolder<ResponseType>>& responses) {
    return TWhiteboardAggregator<ResponseType>::AggregateResponses(responses);
}

}
