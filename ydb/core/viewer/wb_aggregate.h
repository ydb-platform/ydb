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

    static std::unique_ptr<TResponseType> AggregateResponses(TVector<std::unique_ptr<TResponseType>>& responses) {
        std::unique_ptr<TResponseType> result = std::make_unique<TResponseType>();
        for (const auto& response : responses) {
            AggregateMessage(result->Record, response->Record);
        }
        return result;
    }

    static std::unique_ptr<TResponseType> AggregateResponses(TMap<TTabletId, std::unique_ptr<TResponseType>>& responses) {
        std::unique_ptr<TResponseType> result = std::make_unique<TResponseType>();
        for (const auto& response : responses) {
            AggregateMessage(result->Record, response.second->Record);
        }
        return result;
    }
};

template <typename ResponseType>
std::unique_ptr<ResponseType> AggregateWhiteboardResponses(TVector<std::unique_ptr<ResponseType>>& responses) {
    return TWhiteboardAggregator<ResponseType>::AggregateResponses(responses);
}

template <typename ResponseType>
std::unique_ptr<ResponseType> AggregateWhiteboardResponses(TMap<TTabletId, std::unique_ptr<ResponseType>>& responses) {
    return TWhiteboardAggregator<ResponseType>::AggregateResponses(responses);
}

}
