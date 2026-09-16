#include "has_stream_matcher.h"


namespace NKikimr::NWorkloadManager {

bool MatchesStream(const std::optional<bool>& hasStream,
                   const NKqp::TUserRequestContext& userRequestContext) {
    if (!hasStream) {
        return true;
    }
    return *hasStream ? userRequestContext.IsStreamingQuery : !userRequestContext.IsStreamingQuery;
}

}  // namespace NKikimr::NWorkloadManager
