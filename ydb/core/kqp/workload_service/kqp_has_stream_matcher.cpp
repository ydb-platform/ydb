#include "kqp_has_stream_matcher.h"
#include <ydb/core/kqp/common/kqp_user_request_context.h>

namespace NKikimr::NKqp::NWorkload {

bool MatchesStream(const std::optional<bool>& hasStream,
                   const TUserRequestContext& userRequestContext) {
    if (!hasStream) {
        return true;
    }
    return *hasStream ? userRequestContext.IsStreamingQuery : !userRequestContext.IsStreamingQuery;
}

}  // namespace NKikimr::NKqp::NWorkload
