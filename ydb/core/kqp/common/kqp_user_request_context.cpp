#include "kqp_user_request_context.h"

namespace NKikimr::NKqp {
    
    void TUserRequestContext::Out(IOutputStream& o) const {
        o << "{" << " TraceId: " << TraceId << ", Database: " << Database << ", SessionId: " << SessionId << ", CurrentExecutionId: " << CurrentExecutionId << ", CustomerSuppliedId: " << CustomerSuppliedId  << "}";
    }

    void SerializeCtxToMap(const TUserRequestContext& ctx, google::protobuf::Map<TString, TString>& resultMap) {
        resultMap["TraceId"] = ctx.TraceId;
        resultMap["Database"] = ctx.Database;
        resultMap["SessionId"] = ctx.SessionId;
        resultMap["CurrentExecutionId"] = ctx.CurrentExecutionId;
        resultMap["CustomerSuppliedId"] = ctx.CustomerSuppliedId;
    }
}
