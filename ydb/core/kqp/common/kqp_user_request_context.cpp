#include "kqp_user_request_context.h"

namespace NKikimr::NKqp {

    void TUserRequestContext::Out(IOutputStream& o) const {
        o << "{" << " TraceId: " << TraceId << ", Database: " << Database << ", SessionId: " << SessionId << ", PoolId: " << PoolId;
        if (Database != DatabaseId) {
            o << ", DatabaseId: " << DatabaseId;
        }
        if (CustomerSuppliedId) {
            o << ", CustomerSuppliedId: " << CustomerSuppliedId;
        }
        if (CurrentExecutionId) {
            o << ", CurrentExecutionId: " << CurrentExecutionId;
            o << ", RunScriptActorId: " << RunScriptActorId.ToString();
        }
        o << "}";
    }

    void SerializeCtxToMap(const TUserRequestContext& ctx, google::protobuf::Map<TString, TString>& resultMap) {
        resultMap["TraceId"] = ctx.TraceId;
        resultMap["Database"] = ctx.Database;
        resultMap["DatabaseId"] = ctx.DatabaseId;
        resultMap["SessionId"] = ctx.SessionId;
        resultMap["CurrentExecutionId"] = ctx.CurrentExecutionId;
        resultMap["CustomerSuppliedId"] = ctx.CustomerSuppliedId;
        resultMap["PoolId"] = ctx.PoolId;
        resultMap["RunScriptActorId"] = ctx.RunScriptActorId.ToString();  // Only for logging
    }
}
