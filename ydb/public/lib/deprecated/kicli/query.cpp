#include "kicli.h"

namespace NKikimr {
namespace NClient {

TQuery::TQuery(TKikimr& kikimr)
    : Kikimr(&kikimr)
{}

void TQuery::ParseTextParameters(NKikimrMiniKQL::TParams& params, const TString& parameters) {
    if (!parameters.empty()) {
        bool ok = ::google::protobuf::TextFormat::ParseFromString(parameters, &params);
        Y_ABORT_UNLESS(ok);
    }
}

TTextQuery::TTextQuery(TKikimr& kikimr, const TString& program)
    : TQuery(kikimr)
    , TextProgram(program)
{}

TPrepareResult TTextQuery::SyncPrepare() const {
    return AsyncPrepare().GetValue(TDuration::Max());
}

NThreading::TFuture<TPrepareResult> TTextQuery::AsyncPrepare() const {
    return Kikimr->PrepareQuery(*this);
}

TQueryResult TTextQuery::SyncExecute(const NKikimrMiniKQL::TParams& parameters) const {
    return AsyncExecute(parameters).GetValue(TDuration::Max());
}

TQueryResult TTextQuery::SyncExecute(const TString& parameters) const {
    return AsyncExecute(parameters).GetValue(TDuration::Max());
}

NThreading::TFuture<TQueryResult> TTextQuery::AsyncExecute(const NKikimrMiniKQL::TParams& parameters) const {
    return Kikimr->ExecuteQuery(*this, parameters);
}

NThreading::TFuture<TQueryResult> TTextQuery::AsyncExecute(const TString& parameters) const {
    return Kikimr->ExecuteQuery(*this, parameters);
}

TPreparedQuery::TPreparedQuery(const TQuery& textQuery, const TString& program)
    : TQuery(textQuery)
    , CompiledProgram(program)
{}

TQueryResult TPreparedQuery::SyncExecute(const NKikimrMiniKQL::TParams& parameters) const {
    return AsyncExecute(parameters).GetValue(TDuration::Max());
}

TQueryResult TPreparedQuery::SyncExecute(const TString& parameters) const {
    return AsyncExecute(parameters).GetValue(TDuration::Max());
}

NThreading::TFuture<TQueryResult> TPreparedQuery::AsyncExecute(const NKikimrMiniKQL::TParams& parameters) const {
    return Kikimr->ExecuteQuery(*this, parameters);
}

NThreading::TFuture<TQueryResult> TPreparedQuery::AsyncExecute(const TString& parameters) const {
    return Kikimr->ExecuteQuery(*this, parameters);
}

TUnbindedQuery::TUnbindedQuery(const TString& program)
    : CompiledProgram(program)
{}

TUnbindedQuery TPreparedQuery::Unbind() const {
    return TUnbindedQuery(CompiledProgram);
}

}
}
