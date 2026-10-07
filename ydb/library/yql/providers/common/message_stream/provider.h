#pragma once
#include <yql/essentials/ast/yql_expr.h>
#include <library/cpp/threading/future/future.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NFq::NMessageStream {
const NYql::TStructExprType* MakeRawRowType(NYql::TExprContext& ctx);
TString ComposeAuthToken(const THashMap<TString, TString>& properties,
    const TString& fallbackToken = {}, const TString& serviceAccountId = {},
    const TString& serviceAccountSignature = {});

// Metadata transformers inspect the original future to report errors at the source position.
template<class T>
NThreading::TFuture<void> CompletionFuture(const NThreading::TFuture<T>& future) {
    auto completed = NThreading::NewPromise();
    future.NoexceptSubscribe([completed](const auto&) mutable { completed.TrySetValue(); });
    return completed.GetFuture();
}
}
