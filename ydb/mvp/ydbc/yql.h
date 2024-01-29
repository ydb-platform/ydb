#pragma once

#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <library/cpp/yson/writer.h>

TMaybe<TString> KqpResultToYson(const NKikimrMiniKQL::TResult& kqpResult,
    const NYson::EYsonFormat& ysonFormat, NYql::TExprContext& ctx);
