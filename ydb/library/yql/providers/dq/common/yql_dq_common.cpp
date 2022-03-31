#include "yql_dq_common.h"

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>

#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>

#include <ydb/library/yql/sql/sql.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>

#include <util/string/split.h>

namespace NYql {
namespace NCommon {

using namespace NKikimr::NMiniKQL;

TString GetSerializedResultType(const TString& program) {
    TScopedAlloc alloc;
    TTypeEnvironment typeEnv(alloc);

    TRuntimeNode programNode = DeserializeRuntimeNode(program, typeEnv);

    YQL_ENSURE(programNode.IsImmediate() && programNode.GetNode()->GetType()->IsStruct());

    // copy-paste from dq_task_runner.cpp
    auto& programStruct = static_cast<TStructLiteral&>(*programNode.GetNode());
    auto programType = programStruct.GetType();
    YQL_ENSURE(programType);

    auto programRootIdx = programType->FindMemberIndex("Program");
    YQL_ENSURE(programRootIdx);
    TRuntimeNode programRoot = programStruct.GetValue(*programRootIdx);
    YQL_ENSURE(programRoot.GetNode()->GetType()->IsCallable());
    auto programResultType = static_cast<const TCallableType*>(programRoot.GetNode()->GetType());
    YQL_ENSURE(programResultType->GetReturnType()->IsStream());
    auto programResultItemType = static_cast<const TStreamType*>(programResultType->GetReturnType())->GetItemType();

    return SerializeNode(programResultItemType, typeEnv);
}

TMaybe<TString> SqlToSExpr(const TString& query) {
    NSQLTranslation::TTranslationSettings settings;
    settings.SyntaxVersion = 1;
    settings.Mode = NSQLTranslation::ESqlMode::QUERY;
    settings.DefaultCluster = "undefined";
    settings.ClusterMapping[settings.DefaultCluster] = "undefined";
    settings.ClusterMapping["csv"] = "csv";
    settings.ClusterMapping["memory"] = "memory";
    settings.ClusterMapping["ydb"] = "ydb";
    settings.EnableGenericUdfs = true;
    settings.File = "generated.sql";

    auto astRes = NSQLTranslation::SqlToYql(query, settings);
    if (!astRes.Issues.Empty()) {
        Cerr << astRes.Issues.ToString() << Endl;
    }

    if (!astRes.Root) {
        return {};
    }

    TStringStream sexpr;
    astRes.Root->PrintTo(sexpr);
    return sexpr.Str();
}

bool ParseCounterName(TString* prefix, std::map<TString, TString>* labels, TString* name, const TString& counterName) {
    auto pos = counterName.find(":");
    if (pos == TString::npos) {
        return false;
    }
    *prefix = counterName.substr(0, pos);

    auto labelsString = counterName.substr(pos+1);

    *name = "";
    for (const auto& kv : StringSplitter(labelsString).Split(',')) {
        TStringBuf key, value;
        const TStringBuf& line = kv.Token();
        if (!line.empty()) {
            line.Split('=', key, value);
            if (key == "Name") {
                *name = value;
            } else {
                (*labels)[TString(key)] = TString(value);
            }
        }
    }

    return !name->empty();
}

bool IsRetriable(const NDq::TEvDq::TEvAbortExecution::TPtr& ev) {
    const auto statusCode = ev->Get()->Record.GetStatusCode();
    return statusCode != NYql::NDqProto::StatusIds::BAD_REQUEST;
}

bool NeedFallback(const NDq::TEvDq::TEvAbortExecution::TPtr& ev) {
    const auto& issues = ev->Get()->GetIssues();
    for (auto it = issues.begin(); it < issues.end(); it++) {
        if (it->GetCode() == TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR) {
            return true;
        }
    }
    return false;
}

} // namespace NCommon
} // namespace NYql
