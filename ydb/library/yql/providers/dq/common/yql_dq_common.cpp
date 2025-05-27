#include "yql_dq_common.h"

#include <yql/essentials/core/issue/protos/issue_id.pb.h>

#include <yql/essentials/utils/yql_panic.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/providers/common/mkql/yql_type_mkql.h>

#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <util/string/split.h>

namespace NYql {
namespace NCommon {

using namespace NKikimr::NMiniKQL;

TString GetSerializedTypeAnnotation(const NYql::TTypeAnnotationNode* typeAnn) {
    Y_ABORT_UNLESS(typeAnn);

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment typeEnv(alloc);

    NKikimr::NMiniKQL::TTypeBuilder typeBuilder(typeEnv);
    TStringStream errorStream;
    auto type = NCommon::BuildType(*typeAnn, typeBuilder, errorStream);
    Y_ENSURE(type, "Failed to compile type: " << errorStream.Str());
    return SerializeNode(type, typeEnv);
}

TString GetSerializedResultType(const TString& program) {
    TScopedAlloc alloc(__LOCATION__);
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

bool IsRetriable(NYql::NDqProto::StatusIds::StatusCode statusCode) {
    switch (statusCode) {
    case NYql::NDqProto::StatusIds::UNSPECIFIED:
    case NYql::NDqProto::StatusIds::SUCCESS:
    case NYql::NDqProto::StatusIds::BAD_REQUEST:
    case NYql::NDqProto::StatusIds::LIMIT_EXCEEDED:
    case NYql::NDqProto::StatusIds::UNSUPPORTED:
    case NYql::NDqProto::StatusIds::ABORTED:
    case NYql::NDqProto::StatusIds::CANCELLED:
        return false;
    case NYql::NDqProto::StatusIds::UNAVAILABLE:
    default:
        return true;
    }
}

bool IsRetriable(const NDq::TEvDq::TEvAbortExecution::TPtr& ev) {
    return IsRetriable(ev->Get()->Record.GetStatusCode());
}

bool NeedFallback(NYql::NDqProto::StatusIds::StatusCode statusCode) {
    switch (statusCode) {
    case NYql::NDqProto::StatusIds::UNSPECIFIED:
    case NYql::NDqProto::StatusIds::SUCCESS:
    case NYql::NDqProto::StatusIds::ABORTED:
    case NYql::NDqProto::StatusIds::CANCELLED:
    case NYql::NDqProto::StatusIds::BAD_REQUEST:
    case NYql::NDqProto::StatusIds::PRECONDITION_FAILED:
        return false;
    case NYql::NDqProto::StatusIds::LIMIT_EXCEEDED:
    case NYql::NDqProto::StatusIds::UNAVAILABLE:
    default:
        return true;
    }
}

bool NeedFallback(const NDq::TEvDq::TEvAbortExecution::TPtr& ev) {
    return NeedFallback(ev->Get()->Record.GetStatusCode());
}

} // namespace NCommon
} // namespace NYql
