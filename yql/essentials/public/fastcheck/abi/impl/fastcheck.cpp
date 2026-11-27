#include <yql/essentials/public/fastcheck/abi/interface/fastcheck.h>

#include "result.h"

#include <yql/essentials/public/fastcheck/abi/serialization/message.h>
#include <yql/essentials/public/fastcheck/abi/serialization/serialization.h>
#include <yql/essentials/public/fastcheck/internal/fastcheck.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

struct TYqlFastCheckHandle: NYql::NFastCheck::NAbi::TResult {};

namespace NYql::NFastCheck::NAbi {
namespace {

TString Check(const char* data, size_t size) {
    const auto message = ParseMessage<NProto::TFastCheckRequest>(data, size);
    TOptions options;
    Decode(message, options);
    DecodeMap(message.GetClusterMapping(), options.ClusterMapping);
    DecodeMap(message.GetSqlLibs(), options.SqlLibs);
    TIssues issues;
    NProto::TFastCheckResult result;
    result.SetSuccess(CheckProgramImpl(message.GetProgram(), options, issues));
    IssuesToMessage(issues, result.MutableIssues());
    return SerializeMessage(result);
}

} // namespace
} // namespace NYql::NFastCheck::NAbi

extern "C" TYqlFastCheckHandle* YqlFastCheckV1(const char* request, size_t size) noexcept {
    return NYql::NFastCheck::NAbi::Execute<TYqlFastCheckHandle>([&](auto&) {
        return NYql::NFastCheck::NAbi::Check(request, size);
    });
}

extern "C" void YqlFastCheckFreeV1(TYqlFastCheckHandle* handle) noexcept {
    NYql::NFastCheck::NAbi::Free(handle);
}

extern "C" const char* YqlFastCheckResultV1(const TYqlFastCheckHandle* handle, size_t* size) noexcept {
    return NYql::NFastCheck::NAbi::GetOutput(*handle, size);
}

extern "C" const char* YqlFastCheckErrorV1(const TYqlFastCheckHandle* handle, size_t* size) noexcept {
    return NYql::NFastCheck::NAbi::GetError(*handle, size);
}
