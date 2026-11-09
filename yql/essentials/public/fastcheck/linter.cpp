#include "linter.h"

#include <yql/essentials/public/fastcheck/abi/interface/linter.h>
#include <yql/essentials/public/fastcheck/abi/serialization/result.h>
#include <yql/essentials/public/fastcheck/abi/serialization/serialization.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NYql::NFastCheck {
namespace {

NAbi::NProto::TLinterResult ReadResult(TYqlLinterHandle* handle) {
    return NAbi::ReadResult<NAbi::NProto::TLinterResult>(handle,
                                                         YqlLinterFreeV1, YqlLinterResultV1, YqlLinterErrorV1);
}

NAbi::NProto::TLinterResult Invoke(const NAbi::NProto::TLinterRequest& request, const TUdfMeta* metadata) {
    const TString data = NAbi::SerializeMessage(request);
    return ReadResult(YqlLinterV1(data.data(), data.size(), metadata ? &metadata->GetAbi() : nullptr));
}

} // namespace

TSet<TString> ListChecks(const TMaybe<TVector<TCheckFilter>>& filters) {
    NAbi::NProto::TLinterRequest request;
    request.SetListOnly(true);
    NAbi::EncodeFilters(filters, request);
    const auto result = Invoke(request, nullptr);
    return {result.GetCheckNames().begin(), result.GetCheckNames().end()};
}

TChecksResponse RunChecks(const TChecksRequest& request) {
    NAbi::NProto::TLinterRequest message;
    NAbi::Encode(request, message);
    NAbi::EncodeMap(request.ClusterMapping, *message.MutableClusterMapping());
    NAbi::EncodeFilters(request.Filters, message);
    const auto result = Invoke(message, request.UdfMeta);
    TChecksResponse response;
    for (const auto& item : result.GetChecks()) {
        TCheckResponse check{.CheckName = item.GetCheckName(), .Success = item.GetSuccess()};
        IssuesFromMessage(item.GetIssues(), check.Issues);
        response.Checks.push_back(std::move(check));
    }
    return response;
}

TUdfMeta LoadUdfMeta(TStringBuf json) {
    auto* handle = YqlLinterLoadUdfMetaV1(json.data(), json.size());
    Y_DEFER {
        if (handle) {
            YqlUdfMetaFreeV1(handle);
        }
    };
    size_t size = 0;
    const char* error = YqlUdfMetaErrorV1(handle, &size);
    Y_ENSURE(!size, TStringBuf(error, size));
    TUdfMeta result(YqlUdfMetaGetAbiV1(handle));
    handle = nullptr;
    return result;
}

void PreloadIssues() {
    ReadResult(YqlLinterPreloadIssuesV1());
}

} // namespace NYql::NFastCheck
