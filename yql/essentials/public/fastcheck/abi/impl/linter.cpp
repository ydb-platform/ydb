#include <yql/essentials/public/fastcheck/abi/interface/linter.h>

#include "result.h"

#include <yql/essentials/public/fastcheck/abi/serialization/message.h>
#include <yql/essentials/public/fastcheck/abi/serialization/serialization.h>
#include <yql/essentials/public/fastcheck/internal/fastcheck.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/core/issue/yql_issue.h>

#include <library/cpp/json/json_reader.h>

struct TYqlLinterHandle: NYql::NFastCheck::NAbi::TResult {};
struct TYqlUdfMetaHandle: NYql::NFastCheck::NAbi::TResult {
    std::unique_ptr<NYql::TUdfMeta> Metadata;
};

namespace NYql::NFastCheck::NAbi {
namespace {

void FreeMetadata(void* context) noexcept {
    YqlUdfMetaFreeV1(static_cast<TYqlUdfMetaHandle*>(context));
}

const TYqlUdfMeta& MetadataAbi(const void* context) noexcept {
    return static_cast<const TYqlUdfMetaHandle*>(context)->Metadata->GetAbi();
}

int HasModule(const void* context, TYqlUdfMetaString module) noexcept {
    const auto& metadata = MetadataAbi(context);
    return metadata.HasModule(metadata.Context, module);
}

int HasFunction(const void* context, TYqlUdfMetaString module, TYqlUdfMetaString function) noexcept {
    const auto& metadata = MetadataAbi(context);
    return metadata.HasFunction(metadata.Context, module, function);
}

int GetMetadata(const void* context, TYqlUdfMetaString module, TYqlUdfMetaString function, TYqlUdfMetaFunction* result) noexcept {
    const auto& metadata = MetadataAbi(context);
    return metadata.GetMetadata(metadata.Context, module, function, result);
}

TChecksRequest DecodeRequest(const NProto::TLinterRequest& message) {
    TChecksRequest request;
    Decode(message, request);
    DecodeMap(message.GetClusterMapping(), request.ClusterMapping);
    DecodeFilters(message, request.Filters);
    return request;
}

NProto::TLinterResult Run(const TChecksRequest& request, bool listOnly) {
    NProto::TLinterResult result;
    if (listOnly) {
        for (const auto& name : ListChecksImpl(request.Filters)) {
            result.AddCheckNames(name);
        }
    } else {
        for (const auto& check : RunChecksImpl(request).Checks) {
            auto* item = result.AddChecks();
            item->SetCheckName(check.CheckName);
            item->SetSuccess(check.Success);
            IssuesToMessage(check.Issues, item->MutableIssues());
        }
    }
    return result;
}

TString Lint(const char* data, size_t size, const TYqlUdfMeta* metadata) {
    const auto message = ParseMessage<NProto::TLinterRequest>(data, size);
    auto request = DecodeRequest(message);
    TMaybe<TUdfMeta> borrowed;
    if (metadata) {
        if (metadata->Free == FreeMetadata) {
            request.UdfMeta = static_cast<const TYqlUdfMetaHandle*>(metadata->Context)->Metadata.get();
        } else {
            borrowed.ConstructInPlace(*metadata, false);
            request.UdfMeta = &*borrowed;
        }
    }
    return SerializeMessage(Run(request, message.GetListOnly()));
}

} // namespace
} // namespace NYql::NFastCheck::NAbi

extern "C" TYqlLinterHandle* YqlLinterV1(const char* request, size_t size, const TYqlUdfMeta* metadata) noexcept {
    return NYql::NFastCheck::NAbi::Execute<TYqlLinterHandle>([&](auto&) {
        return NYql::NFastCheck::NAbi::Lint(request, size, metadata);
    });
}

extern "C" TYqlUdfMetaHandle* YqlLinterLoadUdfMetaV1(const char* json, size_t size) noexcept {
    return NYql::NFastCheck::NAbi::Execute<TYqlUdfMetaHandle>([&](auto& handle) {
        Y_ENSURE(json || !size, "Null UDF metadata JSON with nonzero size");
        handle.Metadata = NYql::ParseUdfMeta(NJson::ReadJsonFastTree(TStringBuf(json ? json : "", size)));
        return TString();
    });
}

extern "C" void YqlUdfMetaFreeV1(TYqlUdfMetaHandle* handle) noexcept {
    NYql::NFastCheck::NAbi::Free(handle);
}

extern "C" const char* YqlUdfMetaErrorV1(const TYqlUdfMetaHandle* handle, size_t* size) noexcept {
    return NYql::NFastCheck::NAbi::GetError(*handle, size);
}

extern "C" TYqlUdfMeta YqlUdfMetaGetAbiV1(TYqlUdfMetaHandle* handle) noexcept {
    return {handle, NYql::NFastCheck::NAbi::FreeMetadata, NYql::NFastCheck::NAbi::HasModule,
            NYql::NFastCheck::NAbi::HasFunction, NYql::NFastCheck::NAbi::GetMetadata};
}

extern "C" TYqlLinterHandle* YqlLinterPreloadIssuesV1() noexcept {
    return NYql::NFastCheck::NAbi::Execute<TYqlLinterHandle>([](auto&) {
        NYql::PreloadIssueRegistry();
        return TString();
    });
}

extern "C" void YqlLinterFreeV1(TYqlLinterHandle* handle) noexcept {
    NYql::NFastCheck::NAbi::Free(handle);
}

extern "C" const char* YqlLinterResultV1(const TYqlLinterHandle* handle, size_t* size) noexcept {
    return NYql::NFastCheck::NAbi::GetOutput(*handle, size);
}

extern "C" const char* YqlLinterErrorV1(const TYqlLinterHandle* handle, size_t* size) noexcept {
    return NYql::NFastCheck::NAbi::GetError(*handle, size);
}
