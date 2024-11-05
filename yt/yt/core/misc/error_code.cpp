#include "error_code.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/singleton.h>

#include <library/cpp/yt/misc/global.h>

#include <util/string/split.h>

#include <util/system/type_name.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Remove this once we find all duplicate error codes.
YT_DEFINE_GLOBAL(NLogging::TLogger, Logger, "ErrorCode")

////////////////////////////////////////////////////////////////////////////////

bool TErrorCodeRegistry::TErrorCodeInfo::operator==(const TErrorCodeInfo& rhs) const
{
    return Namespace == rhs.Namespace && Name == rhs.Name;
}

TErrorCodeRegistry* TErrorCodeRegistry::Get()
{
    return LeakySingleton<TErrorCodeRegistry>();
}

TErrorCodeRegistry::TErrorCodeInfo TErrorCodeRegistry::Get(int code) const
{
    auto it = CodeToInfo_.find(code);
    if (it != CodeToInfo_.end()) {
        return it->second;
    }
    for (const auto& range : ErrorCodeRanges_) {
        if (range.Contains(code)) {
            return range.Get(code);
        }
    }
    return {"NUnknown", Format("ErrorCode%v", code)};
}

THashMap<int, TErrorCodeRegistry::TErrorCodeInfo> TErrorCodeRegistry::GetAllErrorCodes() const
{
    return CodeToInfo_;
}

std::vector<TErrorCodeRegistry::TErrorCodeRangeInfo> TErrorCodeRegistry::GetAllErrorCodeRanges() const
{
    return ErrorCodeRanges_;
}

void TErrorCodeRegistry::RegisterErrorCode(int code, const TErrorCodeInfo& errorCodeInfo)
{
    if (!CodeToInfo_.insert({code, errorCodeInfo}).second) {
        // TODO(achulkov2): Deal with duplicate TransportError in NRpc and NBus.
        if (code == 100) {
            return;
        }
        // TODO(yuryalekseev): Deal with duplicate SslError in NRpc and NBus.
        if (code == 119) {
            return;
        }
        YT_LOG_FATAL(
            "Duplicate error code (Code: %v, StoredCodeInfo: %v, NewCodeInfo: %v)",
            code,
            CodeToInfo_[code],
            errorCodeInfo);
    }
}

TErrorCodeRegistry::TErrorCodeInfo TErrorCodeRegistry::TErrorCodeRangeInfo::Get(int code) const
{
    return {Namespace, Formatter(code)};
}

bool TErrorCodeRegistry::TErrorCodeRangeInfo::Intersects(const TErrorCodeRangeInfo& other) const
{
    return std::max(From, other.From) <= std::min(To, other.To);
}

bool TErrorCodeRegistry::TErrorCodeRangeInfo::Contains(int value) const
{
    return From <= value && value <= To;
}

void TErrorCodeRegistry::RegisterErrorCodeRange(int from, int to, TString namespaceName, std::function<TString(int)> formatter)
{
    YT_VERIFY(from <= to);

    TErrorCodeRangeInfo newRange{from, to, std::move(namespaceName), std::move(formatter)};
    for (const auto& range : ErrorCodeRanges_) {
        YT_LOG_FATAL_IF(
            range.Intersects(newRange),
            "Intersecting error code ranges registered (FirstRange: %v, SecondRange: %v)",
            range,
            newRange);
    }
    ErrorCodeRanges_.push_back(std::move(newRange));
    CheckCodesAgainstRanges();
}

void TErrorCodeRegistry::CheckCodesAgainstRanges() const
{
    for (const auto& [code, info] : CodeToInfo_) {
        for (const auto& range : ErrorCodeRanges_) {
            YT_LOG_FATAL_IF(
                range.Contains(code),
                "Error code range contains another registered code "
                "(Range: %v, Code: %v, RangeCodeInfo: %v, StandaloneCodeInfo: %v)",
                range,
                code,
                range.Get(code),
                info);
        }
    }
}

TString TErrorCodeRegistry::ParseNamespace(const std::type_info& errorCodeEnumTypeInfo)
{
    TString name;
    // Ensures that "EErrorCode" is found as a substring in the type name and stores the prefix before
    // the first occurrence into #name.
    YT_VERIFY(StringSplitter(
        TypeName(errorCodeEnumTypeInfo)).SplitByString("EErrorCode").Limit(2).TryCollectInto(&name, &std::ignore));

    // TypeName returns name in form "enum ErrorCode" on Windows
    if (name.StartsWith("enum ")) {
        name.remove(0, 5);
    }

    // If the enum was declared directly in the global namespace, #name should be empty.
    // Otherwise, #name should end with "::".
    if (!name.empty()) {
        YT_VERIFY(name.EndsWith("::"));
        name.resize(name.size() - 2);
    }
    return name;
}

void FormatValue(
    TStringBuilderBase* builder,
    const TErrorCodeRegistry::TErrorCodeInfo& errorCodeInfo,
    TStringBuf /*spec*/)
{
    if (errorCodeInfo.Namespace.empty()) {
        Format(builder, "EErrorCode::%v", errorCodeInfo.Name);
        return;
    }
    Format(builder, "%v::EErrorCode::%v", errorCodeInfo.Namespace, errorCodeInfo.Name);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TErrorCodeRegistry::TErrorCodeRangeInfo& errorCodeRangeInfo,
    TStringBuf /*spec*/)
{
    Format(builder, "%v-%v", errorCodeRangeInfo.From, errorCodeRangeInfo.To);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
