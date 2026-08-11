#include "error_code.h"

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/misc/leaky_global.h>

#include <util/string/split.h>

#include <util/system/type_name.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Remove this once we find all duplicate error codes.
static YT_DEFINE_LEAKY_GLOBAL(NLogging::TLogger, Logger, "ErrorCode")

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
        YT_TLOG_FATAL("Duplicate error code")
            .With("Code", code)
            .With("StoredCodeInfo", CodeToInfo_[code])
            .With("NewCodeInfo", errorCodeInfo);
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

void TErrorCodeRegistry::RegisterErrorCodeRange(int from, int to, std::string namespaceName, std::function<std::string(int)> formatter)
{
    YT_VERIFY(from <= to);

    TErrorCodeRangeInfo newRange{from, to, std::move(namespaceName), std::move(formatter)};
    for (const auto& range : ErrorCodeRanges_) {
        YT_TLOG_FATAL_IF(
            range.Intersects(newRange),
            "Intersecting error code ranges registered")
            .With("FirstRange", range)
            .With("SecondRange", newRange);
    }
    ErrorCodeRanges_.push_back(std::move(newRange));
    CheckCodesAgainstRanges();
}

void TErrorCodeRegistry::CheckCodesAgainstRanges() const
{
    for (const auto& [code, info] : CodeToInfo_) {
        for (const auto& range : ErrorCodeRanges_) {
            YT_TLOG_FATAL_IF(
                range.Contains(code),
                "Error code range contains another registered code")
                .With("Range", range)
                .With("Code", code)
                .With("RangeCodeInfo", range.Get(code))
                .With("StandaloneCodeInfo", info);
        }
    }
}

std::string TErrorCodeRegistry::ParseNamespace(const std::type_info& errorCodeEnumTypeInfo)
{
    std::string name;
    // Ensures that "EErrorCode" is found as a substring in the type name and stores the prefix before
    // the first occurrence into #name.
    YT_VERIFY(StringSplitter(
        TypeName(errorCodeEnumTypeInfo)).SplitByString("EErrorCode").Limit(2).TryCollectInto(&name, &std::ignore));

    // TypeName returns name in form "enum ErrorCode" on Windows
    constexpr TStringBuf enumPrefix = "enum ";
    if (name.starts_with(enumPrefix)) {
        name.erase(0, enumPrefix.size());
    }

    // If the enum was declared directly in the global namespace, #name should be empty.
    // Otherwise, #name should end with "::".
    constexpr TStringBuf namespaceSeparator = "::";
    if (!name.empty()) {
        YT_VERIFY(name.ends_with(namespaceSeparator));
        name.resize(name.size() - namespaceSeparator.size());
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
