#include "origin_attributes.h"
#include "error_attributes.h"

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/misc/thread_name.h>
#include <library/cpp/yt/misc/tls.h>

#include <library/cpp/yt/string/format.h>

#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_THREAD_LOCAL(bool, ErrorSanitizerEnabled, false);
YT_DEFINE_THREAD_LOCAL(TInstant, ErrorSanitizerDatetimeOverride);
YT_DEFINE_THREAD_LOCAL(TSharedRef, ErrorSanitizerLocalHostNameOverride);

TErrorSanitizerGuard::TErrorSanitizerGuard(TInstant datetimeOverride, TSharedRef localHostNameOverride)
    : SavedEnabled_(ErrorSanitizerEnabled())
    , SavedDatetimeOverride_(ErrorSanitizerDatetimeOverride())
    , SavedLocalHostNameOverride_(ErrorSanitizerLocalHostNameOverride())
{
    ErrorSanitizerEnabled() = true;
    ErrorSanitizerDatetimeOverride() = datetimeOverride;
    ErrorSanitizerLocalHostNameOverride() = std::move(localHostNameOverride);
}

TErrorSanitizerGuard::~TErrorSanitizerGuard()
{
    YT_ASSERT(ErrorSanitizerEnabled());

    ErrorSanitizerEnabled() = SavedEnabled_;
    ErrorSanitizerDatetimeOverride() = SavedDatetimeOverride_;
    ErrorSanitizerLocalHostNameOverride() = std::move(SavedLocalHostNameOverride_);
}

bool IsErrorSanitizerEnabled() noexcept
{
    return ErrorSanitizerEnabled();
}

////////////////////////////////////////////////////////////////////////////////

bool TOriginAttributes::operator==(const TOriginAttributes& other) const noexcept
{
    return
        Host == other.Host &&
        Datetime == other.Datetime &&
        Pid == other.Pid &&
        Tid == other.Tid &&
        ExtensionData == other.ExtensionData;
}

void TOriginAttributes::Capture()
{
    if (ErrorSanitizerEnabled()) {
        Datetime = ErrorSanitizerDatetimeOverride();
        HostHolder = ErrorSanitizerLocalHostNameOverride();
        Host = HostHolder.empty() ? TStringBuf() : TStringBuf(HostHolder.Begin(), HostHolder.End());
        return;
    }

    Datetime = TInstant::Now();
    Pid = GetPID();
    Tid = TThread::CurrentThreadId();
    ThreadName = GetCurrentThreadName();
    ExtensionData = NDetail::GetExtensionData();
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

std::optional<TOriginAttributes::TErasedExtensionData> GetExtensionData()
{
    using TFunctor = TOriginAttributes::TErasedExtensionData(*)();

    if (auto strong = NGlobal::GetErasedVariable(GetExtensionDataTag)) {
        return strong->AsConcrete<TFunctor>()();
    }
    return std::nullopt;
}

std::string FormatOrigin(const TOriginAttributes& attributes)
{
    using TFunctor = TString(*)(const TOriginAttributes&);

    if (auto strong = NGlobal::GetErasedVariable(FormatOriginTag)) {
        return strong->AsConcrete<TFunctor>()(attributes);
    }

    return Format(
        "%v (pid %v, thread %v)",
        attributes.Host,
        attributes.Pid,
        MakeFormatterWrapper([&] (auto* builder) {
            auto threadName = attributes.ThreadName.ToStringBuf();
            if (threadName.empty()) {
                FormatValue(builder, attributes.Tid, "v");
                return;
            }
            FormatValue(builder, threadName, "v");
        }));
}

////////////////////////////////////////////////////////////////////////////////

TOriginAttributes ExtractFromDictionary(TErrorAttributes* attributes)
{
    using TFunctor = TOriginAttributes(*)(TErrorAttributes*);

    if (auto strong = NGlobal::GetErasedVariable(ExtractFromDictionaryTag)) {
        return strong->AsConcrete<TFunctor>()(attributes);
    }

    return ExtractFromDictionaryDefault(attributes);
}

////////////////////////////////////////////////////////////////////////////////

TOriginAttributes ExtractFromDictionaryDefault(TErrorAttributes* attributes)
{
    TOriginAttributes result;
    if (attributes == nullptr) {
        return result;
    }

    static const std::string HostKey("host");
    result.HostHolder = TSharedRef::FromString(attributes->GetAndRemove(HostKey, std::string()));
    result.Host = result.HostHolder.empty() ? TStringBuf() : TStringBuf(result.HostHolder.Begin(), result.HostHolder.End());

    static const std::string DatetimeKey("datetime");
    result.Datetime = attributes->GetAndRemove(DatetimeKey, TInstant());

    static const std::string PidKey("pid");
    result.Pid = attributes->GetAndRemove(PidKey, TProcessId{});

    static const std::string TidKey("tid");
    result.Tid = attributes->GetAndRemove(TidKey, NThreading::InvalidThreadId);

    static const std::string ThreadNameKey("thread");
    result.ThreadName = {attributes->GetAndRemove(ThreadNameKey, std::string())};

    return result;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
