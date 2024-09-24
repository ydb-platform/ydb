#pragma once

#include <library/cpp/yt/global/access.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/misc/guid.h>
#include <library/cpp/yt/misc/thread_name.h>

#include <library/cpp/yt/threading/public.h>

#include <util/datetime/base.h>

#include <util/system/getpid.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! When this guard is set, newly created errors do not have non-deterministic
//! system attributes and have "datetime" and "host" attributes overridden with a given values.
class TErrorSanitizerGuard
    : public TNonCopyable
{
public:
    TErrorSanitizerGuard(TInstant datetimeOverride, TSharedRef localHostNameOverride);
    ~TErrorSanitizerGuard();

private:
    const bool SavedEnabled_;
    const TInstant SavedDatetimeOverride_;
    const TSharedRef SavedLocalHostNameOverride_;
};

bool IsErrorSanitizerEnabled() noexcept;

////////////////////////////////////////////////////////////////////////////////

struct TOriginAttributes
{
    static constexpr size_t ExtensionDataByteSizeCap = 64;
    using TErasedExtensionData = TErasedStorage<ExtensionDataByteSizeCap>;

    TProcessId Pid;

    NThreading::TThreadId Tid;
    TThreadName ThreadName;

    TInstant Datetime;

    TSharedRef HostHolder;
    mutable TStringBuf Host;

    // Opaque storage for data from yt/yt/core.
    // Currently may contain FiberId, TraceId, SpandId.
    std::optional<TErasedExtensionData> ExtensionData;

    bool operator==(const TOriginAttributes& other) const noexcept;

    void Capture();
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

inline constexpr NGlobal::TVariableTag GetExtensionDataTag = {};
inline constexpr NGlobal::TVariableTag FormatOriginTag = {};
inline constexpr NGlobal::TVariableTag ExtractFromDictionaryTag = {};

////////////////////////////////////////////////////////////////////////////////

// These are "weak" symbols.
// NB(arkady-e1ppa): ExtractFromDictionary symbol is left in yt/yt/core/misc/origin_attributes
// because it depends on ytree for now.
std::optional<TOriginAttributes::TErasedExtensionData> GetExtensionData();
TString FormatOrigin(const TOriginAttributes& attributes);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
