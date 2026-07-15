#pragma once

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/property.h>

#include <yt/yt/library/coredumper/public.h>

#include <any>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Thrown when an assertion is not satisfied and SafeAssertionMode = true.
class TAssertionFailedException
{
public:
    DEFINE_BYVAL_RO_PROPERTY(std::string, Expression);
    DEFINE_BYVAL_RO_PROPERTY(std::string, StackTrace);
    DEFINE_BYVAL_RO_PROPERTY(std::optional<std::string>, CorePath);

public:
    TAssertionFailedException(
        const std::string& expression,
        const std::string& stackTrace,
        const std::optional<std::string>& corePath);
};

////////////////////////////////////////////////////////////////////////////////

std::any CreateSafeAssertionGuard(
    NCoreDump::ICoreDumperPtr coreDumper,
    NConcurrency::TAsyncSemaphorePtr coreSemaphore,
    std::vector<TString> coreNotes = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
