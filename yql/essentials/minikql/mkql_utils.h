#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NMiniKQL {

// TODO: move this class to beter place
class TStatus
{
public:
    inline static const TStatus Ok() {
        return TStatus();
    }

    inline static TStatus Error(const TStringBuf& error) {
        Y_DEBUG_ABORT_UNLESS(!error.empty());
        return TStatus(TString(error));
    }

    inline static TStatus Error(TString&& error) {
        Y_DEBUG_ABORT_UNLESS(!error.empty());
        return TStatus(std::move(error));
    }

    inline static TStatus Error() {
        return TStatus(TString(TStringBuf("Error: ")));
    }

    template <class T>
    inline TStatus& operator<<(const T& t) {
        Error_.append(t);
        return *this;
    }

    inline bool IsOk() const {
        return Error_.empty();
    }

    inline const TString& GetError() const {
        return Error_;
    }

private:
    inline TStatus() = default;

    inline TStatus(TString&& error)
        : Error_(std::move(error))
    {
    }

private:
    TString Error_;
};

} // namespace NMiniKQL
} // namespace NKikimr
