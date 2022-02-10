#pragma once

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NTvmAuth {
    class TClientStatus {
    public:
        enum ECode {
            Ok,
            Warning,
            Error,
            IncompleteTicketsSet,
        };

        TClientStatus(ECode state, TString&& lastError)
            : Code_(state)
            , LastError_(std::move(lastError))
        {
        }

        TClientStatus() = default;
        TClientStatus(const TClientStatus&) = default;
        TClientStatus(TClientStatus&&) = default;

        TClientStatus& operator=(const TClientStatus&) = default;
        TClientStatus& operator=(TClientStatus&&) = default;

        ECode GetCode() const {
            return Code_;
        }

        const TString& GetLastError() const {
            return LastError_;
        }

        TString CreateJugglerMessage() const {
            return TStringBuilder() << GetJugglerCode() << ";TvmClient: " << LastError_ << "\n";
        }

    private:
        int32_t GetJugglerCode() const {
            switch (Code_) {
                case ECode::Ok:
                    return 0; // OK juggler check state
                case ECode::Warning:
                case ECode::IncompleteTicketsSet:
                    return 1; // WARN juggler check state
                case ECode::Error:
                    return 2; // CRIT juggler check state
            }
            return 2; // This should not happen, so set check state as CRIT.
        }

        ECode Code_ = Ok;
        TString LastError_;
    };

    static inline bool operator==(const TClientStatus& l, const TClientStatus& r) noexcept {
        return l.GetCode() == r.GetCode() && l.GetLastError() == r.GetLastError();
    }

    static inline bool operator==(const TClientStatus& l, const TClientStatus::ECode r) noexcept {
        return l.GetCode() == r;
    }

    static inline bool operator==(const TClientStatus::ECode l, const TClientStatus& r) noexcept {
        return r.GetCode() == l;
    }

    static inline bool operator!=(const TClientStatus& l, const TClientStatus& r) noexcept {
        return !(l == r);
    }

    static inline bool operator!=(const TClientStatus& l, const TClientStatus::ECode r) noexcept {
        return !(l == r);
    }

    static inline bool operator!=(const TClientStatus::ECode l, const TClientStatus& r) noexcept {
        return !(l == r);
    }
}
