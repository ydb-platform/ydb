#pragma once

#include "utils.h"

#include <array>

namespace NTvmAuth {
    class TLastError {
    public:
        enum class EType {
            NonRetriable,
            Retriable,
        };

        enum class EScope {
            ServiceTickets,
            PublicKeys,
            Roles,
            TvmtoolConfig,

            COUNT,
        };

        using TLastErr = TMaybe<std::pair<EType, TString>>;

        struct TLastErrors: public TAtomicRefCount<TLastErrors> {
            std::array<TLastErr, (int)EScope::COUNT> Errors;
        };
        using TLastErrorsPtr = TIntrusiveConstPtr<TLastErrors>;

    public:
        TLastError();

        TString GetLastError(bool isOk, EType* type = nullptr) const;

        TString ProcessHttpError(EScope scope, TStringBuf path, int code, const TString& msg) const;
        void ProcessError(EType type, EScope scope, const TStringBuf msg) const;
        void ClearError(EScope scope);
        void ClearErrors();
        void ThrowLastError();

    private:
        template <typename Func>
        void Update(EScope scope, Func func) const;

    private:
        const TString OK_ = "OK";

        mutable NUtils::TProtectedValue<TLastErrorsPtr> LastErrors_;
    };
}
