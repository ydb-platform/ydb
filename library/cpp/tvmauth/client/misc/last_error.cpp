#include "last_error.h" 
 
#include <util/string/builder.h> 
 
namespace NTvmAuth { 
    TLastError::TLastError() 
        : LastErrors_(MakeIntrusiveConst<TLastErrors>()) 
    { 
    } 
 
    TString TLastError::GetLastError(bool isOk, EType* type) const { 
        if (isOk) { 
            return OK_; 
        } 
 
        const TLastErrorsPtr ptr = LastErrors_.Get(); 
 
        for (const TLastErr& err : ptr->Errors) { 
            if (err && err->first == EType::NonRetriable) { 
                if (type) { 
                    *type = EType::NonRetriable; 
                } 
                return err->second; 
            } 
        } 
 
        for (const TLastErr& err : ptr->Errors) { 
            if (err) { 
                if (type) { 
                    *type = EType::Retriable; 
                } 
                return err->second; 
            } 
        } 
 
        if (type) { 
            *type = EType::NonRetriable; 
        } 
        return "Internal client error: failed to collect last useful error message, please report this message to tvm-dev@yandex-team.ru"; 
    } 
 
    TString TLastError::ProcessHttpError(TLastError::EScope scope, 
                                         TStringBuf path, 
                                         int code, 
                                         const TString& msg) const { 
        TString err = TStringBuilder() << "Path:" << path << ".Code=" << code << ": " << msg; 
 
        ProcessError(code >= 400 && code < 500 ? EType::NonRetriable 
                                               : EType::Retriable, 
                     scope, 
                     err); 
 
        return err; 
    } 
 
    void TLastError::ProcessError(TLastError::EType type, TLastError::EScope scope, const TStringBuf msg) const { 
        Update(scope, [&](TLastErr& lastError) { 
            if (lastError && lastError->first == EType::NonRetriable && type == EType::Retriable) { 
                return false; 
            } 
 
            TString err = TStringBuilder() << scope << ": " << msg; 
            err.erase(std::remove(err.begin(), err.vend(), '\r'), err.vend()); 
            std::replace(err.begin(), err.vend(), '\n', ' '); 
 
            lastError = {type, std::move(err)}; 
            return true; 
        }); 
    } 
 
    void TLastError::ClearError(TLastError::EScope scope) { 
        Update(scope, [&](TLastErr& lastError) { 
            if (!lastError) { 
                return false; 
            } 
 
            lastError.Clear(); 
            return true; 
        }); 
    } 
 
    void TLastError::ClearErrors() { 
        for (size_t idx = 0; idx < (size_t)EScope::COUNT; ++idx) { 
            ClearError((EScope)idx); 
        } 
    } 
 
    void TLastError::ThrowLastError() { 
        EType type; 
        TString err = GetLastError(false, &type); 
 
        switch (type) { 
            case EType::NonRetriable: 
                ythrow TNonRetriableException() 
                    << "Failed to start TvmClient. Do not retry: " 
                    << err; 
            case EType::Retriable: 
                ythrow TRetriableException() 
                    << "Failed to start TvmClient. You can retry: " 
                    << err; 
        } 
    } 
 
    template <typename Func> 
    void TLastError::Update(TLastError::EScope scope, Func func) const { 
        Y_VERIFY(scope != EScope::COUNT); 
 
        TLastErrors errs = *LastErrors_.Get(); 
        TLastErr& lastError = errs.Errors[(size_t)scope]; 
 
        if (func(lastError)) { 
            LastErrors_.Set(MakeIntrusiveConst<TLastErrors>(std::move(errs))); 
        } 
    } 
} 
