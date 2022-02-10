#include "utils.h" 
 
#include <library/cpp/tvmauth/client/facade.h> 
 
#include <util/stream/format.h> 
 
namespace NTvmAuth::NInternal { 
    void TClientCaningKnife::StartTvmClientStopping(TTvmClient* c) { 
        if (c && c->Updater_) { 
            c->Updater_->StartTvmClientStopping(); 
        } 
    } 
 
    bool TClientCaningKnife::IsTvmClientStopped(TTvmClient* c) { 
        return c && c->Updater_ ? c->Updater_->IsTvmClientStopped() : true; 
    } 
} 
 
namespace NTvmAuth::NUtils { 
    TString ToHex(const TStringBuf s) { 
        TStringStream res; 
        res.Reserve(2 * s.size()); 
 
        for (char c : s) { 
            res << Hex(c, HF_FULL); 
        } 
 
        return std::move(res.Str()); 
    } 
 
    bool CheckBbEnvOverriding(EBlackboxEnv original, EBlackboxEnv override) noexcept { 
        switch (original) { 
            case EBlackboxEnv::Prod: 
            case EBlackboxEnv::ProdYateam: 
                return override == EBlackboxEnv::Prod || override == EBlackboxEnv::ProdYateam; 
            case EBlackboxEnv::Test: 
                return true; 
            case EBlackboxEnv::TestYateam: 
                return override == EBlackboxEnv::Test || override == EBlackboxEnv::TestYateam; 
            case EBlackboxEnv::Stress: 
                return override == EBlackboxEnv::Stress; 
        } 
 
        return false; 
    } 
} 
