#pragma once 
 
#include "api/settings.h" 
#include "tool/settings.h" 
 
#include <util/string/cast.h> 
#include <util/system/spinlock.h> 
 
#include <optional> 
 
namespace NTvmAuth { 
    class TTvmClient; 
} 
 
namespace NTvmAuth::NInternal { 
    class TClientCaningKnife { 
    public: 
        static void StartTvmClientStopping(TTvmClient* c); 
        static bool IsTvmClientStopped(TTvmClient* c); 
    }; 
} 
 
namespace NTvmAuth::NUtils { 
    TString ToHex(const TStringBuf s); 
 
    inline NTvmAuth::NTvmApi::TClientSettings::TDstMap ParseDstMap(TStringBuf dsts) { 
        NTvmAuth::NTvmApi::TClientSettings::TDstMap res; 
 
        while (dsts) { 
            TStringBuf pair = dsts.NextTok(';'); 
            TStringBuf alias = pair.NextTok(':'); 
            res.insert(decltype(res)::value_type( 
                alias, 
                IntFromString<TTvmId, 10>(pair))); 
        } 
 
        return res; 
    } 
 
    inline NTvmAuth::NTvmApi::TClientSettings::TDstVector ParseDstVector(TStringBuf dsts) { 
        NTvmAuth::NTvmApi::TClientSettings::TDstVector res; 
 
        while (dsts) { 
            res.push_back(IntFromString<TTvmId, 10>(dsts.NextTok(';'))); 
        } 
 
        return res; 
    } 
 
    bool CheckBbEnvOverriding(EBlackboxEnv original, EBlackboxEnv override) noexcept; 
 
    template <class T> 
    class TProtectedValue { 
        class TAssignOp { 
        public: 
            static void Assign(T& l, const T& r) { 
                l = r; 
            } 
 
            template <typename U> 
            static void Assign(std::shared_ptr<U>& l, std::shared_ptr<U>& r) { 
                l.swap(r); 
            } 
 
            template <typename U> 
            static void Assign(TIntrusiveConstPtr<U>& l, TIntrusiveConstPtr<U>& r) { 
                l.Swap(r); 
            } 
        }; 
 
    public: 
        TProtectedValue() = default; 
 
        TProtectedValue(T value) 
            : Value_(value) 
        { 
        } 
 
        T Get() const { 
            with_lock (Lock_) { 
                return Value_; 
            } 
        } 
 
        void Set(T o) { 
            with_lock (Lock_) { 
                TAssignOp::Assign(Value_, o); 
            } 
        } 
 
    private: 
        T Value_; 
        mutable TAdaptiveLock Lock_; 
    }; 
} 
