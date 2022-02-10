#pragma once 
 
namespace NKikimr { 
 
    template<typename T, typename TDerived> 
    class TWrappedValue { 
        T Value; 
 
    public: 
        friend bool operator ==(const TDerived& x, const TDerived& y) { return x.Value == y.Value; } 
        friend bool operator !=(const TDerived& x, const TDerived& y) { return x.Value != y.Value; } 
        friend bool operator < (const TDerived& x, const TDerived& y) { return x.Value <  y.Value; } 
        friend bool operator <=(const TDerived& x, const TDerived& y) { return x.Value <= y.Value; } 
        friend bool operator > (const TDerived& x, const TDerived& y) { return x.Value >  y.Value; } 
        friend bool operator >=(const TDerived& x, const TDerived& y) { return x.Value >= y.Value; } 
 
    protected: 
        TWrappedValue(const T& value) 
            : Value(value) 
        {} 
 
        const T& GetValue() const { 
            return Value; 
        } 
    }; 
 
} // NKikimr 
