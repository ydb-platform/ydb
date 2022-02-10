#pragma once 
#include "dq_input.h" 
 
namespace NYql::NDq { 
 
struct TDqSourceStats : TDqInputStats { 
    ui64 InputIndex = 0; 
 
    explicit TDqSourceStats(ui64 inputIndex) 
        : InputIndex(inputIndex) {} 
}; 
 
class IDqSource : public IDqInput { 
public: 
    using TPtr = TIntrusivePtr<IDqSource>; 
 
    virtual ui64 GetInputIndex() const = 0; 
 
    virtual void Push(NKikimr::NMiniKQL::TUnboxedValueVector&& batch, i64 space) = 0; 
 
    virtual void Finish() = 0;

    virtual const TDqSourceStats* GetStats() const = 0; 
}; 
 
IDqSource::TPtr CreateDqSource(ui64 inputIndex, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes,
    bool collectProfileStats);
 
} // namespace NYql::NDq 
