#pragma once
#include "dq_input.h"

namespace NYql::NDq {

struct TDqSourceStats : TDqInputStats {
    ui64 InputIndex = 0;

    explicit TDqSourceStats(ui64 inputIndex)
        : InputIndex(inputIndex) {}

    template<typename T>
    void FromProto(const T& f)
    {
        this->InputIndex = f.GetInputIndex();
        this->Chunks = f.GetChunks();
        this->Bytes = f.GetBytes();
        this->RowsIn = f.GetRowsIn();
        this->RowsOut = f.GetRowsOut();
        this->MaxMemoryUsage = f.GetMaxMemoryUsage();
        //s->StartTs = TInstant::MilliSeconds(f.GetStartTs());
        //s->FinishTs = TInstant::MilliSeconds(f.GetFinishTs());
    }
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
