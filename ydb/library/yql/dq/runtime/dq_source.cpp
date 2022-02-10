#include "dq_source.h"
#include "dq_input_impl.h"

namespace NYql::NDq {

class TDqSource : public TDqInputImpl<TDqSource, IDqSource> {
    using TBaseImpl = TDqInputImpl<TDqSource, IDqSource>;
    friend TBaseImpl;
public:
    TDqSource(ui64 inputIndex, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes, bool collectProfileStats)
        : TBaseImpl(inputType, maxBufferBytes)
        , InputIndex(inputIndex)
        , BasicStats(InputIndex)
        , ProfileStats(collectProfileStats ? &BasicStats : nullptr) {}

    ui64 GetInputIndex() const override {
        return InputIndex;
    }

    void Push(NKikimr::NMiniKQL::TUnboxedValueVector&& batch, i64 space) override { 
        Y_VERIFY(!batch.empty() || !space);
        if (!batch.empty()) {
            AddBatch(std::move(batch), space);
        }
    }

    const TDqSourceStats* GetStats() const override {
        return &BasicStats;
    }

private:
    const ui64 InputIndex;
    TDqSourceStats BasicStats;
    TDqSourceStats* ProfileStats = nullptr;
};

IDqSource::TPtr CreateDqSource(
    ui64 inputIndex, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes, bool collectStats)
{
    return new TDqSource(inputIndex, inputType, maxBufferBytes, collectStats);
}

} // namespace NYql::NDq
