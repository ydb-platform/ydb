#include "kqp_runtime_impl.h"

namespace NKikimr {
namespace NKqp {

using namespace NMiniKQL;

namespace {

using namespace NYql;
using namespace NDq;

class TKqpApplyEffectsConsumer : public IDqOutputConsumer {
public:
    TKqpApplyEffectsConsumer(NUdf::IApplyContext* applyCtx)
        : ApplyCtx(applyCtx) {}

    EDqFillLevel GetFillLevel() const override {
        return NoLimit;
    }

    size_t GetTotalSize() const override {
        return 0;
    }

    size_t GetOverLimitSize() const override {
        return 0;
    }

    void Consume(NUdf::TUnboxedValue&& value) final {
        value.Apply(*ApplyCtx);
    }

    void WideConsume(NUdf::TUnboxedValue* values, ui32 count) final {
        Y_UNUSED(values);
        Y_UNUSED(count);
        Y_ABORT("WideConsume not supported yet");
    }

    void Consume(NDqProto::TCheckpoint&&) final {
        Y_ABORT("Shouldn't be called");
    }

    void Consume(NDqProto::TWatermark&&) final {
        Y_ABORT("Shouldn't be called");
    }

    void Finish() final {}

    void Flush() final {}

    bool IsFinished() const final {
        return false;
    }

    bool IsEarlyFinished() const final {
        return false;
    }

private:
    NUdf::IApplyContext* ApplyCtx;
};

} // namespace

IDqOutputConsumer::TPtr CreateKqpApplyEffectsConsumer(NUdf::IApplyContext* applyCtx) {
    return MakeIntrusive<TKqpApplyEffectsConsumer>(applyCtx);
}

} // namespace NKqp
} // namespace NKikimr
