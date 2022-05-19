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

    bool IsFull() const override {
        return false;
    }

    void Consume(NUdf::TUnboxedValue&& value) final {
        value.Apply(*ApplyCtx);
    }

    void Consume(NDqProto::TCheckpoint&&) final {
        Y_FAIL("Shouldn't be called");
    }

    void Finish() final {}

private:
    NUdf::IApplyContext* ApplyCtx;
};

} // namespace

IDqOutputConsumer::TPtr CreateKqpApplyEffectsConsumer(NUdf::IApplyContext* applyCtx) {
    return MakeIntrusive<TKqpApplyEffectsConsumer>(applyCtx);
}

} // namespace NKqp
} // namespace NKikimr
