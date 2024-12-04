#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node.h>

#include "dq_async_stats.h" 

namespace NYql::NDq {

struct TDqInputStats : public TDqAsyncStats {

};

class IDqInput : public TSimpleRefCount<IDqInput> {
public:
    using TPtr = TIntrusivePtr<IDqInput>;

    virtual ~IDqInput() = default;

    virtual const TDqInputStats& GetPopStats() const = 0;
    virtual i64 GetFreeSpace() const = 0;
    virtual ui64 GetStoredBytes() const = 0;

    [[nodiscard]]
    virtual bool Empty() const = 0;

    [[nodiscard]]
    virtual bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) = 0;

    virtual bool IsFinished() const = 0;

    virtual NKikimr::NMiniKQL::TType* GetInputType() const = 0;

    inline TMaybe<ui32> GetInputWidth() const {
        auto type = GetInputType();
        if (type->IsMulti()) {
            return static_cast<const NKikimr::NMiniKQL::TMultiType*>(type)->GetElementsCount();
        }
        return {};
    }

    // Checkpointing
    // After pause IDqInput::Pop() stops return batches that were pushed before pause
    // and returns Empty() after all the data before pausing was read.
    // Compute Actor can push data after pause, but program won't receive it until Resume() is called.
    virtual void Pause() = 0;
    virtual void Resume() = 0;
    virtual bool IsPaused() const = 0;
};

} // namespace NYql::NDq
