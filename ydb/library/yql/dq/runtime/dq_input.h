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
    virtual void PauseByCheckpoint() = 0;
    virtual void ResumeByCheckpoint() = 0;
    virtual bool IsPausedByCheckpoint() const = 0;
    // Watermarks
    // Called after receiving watermark (watermark position remembered,
    // but does not pause channel); watermark may be pushed behind new data
    // by reading or checkpoint
    virtual void AddWatermark(TInstant watermark) = 0;
    // Called after watermark is ready for TaskRunner (got watermarks on all channels;
    // implies channel must already contain greater-or-equal watermark);
    // Same as with PauseByCheckpoint, any data added adter Pause is not received until Resume;
    // If called after PauseByCheckpoint(), checkpoint takes priority
    virtual void PauseByWatermark(TInstant watermark) = 0;
    // Called after watermark processed by TaskRunner;
    // If called before PauseByCheckpoint, all watermarks greater than this
    // moved after checkpoint (with no data between checkpoint and watermark)
    virtual void ResumeByWatermark(TInstant watermark) = 0;
    virtual bool IsPausedByWatermark() const = 0;
};

} // namespace NYql::NDq
