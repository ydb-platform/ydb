#pragma once

#include <yql/essentials/minikql/computation/mkql_spiller.h>
#include <yql/essentials/minikql/mkql_alloc.h>

namespace NYql::NDq {

// Accounts the memory that the spiller adapters allocate outside of the MKQL allocator (the packer
// buffers reported through ISpiller::ReportAlloc/ReportFree) in the MKQL allocator of the task with
// OffloadAlloc/OffloadFree (RFC dq_memory_quota_20, 3.1). Scoped to the operator that wraps its spiller,
// so the spilling operators of yql/essentials keep their accounting unchanged.
// Must be used and destroyed on the thread that runs the graph under the allocator it was created with.
class TAccountingSpiller final : public NKikimr::NMiniKQL::ISpiller {
public:
    explicit TAccountingSpiller(TPtr inner)
        : Inner_(std::move(inner))
        , Owner_(NKikimr::NMiniKQL::TlsAllocState)
    {
    }

    ~TAccountingSpiller() override {
        // release whatever was not reported as freed (e.g. a teardown in the middle of a spill)
        if (Active_ && NKikimr::NMiniKQL::TlsAllocState == Owner_) {
            Owner_->OffloadFree(Active_);
        }
    }

    NThreading::TFuture<TKey> Put(NYql::TChunkedBuffer&& blob) override {
        return Inner_->Put(std::move(blob));
    }

    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Get(TKey key) override {
        return Inner_->Get(key);
    }

    NThreading::TFuture<void> Delete(TKey key) override {
        return Inner_->Delete(key);
    }

    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Extract(TKey key) override {
        return Inner_->Extract(key);
    }

    void ReportAlloc(ui64 bytes) override {
        // OffloadAlloc may throw TMemoryLimitExceededException: account only what was actually charged
        NKikimr::NMiniKQL::TlsAllocState->OffloadAlloc(bytes);
        Active_ += bytes;
    }

    void ReportFree(ui64 bytes) override {
        bytes = std::min(bytes, Active_);
        Active_ -= bytes;
        if (bytes) {
            NKikimr::NMiniKQL::TlsAllocState->OffloadFree(bytes);
        }
    }

    ui64 GetActiveBytes() const {
        return Active_;
    }

private:
    const TPtr Inner_;
    NKikimr::NMiniKQL::TAllocState* const Owner_;
    ui64 Active_ = 0;
};

} // namespace NYql::NDq
