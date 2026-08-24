#pragma once

#include "public.h"

#include "storage.h"

#include <library/cpp/threading/hot_swap/hot_swap.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TStorageGate: public IStorage
{
public:
    explicit TStorageGate(IStoragePtr storage);
    ~TStorageGate() override;

    void Attach(IStoragePtr storage);
    void Detach();

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request) override;

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request) override;

    NThreading::TFuture<TZeroBlocksLocalResponse> ZeroBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TZeroBlocksLocalRequest> request) override;

    void ReportIOError() override;

private:
    struct THolder: public TAtomicRefCount<THolder>
    {
        IStoragePtr Storage;
    };

    THotSwap<THolder> Holder;
};

using TStorageGatePtr = std::shared_ptr<TStorageGate>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
