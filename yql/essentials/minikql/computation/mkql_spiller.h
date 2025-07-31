#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <yql/essentials/utils/chunked_buffer.h>

namespace NKikimr::NMiniKQL {

struct ISpiller {
    using TPtr = std::shared_ptr<ISpiller>;
    virtual ~ISpiller(){}
    using TKey = ui64;
    virtual NThreading::TFuture<TKey> Put(NYql::TChunkedBuffer&& blob) = 0;

    ///\return
    ///  nullopt for absent keys
    ///  TFuture
    virtual NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Get(TKey key) = 0;
    virtual NThreading::TFuture<void> Delete(TKey) = 0;
    ///Get + Delete
    ///Stored value may be moved to future
    virtual NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Extract(TKey key) = 0;

    /// This set of functions is used to report memory allocated for spilling.
    /// Typically data is packed into some buffer before spilling. This buffer may not be
    /// allocated with MKQL_alloc -> this memory won't be tracked. That's why the spiller provides an
    /// interface to report allocated memory to the Resource Manager.
    /// Expected usage is:
    /// ReportAlloc(buffer_size) when the buffer is created
    /// ReportFree(buffer_size) when the buffer is written to disk and freed
    virtual void ReportAlloc(ui64 bytes) = 0;
    virtual void ReportFree(ui64 bytes) = 0;
};

}//namespace NKikimr::NMiniKQL
