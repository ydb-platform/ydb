#pragma once
#include <library/cpp/threading/future/core/future.h>
#include <yql/essentials/utils/chunked_buffer.h>

namespace NYql::NDq {

struct IDqSpiller {
    using TPtr = std::shared_ptr<IDqSpiller>;
    using TMemoryReportCallback = std::function<void(ui64)>;
    using TKey = ui64;

    virtual ~IDqSpiller(){}
    virtual NThreading::TFuture<TKey> Put(NYql::TChunkedBuffer&& blob) = 0;

    ///\return
    ///  nullopt for absent keys
    ///  TFuture
    virtual NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Get(TKey key) = 0;
    virtual NThreading::TFuture<void> Delete(TKey) = 0;
    ///Get + Delete
    ///Stored value may be moved to future
    virtual NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Extract(TKey key) = 0;
};

} // namespace NYql::NDq
