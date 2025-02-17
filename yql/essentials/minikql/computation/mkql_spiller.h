#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <yql/essentials/utils/chunked_buffer.h>

namespace NKikimr::NMiniKQL {

struct ISpiller
{
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
};

}//namespace NKikimr::NMiniKQL
