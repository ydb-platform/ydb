#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <ydb/library/actors/util/rope.h>

namespace NKikimr::NMiniKQL {

struct ISpiller
{
    using TPtr = std::shared_ptr<ISpiller>;
    virtual ~ISpiller(){}
    using TKey = ui64;
    virtual NThreading::TFuture<TKey> Put(TRope&& blob) = 0;

    ///\return
    ///  nullopt for absent keys
    ///  TFuture
    virtual NThreading::TFuture<std::optional<TRope>> Get(TKey key) = 0;
    virtual NThreading::TFuture<void> Delete(TKey) = 0;
    ///Get + Delete
    ///Stored value may be moved to future
    virtual NThreading::TFuture<std::optional<TRope>> Extract(TKey key) = 0;
};

}//namespace NKikimr::NMiniKQL
