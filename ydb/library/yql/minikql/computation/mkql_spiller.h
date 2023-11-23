#pragma once
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/actors/util/rope.h>


namespace NKikimr::NMiniKQL {

struct ISpiller {
    using TPtr = std::shared_ptr<ISpiller>;
    virtual ~ISpiller(){}
    using TKey = ui64;
    virtual NThreading::TFuture<TKey> Put(TRope&& blob) = 0;

    ///\return
    ///  nullopt for absent keys
    ///  TFuture
    virtual std::optional<NThreading::TFuture<TRope>> Get(TKey key) = 0;
    virtual NThreading::TFuture<void> Delete(TKey) = 0;
    ///Get + Delete
    ///Stored value may be moved to feature
    virtual std::optional<NThreading::TFuture<TRope>> Extract(TKey key) = 0;

};

ISpiller::TPtr MakeSpiller();

}//namespace NKikimr::NMiniKQL
