#pragma once
#include <util/system/rwlock.h>
#include <memory>
namespace NKikimr::NColumnShard {


// Warning: doing complex logic under WriteGuard may result in blocking many threads in actor system. 
// WriteLock should be used for lightweight computations, 
// for example inserting 1 element in a hashmap (this is useful for e.g. metrics collection)
template<typename TValue>
class TThreadSafeValue {
    struct State{
        TRWMutex ValueMutex_;
        TValue Value_;
    };
    std::shared_ptr<State> State_ = std::make_shared<TThreadSafeValue::State>();
public:
    struct TValueReadGuard {
        ::TReadGuard _;
        const TValue& Value;
    };
    auto ReadGuard() const {
        return TValueReadGuard{::TReadGuard{&State_->ValueMutex_}, State_->Value_};
    }
    struct TValueWriteGuard {
        ::TWriteGuard _;
        TValue& Value;
    };
    auto WriteGuard() const {
        return TValueWriteGuard{::TWriteGuard{&State_->ValueMutex_}, State_->Value_};
    }
};

}