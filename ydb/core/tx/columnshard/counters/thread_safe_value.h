#pragma once
#include <absl/synchronization/mutex.h>
namespace NKikimr::NColumnShard {


// Warning: doing complex logic under WriteGuard may result in blocking many threads in actor system. 
// WriteLock should be used for lightweight computations, for example inserting 1 element in hashmap(this is useful for e.g. metrics collection)
template<typename TValue>
class TThreadSafeValue {
    struct State{
        mutable absl::Mutex ValueMutex_;
        mutable TValue Value_;
    };
    std::shared_ptr<State> State_;
public:
    struct TReadGuard {
        absl::ReaderMutexLock _;
        const TValue& Value;
    };
    auto ReadGuard() const {
        return TReadGuard{absl::ReaderMutexLock{&State_->ValueMutex_}, State_->Value_};
    }
    struct TWriteGuard {
        absl::WriterMutexLock _;
        TValue& Value;
    };
    auto WriteGuard() const {
        return TWriteGuard{absl::WriterMutexLock{&State_->ValueMutex_}, State_->Value_};
    }
};

}