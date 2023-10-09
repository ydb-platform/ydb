#pragma once

#include <util/generic/maybe.h>
#include <util/generic/noncopyable.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/system/yassert.h>

#include <functional>

// probably this thing should have been called TFuture
template <typename T>
class TAsyncResult : TNonCopyable {
private:
    TMutex Mutex;
    TCondVar CondVar;

    TMaybe<T> Result;

    typedef void TOnResult(const T&);

    std::function<TOnResult> OnResult;

public:
    void SetResult(const T& result) {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(!Result, "cannot set result twice");
        Result = result;
        CondVar.BroadCast();

        if (!!OnResult) {
            OnResult(result);
        }
    }

    const T& GetResult() {
        TGuard<TMutex> guard(Mutex);
        while (!Result) {
            CondVar.Wait(Mutex);
        }
        return *Result;
    }

    template <typename TFunc>
    void AndThen(const TFunc& onResult) {
        TGuard<TMutex> guard(Mutex);
        if (!!Result) {
            onResult(*Result);
        } else {
            Y_ASSERT(!OnResult);
            OnResult = std::function<TOnResult>(onResult);
        }
    }
};
