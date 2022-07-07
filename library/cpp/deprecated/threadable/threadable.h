#pragma once

#include <util/system/thread.h>
#include <util/system/defaults.h>
#include <util/generic/ptr.h>

//deprecated. use future.h instead
template <class T>
class TThreadable: public T {
public:
    using TThreadProc = TThread::TThreadProc;

    inline TThreadable()
        : Arg(nullptr)
        , ThreadInit(nullptr)
    {
    }

    inline int Run(void* arg = nullptr, size_t stackSize = 0, TThreadProc init = DefInit) {
        if (Thread_) {
            return 1;
        }

        Arg = arg;
        ThreadInit = init;

        try {
            THolder<TThread> thread(new TThread(TThread::TParams(Dispatch, this, stackSize)));

            thread->Start();
            Thread_.Swap(thread);
        } catch (...) {
            return 1;
        }

        return 0;
    }

    inline int Join(void** result = nullptr) {
        if (!Thread_) {
            return 1;
        }

        try {
            void* ret = Thread_->Join();

            if (result) {
                *result = ret;
            }

            Thread_.Destroy();

            return 0;
        } catch (...) {
        }

        return 1;
    }

protected:
    static TThreadable<T>* This(void* ptr) {
        return (TThreadable<T>*)ptr;
    }

    static void* Dispatch(void* ptr) {
        void* result = This(ptr)->ThreadInit(This(ptr)->Arg);

        return result ? result : (void*)(size_t)This(ptr)->T::Run(This(ptr)->Arg);
    }

    static void* DefInit(void*) {
        return nullptr;
    }

public:
    void* Arg;
    TThreadProc ThreadInit;

private:
    THolder<TThread> Thread_;
};
