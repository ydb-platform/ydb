#include "threadparkpad.h"
#include <util/system/winint.h>

#ifdef _linux_

#include "futex.h"

namespace NActors {
    class TThreadParkPad::TImpl {
        volatile bool Interrupted;
        int Futex;

    public:
        TImpl()
            : Interrupted(false)
            , Futex(0)
        {
        }
        ~TImpl() {
        }

        bool Park() noexcept {
            __atomic_fetch_sub(&Futex, 1, __ATOMIC_SEQ_CST);
            while (__atomic_load_n(&Futex, __ATOMIC_ACQUIRE) == -1)
                SysFutex(&Futex, FUTEX_WAIT_PRIVATE, -1, nullptr, nullptr, 0);
            return IsInterrupted();
        }

        void Unpark() noexcept {
            const int old = __atomic_fetch_add(&Futex, 1, __ATOMIC_SEQ_CST);
            if (old == -1)
                SysFutex(&Futex, FUTEX_WAKE_PRIVATE, -1, nullptr, nullptr, 0);
        }

        void Interrupt() noexcept {
            __atomic_store_n(&Interrupted, true, __ATOMIC_SEQ_CST);
            Unpark();
        }

        bool IsInterrupted() const noexcept {
            return __atomic_load_n(&Interrupted, __ATOMIC_ACQUIRE);
        }
    };

#elif defined _win32_
#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/bt_exception.h>
#include <util/generic/yexception.h>

namespace NActors {
    class TThreadParkPad::TImpl {
        TAtomic Interrupted;
        HANDLE EvHandle;

    public:
        TImpl()
            : Interrupted(false)
        {
            EvHandle = ::CreateEvent(0, false, false, 0);
            if (!EvHandle)
                ythrow TWithBackTrace<yexception>() << "::CreateEvent failed";
        }
        ~TImpl() {
            if (EvHandle)
                ::CloseHandle(EvHandle);
        }

        bool Park() noexcept {
            ::WaitForSingleObject(EvHandle, INFINITE);
            return AtomicGet(Interrupted);
        }

        void Unpark() noexcept {
            ::SetEvent(EvHandle);
        }

        void Interrupt() noexcept {
            AtomicSet(Interrupted, true);
            Unpark();
        }

        bool IsInterrupted() const noexcept {
            return AtomicGet(Interrupted);
        }
    };

#else

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/system/event.h>

namespace NActors {
    class TThreadParkPad::TImpl {
        TAtomic Interrupted;
        TSystemEvent Ev;

    public:
        TImpl()
            : Interrupted(false)
            , Ev(TSystemEvent::rAuto)
        {
        }
        ~TImpl() {
        }

        bool Park() noexcept {
            Ev.Wait();
            return AtomicGet(Interrupted);
        }

        void Unpark() noexcept {
            Ev.Signal();
        }

        void Interrupt() noexcept {
            AtomicSet(Interrupted, true);
            Unpark();
        }

        bool IsInterrupted() const noexcept {
            return AtomicGet(Interrupted);
        }
    };
#endif

    TThreadParkPad::TThreadParkPad()
        : Impl(new TThreadParkPad::TImpl())
    {
    }

    TThreadParkPad::~TThreadParkPad() {
    }

    bool TThreadParkPad::Park() noexcept {
        return Impl->Park();
    }

    void TThreadParkPad::Unpark() noexcept {
        Impl->Unpark();
    }

    void TThreadParkPad::Interrupt() noexcept {
        Impl->Interrupt();
    }

    bool TThreadParkPad::Interrupted() const noexcept {
        return Impl->IsInterrupted();
    }

}
