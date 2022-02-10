#pragma once

#include <util/generic/ptr.h>

namespace NActors {
    class TThreadParkPad {
    private:
        class TImpl;
        THolder<TImpl> Impl;

    public:
        TThreadParkPad();
        ~TThreadParkPad();

        bool Park() noexcept;
        void Unpark() noexcept;
        void Interrupt() noexcept;
        bool Interrupted() const noexcept;
    };

}
