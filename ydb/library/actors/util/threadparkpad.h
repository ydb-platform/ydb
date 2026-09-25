#pragma once

#include <util/generic/ptr.h>

namespace NActors {
    class TThreadParkPad {
    private:
        class TImpl;
        std::unique_ptr<TImpl> Impl;

    public:
        TThreadParkPad();
        ~TThreadParkPad();

        bool Park() noexcept;
        void Unpark() noexcept;
        void Interrupt() noexcept;
        bool Interrupted() const noexcept;
    };

}
