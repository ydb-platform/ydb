#pragma once

namespace NActors {

    /**
     * Allows deterministic thread parking and unparking.
     *
     * Unlike TThreadParkPad spins a little during Park(), which allows the
     * call to Unpark() to be more efficient.
     */
    class TSpinParkPad {
    public:
        TSpinParkPad();
        ~TSpinParkPad();

        /**
         * Parks current thread until Unpark() or Interrupt() are called.
         *
         * Returns true when the unparked thread was interrupted. Only one
         * thread is allowed to call Park() at any given time.
         */
        bool Park() noexcept;

        /**
         * Unparks exactly one call to Park(), which will return false.
         */
        void Unpark() noexcept;

        /**
         * Interrupts the last unparked and all future calls to Park(). Note
         * that when Unpark() is called before Interrupt() the first call to
         * Park() will return false.
         */
        void Interrupt() noexcept;

    private:
        struct TImpl;

        TImpl& Impl();

    private:
        // Inline space used by the internal implementation
        alignas(8) char Data[8];
    };

} // namespace NActors
