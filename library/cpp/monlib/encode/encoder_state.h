#pragma once

#include "encoder_state_enum.h"

#include <util/generic/serialized_enum.h>
#include <util/generic/yexception.h>


namespace NMonitoring {

    template <typename EEncoderState>
    class TEncoderStateImpl {
    public:
        using EState = EEncoderState;

        explicit TEncoderStateImpl(EEncoderState state = EEncoderState::ROOT)
            : State_(state)
        {
        }

        TEncoderStateImpl& operator=(EEncoderState rhs) noexcept {
            State_ = rhs;
            return *this;
        }

        inline bool operator==(EEncoderState rhs) const noexcept {
            return State_ == rhs;
        }

        inline bool operator!=(EEncoderState rhs) const noexcept {
            return !operator==(rhs);
        }

        [[noreturn]] inline void ThrowInvalid(TStringBuf message) const {
            ythrow yexception() << "invalid encoder state: "
                                << ToStr() << ", " << message;
        }

        inline void Expect(EEncoderState expected) const {
            if (Y_UNLIKELY(State_ != expected)) {
                ythrow yexception()
                    << "invalid encoder state: " << ToStr()
                    << ", expected: " << TEncoderStateImpl(expected).ToStr();
            }
        }

        inline void Switch(EEncoderState from, EEncoderState to) {
            Expect(from);
            State_ = to;
        }

        TStringBuf ToStr() const noexcept {
            return NEnumSerializationRuntime::GetEnumNamesImpl<EEncoderState>().at(State_);
        }

    private:
        EEncoderState State_;
    };

    using TEncoderState = TEncoderStateImpl<EEncoderState>;

} // namespace NMonitoring
