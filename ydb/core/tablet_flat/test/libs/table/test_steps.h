#pragma once

#include <util/system/yassert.h>
#include <util/stream/output.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    template<typename TImpl>
    class TSteps {
    public:
        TImpl& To(size_t seq) noexcept
        {
            Y_VERIFY(seq >= Seq, "Invalid sequence flow");

            Seq = seq;

            return static_cast<TImpl&>(*this);
        }

        IOutputStream& Log() const noexcept
        {
            Cerr << "On " << Seq << ": ";

            return Cerr;
        }

        size_t CurrentStep() const noexcept { return Seq; }

    private:
        size_t Seq     = 0;
    };
}
}
}
