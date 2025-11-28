#pragma once

#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/yassert.h>
#include <util/stream/output.h>
#include <source_location>

namespace NKikimr {
namespace NTable {
namespace NTest {

    template<typename TImpl>
    class TSteps {
    public:
        TImpl& To(size_t seq)
        {
            if (Seq.index() == 0) {
                Y_ENSURE(seq >= std::get<size_t>(Seq), "Invalid sequence flow");
            }

            Seq = seq;

            return static_cast<TImpl&>(*this);
        }

        TImpl& To(std::variant<size_t, std::source_location> seq)
        {
            if (Seq.index() == 0 && seq.index() == 0) {
                Y_ENSURE(std::get<size_t>(seq) >= std::get<size_t>(Seq), "Invalid sequence flow");
            }

            Seq = seq;
            return static_cast<TImpl&>(*this);
        }

        TImpl& ToLine(std::source_location location = std::source_location::current())
        {
            Seq = location;

            return static_cast<TImpl&>(*this);
        }

        IOutputStream& Log() const
        {
            Cerr << CurrentStepStr() << ": ";

            return Cerr;
        }

        std::variant<size_t, std::source_location> CurrentStep() const noexcept { return Seq; }

        TString CurrentStepStr() const {
            TStringBuilder sb;
            switch (Seq.index()) {
                case 0: {
                    size_t seq = std::get<size_t>(Seq);
                    sb << "On " << seq;
                    break;
                }
                case 1: {
                    const auto& location = std::get<std::source_location>(Seq);
                    sb << "At " << location.file_name() << ":" << location.line();
                    break;
                }
            }
            return sb;
        }

    private:
        std::variant<size_t, std::source_location> Seq = size_t(0);
    };

}
}
}
