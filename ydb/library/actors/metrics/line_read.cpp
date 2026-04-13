#include "line_read.h"

#include "lines/raw_line_frontend.h"

namespace NActors {

    TLineMeta::TLineMeta() noexcept
        : Frontend(&TRawLineFrontend<>::Descriptor())
    {
    }

    TLineMeta::TLineMeta(const TLineFrontendOps* frontend) noexcept
        : Frontend(frontend ? frontend : &TRawLineFrontend<>::Descriptor())
    {
    }

    TStringBuf TLineMeta::FrontendName() const noexcept {
        return Frontend ? Frontend->Name : TStringBuf();
    }

} // namespace NActors
