#pragma once

#include <library/cpp/colorizer/colors.h>

namespace NYdbWorkload {

using TColorsProvider = NColorizer::TColors& (*)(IOutputStream&);

void SetColorsProvider(TColorsProvider provider);
NColorizer::TColors& GetColors(IOutputStream& out);

} // namespace NYdbWorkload


