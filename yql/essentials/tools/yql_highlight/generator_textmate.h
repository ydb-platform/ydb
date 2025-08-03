#pragma once

#include "generator.h"

namespace NSQLHighlight {

    IGenerator::TPtr MakeTextMateJsonGenerator();

    IGenerator::TPtr MakeTextMateBundleGenerator();

} // namespace NSQLHighlight
