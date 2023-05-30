#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>
#include "llvm/Object/ObjectFile.h"

namespace NYql {
namespace NBacktrace {
TString SymbolizeAndDumpToString(const std::string &moduleName, llvm::object::SectionedAddress moduleOffset, ui64 offset);
}
}
