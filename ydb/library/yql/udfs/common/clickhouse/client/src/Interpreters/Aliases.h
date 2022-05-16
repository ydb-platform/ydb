#pragma once

#include <common/types.h>
#include <Parsers/IAST_fwd.h>

#include <unordered_map>

namespace NDB
{

using Aliases = std::unordered_map<String, ASTPtr>;

}
