#pragma once

#include <memory>
#include <vector>

namespace NDB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;

}
