#pragma once

#include <Parsers/IAST.h>

namespace DB_CHDB
{
class ASTJSONPathRoot : public IAST
{
public:
    String getID(char) const override { return "ASTJSONPathRoot"; }

    ASTPtr clone() const override { return std::make_shared<ASTJSONPathRoot>(*this); }
};

}
