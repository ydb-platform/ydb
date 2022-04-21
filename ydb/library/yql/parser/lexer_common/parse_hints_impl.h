#pragma once

#include "hints.h"

namespace NSQLTranslation {

namespace NDetail {

TVector<TSQLHint> ParseSqlHints(NYql::TPosition commentPos, const TStringBuf& comment);

}

}
