#pragma once

#include "hints.h"

namespace NSQLTranslation {

namespace NDetail {

TVector<TSQLHint> ParseSqlHints(const TStringBuf& comment);

}

}
