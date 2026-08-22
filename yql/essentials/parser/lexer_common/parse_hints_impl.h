#pragma once

#include "hints.h"

namespace NSQLTranslation::NDetail {

TVector<TSQLHint> ParseSqlHints(NYql::TPosition commentPos, const TStringBuf& comment, bool utf8Aware);

} // namespace NSQLTranslation::NDetail
