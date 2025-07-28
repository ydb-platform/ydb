#pragma once

#include <yql/essentials/sql/v1/highlight/sql_highlight.h>

#include <util/stream/output.h>

namespace NSQLHighlight {

    void GenerateVim(IOutputStream& out, const THighlighting& highlighting);

} // namespace NSQLHighlight
