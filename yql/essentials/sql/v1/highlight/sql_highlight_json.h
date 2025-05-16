#pragma once

#include "sql_highlight.h"

#include <library/cpp/json/json_value.h>

namespace NSQLHighlight {

    NJson::TJsonValue ToJson(const THighlighting& highlighting);

} // namespace NSQLHighlight
