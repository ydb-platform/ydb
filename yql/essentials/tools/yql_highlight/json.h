#pragma once

#include <library/cpp/json/json_value.h>

namespace NSQLHighlight {

    void Print(IOutputStream& out, const NJson::TJsonValue& json);

} // namespace NSQLHighlight
