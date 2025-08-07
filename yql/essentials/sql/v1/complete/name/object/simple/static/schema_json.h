#pragma once

#include <library/cpp/json/json_value.h>

#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>

namespace NSQLComplete {

    ISimpleSchema::TPtr MakeStaticSimpleSchema(const NJson::TJsonMap& json);

} // namespace NSQLComplete
