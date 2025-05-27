#pragma once

#include <yql/essentials/sql/v1/complete/name/object/schema.h>

namespace NSQLComplete {

    ISchema::TPtr MakeDispatchSchema(THashMap<TString, ISchema::TPtr> mapping);

} // namespace NSQLComplete
