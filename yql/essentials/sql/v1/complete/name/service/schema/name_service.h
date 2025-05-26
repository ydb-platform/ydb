#pragma once

#include <yql/essentials/sql/v1/complete/name/object/schema.h>
#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    INameService::TPtr MakeSchemaNameService(ISchema::TPtr schema);

} // namespace NSQLComplete
