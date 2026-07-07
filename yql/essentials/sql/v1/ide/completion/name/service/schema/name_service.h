#pragma once

#include <yql/essentials/sql/v1/ide/completion/name/object/schema.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/name_service.h>

namespace NSQLComplete {

INameService::TPtr MakeSchemaNameService(ISchema::TPtr schema);

} // namespace NSQLComplete
