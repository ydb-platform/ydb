#pragma once

#include "documentation.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    INameService::TPtr MakeDocumentingNameService(IDocumentation::TPtr docs, INameService::TPtr origin);

} // namespace NSQLComplete
