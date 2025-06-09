#pragma once

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NYdb::NConsoleClient {

    NSQLComplete::INameService::TPtr MakeDummyNameService();

} // namespace NYdb::NConsoleClient
