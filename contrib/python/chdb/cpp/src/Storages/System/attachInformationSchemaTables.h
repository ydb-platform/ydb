#pragma once
#include <Interpreters/Context_fwd.h>

namespace DB_CHDB
{

class IDatabase;

void attachInformationSchema(ContextMutablePtr context, IDatabase & information_schema_database);

}
