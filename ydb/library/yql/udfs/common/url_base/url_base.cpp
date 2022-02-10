#include <ydb/library/yql/public/udf/udf_helpers.h>

#include "lib/url_base_udf.h"

SIMPLE_MODULE(TUrlModule, EXPORTED_URL_BASE_UDF)
REGISTER_MODULES(TUrlModule)

