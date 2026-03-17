#include "FunctionShowCertificate.h"
#include <Functions/FunctionFactory.h>

namespace DB_CHDB
{

REGISTER_FUNCTION(ShowCertificate)
{
    factory.registerFunction<FunctionShowCertificate>();
}

}
