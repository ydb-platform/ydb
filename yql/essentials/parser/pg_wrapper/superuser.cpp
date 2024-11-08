#include "pg_compat.h"

extern "C" {
#include "miscadmin.h"
}

bool superuser_arg(Oid roleid)
{
    // TODO: make actual check
    return true;
}
