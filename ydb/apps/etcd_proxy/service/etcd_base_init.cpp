#include "etcd_base_init.h"

#include <library/cpp/resource/resource.h>

namespace NEtcd {

std::string GetCreateTablesSQL() {
    return NResource::Find("create.sql"sv);
}

std::string GetLastRevisionSQL() {
    return "select nvl(max(`modified`), 1L), nvl(max(`id`), 1L) from `verhaal` cross join `leases`;";
}

}
