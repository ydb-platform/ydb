#include "etcd_base_init.h"

#include <library/cpp/resource/resource.h>

namespace NEtcd {

std::string GetCreateTablesSQL(const std::string& prefix) {
    return prefix + NResource::Find("create.sql"sv);
}

std::string GetLastRevisionSQL(const std::string& prefix) {
    return prefix + "select nvl(max(`modified`), 1L) from `history`; select nvl(max(`id`), 1L) from `leases`;";
}

}
