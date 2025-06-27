#include "etcd_base_init.h"

#include <library/cpp/resource/resource.h>

namespace NEtcd {

std::string GetCreateTablesSQL(const std::string& prefix) {
    return prefix + NResource::Find("create.sql"sv);
}

std::string GetInitializeTablesSQL(const std::string& prefix) {
    return prefix + "insert into `revision` (`stub`,`revision`,`timestamp`) values (false,0L,CurrentUtcTimestamp()),(true,0L,CurrentUtcTimestamp());";
}

}
