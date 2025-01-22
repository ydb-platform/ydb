#include "etcd_base_init.h"

#include <library/cpp/resource/resource.h>

namespace NEtcd {

TString GetCreateTablesSQL() {
    return NResource::Find("create.sql"sv);
}

TString GetLastRevisionSQL() {
    return "select coalesce(max(`modified`), 0L) + 1L from `verhaal`;";
}

}
