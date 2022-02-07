#pragma once

#include <util/generic/string.h>

namespace NYql {
namespace NCommonAttrs {
    extern TString ACTOR_NODEID_ATTR;
    extern TString ROLE_ATTR;
    extern TString HOSTNAME_ATTR;
    extern TString GRPCPORT_ATTR;
    extern TString REVISION_ATTR;
    extern TString INTERCONNECTPORT_ATTR;
    extern TString EPOCH_ATTR;
    extern TString PINGTIME_ATTR;

    extern TString OPERATIONID_ATTR;
    extern TString OPERATIONSIZE_ATTR;
    extern TString JOBID_ATTR;

    extern TString CLUSTERNAME_ATTR;

    extern TString UPLOAD_EXECUTABLE_ATTR;
}
}
