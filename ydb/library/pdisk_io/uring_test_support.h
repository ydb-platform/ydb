#pragma once
#include "uring_router.h"
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/output.h>

namespace NKikimr::NPDisk {
inline bool RequireUring(TUringRouterConfig config = {}) {
#if defined(__linux__)
    if (TUringRouter::Probe(config)) {
        return true;
    }
#else
    Y_UNUSED(config);
#endif
    UNIT_ASSERT_C(GetTestParam("require_io_uring", "0") != "1",
        "require_io_uring=1: native io_uring is unavailable");
    Cerr << "SKIP: native io_uring is unavailable" << Endl;
    return false;
}
}
