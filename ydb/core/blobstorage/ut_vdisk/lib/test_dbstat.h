#pragma once

#include "defs.h"
#include "helpers.h"
#include "prepare.h"

#include <util/generic/set.h>

///////////////////////////////////////////////////////////////////////////
struct TDbStatTest {
    const ui64 TabletId;

    TDbStatTest(ui64 tabletId)
        : TabletId(tabletId)
    {}

    void operator ()(TConfiguration *conf);
};
