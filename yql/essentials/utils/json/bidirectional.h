#pragma once

#include "from.h"
#include "to.h"

#define JSON_DECLARE_BIDIRECTIONAL(t) \
    JSON_DECLARE_FROM(t, json);       \
    JSON_DECLARE_TO(t, value)
