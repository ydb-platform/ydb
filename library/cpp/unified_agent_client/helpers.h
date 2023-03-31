#pragma once

#include "client.h"

#include <util/charset/utf8.h>

namespace NUnifiedAgent::NPrivate {
    bool IsUtf8(const THashMap<TString, TString>& meta);
}
