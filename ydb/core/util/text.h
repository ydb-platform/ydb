#pragma once
#include "defs.h"

#include <util/stream/str.h>

namespace NKikimr {
namespace NText {

bool OutFlag(bool isFirst, bool isPresent, const char* name, TStringStream &stream);

}  // namespace NText
}  // namespace NKikimr

