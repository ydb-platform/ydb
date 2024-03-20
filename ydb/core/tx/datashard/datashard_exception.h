#pragma once

#include <util/generic/yexception.h>

////////////////////////////////////////////
namespace NKikimr::NDataShard {

#define DATASHARD_EXCEPTION(TExcName) \
class TExcName : public yexception {};

DATASHARD_EXCEPTION(TPageFaultException)
DATASHARD_EXCEPTION(TUniqueConstrainException)

#undef DATASHARD_EXCEPTION
}
