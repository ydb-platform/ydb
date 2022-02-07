#pragma once

#include "defs.h"

////////////////////////////////////////////
namespace NKikimr {

////////////////////////////////////////////
/// The TTabletException class
////////////////////////////////////////////
#define TABLET_EXCEPTION(TExcName) \
class TExcName : public yexception {};

TABLET_EXCEPTION(TNotReadyTabletException)
TABLET_EXCEPTION(TNotExistTabletException)
TABLET_EXCEPTION(TSchemeErrorTabletException)
TABLET_EXCEPTION(TTooLongTxException)

#undef TABLET_EXCEPTION

} // end of the NKikimr namespace

