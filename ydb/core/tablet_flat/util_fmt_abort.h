#pragma once

#include <util/generic/yexception.h>
#include <util/system/yassert.h>

namespace NKikimr::NUtil {

    class TTabletError : public yexception {
    public:
        using yexception::yexception;
    };

} // namespace NKikimr::NUtil

#define Y_TABLET_ERROR(stream) do { ythrow ::NKikimr::NUtil::TTabletError() << stream; } while(false)
