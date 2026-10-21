#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((InvalidReadRange)    (2700))
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELayerAccessMethod,
    ((Unknown)  (1)     ("unknown"))
    ((Local)    (2)     ("local"))
    ((Nbd)      (3)     ("nbd"))
);

DEFINE_ENUM(ELayerFilesystem,
    ((Unknown)      (1)     ("unknown"))
    ((Archive)      (2)     ("archive"))
    ((Ext3)         (3)     ("ext3"))
    ((Ext4)         (4)     ("ext4"))
    ((SquashFS)     (5)     ("squashfs"))
);

////////////////////////////////////////////////////////////////////////////////

template <class... TValidator>
class TConstrainedRichYPath;
using TRichYPath = TConstrainedRichYPath<>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
