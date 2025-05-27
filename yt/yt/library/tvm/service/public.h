#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTvmServiceConfig)
DECLARE_REFCOUNTED_STRUCT(ITvmService)
DECLARE_REFCOUNTED_STRUCT(IDynamicTvmService)

////////////////////////////////////////////////////////////////////////////////

using TUid = ui64;
using TTvmId = ui64;

////////////////////////////////////////////////////////////////////////////////

struct TParsedTicket
{
    TUid DefaultUid;
    THashSet<std::string> Scopes;
};

struct TParsedServiceTicket
{
    TTvmId TvmId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
