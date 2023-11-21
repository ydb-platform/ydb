#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTvmServiceConfig)
DECLARE_REFCOUNTED_STRUCT(ITvmService)
DECLARE_REFCOUNTED_STRUCT(IDynamicTvmService)

////////////////////////////////////////////////////////////////////////////////

struct TParsedTicket
{
    ui64 DefaultUid;
    THashSet<TString> Scopes;
};

using TTvmId = ui64;

struct TParsedServiceTicket
{
    TTvmId TvmId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
