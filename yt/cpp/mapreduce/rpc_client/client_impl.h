#pragma once

#include <yt/cpp/mapreduce/http/context.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TApiClients
{
    NApi::IClientPtr Light;
    NApi::IClientPtr Heavy;
};

TApiClients CreateApiClients(const TClientContext& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
