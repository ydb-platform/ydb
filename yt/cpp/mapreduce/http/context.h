#pragma once

#include "fwd.h"

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/public.h>


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TClientContext
{
    TString ServerName;
    TString Token;
    TMaybe<TString> ImpersonationUser;
    NAuth::IServiceTicketAuthPtrWrapperPtr ServiceTicketAuth;
    NHttpClient::IHttpClientPtr HttpClient;
    bool TvmOnly = false;
    bool UseTLS = false;
    TConfigPtr Config = TConfig::Get();
    TMaybe<TString> ProxyAddress;
};

bool operator==(const TClientContext& lhs, const TClientContext& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
