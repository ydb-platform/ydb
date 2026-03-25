#pragma once

#include "client_common.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TGetUserBannedOptions
    : public TTimeoutOptions
{ };

struct TSetUserBannedOptions
    : public TTimeoutOptions
{ };

struct TListBannedUsersOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IBanClient
{
    virtual ~IBanClient() = default;

    virtual TFuture<void> SetUserBanned(
        const std::string& userName,
        bool isBanned,
        const TSetUserBannedOptions& options = {}) = 0;

    virtual TFuture<bool> GetUserBanned(
        const std::string& userName,
        const TGetUserBannedOptions& options = {}) = 0;

    virtual TFuture<std::vector<std::string>> ListBannedUsers(
        const TListBannedUsersOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
