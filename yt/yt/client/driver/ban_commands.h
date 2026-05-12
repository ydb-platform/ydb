#pragma once

#include "command.h"

namespace NYT::NDriver {

class TGetUserBannedCommand
    : public TTypedCommand<NApi::TGetUserBannedOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetUserBannedCommand);

    static void Register(TRegistrar registrar);

private:
    std::string UserName_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////


class TSetUserBannedCommand
    : public TTypedCommand<NApi::TSetUserBannedOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TSetUserBannedCommand);

    static void Register(TRegistrar registrar);

private:
    std::string UserName_;
    bool IsBanned_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////


class TListBannedUsersCommand
    : public TTypedCommand<NApi::TListBannedUsersOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TListBannedUsersCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
