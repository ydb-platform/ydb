#include "ban_commands.h"

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TGetUserBannedCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("user_name", &TThis::UserName_);
}

void TGetUserBannedCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->GetUserBanned(UserName_, Options))
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "value", result);
}

////////////////////////////////////////////////////////////////////////////////

void TSetUserBannedCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("user_name", &TThis::UserName_);
    registrar.Parameter("is_banned", &TThis::IsBanned_);
}

void TSetUserBannedCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SetUserBanned(UserName_, IsBanned_, Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TListBannedUsersCommand::Register(TRegistrar /*registrar*/)
{ }

void TListBannedUsersCommand::DoExecute(ICommandContextPtr context)
{
    auto bannedUsers = WaitFor(context->GetClient()->ListBannedUsers(Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .DoListFor(bannedUsers, [] (TFluentList fluent, const auto& userName) {
            fluent.Item().Value(userName);
        }));
}

} // namespace NYT::NDriver

