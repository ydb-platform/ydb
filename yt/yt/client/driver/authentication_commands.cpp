#include "authentication_commands.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TSetUserPasswordCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("user", &TThis::User_);
    registrar.Parameter("current_password_sha256", &TThis::CurrentPasswordSha256_)
        .Default();
    registrar.Parameter("new_password_sha256", &TThis::NewPasswordSha256_);
    registrar.ParameterWithUniversalAccessor<bool>(
        "password_is_temporary",
        [] (TThis* command) -> bool& {
            return command->Options.PasswordIsTemporary;
        })
        .Default(false);
}

void TSetUserPasswordCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SetUserPassword(
        User_,
        CurrentPasswordSha256_,
        NewPasswordSha256_,
        Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TIssueTokenCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("user", &TThis::User_);
    registrar.Parameter("password_sha256", &TThis::PasswordSha256_)
        .Default();
    registrar.ParameterWithUniversalAccessor<TString>(
        "description",
        [] (TThis* command) -> TString& {
            return command->Options.Description;
        })
        .Optional();
}

void TIssueTokenCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->IssueToken(
        User_,
        PasswordSha256_,
        Options))
        .ValueOrThrow();

    context->ProduceOutputValue(ConvertToYsonString(result.Token));
}

////////////////////////////////////////////////////////////////////////////////

void TRevokeTokenCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("user", &TThis::User_);
    registrar.Parameter("password_sha256", &TThis::PasswordSha256_)
        .Default();
    registrar.Parameter("token_sha256", &TThis::TokenSha256_);
}

void TRevokeTokenCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->RevokeToken(
        User_,
        PasswordSha256_,
        TokenSha256_,
        Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TListUserTokensCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("user", &TThis::User_);
    registrar.Parameter("password_sha256", &TThis::PasswordSha256_)
        .Default();
    registrar.ParameterWithUniversalAccessor<bool>(
        "with_metadata",
        [] (TThis* command) -> bool& {
            return command->Options.WithMetadata;
        })
        .Default(false);
}

void TListUserTokensCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->ListUserTokens(
        User_,
        PasswordSha256_,
        Options))
        .ValueOrThrow();

    if (Options.WithMetadata) {
        context->ProduceOutputValue(ConvertToYsonString(result.Metadata));
    } else {
        context->ProduceOutputValue(ConvertToYsonString(result.Tokens));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
