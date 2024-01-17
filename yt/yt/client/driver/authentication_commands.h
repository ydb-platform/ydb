#pragma once

#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TSetUserPasswordCommand
    : public TTypedCommand<NApi::TSetUserPasswordOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TSetUserPasswordCommand);

    static void Register(TRegistrar registrar);

private:
    TString User_;

    TString CurrentPasswordSha256_;
    TString NewPasswordSha256_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TIssueTokenCommand
    : public TTypedCommand<NApi::TIssueTokenOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TIssueTokenCommand);

    static void Register(TRegistrar registrar);

private:
    TString User_;

    TString PasswordSha256_;

    void DoExecute(ICommandContextPtr context);
};

////////////////////////////////////////////////////////////////////////////////

class TRevokeTokenCommand
    : public TTypedCommand<NApi::TRevokeTokenOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TRevokeTokenCommand);

    static void Register(TRegistrar registrar);

private:
    TString User_;

    TString PasswordSha256_;
    TString TokenSha256_;

    void DoExecute(ICommandContextPtr context);
};

////////////////////////////////////////////////////////////////////////////////

class TListUserTokensCommand
    : public TTypedCommand<NApi::TListUserTokensOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TListUserTokensCommand);

    static void Register(TRegistrar registrar);

private:
    TString User_;

    TString PasswordSha256_;

    void DoExecute(ICommandContextPtr context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
