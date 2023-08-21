#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TSetUserPasswordCommand
    : public TTypedCommand<NApi::TSetUserPasswordOptions>
{
public:
    TSetUserPasswordCommand();

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
    TIssueTokenCommand();

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
    TRevokeTokenCommand();

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
    TListUserTokensCommand();

private:
    TString User_;

    TString PasswordSha256_;

    void DoExecute(ICommandContextPtr context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
