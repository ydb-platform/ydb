#pragma once

#include "command.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
class TUpdateMembershipCommand
    : public TTypedCommand<TOptions>
{
protected:
    TString Group;
    TString Member;

    TUpdateMembershipCommand()
    {
        this->RegisterParameter("group", Group);
        this->RegisterParameter("member", Member);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAddMemberCommand
    : public TUpdateMembershipCommand<NApi::TAddMemberOptions>
{
private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveMemberCommand
    : public TUpdateMembershipCommand<NApi::TRemoveMemberOptions>
{
private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TParseYPathCommand
    : public TCommandBase
{
public:
    TParseYPathCommand();

private:
    TString Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetVersionCommand
    : public TCommandBase
{
private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetSupportedFeaturesCommand
    : public TCommandBase
{
private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCheckPermissionCommand
    : public TTypedCommand<NApi::TCheckPermissionOptions>
{
public:
    TCheckPermissionCommand();

private:
    TString User;
    NYPath::TRichYPath Path;
    NYTree::EPermission Permission;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCheckPermissionByAclCommand
    : public TTypedCommand<NApi::TCheckPermissionByAclOptions>
{
public:
    TCheckPermissionByAclCommand();

private:
    std::optional<TString> User;
    NYTree::EPermission Permission;
    NYTree::INodePtr Acl;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TTransferAccountResourcesCommand
    : public TTypedCommand<NApi::TTransferAccountResourcesOptions>
{
public:
    TTransferAccountResourcesCommand();

private:
    TString SourceAccount;
    TString DestinationAccount;
    NYTree::INodePtr ResourceDelta;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TTransferPoolResourcesCommand
    : public TTypedCommand<NApi::TTransferPoolResourcesOptions>
{
public:
    TTransferPoolResourcesCommand();

private:
    TString SourcePool;
    TString DestinationPool;
    TString PoolTree;
    NYTree::INodePtr ResourceDelta;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TExecuteBatchOptions
    : public NApi::TMutatingOptions
{
    int Concurrency;
};

class TExecuteBatchCommandRequest
    : public NYTree::TYsonStruct
{
public:
    TString Command;
    NYTree::IMapNodePtr Parameters;
    NYTree::INodePtr Input;

    REGISTER_YSON_STRUCT(TExecuteBatchCommandRequest);

    static void Register(TRegistrar registrar);
};

class TExecuteBatchCommand
    : public TTypedCommand<TExecuteBatchOptions>
{
public:
    TExecuteBatchCommand();

private:
    using TRequestPtr = TIntrusivePtr<TExecuteBatchCommandRequest>;

    std::vector<TRequestPtr> Requests;

    class TRequestExecutor;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TDiscoverProxiesOptions
    : public NApi::TTimeoutOptions
{ };

class TDiscoverProxiesCommand
    : public TTypedCommand<TDiscoverProxiesOptions>
{
public:
    TDiscoverProxiesCommand();

private:
    NApi::EProxyType Type;
    TString Role;
    NApi::NRpcProxy::EAddressType AddressType;
    TString NetworkName;
    bool IgnoreBalancers;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TBalanceTabletCellsCommand
    : public TTypedCommand<NApi::TBalanceTabletCellsOptions>
{
public:
    TBalanceTabletCellsCommand();

private:
    TString TabletCellBundle;
    std::vector<NYPath::TYPath> MovableTables;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
