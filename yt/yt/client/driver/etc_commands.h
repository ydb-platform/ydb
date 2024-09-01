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

    REGISTER_YSON_STRUCT_LITE(TUpdateMembershipCommand);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("group", &TUpdateMembershipCommand::Group);
        registrar.Parameter("member", &TUpdateMembershipCommand::Member);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAddMemberCommand
    : public TUpdateMembershipCommand<NApi::TAddMemberOptions>
{
    REGISTER_YSON_STRUCT_LITE(TAddMemberCommand);

    static void Register(TRegistrar)
    { }

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveMemberCommand
    : public TUpdateMembershipCommand<NApi::TRemoveMemberOptions>
{
    REGISTER_YSON_STRUCT_LITE(TRemoveMemberCommand);

    static void Register(TRegistrar)
    { }

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TParseYPathCommand
    : public TCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TParseYPathCommand);

    static void Register(TRegistrar registrar);

private:
    TString Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetVersionCommand
    : public TCommandBase
{
    REGISTER_YSON_STRUCT_LITE(TGetVersionCommand);

    static void Register(TRegistrar)
    { }

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetSupportedFeaturesCommand
    : public TCommandBase
{
    REGISTER_YSON_STRUCT_LITE(TGetSupportedFeaturesCommand);

    static void Register(TRegistrar)
    { }

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCheckPermissionCommand
    : public TTypedCommand<NApi::TCheckPermissionOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TCheckPermissionCommand);

    static void Register(TRegistrar registrar);

private:
    std::string User;
    NYPath::TRichYPath Path;
    NYTree::EPermission Permission;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCheckPermissionByAclCommand
    : public TTypedCommand<NApi::TCheckPermissionByAclOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TCheckPermissionByAclCommand);

    static void Register(TRegistrar registrar);

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
    REGISTER_YSON_STRUCT_LITE(TTransferAccountResourcesCommand);

    static void Register(TRegistrar registrar);

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
    REGISTER_YSON_STRUCT_LITE(TTransferPoolResourcesCommand);

    static void Register(TRegistrar registrar);

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
    REGISTER_YSON_STRUCT_LITE(TExecuteBatchCommand);

    static void Register(TRegistrar registrar);

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
    REGISTER_YSON_STRUCT_LITE(TDiscoverProxiesCommand);

    static void Register(TRegistrar registrar);

private:
    NApi::EProxyType Type;
    std::string Role;
    NApi::NRpcProxy::EAddressType AddressType;
    std::string NetworkName;
    bool IgnoreBalancers;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TBalanceTabletCellsCommand
    : public TTypedCommand<NApi::TBalanceTabletCellsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TBalanceTabletCellsCommand);

    static void Register(TRegistrar registrar);

private:
    TString TabletCellBundle;
    std::vector<NYPath::TYPath> MovableTables;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
