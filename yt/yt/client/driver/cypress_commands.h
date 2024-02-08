#pragma once

#include "command.h"

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TGetCommand
    : public TTypedCommand<NApi::TGetNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    bool ShouldReturnOnlyValue;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSetCommand
    : public TTypedCommand<NApi::TSetNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TSetCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TMultisetAttributesCommand
    : public TTypedCommand<NApi::TMultisetAttributesNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TMultisetAttributesCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveCommand
    : public TTypedCommand<NApi::TRemoveNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TRemoveCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListCommand
    : public TTypedCommand<NApi::TListNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TListCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    bool ShouldReturnOnlyValue;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateCommand
    : public NYTree::TYsonStructLite
{
public:
    void Execute(ICommandContextPtr context);

    REGISTER_YSON_STRUCT_LITE(TCreateCommand);

    static void Register(TRegistrar);

private:
    NObjectClient::EObjectType Type;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateNodeCommand
    : public TTypedCommand<NApi::TCreateNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TCreateNodeCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    NObjectClient::EObjectType Type;
    NYTree::INodePtr Attributes;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateObjectCommand
    : public TTypedCommand<NApi::TCreateObjectOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TCreateObjectCommand);

    static void Register(TRegistrar registrar);

private:
    NObjectClient::EObjectType Type;
    NYTree::INodePtr Attributes;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TLockCommand
    : public TTypedCommand<NApi::TLockNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TLockCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;
    NCypressClient::ELockMode Mode;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUnlockCommand
    : public TTypedCommand<NApi::TUnlockNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TUnlockCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCopyCommand
    : public TTypedCommand<NApi::TCopyNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TCopyCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TMoveCommand
    : public TTypedCommand<NApi::TMoveNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TMoveCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TExistsCommand
    : public TTypedCommand<NApi::TNodeExistsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TExistsCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TLinkCommand
    : public TTypedCommand<NApi::TLinkNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TLinkCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath LinkPath;
    NYPath::TRichYPath TargetPath;
    NYTree::INodePtr Attributes;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateCommand
    : public TTypedCommand<NApi::TConcatenateNodesOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TConcatenateCommand);

    static void Register(TRegistrar registrar);

private:
    std::vector<NYPath::TRichYPath> SourcePaths;
    NYPath::TRichYPath DestinationPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TExternalizeCommand
    : public TTypedCommand<NApi::TExternalizeNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TExternalizeCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath Path;
    NObjectClient::TCellTag CellTag;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TInternalizeCommand
    : public TTypedCommand<NApi::TInternalizeNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TInternalizeCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
