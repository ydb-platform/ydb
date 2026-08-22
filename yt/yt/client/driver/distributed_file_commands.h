#pragma once

#include "file_commands.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TStartDistributedWriteFileSessionCommand
    : public TTypedCommand<NApi::TDistributedWriteFileSessionStartOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TStartDistributedWriteFileSessionCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPingDistributedWriteFileSessionCommand
    : public TTypedCommand<NApi::TDistributedWriteFileSessionPingOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TPingDistributedWriteFileSessionCommand);

    static void Register(TRegistrar registrar);

private:
    NYTree::INodePtr Session;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TFinishDistributedWriteFileSessionCommand
    : public TTypedCommand<NApi::TDistributedWriteFileSessionFinishOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TFinishDistributedWriteFileSessionCommand);

    static void Register(TRegistrar registrar);

private:
    NYTree::INodePtr Session;
    std::vector<NYTree::INodePtr> Results;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteFileFragmentCommand
    : public TTypedCommand<NApi::TFileFragmentWriterOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TWriteFileFragmentCommand);

    static void Register(TRegistrar registrar);

private:
    NYTree::INodePtr Cookie;

    NApi::IFileFragmentWriterPtr CreateFileWriter(const ICommandContextPtr& context);

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
