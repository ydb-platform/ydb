#pragma once

#include "command.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TPipelineCommandBase
    : public virtual NYTree::TYsonStructLite
{
public:
    REGISTER_YSON_STRUCT_LITE(TPipelineCommandBase);

    static void Register(TRegistrar registrar);

protected:
    NYPath::TYPath PipelinePath;
};

////////////////////////////////////////////////////////////////////////////////

class TGetPipelineSpecCommand
    : public TTypedCommand<NApi::TGetPipelineSpecOptions>
    , public TPipelineCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetPipelineSpecCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath SpecPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSetPipelineSpecCommand
    : public TTypedCommand<NApi::TSetPipelineSpecOptions>
    , public TPipelineCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TSetPipelineSpecCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath SpecPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemovePipelineSpecCommand
    : public TTypedCommand<NApi::TSetPipelineSpecOptions>
    , public TPipelineCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TRemovePipelineSpecCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath SpecPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetPipelineDynamicSpecCommand
    : public TTypedCommand<NApi::TGetPipelineDynamicSpecOptions>
    , public TPipelineCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetPipelineDynamicSpecCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath SpecPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSetPipelineDynamicSpecCommand
    : public TTypedCommand<NApi::TSetPipelineDynamicSpecOptions>
    , public TPipelineCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TSetPipelineDynamicSpecCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath SpecPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemovePipelineDynamicSpecCommand
    : public TTypedCommand<NApi::TSetPipelineDynamicSpecOptions>
    , public TPipelineCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TRemovePipelineDynamicSpecCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath SpecPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TStartPipelineCommand
    : public TTypedCommand<NApi::TStartPipelineOptions>
    , public TPipelineCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TStartPipelineCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TStopPipelineCommand
    : public TTypedCommand<NApi::TStopPipelineOptions>
    , public TPipelineCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TStopPipelineCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPausePipelineCommand
    : public TTypedCommand<NApi::TPausePipelineOptions>
    , public TPipelineCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TPausePipelineCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetPipelineStateCommand
    : public TTypedCommand<NApi::TGetPipelineStateOptions>
    , public TPipelineCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetPipelineStateCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetFlowViewCommand
    : public TTypedCommand<NApi::TGetFlowViewOptions>
    , public TPipelineCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetFlowViewCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath ViewPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
