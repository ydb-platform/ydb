#pragma once

#include "command.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TStartPipelineCommand
    : public TTypedCommand<NApi::TStartPipelineOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TStartPipelineCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath PipelinePath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TStopPipelineCommand
    : public TTypedCommand<NApi::TStopPipelineOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TStopPipelineCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath PipelinePath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPausePipelineCommand
    : public TTypedCommand<NApi::TStopPipelineOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TPausePipelineCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath PipelinePath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetPipelineStatusCommand
    : public TTypedCommand<NApi::TGetPipelineStatusOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetPipelineStatusCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TYPath PipelinePath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
