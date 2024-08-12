#pragma once

#include "command.h"

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
class TSimpleOperationCommandBase
    : public virtual TTypedCommandBase<TOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TSimpleOperationCommandBase);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("operation_id", &TThis::OperationId)
            .Default();
        registrar.Parameter("operation_alias", &TThis::OperationAlias)
            .Default();

        registrar.Postprocessor([] (TThis* command) {
            auto operationId = command->OperationId;
            if (operationId && command->OperationAlias.has_value() ||
                !operationId && !command->OperationAlias.has_value())
            {
                THROW_ERROR_EXCEPTION("Exactly one of \"operation_id\" and \"operation_alias\" should be set")
                    << TErrorAttribute("operation_id", command->OperationId)
                    << TErrorAttribute("operation_alias", command->OperationAlias);
            }

            if (command->OperationId) {
                command->OperationIdOrAlias = command->OperationId;
            } else {
                command->OperationIdOrAlias = *command->OperationAlias;
            }
        });
    }

protected:
    // Is calculated by OperationId and OperationAlias.
    NScheduler::TOperationIdOrAlias OperationIdOrAlias;

private:
    NScheduler::TOperationId OperationId;
    std::optional<TString> OperationAlias;
};

////////////////////////////////////////////////////////////////////////////////

class TDumpJobContextCommand
    : public TTypedCommand<NApi::TDumpJobContextOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TDumpJobContextCommand);

    static void Register(TRegistrar registrar);

private:
    NJobTrackerClient::TJobId JobId;
    NYPath::TYPath Path;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetJobInputCommand
    : public TTypedCommand<NApi::TGetJobInputOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetJobInputCommand);

    static void Register(TRegistrar registrar);

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetJobInputPathsCommand
    : public TTypedCommand<NApi::TGetJobInputPathsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetJobInputPathsCommand);

    static void Register(TRegistrar registrar);

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetJobSpecCommand
    : public TTypedCommand<NApi::TGetJobSpecOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetJobSpecCommand);

    static void Register(TRegistrar registrar);

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetJobStderrCommand
    : public TSimpleOperationCommandBase<NApi::TGetJobStderrOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetJobStderrCommand);

    static void Register(TRegistrar registrar);

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetJobFailContextCommand
    : public TSimpleOperationCommandBase<NApi::TGetJobFailContextOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetJobFailContextCommand);

    static void Register(TRegistrar registrar);

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListOperationsCommand
    : public TTypedCommand<NApi::TListOperationsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TListOperationsCommand);

    static void Register(TRegistrar registrar);

private:
    bool EnableUIMode = false;

    void BuildOperations(const NApi::TListOperationsResult& result, NYTree::TFluentMap fluent);

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListJobsCommand
    : public TSimpleOperationCommandBase<NApi::TListJobsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TListJobsCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetJobCommand
    : public TSimpleOperationCommandBase<NApi::TGetJobOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetJobCommand);

    static void Register(TRegistrar registrar);

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAbandonJobCommand
    : public TTypedCommand<NApi::TAbandonJobOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAbandonJobCommand);

    static void Register(TRegistrar registrar);

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPollJobShellCommand
    : public TTypedCommand<NApi::TPollJobShellOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TPollJobShellCommand);

    static void Register(TRegistrar registrar);

private:
    NJobTrackerClient::TJobId JobId;
    NYTree::INodePtr Parameters;
    std::optional<TString> ShellName;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAbortJobCommand
    : public TTypedCommand<NApi::TAbortJobOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAbortJobCommand);

    static void Register(TRegistrar registrar);

    void DoExecute(ICommandContextPtr context) override;

private:
    NJobTrackerClient::TJobId JobId;
};

////////////////////////////////////////////////////////////////////////////////

class TDumpJobProxyLogCommand
    : public TTypedCommand<NApi::TDumpJobProxyLogOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TDumpJobProxyLogCommand);

    static void Register(TRegistrar registrar);

    void DoExecute(ICommandContextPtr context) override;

private:
    NJobTrackerClient::TJobId JobId;
    NJobTrackerClient::TOperationId OperationId;
    NYPath::TYPath Path;
};

////////////////////////////////////////////////////////////////////////////////

class TStartOperationCommandBase
    : public TTypedCommand<NApi::TStartOperationOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TStartOperationCommandBase);

    static void Register(TRegistrar registrar);

protected:
    NScheduler::EOperationType OperationType;

private:
    NYTree::INodePtr Spec;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TStartOperationCommand
    : public TStartOperationCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TStartOperationCommand);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TMapCommand
    : public TStartOperationCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TMapCommand);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TMergeCommand
    : public TStartOperationCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TMergeCommand);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TSortCommand
    : public TStartOperationCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TSortCommand);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TEraseCommand
    : public TStartOperationCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TEraseCommand);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TReduceCommand
    : public TStartOperationCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TReduceCommand);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TJoinReduceCommand
    : public TStartOperationCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TJoinReduceCommand);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TMapReduceCommand
    : public TStartOperationCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TMapReduceCommand);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyCommand
    : public TStartOperationCommandBase
{
public:
    REGISTER_YSON_STRUCT_LITE(TRemoteCopyCommand);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TAbortOperationCommand
    : public TSimpleOperationCommandBase<NApi::TAbortOperationOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAbortOperationCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSuspendOperationCommand
    : public TSimpleOperationCommandBase<NApi::TSuspendOperationOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TSuspendOperationCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TResumeOperationCommand
    : public TSimpleOperationCommandBase<NApi::TResumeOperationOptions>
{
    REGISTER_YSON_STRUCT_LITE(TResumeOperationCommand);

    static void Register(TRegistrar)
    { }

public:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCompleteOperationCommand
    : public TSimpleOperationCommandBase<NApi::TCompleteOperationOptions>
{
    REGISTER_YSON_STRUCT_LITE(TCompleteOperationCommand);

    static void Register(TRegistrar)
    { }

public:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUpdateOperationParametersCommand
    : public TSimpleOperationCommandBase<NApi::TUpdateOperationParametersOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TUpdateOperationParametersCommand);

    static void Register(TRegistrar registrar);

private:
    NYTree::INodePtr Parameters;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetOperationCommand
    : public TSimpleOperationCommandBase<NApi::TGetOperationOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetOperationCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
