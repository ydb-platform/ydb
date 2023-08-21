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
private:
    NScheduler::TOperationId OperationId;
    std::optional<TString> OperationAlias;

protected:
    // Is calculated by two fields above.
    NScheduler::TOperationIdOrAlias OperationIdOrAlias;

public:
    TSimpleOperationCommandBase()
    {
        this->RegisterParameter("operation_id", OperationId)
            .Default();
        this->RegisterParameter("operation_alias", OperationAlias)
            .Default();

        this->RegisterPostprocessor([&] {
            if (!OperationId.IsEmpty() && OperationAlias.operator bool() ||
                OperationId.IsEmpty() && !OperationAlias.operator bool())
            {
                THROW_ERROR_EXCEPTION("Exactly one of \"operation_id\" and \"operation_alias\" should be set")
                    << TErrorAttribute("operation_id", OperationId)
                    << TErrorAttribute("operation_alias", OperationAlias);
            }

            if (OperationId) {
                OperationIdOrAlias = OperationId;
            } else {
                OperationIdOrAlias = *OperationAlias;
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDumpJobContextCommand
    : public TTypedCommand<NApi::TDumpJobContextOptions>
{
public:
    TDumpJobContextCommand();

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
    TGetJobInputCommand();

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetJobInputPathsCommand
    : public TTypedCommand<NApi::TGetJobInputPathsOptions>
{
public:
    TGetJobInputPathsCommand();

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetJobSpecCommand
    : public TTypedCommand<NApi::TGetJobSpecOptions>
{
public:
    TGetJobSpecCommand();

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetJobStderrCommand
    : public TSimpleOperationCommandBase<NApi::TGetJobStderrOptions>
{
public:
    TGetJobStderrCommand();

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetJobFailContextCommand
    : public TSimpleOperationCommandBase<NApi::TGetJobFailContextOptions>
{
public:
    TGetJobFailContextCommand();

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListOperationsCommand
    : public TTypedCommand<NApi::TListOperationsOptions>
{
public:
    TListOperationsCommand();

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
    TListJobsCommand();

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetJobCommand
    : public TSimpleOperationCommandBase<NApi::TGetJobOptions>
{
public:
    TGetJobCommand();

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAbandonJobCommand
    : public TTypedCommand<NApi::TAbandonJobOptions>
{
public:
    TAbandonJobCommand();

private:
    NJobTrackerClient::TJobId JobId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPollJobShellCommand
    : public TTypedCommand<NApi::TPollJobShellOptions>
{
public:
    TPollJobShellCommand();

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
private:
    NJobTrackerClient::TJobId JobId;

public:
    TAbortJobCommand();

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TStartOperationCommand
    : public TTypedCommand<NApi::TStartOperationOptions>
{
public:
    explicit TStartOperationCommand(
        std::optional<NScheduler::EOperationType> operationType = std::optional<NScheduler::EOperationType>());

private:
    NYTree::INodePtr Spec;
    NScheduler::EOperationType OperationType;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TMapCommand
    : public TStartOperationCommand
{
public:
    TMapCommand();
};

////////////////////////////////////////////////////////////////////////////////

class TMergeCommand
    : public TStartOperationCommand
{
public:
    TMergeCommand();
};

////////////////////////////////////////////////////////////////////////////////

class TSortCommand
    : public TStartOperationCommand
{
public:
    TSortCommand();
};

////////////////////////////////////////////////////////////////////////////////

class TEraseCommand
    : public TStartOperationCommand
{
public:
    TEraseCommand();
};

////////////////////////////////////////////////////////////////////////////////

class TReduceCommand
    : public TStartOperationCommand
{
public:
    TReduceCommand();
};

////////////////////////////////////////////////////////////////////////////////

class TJoinReduceCommand
    : public TStartOperationCommand
{
public:
    TJoinReduceCommand();
};

////////////////////////////////////////////////////////////////////////////////

class TMapReduceCommand
    : public TStartOperationCommand
{
public:
    TMapReduceCommand();
};

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyCommand
    : public TStartOperationCommand
{
public:
    TRemoteCopyCommand();
};

////////////////////////////////////////////////////////////////////////////////

class TAbortOperationCommand
    : public TSimpleOperationCommandBase<NApi::TAbortOperationOptions>
{
public:
    TAbortOperationCommand();

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSuspendOperationCommand
    : public TSimpleOperationCommandBase<NApi::TSuspendOperationOptions>
{
public:
    TSuspendOperationCommand();

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TResumeOperationCommand
    : public TSimpleOperationCommandBase<NApi::TResumeOperationOptions>
{
public:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCompleteOperationCommand
    : public TSimpleOperationCommandBase<NApi::TCompleteOperationOptions>
{
public:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUpdateOperationParametersCommand
    : public TSimpleOperationCommandBase<NApi::TUpdateOperationParametersOptions>
{
public:
    TUpdateOperationParametersCommand();

private:
    NYTree::INodePtr Parameters;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetOperationCommand
    : public TSimpleOperationCommandBase<NApi::TGetOperationOptions>
{
public:
    TGetOperationCommand();

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
