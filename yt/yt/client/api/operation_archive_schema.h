#pragma once

#include <yt/yt/client/table_client/name_table.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TOrderedByIdTableDescriptor
{
    TOrderedByIdTableDescriptor();

    static const TOrderedByIdTableDescriptor& Get();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& nameTable);

        const int IdHash;
        const int IdHi;
        const int IdLo;
        const int State;
        const int AuthenticatedUser;
        const int OperationType;
        const int Progress;
        const int Spec;
        const int BriefProgress;
        const int BriefSpec;
        const int StartTime;
        const int FinishTime;
        const int FilterFactors;
        const int Result;
        const int Events;
        const int Alerts;
        const int SlotIndex; // TODO(renadeen): delete this column when version with this comment will be on every cluster
        const int UnrecognizedSpec;
        const int FullSpec;
        const int RuntimeParameters;
        const int SlotIndexPerPoolTree;
        const int TaskNames;
        const int ExperimentAssignments;
        const int ExperimentAssignmentNames;
        const int ControllerFeatures;
        const int AlertEvents;
        const int ProvidedSpec;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Index;
};

////////////////////////////////////////////////////////////////////////////////

struct TOrderedByStartTimeTableDescriptor
{
    TOrderedByStartTimeTableDescriptor();

    static const TOrderedByStartTimeTableDescriptor& Get();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& nameTable);

        const int StartTime;
        const int IdHi;
        const int IdLo;
        const int OperationType;
        const int State;
        const int AuthenticatedUser;
        const int FilterFactors;
        const int Pool;
        const int Pools;
        const int HasFailedJobs;
        const int Acl;
        const int PoolTreeToPool;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Index;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobTableDescriptor
{
    TJobTableDescriptor();

    static const TJobTableDescriptor& Get();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& nameTable);

        const int OperationIdHi;
        const int OperationIdLo;
        const int JobIdHi;
        const int JobIdLo;
        const int Type;
        const int State;
        const int TransientState;
        const int StartTime;
        const int FinishTime;
        const int UpdateTime;
        const int Address;
        const int Error;
        const int Statistics;
        const int BriefStatistics;
        const int StatisticsLz4;
        const int Events;
        const int StderrSize;
        const int HasSpec;
        const int HasFailContext;
        const int FailContextSize;
        const int CoreInfos;
        const int JobCompetitionId;
        const int ProbingJobCompetitionId;
        const int HasCompetitors;
        const int HasProbingCompetitors;
        const int ExecAttributes;
        const int TaskName;
        const int PoolTree;
        const int MonitoringDescriptor;
        const int JobCookie;
        const int ControllerState;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Index;
};

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NApi
