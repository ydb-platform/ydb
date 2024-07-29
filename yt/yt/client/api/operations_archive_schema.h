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
        const int SchedulingAttributesPerPoolTree;
        // COMPAT(omgronny)
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

} //namespace NYT::NApi
