#pragma once

#include <yt/yt/client/table_client/name_table.h>

namespace NYT::NApi {

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
