#include "operations_archive_schema.h"

namespace NYT::NApi {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TOrderedByStartTimeTableDescriptor::TOrderedByStartTimeTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

const TOrderedByStartTimeTableDescriptor& TOrderedByStartTimeTableDescriptor::Get()
{
    static const TOrderedByStartTimeTableDescriptor descriptor;
    return descriptor;
}

TOrderedByStartTimeTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : StartTime(nameTable->RegisterName("start_time"))
    , IdHi(nameTable->RegisterName("id_hi"))
    , IdLo(nameTable->RegisterName("id_lo"))
    , OperationType(nameTable->RegisterName("operation_type"))
    , State(nameTable->RegisterName("state"))
    , AuthenticatedUser(nameTable->RegisterName("authenticated_user"))
    , FilterFactors(nameTable->RegisterName("filter_factors"))
    , Pool(nameTable->RegisterName("pool"))
    , Pools(nameTable->RegisterName("pools"))
    , HasFailedJobs(nameTable->RegisterName("has_failed_jobs"))
    , Acl(nameTable->RegisterName("acl"))
    , PoolTreeToPool(nameTable->RegisterName("pool_tree_to_pool"))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
