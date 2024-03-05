#include "operations_archive_schema.h"

namespace NYT::NApi {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TOrderedByIdTableDescriptor::TOrderedByIdTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

const TOrderedByIdTableDescriptor& TOrderedByIdTableDescriptor::Get()
{
    static const TOrderedByIdTableDescriptor descriptor;
    return descriptor;
}

TOrderedByIdTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : IdHash(nameTable->RegisterName("id_hash"))
    , IdHi(nameTable->RegisterName("id_hi"))
    , IdLo(nameTable->RegisterName("id_lo"))
    , State(nameTable->RegisterName("state"))
    , AuthenticatedUser(nameTable->RegisterName("authenticated_user"))
    , OperationType(nameTable->RegisterName("operation_type"))
    , Progress(nameTable->RegisterName("progress"))
    , Spec(nameTable->RegisterName("spec"))
    , BriefProgress(nameTable->RegisterName("brief_progress"))
    , BriefSpec(nameTable->RegisterName("brief_spec"))
    , StartTime(nameTable->RegisterName("start_time"))
    , FinishTime(nameTable->RegisterName("finish_time"))
    , FilterFactors(nameTable->RegisterName("filter_factors"))
    , Result(nameTable->RegisterName("result"))
    , Events(nameTable->RegisterName("events"))
    , Alerts(nameTable->RegisterName("alerts"))
    , SlotIndex(nameTable->RegisterName("slot_index"))
    , UnrecognizedSpec(nameTable->RegisterName("unrecognized_spec"))
    , FullSpec(nameTable->RegisterName("full_spec"))
    , RuntimeParameters(nameTable->RegisterName("runtime_parameters"))
    , SlotIndexPerPoolTree(nameTable->RegisterName("slot_index_per_pool_tree"))
    , TaskNames(nameTable->RegisterName("task_names"))
    , ExperimentAssignments(nameTable->RegisterName("experiment_assignments"))
    , ExperimentAssignmentNames(nameTable->RegisterName("experiment_assignment_names"))
    , ControllerFeatures(nameTable->RegisterName("controller_features"))
    , AlertEvents(nameTable->RegisterName("alert_events"))
    , ProvidedSpec(nameTable->RegisterName("provided_spec"))
{ }

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
