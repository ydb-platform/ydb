#include "operation_archive_schema.h"

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

TJobTableDescriptor::TJobTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

const TJobTableDescriptor& TJobTableDescriptor::Get()
{
    static const TJobTableDescriptor descriptor;
    return descriptor;
}

TJobTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : OperationIdHi(nameTable->RegisterName("operation_id_hi"))
    , OperationIdLo(nameTable->RegisterName("operation_id_lo"))
    , JobIdHi(nameTable->RegisterName("job_id_hi"))
    , JobIdLo(nameTable->RegisterName("job_id_lo"))
    , Type(nameTable->RegisterName("type"))
    , State(nameTable->RegisterName("state"))
    , TransientState(nameTable->RegisterName("transient_state"))
    , StartTime(nameTable->RegisterName("start_time"))
    , FinishTime(nameTable->RegisterName("finish_time"))
    , UpdateTime(nameTable->RegisterName("update_time"))
    , Address(nameTable->RegisterName("address"))
    , Error(nameTable->RegisterName("error"))
    , Statistics(nameTable->RegisterName("statistics"))
    , BriefStatistics(nameTable->RegisterName("brief_statistics"))
    , StatisticsLz4(nameTable->RegisterName("statistics_lz4"))
    , Events(nameTable->RegisterName("events"))
    , StderrSize(nameTable->RegisterName("stderr_size"))
    , HasSpec(nameTable->RegisterName("has_spec"))
    , HasFailContext(nameTable->RegisterName("has_fail_context"))
    , FailContextSize(nameTable->RegisterName("fail_context_size"))
    , CoreInfos(nameTable->RegisterName("core_infos"))
    , JobCompetitionId(nameTable->RegisterName("job_competition_id"))
    , ProbingJobCompetitionId(nameTable->RegisterName("probing_job_competition_id"))
    , HasCompetitors(nameTable->RegisterName("has_competitors"))
    , HasProbingCompetitors(nameTable->RegisterName("has_probing_competitors"))
    , ExecAttributes(nameTable->RegisterName("exec_attributes"))
    , TaskName(nameTable->RegisterName("task_name"))
    , PoolTree(nameTable->RegisterName("pool_tree"))
    , MonitoringDescriptor(nameTable->RegisterName("monitoring_descriptor"))
    , JobCookie(nameTable->RegisterName("job_cookie"))
    , ControllerState(nameTable->RegisterName("controller_state"))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
