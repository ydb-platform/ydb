#include "mon_action.h"

namespace NActors::NAudit {

TString ToString(EAuditableAction action) {
    switch (action) {
        case EAuditableAction::Unknown:
            return "HTTP REQUEST";

        // viewer
        case EAuditableAction::ExecuteQuery:
            return "EXECUTE SQL QUERY";
        case EAuditableAction::UpdateAcl:
            return "UPDATE ACCESS RIGHTS";
        case EAuditableAction::PlanToSvg:
            return "CONVERT PLAN TO SVG";
        case EAuditableAction::ChangePDiskStatus:
            return "CHANGE PDISK STATUS";
        case EAuditableAction::CancelOperation:
            return "CANCEL OPERATION";
        case EAuditableAction::ForgetOperation:
            return "FORGET OPERATION";
        case EAuditableAction::ExecuteScript:
            return "EXECUTE SCRIPT QUERY";
        case EAuditableAction::MakeDirectory:
            return "CREATE SCHEME DIRECTORY";

        // tablets
        case EAuditableAction::KillTablet:
            return "KILL TABLET";
        case EAuditableAction::RestartTablet:
            return "RESTART TABLET";

        // actors, blobstorageproxy
        case EAuditableAction::SetPutSamplingRate:
            return "SET PUT SAMPLING RATE";
        case EAuditableAction::SetGetSamplingRate:
            return "SET GET SAMPLING RATE";
        case EAuditableAction::SetDiscoverSamplingRate:
            return "SET DISCOVER SAMPLING RATE";
        case EAuditableAction::SubmitTimeStats:
            return "SUBMIT TIME STATISTICS";

        // actors, failure_injection
        case EAuditableAction::FailureInjectionModify:
            return "FAILURE INJECTION MODIFY";
        case EAuditableAction::FailureInjectionTerminate:
            return "FAILURE INJECTION TERMINATE";

        // actors, icb
        case EAuditableAction::UpdateControlBoard:
            return "UPDATE CONTROL BOARD";
        case EAuditableAction::RestoreControlDefaults:
            return "RESTORE CONTROL DEFAULTS";

        // actors, kqp_proxy
        case EAuditableAction::ForceShutdownKqp:
            return "FORCE SHUTDOWN KQP";

        // actors, load_test
        case EAuditableAction::StartLoadTest:
            return "START LOAD TEST";
        case EAuditableAction::StopLoadTest:
            return "STOP LOAD TEST";

        // actors, logger
        case EAuditableAction::ChangeLogPriority:
            return "CHANGE LOG PRIORITY";
        case EAuditableAction::ChangeLogSamplingPriority:
            return "CHANGE LOG SAMPLING PRIORITY";
        case EAuditableAction::ChangeLogSamplingRate:
            return "CHANGE LOG SAMPLING RATE";

        // actors, pdisks
        case EAuditableAction::LockChunksByCount:
            return "LOCK CHUNKS BY COUNT";
        case EAuditableAction::LockChunksByColor:
            return "LOCK CHUNKS BY COLOR";
        case EAuditableAction::UnlockChunks:
            return "UNLOCK CHUNKS";
        case EAuditableAction::RestartPDisk:
            return "RESTART PDISK";
        case EAuditableAction::StopPDisk:
            return "STOP PDISK";

        // actors, vdiks
        case EAuditableAction::EvictVdisk:
            return "EVICT VDISK";

        // actors, pql2
        case EAuditableAction::UpdatePql2Cache:
            return "UPDATE PQL2 CACHE";


        // actors, profiler
        case EAuditableAction::ProfilerStart:
            return "START PROFILING";
        case EAuditableAction::ProfilerStopDisplay:
            return "STOP PROFILING DISPLAY";
        case EAuditableAction::ProfilerStopSave:
            return "STOP PROFILING SAVE";
        case EAuditableAction::ProfilerStopLog:
            return "STOP PROFILING LOG";


        // actors, sqsgc
        case EAuditableAction::SqsgcRescan:
            return "SQS GC RESCAN";
        case EAuditableAction::SqsgcClean:
            return "SQS GC CLEAN";
        case EAuditableAction::SqsgcClearHistory:
            return "SQS GC CLEAR HISTORY";

        // cms
        case EAuditableAction::RemoveVolatileYamlConfig:
            return "REMOVE VOLATILE YAML CONFIG";
        case EAuditableAction::ConfigureYamlConfig:
            return "CONFIGURE YAML CONFIG";
        case EAuditableAction::ConfigureVolatileYamlConfig:
            return "CONFIGURE VOLATILE YAML CONFIG";
        case EAuditableAction::ConfigureConsole:
            return "CONFIGURE CONSOLE";
        case EAuditableAction::ToggleConfigValidator:
            return "TOGGLE CONFIG VALIDATOR";
        case EAuditableAction::CmsApiWalle:
            return "WALLE API ACCESS";

        // memory
        case EAuditableAction::LogHeapStatistics:
            return "LOG HEAP STATISTICS";

        // nodetabmon
        case EAuditableAction::NodetabmonKillTablet:
            return "KILL TABLET VIA NODETABMON";

        // trace
        case EAuditableAction::CreateTrace:
            return "CREATE TRACE";
        case EAuditableAction::DeleteTrace:
            return "DELETE TRACE";
        case EAuditableAction::MakeTraceSnapshot:
            return "MAKE TRACE SNAPSHOT";
        case EAuditableAction::SetTraceTimeout:
            return "SET TRACE TIMEOUT";

        // fq_diag
        case EAuditableAction::SubmitQuota:
            return "SUBMIT QUOTA";

        // root
        case EAuditableAction::Login:
            return "LOGIN";
        case EAuditableAction::Logout:
            return "LOGOUT";
    }
}

}
