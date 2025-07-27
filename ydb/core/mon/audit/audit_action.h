#pragma once

#include <util/generic/string.h>

namespace NActors::NAudit {

//
// Identifiers of modifying actions in the HTTP API.
//
// These values may appear in audit logs,
// but this enum does *not* determine whether an action should be audited.
//
// Use `EAuditableAction::Unknown` if the action name is unknown or irrelevant.
//
enum class EAuditableAction {
    Unknown,

    // viewer
    ExecuteQuery,
    UpdateAcl,
    PlanToSvg, // 25.1
    ChangePDiskStatus,
    CancelOperation,
    ForgetOperation,
    ExecuteScript,
    MakeDirectory,

    // tablets
    KillTablet,
    RestartTablet,

    // actors, blobstorageproxy
    SetPutSamplingRate, // 25.1
    SetGetSamplingRate, // 25.1
    SetDiscoverSamplingRate, // 25.1
    SubmitTimeStats, // 25.1

    // actors, failure_injection
    FailureInjectionTerminate,
    FailureInjectionModify,

    // actors, icb
    UpdateImmediateControlBoard,

    // actors, kqp_proxy
    ForceShutdownKqp,

    // actors, load_test
    StartLoadTest,
    StopLoadTest,

    // actors, logger
    UpdateLoggerSettings,

    // actors, pdisks
    PDiskChunkLock,
    PDiskChunkUnlock,
    PDiskRestart,
    PDiskStop,

    // actors, vdiks
    EvictVdisk,

    // actors, pql2
    UpdatePqNodeCacheLimit,

    // actors, profiler
    ProfilerStart,
    ProfilerStop,

    // actors, sqsgc
    SqsgcRescan,
    SqsgcClean,
    SqsgcClearHistory,

    // cms
    RemoveVolatileYamlConfig,
    ConfigureYamlConfig,
    ConfigureVolatileYamlConfig,
    ConfigureConsole,
    ToggleConfigValidator,
    CmsApiWalle,

    // memory
    LogHeapStatistics,

    // nodetabmon
    NodetabmonKillTablet,

    // trace
    CreateTrace,
    DeleteTrace,
    MakeTraceSnapshot,
    SetTraceTimeout,

    // fq_diag
    SubmitQuota,

    // root
    Login,
    Logout,
};

TString ToString(EAuditableAction action);

}
