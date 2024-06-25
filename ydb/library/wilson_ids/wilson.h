#pragma once

#include <util/system/types.h>

namespace NKikimr {
    struct TComponentTracingLevels {
#ifdef DEFINE_TRACING_LEVELS
#error "Macro collision: DEFINE_TRACING_LEVELS"
#endif

#define DEFINE_TRACING_LEVELS(COMPONENT, MINIMAL, BASIC, DETAILED, DIAGNOSTIC, TRACE) \
        struct COMPONENT { \
            enum : ui8 { \
                TopLevel = MINIMAL, \
                Basic = BASIC, \
                Detailed = DETAILED, \
                Diagnostic = DIAGNOSTIC, \
                Trace = TRACE, \
            }; \
        };


        DEFINE_TRACING_LEVELS(THttp, 0, 5, 10, 14, 15)
        DEFINE_TRACING_LEVELS(TGrpcProxy, 0, 5, 10, 14, 15)
        DEFINE_TRACING_LEVELS(TQueryProcessor, 1, 6, 10, 14, 15)
        DEFINE_TRACING_LEVELS(TDistributedTransactions, 2, 7, 11, 14, 15)
        DEFINE_TRACING_LEVELS(TTablet, 3, 8, 12, 14, 15)
        DEFINE_TRACING_LEVELS(TDistributedStorage, 4, 9, 13, 14, 15)

#undef DEFINE_TRACING_LEVELS

        enum : ui8 {
            // The most verbose detalisation level used in production
            ProductionVerbose = 13,
        };
    };


    struct TWilson {
        enum {
            BlobStorage = TComponentTracingLevels::TDistributedStorage::TopLevel,
            DsProxyInternals = TComponentTracingLevels::TDistributedStorage::Detailed,
            VDiskTopLevel = TComponentTracingLevels::TDistributedStorage::Basic,
            VDiskInternals = TComponentTracingLevels::TDistributedStorage::Detailed,
            PDiskTopLevel = TComponentTracingLevels::TDistributedStorage::Basic,
            PDiskBasic = TComponentTracingLevels::TDistributedStorage::Detailed,
            PDiskDetailed = TComponentTracingLevels::TDistributedStorage::Detailed,
        };
    };

    struct TWilsonKqp {
        enum {
            KqpSession = TComponentTracingLevels::TQueryProcessor::TopLevel,
                CompileService = TComponentTracingLevels::TQueryProcessor::Basic,
                CompileActor = TComponentTracingLevels::TQueryProcessor::Basic,
                SessionAcquireSnapshot = TComponentTracingLevels::TQueryProcessor::Basic,

                    ExecuterTableResolve = TComponentTracingLevels::TQueryProcessor::Detailed,
                    ExecuterShardsResolve = TComponentTracingLevels::TQueryProcessor::Detailed,

                LiteralExecuter = TComponentTracingLevels::TQueryProcessor::Basic,

                DataExecuter = TComponentTracingLevels::TQueryProcessor::Basic,
                    DataExecuterAcquireSnapshot = TComponentTracingLevels::TQueryProcessor::Detailed,
                    DataExecuterRunTasks = TComponentTracingLevels::TQueryProcessor::Detailed,

                ScanExecuter = TComponentTracingLevels::TQueryProcessor::Basic,
                    ScanExecuterRunTasks = TComponentTracingLevels::TQueryProcessor::Detailed,

                KqpNodeSendTasks = TComponentTracingLevels::TQueryProcessor::Basic,

                ProposeTransaction = TComponentTracingLevels::TQueryProcessor::Basic,

                ComputeActor = TComponentTracingLevels::TQueryProcessor::Basic,

                ReadActor = TComponentTracingLevels::TQueryProcessor::Basic,
                    ReadActorShardsResolve = TComponentTracingLevels::TQueryProcessor::Detailed,

                LookupActor = TComponentTracingLevels::TQueryProcessor::Basic,
                    LookupActorShardsResolve = TComponentTracingLevels::TQueryProcessor::Detailed,

            BulkUpsertActor = TComponentTracingLevels::TQueryProcessor::TopLevel,
        };
    };

    struct TWilsonTablet {
        enum {
            TabletTopLevel = TComponentTracingLevels::TTablet::TopLevel,
            TabletBasic = TComponentTracingLevels::TTablet::Basic,
            TabletDetailed = TComponentTracingLevels::TTablet::Detailed,
        };
    };

    struct TWilsonGrpc {
        enum {
            RequestProxy = TComponentTracingLevels::TGrpcProxy::TopLevel,
            RequestActor = TComponentTracingLevels::TGrpcProxy::TopLevel,
            RequestCheckActor = TComponentTracingLevels::TGrpcProxy::Basic,
        };
    };

} // NKikimr
