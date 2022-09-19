#pragma once

namespace NKikimr {

    struct TWilson {
        enum {
            BlobStorage = 8, // DS proxy and lower levels
            DsProxyInternals = 9,
            VDiskTopLevel = 12,
            VDiskInternals = 13,
        };
    };

    struct TWilsonKqp {
        enum {
            KqpSession = 10,
                CompileRequest = 9,
                    CompileService = 8,
                        CompileActor = 7,

                KqpSessionCreateAndSendPropose = 9,

                ExecuterReadyState = 9,
                ExecuterWaitResolveState = 9,
                    ExecuterTableResolve = 9,
                ExecuterZombieState = 9,

                ComputeActor = 6,

                LiteralExecuter = 9,
                    LiteralExecuterPrepareTasks = 9,
                    LiteralExecuterRunTasks = 9,

                DataExecuter = 9,
                    DataExecuterPrepareState = 9,
                        DataExecuterPrepateTasks = 9,
                    DataExecuterExecuteState = 9,
                        DataExecuterSendTasksAndTxs = 9,
                    DataExecuterWaitSnapshotState = 9,
                    ProposeTransaction = 8,

                ScanExecuter = 9,
                    ScanExecuterExecuteState = 9,
                    ScanExecuterPrepareTasks = 9,
                    KqpPlanner = 8,
                        KqpNodeSendTasks = 7,
        };
    };

} // NKikimr
