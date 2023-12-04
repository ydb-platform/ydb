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
            KqpSession = 8,
                CompileService = 9,
                CompileActor = 9,
                SessionAcquireSnapshot = 9,

                    ExecuterTableResolve = 10,
                    ExecuterShardsResolve = 10,

                LiteralExecuter = 9,

                DataExecuter = 9,
                    DataExecuterAcquireSnapshot = 10,
                    DataExecuterRunTasks = 10,

                ScanExecuter = 9,
                    ScanExecuterRunTasks = 10,

                KqpNodeSendTasks = 9,

                ProposeTransaction = 9,

                ComputeActor = 9,
        };
    };

    struct TWilsonTablet {
        enum {
            Tablet = 15
        };
    };

} // NKikimr
