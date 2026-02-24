#include "config_examples.h"

#include <util/string/subst.h>

namespace NKikimr {

TVector<TConfigTemplate> BuildExamples() {
    TVector<TConfigTemplate> result = {
        TConfigTemplate{
            .LoadName = "KqpLoad",
            .Template = R"_(KqpLoad: {
    DurationSeconds: 30
    WindowDuration: 1
    WorkingDir: "${TENANT_NAME}"
    NumOfSessions: 64
    UniformPartitionsCount: 1000
    DeleteTableOnFinish: 1
    WorkloadType: 0
    QueryType: "data"
    Kv: {
        InitRowCount: 1000
        PartitionsByLoad: true
        MaxFirstKey: 18446744073709551615
        StringLen: 8
        ColumnsCnt: 2
        RowsCnt: 1
    }
})_"
        },
        TConfigTemplate{
            .LoadName = "KeyValueLoad",
            .Template = R"_(KeyValueLoad: {
    TargetTabletId: xxx
    DurationSeconds: 120
    Workers {
        KeyPrefix: "LoadTest_"
        MaxInFlight: 128
        Size: 1024
        IsInline: false
        LoopAtKeyCount: 0
    }
})_"
        },
        TConfigTemplate{
            .LoadName = "StorageLoad",
            .Template = R"_(StorageLoad: {
    DurationSeconds: 60
    Tablets: {
        Tablets: { TabletId: 5000 Channel: 0 GroupId: 2181038080 Generation: 1 }
        WriteSizes: { Weight: 1.0 Min: 4096 Max: 4096}
        WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 50000 MaxUs: 50000 } }
        MaxInFlightWriteRequests: 1

        ReadSizes: { Weight: 1.0 Min: 4096 Max: 4096 }
        ReadIntervals: { Weight: 1.0 Uniform: { MinUs: 0 MaxUs: 0 } }
        MaxInFlightReadRequests: 16
        FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 10000000 MaxUs: 10000000 } }
        PutHandleClass: TabletLog
        GetHandleClass: FastRead
    }
})_"
        },
        TConfigTemplate{
            .LoadName = "MemoryLoad",
            .Template = R"_(MemoryLoad: {
    DurationSeconds: 3600
    BlockSize: 1048576
    IntervalUs: 9000000
})_"
        },
        TConfigTemplate{
            .LoadName = "InterconnectLoad",
            .Template = R"_(InterconnectLoad: {
    DurationSeconds: 101
    InFlyMax: 3
    NodeHops: [1, 50000]
    SizeMin: 0
    SizeMax: 0
    IntervalMinUs: 0
    IntervalMaxUs: 0
    SoftLoad: false
    UseProtobufWithPayload: false
})_"
        }
#ifdef __linux__
        ,TConfigTemplate{
            .LoadName = "NBS2Load",
            .Template = R"_(NBS2Load: {
    DurationSeconds: 20
    DirectPartitionId: ""
    RangeTest {
        Start: 0
        End: 32767
        RequestsCount: 1000
        ReadRate: 100
        WriteRate: 0
        LoadType: LOAD_TYPE_RANDOM
        IoDepth: 1
    }
})_"
        }
        ,TConfigTemplate{
            .LoadName = "PersistentBufferWriteLoad",
            .Template = R"_(PersistentBufferWriteLoad: {
    DurationSeconds: 60
    DDiskId: {
        NodeId: 1
        PDiskId: 1
        DDiskSlotId: 1
    }
    WriteInfos: [
        { Size: 4096 Weight: 5 }
        { Size: 8192 Weight: 5 }
        { Size: 32768 Weight: 1 }
    ]
})_"
        }
#endif
    };
    return result;
}

TConstArrayRef<TConfigTemplate> GetConfigTemplates() {
    static const TVector<TConfigTemplate> kExamples = BuildExamples();
    return kExamples;
}

TConfigExample ApplyTemplateParams(const TConfigTemplate& templ, const TString& tenantName) {
    TString text = templ.Template;
    SubstGlobal(text, "${TENANT_NAME}", tenantName);
    TString escaped = text;
    SubstGlobal(escaped, "\"", "\\\"");
    SubstGlobal(escaped, "\n", "\\n");
    return TConfigExample {
        .LoadName = templ.LoadName,
        .Text = text,
        .Escaped = escaped
    };
}

}  // namespace NKikimr
