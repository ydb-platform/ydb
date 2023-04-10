#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

namespace NYdb {
    namespace NConsoleClient {
        class TTopicWorkloadStats {
            public:
                TTopicWorkloadStats();

                void AddWriterEvent(ui64 messageSize, ui64 writeTime, ui64 inflightMessages);
                void AddReaderEvent(ui64 messageSize, ui64 fullTime);
                void AddLagEvent(ui64 lagMessages, ui64 lagTime);

                ui64 WriteBytes;
                ui64 WriteMessages;
                NHdr::THistogram WriteTimeHist;
                ui64 InflightMessages;
                ui64 LagMessages;
                NHdr::THistogram LagTimeHist;
                ui64 ReadBytes;
                ui64 ReadMessages;
                NHdr::THistogram FullTimeHist;
        };
    }
}