#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

namespace NYdb {
    namespace NConsoleClient {
        class TTopicWorkloadStats {
        public:
            struct WriterEvent {
                ui64 MessageSize;
                ui64 WriteTime;
                ui64 InflightMessages;
            };
            typedef THolder<WriterEvent> WriterEventRef;

            struct ReaderEvent {
                ui64 MessageSize;
                ui64 FullTime;
            };
            typedef THolder<ReaderEvent> ReaderEventRef;

            struct LagEvent {
                ui64 LagMessages;
                ui64 LagTime;
            };
            typedef THolder<LagEvent> LagEventRef;

            TTopicWorkloadStats();

            void AddWriterEvent(const WriterEvent& event);
            void AddReaderEvent(const ReaderEvent& event);
            void AddLagEvent(const LagEvent& event);

            ui64 WriteBytes;
            ui64 WriteMessages;
            NHdr::THistogram WriteTimeHist;
            NHdr::THistogram InflightMessagesHist;
            NHdr::THistogram LagMessagesHist;
            NHdr::THistogram LagTimeHist;
            ui64 ReadBytes;
            ui64 ReadMessages;
            NHdr::THistogram FullTimeHist;

        private:
            constexpr static ui64 HighestTrackableTime = 100000;
            constexpr static ui64 HighestTrackableMessageCount = 10000;
        };
    }
}