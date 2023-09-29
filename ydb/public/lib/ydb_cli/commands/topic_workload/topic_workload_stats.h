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
            struct ReaderEvent {
                ui64 MessageSize;
                ui64 FullTime;
            };
            struct LagEvent {
                ui64 LagMessages;
                ui64 LagTime;
            };
            struct SelectEvent {
                ui64 Time;
            };
            struct UpsertEvent {
                ui64 Time;
            };
            struct CommitTxEvent {
                ui64 Time;
            };

            TTopicWorkloadStats();

            void AddEvent(const WriterEvent& event);
            void AddEvent(const ReaderEvent& event);
            void AddEvent(const LagEvent& event);
            void AddEvent(const SelectEvent& event);
            void AddEvent(const UpsertEvent& event);
            void AddEvent(const CommitTxEvent& event);

            ui64 WriteBytes;
            ui64 WriteMessages;
            NHdr::THistogram WriteTimeHist;
            NHdr::THistogram InflightMessagesHist;
            NHdr::THistogram LagMessagesHist;
            NHdr::THistogram LagTimeHist;
            ui64 ReadBytes;
            ui64 ReadMessages;
            NHdr::THistogram FullTimeHist;
            NHdr::THistogram SelectTimeHist;
            NHdr::THistogram UpsertTimeHist;
            NHdr::THistogram CommitTxTimeHist;

        private:
            constexpr static ui64 HighestTrackableTime = 100000;
            constexpr static ui64 HighestTrackableMessageCount = 10000;
        };
    }
}
