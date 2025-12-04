#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>
#include <library/cpp/histogram/hdr/histogram.h>

namespace NYdb {
    namespace NConsoleClient {
        class TSqsWorkloadStats {
        public:
            struct SendRequestStartEvent {
                TString RequestUUID;
                ui64 MessageCount;
                ui64 RequestTime;
            };
            struct SendRequestDoneEvent {
                ui64 MessageCount;
                ui64 TotalBytes;
                ui64 RequestTime;
            };
            struct ReceiveRequestDoneEvent {
                ui64 MessageCount;
                ui64 RequestTime;
            };
            struct DeleteRequestDoneEvent {
                ui64 MessageCount;
                ui64 RequestTime;
            };
            struct GotMessageEvent {
                ui64 MessageSize;
                ui64 EndToEndLatency;
            };

            TSqsWorkloadStats();

            void AddEvent(const SendRequestStartEvent& event);
            void AddEvent(const SendRequestDoneEvent& event);
            void AddEvent(const ReceiveRequestDoneEvent& event);
            void AddEvent(const DeleteRequestDoneEvent& event);
            void AddEvent(const GotMessageEvent& event);

            ui64 WriteBytes;
            ui64 WriteMessages;
            NHdr::THistogram SendRequestTimeHist;
            ui64 ReadMessages;
            NHdr::THistogram ReceiveRequestTimeHist;
            ui64 DeleteMessages;
            NHdr::THistogram DeleteRequestTimeHist;
            ui64 ReadBytes;
            NHdr::THistogram EndToEndLatencyHist;

        private:
            constexpr static ui64 HighestTrackableTime = 100000000;
        };
    }
}

