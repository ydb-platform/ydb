#pragma once

#include <library/cpp/histogram/hdr/histogram.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

namespace NYdb {
    namespace NConsoleClient {
        class TSqsWorkloadStats {
        public:
            struct RequestDoneEvent {
                ui64 RequestTime;
            };
            struct SendRequestErrorEvent {};
            struct SendRequestDoneEvent {
                ui64 RequestTime;
            };
            struct SentMessagesEvent {
                ui64 TotalBytes;
                ui64 MessagesCount;
            };
            struct ReceiveRequestDoneEvent {
                ui64 RequestTime;
            };
            struct ReceiveRequestErrorEvent {};
            struct DeleteRequestDoneEvent {
                ui64 RequestTime;
            };
            struct DeleteRequestErrorEvent {};
            struct GotMessageEvent {
                ui64 MessagesSize;
                ui64 EndToEndLatency;
                ui64 BatchSize;
            };
            struct DeletedMessagesEvent {
                ui64 MessagesCount;
            };
            struct FinishProcessMessagesEvent {
                ui64 MessagesCount;
            };
            struct PushAsyncRequestTaskToQueueEvent {};
            struct ErrorWhileProcessingMessagesEvent {};

            TSqsWorkloadStats();

            void AddEvent(const SendRequestDoneEvent& event);
            void AddEvent(const SentMessagesEvent& event);
            void AddEvent(const ReceiveRequestDoneEvent& event);
            void AddEvent(const DeleteRequestDoneEvent& event);
            void AddEvent(const GotMessageEvent& event);
            void AddEvent(const SendRequestErrorEvent& event);
            void AddEvent(const ReceiveRequestErrorEvent& event);
            void AddEvent(const DeleteRequestErrorEvent& event);
            void AddEvent(const DeletedMessagesEvent& event);
            void AddEvent(const FinishProcessMessagesEvent& event);
            void AddEvent(const PushAsyncRequestTaskToQueueEvent& event);
            void AddEvent(const ErrorWhileProcessingMessagesEvent& event);

            ui64 WriteBytes;
            ui64 WriteMessages;
            NHdr::THistogram SendRequestTimeHist;
            ui64 ReadMessages;
            ui64 SendRequestErrors;
            ui64 SendRequestsSuccess;
            NHdr::THistogram ReceiveRequestTimeHist;
            ui64 ReceiveRequestErrors;
            ui64 ReceiveRequestsSuccess;
            ui64 DeleteMessages;
            NHdr::THistogram DeleteRequestTimeHist;
            ui64 DeleteRequestErrors;
            ui64 DeleteRequestsSuccess;
            ui64 ReadBytes;
            NHdr::THistogram EndToEndLatencyHist;
            ui64 MessagesInFlight;
            NHdr::THistogram MessagesInFlightHist;
            i64 AsyncRequestTasks;
            ui64 ErrorsWhileProcessingMessages;

        private:
            constexpr static ui64 HighestTrackableTime = 100000000;
            constexpr static ui64 HighestTrackableMessageCount = 10000000;
        };
    } // namespace NConsoleClient
} // namespace NYdb
