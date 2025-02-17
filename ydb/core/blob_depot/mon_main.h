#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    #define KEYVALUE_TABLE(BODY) \
        TABLE_CLASS("table") { \
            TABLEHEAD() { \
                TABLER() { \
                    TABLEH() { s << "Parameter"; } \
                    TABLEH() { s << "Value"; } \
                } \
            } \
            TABLEBODY() { \
                BODY \
            } \
        }

    #define KEYVALUE_P(KEY, VALUE) \
        TABLER() { \
            TABLED() { __stream << (KEY); } \
            TABLED() { __stream << (VALUE); } \
        }

    #define KEYVALUE_UP(KEY, NAME, VALUE) \
        TABLER() { \
            TABLED() { __stream << (KEY); } \
            TABLED() { \
                __stream << "<div class=synced id=" << NAME << '>' << (VALUE) << "</div>"; \
            } \
        }

    inline TString FormatByteSize(ui64 size) {
        static const char *suffixes[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB", nullptr};
        TStringStream s;
        FormatHumanReadable(s, size, 1024, 2, suffixes);
        return s.Str();
    }

    class TJsonHandler {
    public:
        using TRenderJson = std::function<NJson::TJsonValue(bool pretty)>;

    private:
        struct THistory {
            ui64 Sequence;
            NJson::TJsonValue Json;
        };

        struct TLongPoll {
            TMonotonic When;
            TActorId Sender;
            ui64 Cookie;
            ui64 BaseSequence;
        };

    private:
        TRenderJson RenderJson;
        ui32 TimerEv;
        ui32 UpdateEv;
        TActorId SelfId;
        ui32 Generation;
        std::deque<THistory> History;
        ui64 Sequence = 0;
        bool Invalid = true;
        TMonotonic LastUpdatedTimestamp;
        std::deque<TLongPoll> LongPolls;
        bool LongPollTimerPending = false;
        bool UpdateTimerPending = false;

        const TDuration LongPollTimeout = TDuration::Seconds(10);
        const TDuration UpdateTimeout = TDuration::MilliSeconds(750);

    public:
        TJsonHandler(TRenderJson renderJson, ui32 timerEv, ui32 updateEv);

        void Setup(TActorId selfId, ui32 generation);

        void ProcessRenderPage(const TCgiParameters& cgi, TActorId sender, ui64 cookie);
        void Invalidate(); // called then RenderJson may generate another value

        void HandleTimer();
        void HandleUpdate();

    private:
        void UpdateHistory(TMonotonic now);
        void IssueResponse(const TLongPoll& lp);
    };

} // NKikimr::NBlobDepot
