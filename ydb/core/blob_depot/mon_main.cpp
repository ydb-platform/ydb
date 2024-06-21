#include "blob_depot_tablet.h"
#include "data.h"
#include "garbage_collection.h"
#include "blocks.h"
#include "space_monitor.h"
#include "mon_main.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TTxMonData : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::unique_ptr<NMon::TEvRemoteHttpInfo::THandle> Request;
        TStringStream Stream;

        enum class ETable {
            Data,
            RefCount,
            Trash,
            Barriers,
            Blocks,
            Storage,
        };

        static constexpr const char *TableName(ETable table) {
            switch (table) {
                case ETable::Data: return "data";
                case ETable::RefCount: return "refcount";
                case ETable::Trash: return "trash";
                case ETable::Barriers: return "barriers";
                case ETable::Blocks: return "blocks";
                case ETable::Storage: return "storage";
            }
        }

        static ETable TableByName(const TString& name) {
            for (const ETable table : {ETable::Data, ETable::RefCount, ETable::Trash, ETable::Barriers, ETable::Blocks,
                    ETable::Storage}) {
                if (name == TableName(table)) {
                    return table;
                }
            }
            return ETable::Data;
        }

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_MON_DATA; }

        TTxMonData(TBlobDepot *self, NMon::TEvRemoteHttpInfo::TPtr ev)
            : TTransactionBase(self)
            , Request(ev.Release())
        {}

        bool Execute(TTransactionContext& /*txc*/, const TActorContext&) override {
            const TCgiParameters& cgi = Request->Get()->Cgi();
            const ETable table = TableByName(cgi.Get("table"));

            void (TTxMonData::*render)(bool) = nullptr;
            switch (table) {
                case ETable::Data:
                    render = &TTxMonData::RenderDataTable;
                    break;

                case ETable::RefCount:
                    render = &TTxMonData::RenderRefCountTable;
                    break;

                case ETable::Trash:
                    render = &TTxMonData::RenderTrashTable;
                    break;

                case ETable::Barriers:
                    render = &TTxMonData::RenderBarriersTable;
                    break;

                case ETable::Blocks:
                    render = &TTxMonData::RenderBlocksTable;
                    break;

                case ETable::Storage:
                    render = &TTxMonData::RenderStorageTable;
                    break;
            }
            if (!render) {
                Y_ABORT();
            }

            HTML(Stream) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        Stream << "Data";
                    }
                    DIV_CLASS("panel-body") {
                        Stream << "<ul class='nav nav-tabs'>";
                        for (const ETable tab : {ETable::Data, ETable::RefCount, ETable::Trash, ETable::Barriers,
                                ETable::Blocks, ETable::Storage}) {
                            Stream << "<li" << (table == tab ? " class='active'" : "") << ">"
                                << "<a href='app?TabletID=" << Self->TabletID() << "&page=data&table="
                                << TableName(tab) << "'>" << TableName(tab) << "</a></li>";
                        }
                        Stream << "</ul>";

                        TABLE_CLASS("table") {
                            TABLEHEAD() {
                                TABLER() {
                                    (this->*render)(true);
                                }
                            }
                            TABLEBODY() {
                                (this->*render)(false);
                            }
                        }
                    }
                }
            }

            return true;
        }

        void RenderDataTable(bool header) {
            std::optional<TData::TKey> seek;
            ui32 rowsAfter = 100;
            ui32 rowsBefore = 0;

            HTML(Stream) {
                if (header) {
                    TABLEH() { Stream << "key"; }
                    TABLEH() { Stream << "value chain"; }
                    TABLEH() { Stream << "keep state"; }
                    TABLEH() { Stream << "barrier"; }
                } else {
                    const TCgiParameters& cgi = Request->Get()->Cgi();
                    if (cgi.Has("seek")) {
                        if (const TString& value = cgi.Get("seek")) {
                            if (Self->Config.HasVirtualGroupId()) {
                                TString error;
                                TLogoBlobID id;
                                if (TLogoBlobID::Parse(id, value, error)) {
                                    seek.emplace(id);
                                } else {
                                    DIV() {
                                        Stream << "invalid seek value: " << error;
                                    }
                                }
                            } else {
                                seek.emplace(value);
                            }
                        }
                    }
                    if (cgi.Has("rowsBefore") && !TryFromString(cgi.Get("rowsBefore"), rowsBefore)) {
                        DIV() {
                            Stream << "invalid rowsBefore value";
                        }
                    }
                    if (cgi.Has("rowsAfter") && !TryFromString(cgi.Get("rowsAfter"), rowsAfter)) {
                        DIV() {
                            Stream << "invalid rowsAfter value";
                        }
                    }

                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("control-group") {
                            LABEL_CLASS_FOR("control-label", "inputSeek") {
                                Stream << "Seek";
                            }
                            DIV_CLASS("controls") {
                                Stream << "<input id='inputSeek' name='seek' type='text' value='" << cgi.Get("seek") << "'/>";
                            }
                        }
                        DIV_CLASS("control-group") {
                            LABEL_CLASS_FOR("control-label", "inputRowsBefore") {
                                Stream << "Rows before";
                            }
                            DIV_CLASS("controls") {
                                Stream << "<input id='inputRowsBefore' name='rowsBefore' type='number' value='" <<
                                    rowsBefore << "'/>";
                            }
                        }
                        DIV_CLASS("control-group") {
                            LABEL_CLASS_FOR("control-label", "inputRowsAfter") {
                                Stream << "Rows after";
                            }
                            DIV_CLASS("controls") {
                                Stream << "<input id='inputRowsAfter' name='rowsAfter' type='number' value='" <<
                                   rowsAfter << "'/>";
                            }
                        }
                        DIV_CLASS("control-group") {
                            DIV_CLASS("controls") {
                                Stream << "<input type='hidden' name='TabletID' value='" << Self->TabletID() << "'/>";
                                Stream << "<input type='hidden' name='page' value='data'/>";
                                Stream << "<input type='hidden' name='table' value='data'/>";
                                Stream << "<button type='submit' class='btn btn-default'>Show</button>";
                            }
                        }
                    }

                    Self->Data->ShowRange(seek, rowsBefore, rowsAfter, [&](const TData::TKey& key, const TData::TValue& value) {
                        TABLER() {
                            TABLED() {
                                key.Output(Stream);
                            }
                            TABLED() {
                                bool first = true;
                                for (const auto& item : value.ValueChain) {
                                    if (first) {
                                        first = false;
                                    } else {
                                        Stream << "<br/>";
                                    }
                                    Stream << TBlobSeqId::FromProto(item.GetLocator().GetBlobSeqId()).ToString();
                                    if (item.HasSubrangeBegin() || item.HasSubrangeEnd()) {
                                        Stream << "[";
                                        if (item.HasSubrangeBegin()) {
                                            Stream << item.GetSubrangeBegin();
                                        }
                                        Stream << ":";
                                        if (item.HasSubrangeEnd()) {
                                            Stream << item.GetSubrangeEnd();
                                        }
                                        Stream << "]";
                                    }
                                }
                            }
                            TABLED() {
                                Stream << NKikimrBlobDepot::EKeepState_Name(value.KeepState);
                            }
                            TABLED() {
                                if (Self->Config.HasVirtualGroupId()) {
                                    bool underSoft, underHard;
                                    Self->BarrierServer->GetBlobBarrierRelation(key.GetBlobId(), &underSoft, &underHard);
                                    Stream << (underSoft ? 'S' : '-') << (underHard ? 'H' : '-');
                                }
                            }
                        }
                    });
                }
            }
        }

        void RenderRefCountTable(bool header) {
            HTML(Stream) {
                if (header) {
                    TABLEH() { Stream << "blob id"; }
                    TABLEH() { Stream << "refcount"; }
                } else {
                    Self->Data->EnumerateRefCount([&](TLogoBlobID id, ui32 count) {
                        TABLER() {
                            TABLED() { Stream << id; }
                            TABLED() { Stream << count; }
                        }
                    });
                }
            }
        }

        void RenderTrashTable(bool header) {
            HTML(Stream) {
                if (header) {
                    TABLEH() { Stream << "group id"; }
                    TABLEH() { Stream << "blob id"; }
                    TABLEH() { Stream << "in flight"; }
                } else {
                    Self->Data->EnumerateTrash([&](ui32 groupId, TLogoBlobID blobId, bool inFlight) {
                        TABLER() {
                            TABLED() { Stream << groupId; }
                            TABLED() { Stream << blobId; }
                            TABLED() { Stream << (inFlight ? "*" : ""); }
                        }
                    });
                }
            }
        }

        void RenderBarriersTable(bool header) {
            HTML(Stream) {
                if (header) {
                    TABLEH() { Stream << "tablet id"; }
                    TABLEH() { Stream << "channel"; }
                    TABLEH() { Stream << "soft"; }
                    TABLEH() { Stream << "hard"; }
                } else {
                    Self->BarrierServer->Enumerate([&](ui64 tabletId, ui8 channel, TGenStep softGenCtr, TGenStep soft,
                            TGenStep hardGenCtr, TGenStep hard) {
                        TABLER() {
                            TABLED() { Stream << tabletId; }
                            TABLED() { Stream << int(channel); }
                            TABLED() { softGenCtr.Output(Stream); Stream << " => "; soft.Output(Stream); }
                            TABLED() { hardGenCtr.Output(Stream); Stream << " => "; hard.Output(Stream); }
                        }
                    });
                }
            }
        }

        void RenderBlocksTable(bool header) {
            HTML(Stream) {
                if (header) {
                    TABLEH() { Stream << "tablet id"; }
                    TABLEH() { Stream << "blocked generation"; }
                } else {
                    Self->BlocksManager->Enumerate([&](ui64 tabletId, ui32 blockedGeneration) {
                        TABLER() {
                            TABLED() { Stream << tabletId; }
                            TABLED() { Stream << blockedGeneration; }
                        }
                    });
                }
            }
        }

        void RenderStorageTable(bool header) {
            HTML(Stream) {
                if (header) {
                    TABLEH() { Stream << "group id"; }
                    TABLEH() { Stream << "bytes stored<br>in current generation"; }
                    TABLEH() { Stream << "bytes stored<br>total"; }
                    TABLEH() { Stream << "status flag"; }
                    TABLEH() { Stream << "free space share"; }
                } else {
                    const ui32 generation = Self->Executor()->Generation();
                    auto *info = Self->Info();
                    std::map<ui32, std::tuple<ui64, ui64>> space;

                    Self->Data->EnumerateRefCount([&](TLogoBlobID id, ui32 /*refCount*/) {
                        const ui32 groupId = info->GroupFor(id.Channel(), id.Generation());
                        auto& [current, total] = space[groupId];
                        total += id.BlobSize();
                        if (id.Generation() == generation) {
                            current += id.BlobSize();
                        }
                    });

                    for (const auto& [groupId, _] : Self->SpaceMonitor->Groups) {
                        space.try_emplace(groupId);
                    }

                    for (const auto& [groupId, value] : space) {
                        const auto& [current, total] = value;
                        TABLER() {
                            TABLED() { Stream << groupId; }
                            TABLED() { Stream << FormatByteSize(current); }
                            TABLED() { Stream << FormatByteSize(total); }

                            const auto it = Self->SpaceMonitor->Groups.find(groupId);
                            if (it != Self->SpaceMonitor->Groups.end()) {
                                const auto& groupInfo = it->second;
                                TABLED() {
                                    Stream << groupInfo.StatusFlags.ToString();
                                }
                                TABLED() {
                                    if (groupInfo.ApproximateFreeSpaceShare) {
                                        Stream << Sprintf("%.1f %%", 100 * groupInfo.ApproximateFreeSpaceShare);
                                    } else {
                                        Stream << "<unknown>";
                                    }
                                }
                            } else {
                                TABLED() {}
                                TABLED() {}
                            }
                        }
                    }
                }
            }
        }

        void Complete(const TActorContext&) override {
            TActivationContext::Send(new IEventHandle(Request->Sender, Self->SelfId(), new NMon::TEvRemoteHttpInfoRes(
                Stream.Str()), 0, Request->Cookie));
        }
    };

    bool TBlobDepot::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) {
        if (!Executor() || !Executor()->GetStats().IsActive) {
            return false;
        } else if (!ev) {
            return true;
        }

        TStringStream s;

        const TCgiParameters& cgi = ev->Get()->Cgi();

        if (cgi.Has("json")) {
            JsonHandler.ProcessRenderPage(cgi, ev->Sender, ev->Cookie);
            return true;
        } else if (cgi.Has("page")) {
            const TString& page = cgi.Get("page");
            if (page == "data") {
                Execute(std::make_unique<TTxMonData>(this, ev));
                return true;
            } else {
                Send(ev->Sender, new NMon::TEvRemoteBinaryInfoRes(TStringBuilder()
                    << "HTTP/1.1 403 Page not found\r\n"
                    << "Content-Type: text/html\r\n"
                    << "Connection: close\r\n"
                    << "\r\n"
                    << "<html><body>Page " << page << " is not found</body></html>"), ev->Cookie);
                return true;
            }
        } else {
            RenderMainPage(s);
        }

        Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(s.Str()), 0, ev->Cookie);
        return true;
    }

    void TBlobDepot::RenderMainPage(IOutputStream& s) {
        HTML(s) {
            s << "<a href='app?TabletID=" << TabletID() << "&page=data'>Contained data</a><br>";

            s << R"(<script>
function ready() {
    doFetch();
}

function doFetch(gen, sequence) {
    var p = {
        "TabletID": ")" << TabletID() << R"(",
        "json": 1,
        "pretty": 1
    };
    if (gen !== undefined) {
        p.generation = gen;
    }
    if (sequence !== undefined) {
        p.sequence = sequence;
    }
    const params = new URLSearchParams(p);
    var url = "app?" + params.toString();
    fetch(url)
        .then((r) => {
            try {
                return r.json();
            } catch {
                document.getElementById("error").textContent = "failed to fetch JSON";
            }
        })
        .then((json) => { processJson(json); });
}

function processJson(json) {
    //console.log("received " + JSON.stringify(json));
    if (json.error !== undefined) {
        alert("json error: " + json.error);
        location.reload();
        return;
    } else if (!json.no_change) {
        var data = json.data;
        if (data !== undefined) {
            for (const key in data) {
                var elem = document.getElementById(key);
                if (elem) {
                    elem.textContent = data[key];
                } else {
                    console.log("element not found: " + key);
                }
            }
        }
    }
    doFetch(json.generation, json.sequence);
}

document.addEventListener("DOMContentLoaded", ready);
</script>
)";

#define UP(NAME, VALUE) "<div class=synced id=" << NAME << '>' << VALUE << "</div>"

            s << "<strong><font color='red'><div id='error'/></font></strong>";

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    s << "Stats";
                }
                DIV_CLASS("panel-body") {
                    KEYVALUE_TABLE({

                        ui64 total = 0;
                        ui64 trashInFlight = 0;
                        ui64 trashPending = 0;
                        Data->EnumerateRefCount([&](TLogoBlobID id, ui32 /*refCount*/) {
                            total += id.BlobSize();
                        });
                        Data->EnumerateTrash([&](ui32 /*groupId*/, TLogoBlobID id, bool inFlight) {
                            (inFlight ? trashInFlight : trashPending) += id.BlobSize();
                        });

                        KEYVALUE_UP("Data, bytes", "data", FormatByteSize(total));
                        KEYVALUE_UP("Trash in flight, bytes", "trash_in_flight", FormatByteSize(trashInFlight));
                        KEYVALUE_UP("Trash pending, bytes", "trash_pending", FormatByteSize(trashPending));

                        std::vector<ui32> groups;
                        for (const auto& [groupId, _] : Groups) {
                            groups.push_back(groupId);
                        }
                        std::sort(groups.begin(), groups.end());
                        for (const ui32 groupId : groups) {
                            TGroupInfo& group = Groups[groupId];
                            KEYVALUE_UP(TStringBuilder() << "Data in GroupId# " << groupId << ", bytes",
                                'g' << groupId, FormatByteSize(group.AllocatedBytes));
                        }
                    })
                }
            }

            if (Configured && Config.GetIsDecommittingGroup()) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        s << "Decommission";
                    }
                    DIV_CLASS("panel-body") {
                        KEYVALUE_TABLE({
                            KEYVALUE_UP("Decommit state", "d.state", DecommitState);
                            KEYVALUE_UP("Assimilator state", "d.running", GroupAssimilatorId ? "running" : "stopped");
                            KEYVALUE_UP("Last assimilated blob id", "d.last_assimilated_blob_id",
                                Data->LastAssimilatedBlobId ?  Data->LastAssimilatedBlobId->ToString() : "<null>");
                            KEYVALUE_UP("Skip blocks up to", "d.skip_blocks_up_to",
                                AsStats.SkipBlocksUpTo ? ToString(*AsStats.SkipBlocksUpTo) : "<null>");
                            KEYVALUE_UP("Skip barriers up to", "d.skip_barriers_up_to",
                                AsStats.SkipBarriersUpTo ? TStringBuilder() << std::get<0>(*AsStats.SkipBarriersUpTo)
                                << ':' << (int)std::get<1>(*AsStats.SkipBarriersUpTo) : "<null>"_sb);
                            KEYVALUE_UP("Skip blobs up to", "d.skip_blobs_up_to",
                                AsStats.SkipBlobsUpTo ? AsStats.SkipBlobsUpTo->ToString() : "<null>");
                            KEYVALUE_UP("Copy iteration", "d.copy_iteration", AsStats.CopyIteration);
                            KEYVALUE_UP("Bytes to copy", "d.bytes_to_copy", FormatByteSize(AsStats.BytesToCopy));
                            KEYVALUE_UP("Bytes already copied", "d.bytes_copied", FormatByteSize(AsStats.BytesCopied));
                            KEYVALUE_UP("Copy speed, bytes per second", "d.copy_speed", FormatByteSize(AsStats.CopySpeed) + "/s");
                            KEYVALUE_UP("Copy time remaining", "d.copy_time_remaining", AsStats.CopyTimeRemaining);
                            KEYVALUE_UP("Last read blob id", "d.last_read_blob_id", AsStats.LastReadBlobId);
                            KEYVALUE_UP("Latest successful get", "d.latest_ok_get", AsStats.LatestOkGet);
                            KEYVALUE_UP("Latest erroneous get", "d.latest_error_get", AsStats.LatestErrorGet);
                            KEYVALUE_UP("Latest successful put", "d.latest_ok_put", AsStats.LatestOkPut);
                            KEYVALUE_UP("Latest erroneous put", "d.latest_error_put", AsStats.LatestErrorPut);
                            KEYVALUE_UP("Blobs read with OK", "d.blobs_read_ok", AsStats.BlobsReadOk);
                            KEYVALUE_UP("Blobs read with NODATA", "d.blobs_read_nodata", AsStats.BlobsReadNoData);
                            KEYVALUE_UP("Blobs read with error", "d.blobs_read_error", AsStats.BlobsReadError);
                            KEYVALUE_UP("Blobs put with OK", "d.blobs_put_ok", AsStats.BlobsPutOk);
                            KEYVALUE_UP("Blobs put with error", "d.blobs_put_error", AsStats.BlobsPutError);
                        })
                    }
                }
            }

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    s << "Data";
                }
                DIV_CLASS("panel-body") {
                    Data->RenderMainPage(s);
                }
            }
        }
    }

    NJson::TJsonValue TBlobDepot::RenderJson(bool pretty) {
        NJson::TJsonMap json;

        const auto formatSize = [&](ui64 size) -> NJson::TJsonValue {
            if (pretty) {
                return FormatByteSize(size);
            } else {
                return size;
            }
        };

        ui64 total = 0;
        ui64 trashInFlight = 0;
        ui64 trashPending = 0;
        Data->EnumerateRefCount([&](TLogoBlobID id, ui32 /*refCount*/) {
            total += id.BlobSize();
        });
        Data->EnumerateTrash([&](ui32 /*groupId*/, TLogoBlobID id, bool inFlight) {
            (inFlight ? trashInFlight : trashPending) += id.BlobSize();
        });

        NJson::TJsonMap data{
            {"data", formatSize(total)},
            {"trash_in_flight", formatSize(trashInFlight)},
            {"trash_pending", formatSize(trashPending)},
        };

        for (const auto& [groupId, group] : Groups) {
            data[TStringBuilder() << "g" << groupId] = formatSize(group.AllocatedBytes);
        }

        if (Configured && Config.GetIsDecommittingGroup()) {
            data["d.running"] = GroupAssimilatorId ? "running" : "stopped";
            data["d.state"] = TStringBuilder() << DecommitState;
            data["d.last_assimilated_blob_id"] = Data->LastAssimilatedBlobId ? Data->LastAssimilatedBlobId->ToString() : "<null>";
            AsStats.ToJson(data, pretty);
        }

        json["data"] = std::move(data);

        return json;
    }

    TJsonHandler::TJsonHandler(TRenderJson renderJson, ui32 timerEv, ui32 updateEv)
        : RenderJson(renderJson)
        , TimerEv(timerEv)
        , UpdateEv(updateEv)
    {}

    void TJsonHandler::Setup(TActorId selfId, ui32 generation) {
        SelfId = selfId;
        Generation = generation;
    }

    void TJsonHandler::ProcessRenderPage(const TCgiParameters& cgi, TActorId sender, ui64 cookie) {
        const TMonotonic now = TActivationContext::Monotonic();

        auto issueResponse = [&](const NJson::TJsonValue& json) {
            TStringStream s;
            NJson::WriteJson(&s, &json);
            TActivationContext::Send(new IEventHandle(sender, SelfId, new NMon::TEvRemoteJsonInfoRes(s.Str()), 0, cookie));
        };

        auto issueError = [&](TString message) {
            issueResponse(NJson::TJsonMap{{"error", std::move(message)}});
        };

        if (!cgi.Has("pretty")) {
            return issueResponse(RenderJson(false));
        }

        UpdateHistory(now);

        if (cgi.Has("generation")) {
            ui32 gen;
            if (!TryFromString(cgi.Get("generation"), gen)) {
                return issueError("incorrect 'generation' parameter");
            } else if (gen != Generation) {
                return issueError("generation mismatch");
            }
        }

        if (cgi.Has("sequence")) {
            ui64 seq;
            if (!TryFromString(cgi.Get("sequence"), seq)) {
                return issueError("incorrect 'sequence' parameter");
            } else if (seq == Sequence) {
                const TMonotonic when = now + LongPollTimeout;
                LongPolls.emplace_back(TJsonHandler::TLongPoll{when, sender, cookie, seq});
                if (!LongPollTimerPending) {
                    TActivationContext::Schedule(when, new IEventHandle(TimerEv, 0, SelfId, {}, nullptr, 0));
                    LongPollTimerPending = true;
                }
                return;
            } else {
                return IssueResponse({TMonotonic(), sender, cookie, seq});
            }
        }

        IssueResponse({TMonotonic(), sender, cookie, 0});
    }

    void TJsonHandler::Invalidate() {
        Invalid = true;
        if (!LongPolls.empty() && !UpdateTimerPending) {
            const TMonotonic when = LastUpdatedTimestamp + UpdateTimeout;
            TActivationContext::Schedule(when, new IEventHandle(UpdateEv, 0, SelfId, {}, nullptr, 0));
            UpdateTimerPending = true;
        }
    }

    void TJsonHandler::HandleTimer() {
        Y_ABORT_UNLESS(LongPollTimerPending);

        NJson::TJsonMap json{{"no_change", true}, {"generation", Generation}, {"sequence", Sequence}};
        TStringStream s;
        NJson::WriteJson(&s, &json);

        const TMonotonic now = TActivationContext::Monotonic();
        for (; !LongPolls.empty() && LongPolls.front().When <= now; LongPolls.pop_front()) {
            const auto& item = LongPolls.front();
            TActivationContext::Send(new IEventHandle(item.Sender, SelfId, new NMon::TEvRemoteJsonInfoRes(s.Str()), 0, item.Cookie));
        }

        if (LongPolls.empty()) {
            LongPollTimerPending = false;
        } else {
            TActivationContext::Schedule(LongPolls.front().When, new IEventHandle(TimerEv, 0, SelfId, {}, nullptr, 0));
        }
    }

    void TJsonHandler::HandleUpdate() {
        Y_ABORT_UNLESS(UpdateTimerPending);
        UpdateTimerPending = false;
        if (!LongPolls.empty()) {
            UpdateHistory(TActivationContext::Monotonic());
        }
    }

    void TJsonHandler::UpdateHistory(TMonotonic now) {
        if (LastUpdatedTimestamp + UpdateTimeout <= now && Invalid) {
            NJson::TJsonValue json = RenderJson(true);
            json["generation"] = Generation;
            json["sequence"] = Sequence;
            if (History.empty() || json != History.back().Json) {
                json["sequence"] = ++Sequence;
                History.emplace_back(THistory{Sequence, std::move(json)});
                while (History.size() > 3) {
                    History.pop_front();
                }

                TStringStream s;
                NJson::WriteJson(&s, &History.back().Json);

                for (const auto& lp : LongPolls) {
                    IssueResponse(lp);
                }
                LongPolls.clear();
            }

            LastUpdatedTimestamp = now;
            Invalid = false;
        }
        if (Invalid && !UpdateTimerPending) {
            const TMonotonic when = LastUpdatedTimestamp + UpdateTimeout;
            TActivationContext::Schedule(when, new IEventHandle(UpdateEv, 0, SelfId, {}, nullptr, 0));
            UpdateTimerPending = true;
        }
    }

    void TJsonHandler::IssueResponse(const TLongPoll& lp) {
        const NJson::TJsonValue *base = nullptr;

        if (lp.BaseSequence) {
            auto comp = [](const THistory& item, ui64 sequence) { return item.Sequence < sequence; };
            auto it = std::lower_bound(History.begin(), History.end(), lp.BaseSequence, comp);
            if (it != History.end() && it->Sequence == lp.BaseSequence) {
                base = &it->Json;
            }
        }

        Y_ABORT_UNLESS(!History.empty());

        auto issueResponse = [&](const NJson::TJsonValue& json) {
            TStringStream s;
            NJson::WriteJson(&s, &json);
            TActivationContext::Send(new IEventHandle(lp.Sender, SelfId, new NMon::TEvRemoteJsonInfoRes(s.Str()), 0,
                lp.Cookie));
        };

        if (base) {
            NJson::TJsonValue response = History.back().Json;
            const auto& baseMap = (*base)["data"].GetMapSafe();
            auto& map = response["data"].GetMapSafe();
            for (auto it = map.begin(); it != map.end(); ) {
                if (const auto jt = baseMap.find(it->first); jt != baseMap.end() && jt->second == it->second) {
                    map.erase(it++);
                } else {
                    ++it;
                }
            }
            issueResponse(response);
        } else {
            issueResponse(History.back().Json);
        }
    }

} // NKikimr::NBlobDepot
