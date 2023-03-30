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
                Y_FAIL();
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
        if (cgi.Has("page")) {
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

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    s << "Stats";
                }
                DIV_CLASS("panel-body") {
                    KEYVALUE_TABLE({

                        TABLER() {
                            TABLED() { s << "Data, bytes"; }
                            TABLED() {
                                ui64 total = 0;
                                Data->EnumerateRefCount([&](TLogoBlobID id, ui32 /*refCount*/) {
                                    total += id.BlobSize();
                                });
                                s << FormatByteSize(total);
                            }
                        }

                        ui64 trashInFlight = 0;
                        ui64 trashPending = 0;
                        Data->EnumerateTrash([&](ui32 /*groupId*/, TLogoBlobID id, bool inFlight) {
                            (inFlight ? trashInFlight : trashPending) += id.BlobSize();
                        });

                        KEYVALUE_P("Trash in flight, bytes", FormatByteSize(trashInFlight));
                        KEYVALUE_P("Trash pending, bytes", FormatByteSize(trashPending));

                        std::vector<ui32> groups;
                        for (const auto& [groupId, _] : Groups) {
                            groups.push_back(groupId);
                        }
                        std::sort(groups.begin(), groups.end());
                        for (const ui32 groupId : groups) {
                            TGroupInfo& group = Groups[groupId];
                            KEYVALUE_P(TStringBuilder() << "Data in GroupId# " << groupId << ", bytes",
                                FormatByteSize(group.AllocatedBytes));
                        }
                    })
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

} // NKikimr::NBlobDepot
