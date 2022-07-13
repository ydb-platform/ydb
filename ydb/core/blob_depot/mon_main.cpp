#include "blob_depot_tablet.h"
#include "data.h"
#include "garbage_collection.h"
#include "blocks.h"

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
        };

        static constexpr const char *TableName(ETable table) {
            switch (table) {
                case ETable::Data: return "data";
                case ETable::RefCount: return "refcount";
                case ETable::Trash: return "trash";
                case ETable::Barriers: return "barriers";
                case ETable::Blocks: return "blocks";
            }
        }

        static ETable TableByName(const TString& name) {
            for (const ETable table : {ETable::Data, ETable::RefCount, ETable::Trash, ETable::Barriers, ETable::Blocks}) {
                if (name == TableName(table)) {
                    return table;
                }
            }
            return ETable::Data;
        }

    public:
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
                        for (const ETable tab : {ETable::Data, ETable::RefCount, ETable::Trash, ETable::Barriers, ETable::Blocks}) {
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
            HTML(Stream) {
                if (header) {
                    TABLEH() { Stream << "key"; }
                    TABLEH() { Stream << "value chain"; }
                    TABLEH() { Stream << "keep state"; }
                    TABLEH() { Stream << "barrier"; }
                } else {
                    Self->Data->ScanRange(nullptr, nullptr, 0, [&](const TData::TKey& key, const TData::TValue& value) {
                        TABLER() {
                            TABLED() {
                                key.Output(Stream, Self->Config);
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
                                if (Self->Config.GetOperationMode() == NKikimrBlobDepot::EOperationMode::VirtualGroup) {
                                    bool underSoft, underHard;
                                    Self->BarrierServer->GetBlobBarrierRelation(key.GetBlobId(), &underSoft, &underHard);
                                    Stream << (underSoft ? 'S' : '-') << (underHard ? 'H' : '-');
                                }
                            }
                        }
                        return true;
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
                    TABLEH() { Stream << "last record"; }
                    TABLEH() { Stream << "soft"; }
                    TABLEH() { Stream << "hard"; }
                } else {
                    Self->BarrierServer->Enumerate([&](ui64 tabletId, ui8 channel, ui32 recordGen, ui32 perGenerationCounter,
                            TGenStep soft, TGenStep hard) {
                        TABLER() {
                            TABLED() { Stream << tabletId; }
                            TABLED() { Stream << int(channel); }
                            TABLED() { Stream << recordGen << ":" << perGenerationCounter; }
                            TABLED() { soft.Output(Stream); }
                            TABLED() { hard.Output(Stream); }
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
        }
    }

} // NKikimr::NBlobDepot
