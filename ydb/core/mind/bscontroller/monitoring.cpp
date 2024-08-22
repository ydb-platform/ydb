#include "impl.h"

#include <library/cpp/json/json_writer.h>
#include <google/protobuf/util/json_util.h>


namespace NKikimr {
namespace NBsController {

static const char *DataSizeSuffix[] = {"B", "KiB", "MiB", "GiB", nullptr};

static void RenderBytesCell(IOutputStream& out, ui64 bytes) {
    HTML(out) {
        TABLED_ATTRS({{"data-text", ToString(bytes)}}) {
            FormatHumanReadable(out, bytes, 1024, 2, DataSizeSuffix);
        }
    }
}

template<typename T>
static TString PrintMaybe(const TMaybe<T>& m) {
    if (m) {
        return TStringBuilder() << *m;
    } else {
        return "&lt;<i>null</i>&gt;";
    }
}

struct TOperationLogEntry {
    using T = Schema::OperationLog;
    T::Index::Type Index;
    T::Timestamp::Type Timestamp;
    T::Request::Type Request;
    T::Response::Type Response;
    T::ExecutionTime::Type ExecutionTime;
};

class TBlobStorageController::TTxMonEvent_OperationLog : public TTransactionBase<TBlobStorageController> {
    const TActorId RespondTo;
    const TCgiParameters Params;

    TVector<TOperationLogEntry> Logs;
    ui64 NumRows = 0;

public:
    TTxMonEvent_OperationLog(const TActorId& sender, TCgiParameters params, TSelf *controller)
        : TBase(controller)
        , RespondTo(sender)
        , Params(std::move(params))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_MON_EVENT_OPERATION_LOG; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        ui64 count = FromStringWithDefault<ui64>(Params.Get("RowsCount"), 1000);
        ui64 offset = FromStringWithDefault<ui64>(Params.Get("RowsOffset"), Self->NextOperationLogIndex - 1);
        if (!count) {
            count = 1;
        }
        if (!LoadOperationLog(txc, count, offset)) {
            return false;
        }
        TStringStream str;
        RenderOperationLog(str, count, offset);
        TActivationContext::Send(new IEventHandle(RespondTo, Self->SelfId(), new NMon::TEvRemoteHttpInfoRes(str.Str())));
        return true;
    }

    void Complete(const TActorContext&) override
    {}

    bool LoadOperationLog(TTransactionContext& txc, ui64 count, ui64 offset) {
        NIceDb::TNiceDb db(txc.DB);
        using T = Schema::OperationLog;

        // obtain the very first record index
        ui64 firstRecordIndex = 0;
        {
            auto table = db.Table<T>().Select();
            if (!table.IsReady()) {
                return false;
            } else if (!table.EndOfSet()) {
                firstRecordIndex = table.GetValue<T::Index>();
                NumRows = Self->NextOperationLogIndex - firstRecordIndex;
            }
        }

        // scan the table
        auto table = db.Table<T>().Reverse().LessOrEqual(offset).Select();
        if (!table.IsReady()) {
            return false;
        }
        Logs.reserve(count);
        for (; !table.EndOfSet() && count; --count) {
            const auto& index = table.GetValue<Schema::OperationLog::Index>();
            const auto& timestamp = table.GetValue<Schema::OperationLog::Timestamp>();
            const auto& request = table.GetValue<Schema::OperationLog::Request>();
            const auto& response = table.GetValue<Schema::OperationLog::Response>();
            const auto& executionTime = table.GetValue<Schema::OperationLog::ExecutionTime>();
            Logs.emplace_back(TOperationLogEntry{index, timestamp, request, response, executionTime});
            if (!table.Next()) {
                return false;
            }
        }
        return true;
    }

    void RenderOperationLog(IOutputStream& out, const ui64 count, const ui64 offset) {
        Self->RenderHeader(out);

        HTML(out) {
            TAG(TH3) {
                out << "Operation Log";
            }
            TABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Index"; }
                        TABLEH() { out << "Timestamp"; }
                        TABLEH() { out << "Request"; }
                        TABLEH() { out << "Response"; }
                        TABLEH() { out << "Execution<br/>time"; }
                    }
                }
                TABLEBODY() {
                    for (size_t id = 0; id < Logs.size(); ++id) {
                        TABLER() {
                            const auto &entry = Logs[id];
                            NKikimrBlobStorage::TConfigRequest request;
                            NKikimrBlobStorage::TConfigResponse response;
                            Y_PROTOBUF_SUPPRESS_NODISCARD request.ParseFromString(entry.Request);
                            Y_PROTOBUF_SUPPRESS_NODISCARD response.ParseFromString(entry.Response);
                            TABLED() {
                                out << "<a href='?TabletID=" << Self->TabletID() << "&page=OperationLogEntry"
                                    "&RowIndex=" << entry.Index << "'>" << entry.Index << "</a>";
                            }
                            TABLED() { out << entry.Timestamp.ToRfc822String(); }
                            auto limitString = [] (const TString& str) {
                                const size_t STR_LIMIT = 4096;
                                if (str.size() > STR_LIMIT) {
                                    return TString(str, 0, STR_LIMIT) + "\n... truncated";
                                }
                                return str;
                            };
                            TABLED() {
                                out << "<input class='hide' id='request-" << id << "' type='checkbox'>"
                                   "<label for='request-" << id << "'>Show</label>"
                                   "<pre>" << limitString(request.DebugString()) << "</pre>";
                            }
                            TABLED() {
                                out << "<input class='hide' id='response-" << id << "' type='checkbox'>"
                                   "<label for='response-" << id << "'>Show</label>"
                                   "<pre>" << limitString(response.DebugString()) << "</pre>";
                            }
                            TABLED() {
                                out << entry.ExecutionTime;
                            }
                        }
                    }
                }
            }
        }

        if (NumRows) {
            ui64 firstPageIndex = Self->NextOperationLogIndex - 1;
            ui64 lastPageIndex = firstPageIndex - (NumRows / count) * count;
            if (NumRows % count == 0) {
                lastPageIndex += count;
            }

            if (offset + count <= firstPageIndex) {
                out << "<a href='?TabletID=" << Self->TabletID() << "&page=OperationLog&RowsCount=" << count << "&RowsOffset="
                    << (offset + count) << "' style='padding-right: 15px;'>Previous</a>";
            }
            if (offset >= lastPageIndex + count) {
                out << "<a href='?TabletID=" << Self->TabletID() << "&page=OperationLog&RowsCount=" << count << "&RowsOffset="
                    << (offset - count) << "' style='padding-right: 15px;'>Next</a>";
            }

            out << "<a href='?TabletID=" << Self->TabletID() << "&page=OperationLog&RowsCount=" << count << "&RowsOffset="
                << firstPageIndex << "' style='padding-right: 15px;'>First page</a>";

            out << "<a href='?TabletID=" << Self->TabletID() << "&page=OperationLog&RowsCount=" << count << "&RowsOffset="
                << lastPageIndex << "' style='padding-right: 15px;'>Last page</a>";
        }

        Self->RenderFooter(out);
    }
};

class TBlobStorageController::TTxMonEvent_OperationLogEntry : public TTransactionBase<TBlobStorageController> {
    const TActorId RespondTo;
    const TCgiParameters Params;

    TOperationLogEntry Entry;
    ui64 RowIndex = 0;
    bool HasEntry = false;

public:
    TTxMonEvent_OperationLogEntry(const TActorId& sender, TCgiParameters params, TSelf *controller)
        : TBase(controller)
        , RespondTo(sender)
        , Params(std::move(params))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_MON_EVENT_OPERATION_LOG_ENTRY; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        RowIndex = FromStringWithDefault<ui64>(Params.Get("RowIndex"), Self->NextOperationLogIndex - 1);
        if (!LoadOperationLogEntry(txc)) {
            return false;
        }
        TStringStream str;
        RenderOperationLogEntry(str);
        TActivationContext::Send(new IEventHandle(RespondTo, Self->SelfId(), new NMon::TEvRemoteHttpInfoRes(str.Str())));
        return true;
    }

    void Complete(const TActorContext&) override
    {}

private:
    bool LoadOperationLogEntry(TTransactionContext& txc) {
        NIceDb::TNiceDb db(txc.DB);
        using T = Schema::OperationLog;

        auto table = db.Table<T>().Key(RowIndex).Select();
        if (!table.IsReady()) {
            return false;
        }

        if (table.EndOfSet()) {
            HasEntry = false;
            return true;
        }

        const auto& index = table.GetValue<Schema::OperationLog::Index>();
        const auto& timestamp = table.GetValue<Schema::OperationLog::Timestamp>();
        const auto& request = table.GetValue<Schema::OperationLog::Request>();
        const auto& response = table.GetValue<Schema::OperationLog::Response>();
        const auto& executionTime = table.GetValue<Schema::OperationLog::ExecutionTime>();

        Entry = TOperationLogEntry{index, timestamp, request, response, executionTime};
        HasEntry = true;
        return true;
    }

    void RenderOperationLogEntry(IOutputStream& out) {
        Self->RenderHeader(out);

        HTML(out) {
            TAG(TH3) {
                out << "Operation Log ";
                if (HasEntry) {
                    out << "(entry " << RowIndex << ")";
                } else {
                    out << "(missing entry)";
                }
            }
            TABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Index"; }
                        TABLEH() { out << "Timestamp"; }
                        TABLEH() { out << "Request"; }
                        TABLEH() { out << "Response"; }
                        TABLEH() { out << "Execution<br/>time"; }
                    }
                }
                if (HasEntry) {
                    TABLEBODY() {
                        TABLER() {
                            NKikimrBlobStorage::TConfigRequest request;
                            NKikimrBlobStorage::TConfigResponse response;
                            Y_PROTOBUF_SUPPRESS_NODISCARD request.ParseFromString(Entry.Request);
                            Y_PROTOBUF_SUPPRESS_NODISCARD response.ParseFromString(Entry.Response);
                            TABLED() { out << Entry.Index; }
                            TABLED() { out << Entry.Timestamp.ToRfc822String(); }
                            TABLED() { out << "<pre>" << request.DebugString() << "</pre>"; }
                            TABLED() { out << "<pre>" << response.DebugString() << "</pre>"; }
                            TABLED() { out << Entry.ExecutionTime; }
                        }
                    }
                }
            }
        }

        Self->RenderFooter(out);
    }
};

class TBlobStorageController::TTxMonEvent_HealthEvents
    : public TTransactionBase<TBlobStorageController>
{
    const TActorId RespondTo;
    const bool Json;
    const ui64 Offset = 0;
    const ui64 NumRows = 1000;

    TInstant Since;

    struct TReassignItem {
        TVDiskID VDiskId;
        TVSlotId From;
        TVSlotId To;
        TString FromFqdn, FromPath;
        TString ToFqdn, ToPath;
    };

    struct TEvent {
        TInstant Timestamp;
        TString Message;
        std::vector<TReassignItem> Reassign;
        NJson::TJsonValue Json;

        TEvent(TInstant timestamp, TString message, std::vector<TReassignItem> reassign = {})
            : Timestamp(timestamp)
            , Message(message)
            , Reassign(std::move(reassign))
        {}
    };
    std::deque<TEvent> Events;

    static NJson::TJsonValue ToJson(const TVDiskID& vdiskId) {
        NJson::TJsonValue j(NJson::JSON_MAP);
        j["GroupId"] = vdiskId.GroupID.GetRawId();
        j["GroupGeneration"] = vdiskId.GroupGeneration;
        j["FailRealmIdx"] = vdiskId.FailRealm;
        j["FailDomainIdx"] = vdiskId.FailDomain;
        j["VDiskIdx"] = vdiskId.VDisk;
        return j;
    }

    static NJson::TJsonValue ToJson(const TPDiskId& pdiskId) {
        NJson::TJsonValue j(NJson::JSON_MAP);
        j["NodeId"] = pdiskId.NodeId;
        j["PDiskId"] = pdiskId.PDiskId;
        return j;
    }

    static NJson::TJsonValue ToJson(const TVSlotId& vslotId) {
        auto j = ToJson(vslotId.ComprisingPDiskId());
        j["VSlotId"] = vslotId.VSlotId;
        return j;
    }

    template<typename T>
    static NJson::TJsonValue ToJson(const std::optional<T>& x) {
        return x ? ToJson(*x) : NJson::TJsonValue(NJson::JSON_NULL);
    }

    template<typename T>
    static NJson::TJsonValue ToJsonOrNullOnDefault(const T& x) {
        return x != T() ? NJson::TJsonValue(x) : NJson::TJsonValue(NJson::JSON_NULL);
    }

public:
    TTxMonEvent_HealthEvents(const TActorId& sender, const TCgiParameters& cgi, TSelf *controller)
        : TBase(controller)
        , RespondTo(sender)
        , Json(cgi.Has("fmt") && cgi.Get("fmt") == "json")
        , Offset(FromStringWithDefault<ui64>(cgi.Get("RowsOffset"), 0))
        , NumRows(FromStringWithDefault<ui64>(cgi.Get("RowsCount"), 1000))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_MON_EVENT_HEALTH_EVENTS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const TInstant now = TActivationContext::Now();
        Since = now - TDuration::Days(14) - TDuration::FromValue(now.GetValue() % TDuration::Days(1).GetValue());
        Events.clear();

        NIceDb::TNiceDb db(txc.DB);
        using T = Schema::OperationLog;
        auto table = db.Table<T>().Reverse().Select();
        if (!table.IsReady()) {
            return false;
        }
        while (table.IsValid()) {
            const TInstant timestamp = table.GetValue<T::Timestamp>();
            if (timestamp < Since) {
                break;
            }

            NKikimrBlobStorage::TConfigRequest request;
            NKikimrBlobStorage::TConfigResponse response;
            if (!request.ParseFromString(table.GetValue<T::Request>())) {
                Events.emplace_front(timestamp, Sprintf("Failed to parse Request Index# %" PRIu64, table.GetValue<T::Index>()));
            } else if (!response.ParseFromString(table.GetValue<T::Response>())) {
                Events.emplace_front(timestamp, Sprintf("Failed to parse Response Index# %" PRIu64, table.GetValue<T::Index>()));
            } else {
                for (size_t k = 0; k < request.CommandSize() && k < response.StatusSize(); ++k) {
                    const auto& r = response.GetStatus(k);
                    if (!r.GetSuccess()) {
                        continue;
                    }
                    std::vector<TReassignItem> reassign;
                    for (const auto& item : r.GetReassignedItem()) {
                        TReassignItem r;
                        r.VDiskId = VDiskIDFromVDiskID(item.GetVDiskId());
                        r.From = item.GetFrom();
                        r.To = item.GetTo();
                        r.FromFqdn = item.GetFromFqdn();
                        r.FromPath = item.GetFromPath();
                        r.ToFqdn = item.GetToFqdn();
                        r.ToPath = item.GetToPath();
                        reassign.push_back(std::move(r));
                    }

                    const auto& q = request.GetCommand(k);
                    switch (q.GetCommandCase()) {
                        case NKikimrBlobStorage::TConfigRequest::TCommand::kEnableSelfHeal: {
                            const auto& cmd = q.GetEnableSelfHeal();
                            const char *message = cmd.GetEnable() ? "Self-Heal enabled" : "Self-Heal disabled";
                            Events.emplace_front(timestamp, message);
                            auto& j = Events.front().Json;
                            j["Event"] = message;
                            break;
                        }

                        case NKikimrBlobStorage::TConfigRequest::TCommand::kReassignGroupDisk: {
                            const auto& cmd = q.GetReassignGroupDisk();

                            const TVDiskID vdiskId(TGroupId::FromProto(&cmd, &NKikimrBlobStorage::TReassignGroupDisk::GetGroupId), cmd.GetGroupGeneration(),
                                cmd.GetFailRealmIdx(), cmd.GetFailDomainIdx(), cmd.GetVDiskIdx());

                            std::optional<TPDiskId> pdiskId;
                            if (cmd.HasTargetPDiskId()) {
                                const auto& x = cmd.GetTargetPDiskId();
                                pdiskId.emplace(x.GetNodeId(), x.GetPDiskId());
                            }

                            TStringStream msg;
                            msg << "VDisk reassign request"
                                << "<br/>VDiskId# " << vdiskId
                                << "<br/>GroupId# " << vdiskId.GroupID;

                            if (pdiskId) {
                                msg << "<br/>PDiskId# " << *pdiskId;
                            }

                            Events.emplace_front(timestamp, msg.Str(), std::move(reassign));
                            auto& j = Events.front().Json;
                            j["Event"] = "ReassignGroupDisk";
                            j["VDiskId"] = ToJson(vdiskId);
                            j["TargetPDiskId"] = ToJson(pdiskId);
                            break;
                        }

                        case NKikimrBlobStorage::TConfigRequest::TCommand::kSanitizeGroup: {
                            const auto& cmd = q.GetSanitizeGroup();

                            const ui32 groupId = cmd.GetGroupId();
                            TStringStream msg;
                            msg << "Group sanitizing request"
                                << "<br/>GroupId# " << groupId;

                            Events.emplace_front(timestamp, msg.Str(), std::move(reassign));
                            auto& j = Events.front().Json;
                            j["Event"] = "SanitizeGroup";
                            j["GroupId"] = ToString(groupId);
                            break;
                        }

                        case NKikimrBlobStorage::TConfigRequest::TCommand::kUpdateDriveStatus: {
                            const auto& cmd = q.GetUpdateDriveStatus();

                            const auto& key = cmd.GetHostKey();
                            TString fqdn = key.GetFqdn();
                            i32 icPort = key.GetIcPort();
                            ui32 nodeId = key.GetNodeId();
                            if (!fqdn) {
                                if (auto x = Self->HostRecords->GetHostId(nodeId)) {
                                    std::tie(fqdn, icPort) = *x;
                                }
                            }
                            if (!nodeId) {
                                if (auto x = Self->HostRecords->ResolveNodeId(std::make_tuple(fqdn, icPort))) {
                                    nodeId = *x;
                                }
                            }

                            TStringStream msg;
                            msg << "Update drive status"
                                << "<br/>FQDN# " << (fqdn ? fqdn : "unknown")
                                << "<br/>NodeId# " << (nodeId ? ToString(nodeId) : "unknown")
                                << "<br/>Path# " << cmd.GetPath();

                            if (const auto& pdiskId = cmd.GetPDiskId()) {
                                msg << "<br/>PDiskId# " << pdiskId;
                            }

                            msg << "<br/>to " << NKikimrBlobStorage::EDriveStatus_Name(cmd.GetStatus());
                            Events.emplace_front(timestamp, msg.Str(), std::move(reassign));
                            auto& j = Events.front().Json;
                            j["Event"] = "UpdateDriveStatus";
                            j["FQDN"] = ToJsonOrNullOnDefault(fqdn);
                            j["IcPort"] = ToJsonOrNullOnDefault(icPort);
                            j["NodeId"] = ToJsonOrNullOnDefault(nodeId);
                            j["Path"] = ToJsonOrNullOnDefault(cmd.GetPath());
                            j["PDiskId"] = ToJsonOrNullOnDefault(cmd.GetPDiskId());
                            j["Status"] = NKikimrBlobStorage::EDriveStatus_Name(cmd.GetStatus());
                            break;
                        }

                        default:
                            break;
                    }
                }
            }

            if (!table.Next()) {
                return false;
            }
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        TActivationContext::Send(new IEventHandle(RespondTo, Self->SelfId(), Json
            ? static_cast<IEventBase*>(new NMon::TEvRemoteJsonInfoRes(GenerateJson()))
            : static_cast<IEventBase*>(new NMon::TEvRemoteHttpInfoRes(GenerateHtml()))));
    }

    TString GenerateHtml() {
        TStringStream s;
        Self->RenderHeader(s);

        HTML(s) {
            ui64 offset = Offset;

            TAG(TH3) {
                s << "Health events";
            }
            TAG(TH3) {
                s << "Health-related operations since " << Since.ToRfc822StringLocal();
            }
            TABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { s << "UTC ts"; }
                        TABLEH() { s << "Local ts"; }
                        TABLEH() { s << "Message"; }
                        TABLEH() { s << "Reassign"; }
                    }
                }
                TABLEBODY() {
                    auto it = Events.begin();
                    std::advance(it, Min<size_t>(offset, Events.size()));
                    for (ui64 i = 0; i < NumRows && it != Events.end(); ++i, ++offset, ++it) {
                        const TEvent& event = *it;
                        TABLER() {
                            TABLED() { s << event.Timestamp.ToStringUpToSeconds(); }
                            TABLED() { s << event.Timestamp.ToRfc822StringLocal(); }
                            TABLED() { s << event.Message; }
                            TABLED() {
                                for (const TReassignItem& item : event.Reassign) {
                                    s << "<p>VDiskId# " << item.VDiskId << " (" << item.VDiskId.GroupID << ")"
                                        << "<br/>From# " << item.From
                                        << " " << item.FromFqdn << ":" << item.FromPath
                                        << "<br/>To# " << item.To
                                        << " " << item.ToFqdn << ":" << item.ToPath
                                        << "</p>";
                                }
                            }
                        }
                    }
                }
            }

            bool first = true;
            auto renderPageLink = [&](const char *name, ui64 offset) {
                s << "<a href='?TabletID=" << Self->TabletID() << "&page=HealthEvents&RowsCount=" << NumRows
                    << "&RowsOffset=" << offset << "'" << (first ? "" : " style='padding-left: 15px;'")
                    << ">" << name << "</a>";
                first = false;
            };

            if (Offset) {
                renderPageLink("Previous", Offset < NumRows ? 0 : Offset - NumRows);
            }
            if (offset != Events.size()) {
                renderPageLink("Next", offset);
            }
            if (Offset) {
                renderPageLink("First page", 0);
            }
            const ui64 lastPageOffset = Events.size() - Events.size() % NumRows;
            if (Offset < lastPageOffset) {
                renderPageLink("Last page", lastPageOffset);
            }
        }

        Self->RenderFooter(s);
        return s.Str();
    }

    TString GenerateJson() {
        NJson::TJsonValue json, events(NJson::JSON_ARRAY);
        for (auto& event : Events) {
            auto& j = event.Json;
            j["Timestamp"] = event.Timestamp.ToString();
            if (!event.Reassign.empty()) {
                NJson::TJsonValue reassign(NJson::JSON_ARRAY);
                for (const TReassignItem& item : event.Reassign) {
                    NJson::TJsonValue x;
                    x["VDiskId"] = ToJson(item.VDiskId);
                    x["From"] = ToJson(item.From);
                    x["From"]["FQDN"] = ToJsonOrNullOnDefault(item.FromFqdn);
                    x["From"]["Path"] = ToJsonOrNullOnDefault(item.FromPath);
                    x["To"] = ToJson(item.To);
                    x["To"]["FQDN"] = ToJsonOrNullOnDefault(item.ToFqdn);
                    x["To"]["Path"] = ToJsonOrNullOnDefault(item.ToPath);
                    reassign.AppendValue(std::move(x));
                }
                j["Reassign"] = std::move(reassign);
            }
            events.AppendValue(std::move(j));
        }
        json["Events"] = std::move(events);
        TStringStream s;
        NJson::WriteJson(&s, &json);
        return s.Str();
    }
};

class TBlobStorageController::TTxMonEvent_SetDown : public TTransactionBase<TBlobStorageController> {
public:
    const TActorId Source;
    const TGroupId GroupId;
    const bool Down;
    const bool Persist;
    TString Response;

    TTxMonEvent_SetDown(const TActorId& source, TGroupId groupId, bool down, bool persist, TSelf* bsc)
        : TBase(bsc)
        , Source(source)
        , GroupId(groupId)
        , Down(down)
        , Persist(persist)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_MON_EVENT_SET_DOWN; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TGroupInfo* group = Self->FindGroup(GroupId);
        if (group == nullptr) {
            Response = "{\"Error\":\"Group " + ToString(GroupId.GetRawId()) + " not found\"}";
            return true;
        }
        group->Down = Down;
        if (Persist) {
            NIceDb::TNiceDb db(txc.DB);
            typename TGroupId::Type groupId = GroupId.GetRawId();
            db.Table<Schema::Group>().Key(groupId).Update<Schema::Group::Down>(Down);
            group->PersistedDown = Down;
        }
        Response = "{\"GroupId\":" + ToString(GroupId.GetRawId()) + ',' + "\"Down\":" + (Down ? "true" : "false") + "}";
        return true;
    }

    void Complete(const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXMO01, "TBlobStorageController::TTxMonEvent_SetDown",
            (GroupId.GetRawId(), GroupId.GetRawId()), (Down, Down), (Persist, Persist), (Response, Response));
        TActivationContext::Send(new IEventHandle(Source, Self->SelfId(), new NMon::TEvRemoteJsonInfoRes(Response)));
    }
};

class TBlobStorageController::TTxMonEvent_GetDown : public TTransactionBase<TBlobStorageController> {
public:
    const TActorId Source;
    const TGroupId GroupId;
    TString Response;

    TTxMonEvent_GetDown(const TActorId& source, TGroupId groupId, TSelf* bsc)
        : TBase(bsc)
        , Source(source)
        , GroupId(groupId)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_MON_EVENT_GET_DOWN; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        NJson::TJsonValue json;

        auto reportGroup = [](const TGroupInfo& group) {
            NJson::TJsonValue item;
            item["GroupId"] = group.ID.GetRawId();
            item["Down"] = group.Down;
            item["PersistedDown"] = group.PersistedDown;
            return item;
        };

        if (GroupId.GetRawId()) {
            if (TGroupInfo* group = Self->FindGroup(GroupId)) {
                json = reportGroup(*group);
            } else {
                json["Error"] = Sprintf("GroupId# %" PRIu32 " not found", GroupId.GetRawId());
            }
        } else {
            for (const auto& kv : Self->GroupMap) {
                json.AppendValue(reportGroup(*kv.second));
            }
        }

        TStringStream stream;
        NJson::WriteJson(&stream, &json);
        Response = stream.Str();

        return true;
    }

    void Complete(const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXMO02, "TBlobStorageController::TTxMonEvent_GetDown", (GroupId.GetRawId(), GroupId.GetRawId()),
            (Response, Response));
        TActivationContext::Send(new IEventHandle(Source, Self->SelfId(), new NMon::TEvRemoteJsonInfoRes(Response)));
    }
};

class TDisableSelfHealActor : public TActorBootstrapped<TDisableSelfHealActor> {
    const TActorId MonProxy;
    const TString Url;

public:
    TDisableSelfHealActor(TActorId monProxy, TString url)
        : MonProxy(monProxy)
        , Url(std::move(url))
    {}

    void Bootstrap(const TActorId& parent) {
        auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        ev->Record.MutableRequest()->AddCommand()->MutableEnableSelfHeal()->SetEnable(false);
        Send(parent, ev.Release());
        Become(&TThis::StateFunc);
    }

    void HandleResponse() {
        Send(MonProxy, new NMon::TEvRemoteBinaryInfoRes(TStringBuilder()
            << NMonitoring::HTTPOKHTML
            << "<html><head><meta http-equiv=\"refresh\" content=\"0; " << Url << "\"></head></html>"
        ));
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        cFunc(TEvBlobStorage::EvControllerConfigResponse, HandleResponse)
    )
};

void TBlobStorageController::ProcessPostQuery(const NActorsProto::TRemoteHttpInfo& query, TActorId sender) {
    THttpHeaders headers;
    for (const auto& header : query.GetHeaders()) {
        headers.AddHeader(header.GetName(), header.GetValue());
    }

    TCgiParameters params;
    for (const auto& param : query.GetQueryParams()) {
        params.emplace(param.GetKey(), param.GetValue());
    }
    for (const auto& param : query.GetPostParams()) {
        params.emplace(param.GetKey(), param.GetValue());
    }

    auto sendResponse = [&](TString message, TString contentType, TString content) {
        Send(sender, new NMon::TEvRemoteBinaryInfoRes(TStringBuilder() << "HTTP/1.1 " << message << "\r\n"
            "Content-Type: " << contentType << "\r\n"
            "\r\n" << content));
    };

    if (params.count("exec")) {
        auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto *record = request->Record.MutableRequest();

        TString contentType;
        if (const auto *header = headers.FindHeader("Content-Type")) {
            TStringBuf value = header->Value();
            contentType = value.NextTok(';');
        }
        bool success = false;
        if (contentType == "application/x-protobuf-text") {
            NProtoBuf::TextFormat::Parser parser;
            success = parser.ParseFromString(query.GetPostContent(), record);
        } else if (contentType == "application/x-protobuf") {
            success = record->ParseFromString(query.GetPostContent());
        } else if (contentType == "application/json") {
            const auto status = google::protobuf::util::JsonStringToMessage(query.GetPostContent(), record);
            success = status.ok();
        } else {
            return sendResponse("400 Bad request", "text/plain", "unsupported Content-Type header value");
        }
        if (!success) {
            return sendResponse("400 Bad request", "text/plain", "failed to parse request");
        }

        TString accept = "application/x-protobuf-text";
        if (const auto *header = headers.FindHeader("Accept")) {
            accept = header->Value();
            if (accept == "*/*") {
                accept = "application/x-protobuf-text";
            }
            if (accept != "application/x-protobuf-text" && accept != "application/x-protobuf" && accept != "application/json") {
                return sendResponse("400 Bad request", "text/plain", "unsupported Accept header value");
            }
        }

        if (request) {
            class TQueryExecActor : public TActor<TQueryExecActor> {
                const TActorId Sender;
                const TString Accept;

            public:
                TQueryExecActor(const TActorId& sender, const TString& accept)
                    : TActor(&TThis::StateFunc)
                    , Sender(sender)
                    , Accept(accept)
                {}

                STRICT_STFUNC(StateFunc,
                    hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle)
                )

                void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev) {
                    const auto& response = ev->Get()->Record.GetResponse();

                    TStringStream s;
                    s << "HTTP/1.1 200 OK\r\n"
                        "Content-Type: " << Accept << "\r\n"
                        "\r\n";

                    TString data;

                    if (Accept == "application/x-protobuf-text") {
                        NProtoBuf::TextFormat::Printer p;
                        p.SetSingleLineMode(true);
                        p.PrintToString(response, &data);
                    } else if (Accept == "application/x-protobuf") {
                        const bool success = response.SerializeToString(&data);
                        Y_ABORT_UNLESS(success);
                    } else if (Accept == "application/json") {
                        google::protobuf::util::MessageToJsonString(response, &data);
                    } else {
                        Y_ABORT();
                    }
                    s << data;

                    Send(Sender, new NMon::TEvRemoteBinaryInfoRes(s.Str()));
                    PassAway();
                }
            };

            const TActorId& processorId = Register(new TQueryExecActor(sender, accept));
            TActivationContext::Send(new IEventHandle(SelfId(), processorId, request.release()));
        }
    }
}

bool TBlobStorageController::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) {
    if (!Executor() || !Executor()->GetStats().IsActive) {
        return false;
    }
    if (!ev) {
        return true;
    }
    if (const auto& ext = ev->Get()->ExtendedQuery; ext && ext->GetMethod() == HTTP_METHOD_POST) {
        ProcessPostQuery(*ext, ev->Sender);
        return true;
    }

    THolder<TTransactionBase<TBlobStorageController>> tx;
    TStringStream str;
    const TCgiParameters& cgi(ev->Get()->Cgi());

    if (!cgi.count("page")) {
        RenderMonPage(str);
    } else {
        const TString& page = cgi.Get("page");
        if (page == "SetDown") {
            ui32 groupId = FromStringWithDefault<ui32>(cgi.Get("group"), 0);
            const bool down = FromStringWithDefault<i32>(cgi.Get("down"), 0);
            const bool persist = FromStringWithDefault<i32>(cgi.Get("persist"), 0);
            tx.Reset(new TTxMonEvent_SetDown(ev->Sender, TGroupId::FromValue(groupId), down, persist, this));
        } else if (page == "GetDown") {
            ui32 groupId = FromStringWithDefault<ui32>(cgi.Get("group"), 0);
            tx.Reset(new TTxMonEvent_GetDown(ev->Sender, TGroupId::FromValue(groupId), this));
        } else if (page == "OperationLog") {
            tx.Reset(new TTxMonEvent_OperationLog(ev->Sender, cgi, this));
        } else if (page == "OperationLogEntry") {
            tx.Reset(new TTxMonEvent_OperationLogEntry(ev->Sender, cgi, this));
        } else if (page == "HealthEvents") {
            Execute(new TTxMonEvent_HealthEvents(ev->Sender, cgi, this));
            return true;
        } else if (page == "SelfHeal") {
            bool hiddenAction = cgi.Has("action") && cgi.Get("action") == "disableSelfHeal";
            if (cgi.Has("disable") && cgi.Get("disable") == "1" && hiddenAction) {
                Register(new TDisableSelfHealActor(ev->Sender, TStringBuilder() << "?TabletID="
                    << TabletID() << "&page=SelfHeal"));
            } else {
                TActivationContext::Send(new IEventHandle(SelfHealId, ev->Sender, ev->Release().Release(), 0,
                    SelfHealEnable));
            }
            return true;
        } else if (page == "Groups") {
            const ui64 boxId = FromStringWithDefault<ui64>(cgi.Get("BoxId"), -1);
            const ui64 storagePoolId = FromStringWithDefault<ui64>(cgi.Get("StoragePoolId"), -1);
            RenderGroupsInStoragePool(str, TBoxStoragePoolId(boxId, storagePoolId));
        } else if (page == "GroupDetail") {
            const ui32 groupId = FromStringWithDefault<ui32>(cgi.Get("GroupId"), -1);
            RenderGroupDetail(str, TGroupId::FromValue(groupId));
        } else if (page == "Scrub") {
            ScrubState.Render(str);
        } else if (page == "InternalTables") {
            const TString table = cgi.Has("table") ? cgi.Get("table") : "pdisks";
            RenderInternalTables(str, table);
        } else if (page == "VirtualGroups") {
            RenderVirtualGroups(str);
        } else if (page == "StopGivingGroups") {
            StopGivingGroups = true;
            str << "OK";
        } else if (page == "StartGivingGroups") {
            StopGivingGroups = false;
            str << "OK";
        } else {
            str << "Invalid URL";
        }
    }

    if (tx) {
        Execute(tx.Release());
    } else {
        Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    }

    return true;
}

void TBlobStorageController::RenderResourceValues(IOutputStream& out, const TResourceRawValues& current) {
    RenderBytesCell(out, current.DataSize);
}

void TBlobStorageController::RenderHeader(IOutputStream& out) {
    out << "<html>";
    out << "<script>"
           "$('.container').toggleClass('container container-fluid');"
           "</script>";
    out << "<style>";
    out << ".hide, .hide + label ~ pre { display: none; }";
    out << ".hide + label, .hide:checked + label { padding: 0; user-select: none; "
           " cursor: pointer; color: red; cursor: pointer; border-bottom: 1px solid red; }";
    out << ".hide:checked + label { color: red; border-bottom: 0; }";
    out << ".hide:checked + label + pre { display: block; }";
    out << "</style>";
    out << "<h2>BlobStorage Controller</h2>";
}

void TBlobStorageController::RenderFooter(IOutputStream& out) {
    out << "</html>";
}

void TBlobStorageController::RenderMonPage(IOutputStream& out) {
    RenderHeader(out);

    out << "<a href='app?TabletID=" << TabletID() << "&page=OperationLog'>Operation Log</a><br>";
    out << "<a href='app?TabletID=" << TabletID() << "&page=SelfHeal'>Self Heal Status</a> (" <<
        (SelfHealEnable ? "enabled" : "disabled") << ")<br>";
    out << "<a href='app?TabletID=" << TabletID() << "&page=HealthEvents'>Health events</a><br>";
    out << "<a href='app?TabletID=" << TabletID() << "&page=Scrub'>Scrub state</a><br>";
    out << "<a href='app?TabletID=" << TabletID() << "&page=VirtualGroups'>Virtual groups</a><br>";
    out << "<a href='app?TabletID=" << TabletID() << "&page=InternalTables'>Internal tables</a><br>";

    HTML(out) {
        DIV_CLASS("panel panel-info") {
            DIV_CLASS("panel-heading") {
                out << "Settings";
            }
            DIV_CLASS("panel-body") {
                TABLE_CLASS("table table-condensed") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { out << "Parameter"; }
                            TABLEH() { out << "Value"; }
                        }
                    }
                    TABLEBODY() {
                        TABLER() {
                            TABLED() { out << "Donor Mode"; }
                            TABLED() { out << (DonorMode ? "enabled" : "disabled"); }
                        }
                        TABLER() {
                            TABLED() { out << "Self Heal"; }
                            TABLED() { out << (SelfHealEnable ? "enabled" : "disabled"); }
                        }
                        TABLER() {
                            TABLED() { out << "Serial drive management stage"; }
                            TABLED() { out << SerialManagementStage; }
                        }
                        TABLER() {
                            TABLED() { out << "Default Max Slots"; }
                            TABLED() { out << DefaultMaxSlots; }
                        }
                        TABLER() {
                            TABLED() { out << "PDisk space margin (&#8240;)"; }
                            TABLED() { out << PDiskSpaceMarginPromille; }
                        }
                        TABLER() {
                            TABLED() { out << "PDisk space color border"; }
                            TABLED() { out << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(PDiskSpaceColorBorder); }
                        }
                    }
                }
            }
        }
    }

    RenderFooter(out);
}

void TBlobStorageController::RenderInternalTables(IOutputStream& out, const TString& table) {
    RenderHeader(out);

    auto gen_li = [&](const TString& component) {
        out << "<li" << (component == table ? " class='active'" : "") << ">";
        out << "<a href='app?TabletID=" << TabletID() << "&page=InternalTables&table=" << component << "'>";
        out << component << "</a>";
        out << "</li>";
    };

    out << "<ul class='nav nav-tabs'>";
    gen_li("pdisks");
    gen_li("vdisks");
    gen_li("groups");
    gen_li("boxes");
    gen_li("serials");
    out << "</ul>";

    HTML(out) {
        if (table == "pdisks") {
            TABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TAG_ATTRS(TTableH, {{"title", "NodeId:PDiskId"}}) { out << "Id"; }
                        TABLEH() { out << "Type"; }
                        TABLEH() { out << "Kind"; }
                        TABLEH() { out << "Path"; }
                        TABLEH() { out << "Guid"; }
                        TABLEH() { out << "BoxId"; }
                        TABLEH() { out << "Total Size"; }
                        TABLEH() { out << "Status"; }
                        TABLEH() { out << "State"; }
                        TABLEH() { out << "ExpectedSerial"; }
                        TABLEH() { out << "LastSeenSerial"; }
                        TABLEH() { out << "LastSeenPath"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& [pdiskId, pdisk] : PDisks) {
                        const auto& m = pdisk->Metrics;
                        TAG_ATTRS(TTableR, {{"title", m.DebugString()}}) {
                            TPDiskCategory category(pdisk->Kind);
                            TABLED() { out << pdiskId; }
                            TABLED() { out << category.TypeStrShort(); }
                            TABLED() { out << category.Kind(); }
                            TABLED() { out << pdisk->Path; }
                            TABLED() { out << pdisk->Guid; }
                            TABLED() { out << pdisk->BoxId; }
                            RenderBytesCell(out, m.GetTotalSize());
                            TABLED() { out << NKikimrBlobStorage::EDriveStatus_Name(pdisk->Status); }
                            TABLED() {
                                if (const auto& m = pdisk->Metrics; m.HasState()) {
                                    out << NKikimrBlobStorage::TPDiskState::E_Name(m.GetState());
                                }
                            }
                            TABLED() { out << pdisk->ExpectedSerial.Quote(); }
                            TABLED() {
                                TString color = pdisk->ExpectedSerial == pdisk->LastSeenSerial ? "green" : "red";
                                out << "<font color='" << color << "'>" << pdisk->LastSeenSerial.Quote() << "</font>";
                            }
                            TABLED() { out << pdisk->LastSeenPath; }
                        }
                    }
                }
            }
        } else if (table == "vdisks") {
            RenderVSlotTable(out, [&] {
                for (const auto& [key, value] : VSlots) {
                    RenderVSlotRow(out, *value);
                }
            });
        } else if (table == "groups") {
            RenderGroupTable(out, [&] {
                for (const auto& [key, value] : GroupMap) {
                    RenderGroupRow(out, *value);
                }
            });
        } else if (table == "boxes") {
            TABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TAG_ATTRS(TTableH, {{"colspan", "3"}}) { out << "Box attributes"; }
                        TAG_ATTRS(TTableH, {{"colspan", "7"}}) { out << "Storage pools"; }
                    }
                    TABLER() {
                        TABLEH() { out << "BoxId"; }
                        TABLEH() { out << "Name"; }
                        TABLEH() { out << "Generation"; }

                        TABLEH() { out << "StoragePoolId"; }
                        TABLEH() { out << "Name"; }
                        TABLEH() { out << "ErasureSpecies"; }
                        TABLEH() { out << "VDiskKind"; }
                        TABLEH() { out << "Kind"; }
                        TABLEH() { out << "NumGroups"; }
                        TABLEH() { out << "Detail"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& [boxId, box] : Boxes) {
                        auto renderBoxPart = [&, boxId = boxId, &box = box] {
                            TABLED() { out << boxId; }
                            TABLED() { out << box.Name; }
                            TABLED() { out << box.Generation.GetOrElse(0); }
                        };

                        const TBoxStoragePoolId storagePoolId(boxId, Min<Schema::BoxStoragePool::StoragePoolId::Type>());
                        size_t enlistedStoragePools = 0;
                        for (auto it = StoragePools.lower_bound(storagePoolId); it != StoragePools.end() &&
                                std::get<0>(it->first) == boxId; ++it, ++enlistedStoragePools) {
                            TABLER() {
                                renderBoxPart();
                                TABLED() { out << std::get<1>(it->first); }
                                TABLED() { out << it->second.Name; }
                                TABLED() { out << TBlobStorageGroupType::ErasureSpeciesName(it->second.ErasureSpecies); }
                                TABLED() { out << NKikimrBlobStorage::TVDiskKind::EVDiskKind_Name(it->second.VDiskKind); }
                                TABLED() { out << it->second.Kind; }
                                TABLED() { out << it->second.NumGroups; }
                                TABLED() {
                                    out << "<a href='?TabletID=" << TabletID()
                                        << "&page=Groups&BoxId=" << boxId
                                        << "&StoragePoolId=" << std::get<1>(it->first)
                                        << "'>Detail</a>";
                                }
                            }
                        }
                        if (!enlistedStoragePools) {
                            TABLER() {
                                renderBoxPart();
                                TABLED_ATTRS({{"colspan", "7"}, {"align", "center"}}) {
                                    out << "No storage pools";
                                }
                            }
                        }
                    }
                }
            }
        } else if (table == "serials") {
            TABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Serial"; }
                        TAG_ATTRS(TTableH, {{"title", "NodeId:PDiskId"}}) { out << "PDisk id"; }
                        TABLEH() { out << "BoxId"; }
                        TABLEH() { out << "Guid"; }
                        TABLEH() { out << "LifeStage"; }
                        TABLEH() { out << "Kind"; }
                        TABLEH() { out << "PDiskType"; }
                        TABLEH() { out << "Path"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& [serial, info] : DrivesSerials) {
                        TABLER() {
                            TABLED() { out << serial.Serial.Quote(); }
                            TABLED() { out << "(" << PrintMaybe(info->NodeId) << ":" << PrintMaybe(info->PDiskId) << ")"; }
                            TABLED() { out << info->BoxId; }
                            TABLED() { out << PrintMaybe(info->Guid); }
                            TABLED() { out << info->LifeStage; }
                            TABLED() { out << info->Kind; }
                            TABLED() { out << info->PDiskType; }
                            TABLED() { out << PrintMaybe(info->Path); }
                        }
                    }
                }
            }
        }
    }

    RenderFooter(out);
}

void TBlobStorageController::RenderGroupDetail(IOutputStream &out, TGroupId groupId) {
    RenderHeader(out);
    HTML(out) {
        TAG(TH3) {
            out << "VDisks for group " << groupId;
        }

        if (TGroupInfo *group = FindGroup(groupId)) {
            RenderVSlotTable(out, [&] {
                std::vector<const TVSlotInfo*> donors;
                for (const TVSlotInfo *slot : group->VDisksInGroup) {
                    RenderVSlotRow(out, *slot);
                    for (const TVSlotId& vslotId : slot->Donors) {
                        if (const auto *x = FindVSlot(vslotId)) {
                            donors.push_back(x);
                        }
                    }
                }
                for (const TVSlotInfo *slot : donors) {
                    RenderVSlotRow(out, *slot);
                }
            });
        }
    }
    RenderFooter(out);
}

void TBlobStorageController::RenderGroupsInStoragePool(IOutputStream &out, const TBoxStoragePoolId& id) {
    RenderHeader(out);
    HTML(out) {
        TAG(TH3) {
            TString name;
            if (const auto it = StoragePools.find(id); it != StoragePools.end()) {
                name = it->second.Name;
            }
            out << "Groups in storage pool " << name << " (" << std::get<0>(id) << ", " << std::get<1>(id) << ")";
        }
        RenderGroupTable(out, [&] {
            auto range = StoragePoolGroups.equal_range(id);
            for (auto it = range.first; it != range.second; ++it) {
                if (TGroupInfo *group = FindGroup(it->second)) {
                    RenderGroupRow(out, *group);
                }
            }
        });
    }
    RenderFooter(out);
}

void TBlobStorageController::RenderVSlotTable(IOutputStream& out, std::function<void()> callback) {
    HTML(out) {
        TABLE_CLASS("table") {
            TABLEHEAD() {
                TABLER() {
                    TAG_ATTRS(TTableH, {{"colspan", "10"}}) { out << "VDisk attributes"; }
                    TAG_ATTRS(TTableH, {{"colspan", "1"}}) { out << "Current"; }
                    TAG_ATTRS(TTableH, {{"colspan", "1"}}) { out << "Maximum"; }
                }
                TABLER() {
                    TAG_ATTRS(TTableH, {{"title", "ID:Gen:Ring:Domain:VDisk"}}) { out << "VDisk id"; }
                    TABLEH() { out << "PDisk id"; }
                    TABLEH() { out << "Kind"; }
                    TABLEH() { out << "VSlot id"; }
                    TABLEH() { out << "Allocated"; }
                    TABLEH() { out << "Available"; }
                    TABLEH() { out << "Status"; }
                    TABLEH() { out << "IsReady"; }
                    TABLEH() { out << "LastSeenReady"; }
                    TABLEH() { out << "ReplicationTime"; }
                    TABLEH() { out << "Donors"; }

                    TABLEH() { out << "Data Size"; }

                    TABLEH() { out << "Data Size"; }
                }
            }
            TABLEBODY() {
                callback();
            }
        }
    }
}

void TBlobStorageController::RenderVSlotRow(IOutputStream& out, const TVSlotInfo& vslot) {
    HTML(out) {
        TAG_ATTRS(TTableR, {{"title", vslot.Metrics.DebugString()}, {"id", vslot.GetVDiskId().ToString()}}) {
            TABLED() {
                out << vslot.GetVDiskId();
            }
            TABLED() {
                out << vslot.VSlotId.ComprisingPDiskId();
            }
            TABLED() {
                out << vslot.Kind;
            }
            TABLED() {
                out << vslot.VSlotId;
            }
            RenderBytesCell(out, vslot.Metrics.GetAllocatedSize());
            RenderBytesCell(out, vslot.Metrics.GetAvailableSize());
            TABLED() { out << vslot.GetStatusString(); }
            TABLED() { out << (vslot.IsReady ? "YES" : ""); }
            TABLED() {
                if (vslot.LastSeenReady != TInstant::Zero()) {
                    out << vslot.LastSeenReady;
                }
            }
            TABLED() {
                TDuration time = vslot.ReplicationTime;
                if (vslot.GetStatus() == NKikimrBlobStorage::EVDiskStatus::REPLICATING) {
                    time += TActivationContext::Now() - vslot.LastGotReplicating;
                }
                out << time;
            }
            TABLED() {
                if (vslot.Mood == TMood::Donor) {
                    const auto *x = FindAcceptor(vslot);
                    out << "<strong>donor for <a href='#" << x->GetVDiskId() << "'>" << x->VSlotId << "</a></strong>";
                } else {
                    bool first = true;
                    for (const TVSlotId& donorVSlotId : vslot.Donors) {
                        out << (std::exchange(first, false) ? "" : "<br/>");
                        const TVSlotInfo *donor = FindVSlot(donorVSlotId);
                        const TVDiskID vdiskId = donor->GetVDiskId();
                        out << "<a href='#" << vdiskId.ToString() << "'>" << vdiskId << "</a> at " << donorVSlotId;
                    }
                }
            }
            RenderResourceValues(out, vslot.GetResourceCurrentValues());
            RenderResourceValues(out, vslot.GetResourceMaximumValues());
        }
    }
}

void TBlobStorageController::RenderGroupTable(IOutputStream& out, std::function<void()> callback) {
    HTML(out) {
        TABLE_CLASS("table") {
            TABLEHEAD() {
                TABLER() {
                    TAG_ATTRS(TTableH, {{"title", "GroupId:Gen"}}) { out << "ID"; }
                    TABLEH() { out << "Erasure species"; }
                    TABLEH() { out << "Storage pool"; }
                    TABLEH() { out << "Encryption mode"; }
                    TABLEH() { out << "Life cycle phase"; }
                    TABLEH() { out << "Allocated size"; }
                    TABLEH() { out << "Available size"; }
                    TAG_ATTRS(TTableH, {{"title", "Data Size"}}) { out << "Data<br/>Size"; }
                    TAG_ATTRS(TTableH, {{"title", "PutTabletLog Latency"}}) { out << "PutTabletLog<br/>Latency"; }
                    TAG_ATTRS(TTableH, {{"title", "PutUserData Latency"}}) { out << "PutUserData<br/>Latency"; }
                    TAG_ATTRS(TTableH, {{"title", "GetFast Latency"}}) { out << "GetFast<br/>Latency"; }
                    TABLEH() { out << "Seen operational"; }
                    TABLEH() { out << "Operating<br/>status"; }
                    TABLEH() { out << "Expected<br/>status"; }
                    TABLEH() { out << "Donors"; }
                }
            }
            TABLEBODY() {
                callback();
            }
        }
    }
}

void TBlobStorageController::RenderGroupRow(IOutputStream& out, const TGroupInfo& group) {
    HTML(out) {
        ui64 allocatedSize = 0;
        ui64 availableSize = 0;
        ui32 satisfactionRank = 0;
        for (const TVSlotInfo *vslot : group.VDisksInGroup) {
            const auto& m = vslot->Metrics;
            allocatedSize += m.GetAllocatedSize();
            availableSize += m.GetAvailableSize();
            satisfactionRank = std::max(satisfactionRank, m.GetSatisfactionRank());
        }

        auto renderLatency = [&](const auto& perc) {
            TABLED() {
                if (perc) {
                    out << perc->ToString();
                } else {
                    out << "-";
                }
            }
        };

        TABLER() {
            TString storagePool = "<strong>none</strong>";
            if (auto it = StoragePools.find(group.StoragePoolId); it != StoragePools.end()) {
                storagePool = TStringBuilder() << "<a href='?TabletID=" << TabletID()
                    << "&page=Groups&BoxId=" << std::get<0>(group.StoragePoolId)
                    << "&StoragePoolId=" << std::get<1>(group.StoragePoolId)
                    << "'>" << it->second.Name << "</a>";
            }

            TABLED() {
                out << "<a href='?TabletID=" << TabletID() << "&page=GroupDetail&GroupId=" << group.ID << "'>";
                out << group.ID;
                out << "</a>:" << group.Generation;
            }

            TABLED() { out << TBlobStorageGroupType::ErasureSpeciesName(group.ErasureSpecies); }
            TABLED() { out << storagePool; }
            TABLED() { out << group.EncryptionMode; }
            TABLED() { out << group.LifeCyclePhase; }
            RenderBytesCell(out, allocatedSize);
            RenderBytesCell(out, availableSize);
            RenderResourceValues(out, group.GetResourceCurrentValues());
            renderLatency(group.LatencyStats.PutTabletLog);
            renderLatency(group.LatencyStats.PutUserData);
            renderLatency(group.LatencyStats.GetFast);
            TABLED() { out << (group.SeenOperational ? "YES" : ""); }

            const auto& status = group.Status;
            TABLED() { out << NKikimrBlobStorage::TGroupStatus::E_Name(status.OperatingStatus); }
            TABLED() { out << NKikimrBlobStorage::TGroupStatus::E_Name(status.ExpectedStatus); }
            TABLED() {
                ui32 numDonors = 0;
                for (const auto& vdisk : group.VDisksInGroup) {
                    numDonors += vdisk->Donors.size();
                }
                out << numDonors;
            }
        }
    }
}

} // NBlobStorageController
} // NKikimr
