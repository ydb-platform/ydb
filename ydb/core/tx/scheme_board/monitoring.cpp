#include "backup.h"
#include "mon_events.h"
#include "monitoring.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/tx/tx.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>

#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/list.h>
#include <util/generic/variant.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

// additional html elements
namespace NMonitoring {
    const char NavTag[] = "nav";
    using TNav = TTag<NavTag>;
}

namespace NKikimr {
namespace NSchemeBoard {

using namespace NJson;

struct TBackupLimits {
    ui32 DefaultInFlight = 1'000;
    ui32 MinInFlight = 1;
    ui32 MaxInFlight = 10'000;
};

void Alert(IOutputStream& str, TStringBuf type, TStringBuf text) {
    HTML(str) {
        DIV_CLASS(TStringBuilder() << "alert alert-" << type) {
            if (type == "warning") {
                STRONG() {
                    str << "Warning: ";
                }
            }
            str << text;
        }
    }
}

void Warning(IOutputStream& str, TStringBuf text) {
    Alert(str, "warning", text);
}
void Danger(IOutputStream& str, TStringBuf text) {
    Alert(str, "danger", text);
}
void Info(IOutputStream& str, TStringBuf text) {
    Alert(str, "info", text);
}
void Success(IOutputStream& str, TStringBuf text) {
    Alert(str, "success", text);
}

class TMonitoring: public TActorBootstrapped<TMonitoring> {
    static constexpr char ROOT[] = "scheme_board";
    static constexpr TBackupLimits BackupLimits = TBackupLimits();

    static constexpr TStringBuf LogPrefix() {
        return "monitoring"sv;
    }

    using TActivity = NKikimrServices::TActivity;
    using EActivityType = TActivity::EType;
    using EContentType = NMon::IEvHttpInfoRes::EContentType;

    enum class ERequestType {
        Unknown,
        Index,
        Populator,
        ReplicaPopulator,
        Replica,
        Subscriber,
        SubscriberProxy,
        ReplicaSubscriber,
        Cache,
        Describe,
        Resolver,
        Resolve,
        Backup,
        Restore,
    };

    enum class EAttributeType {
        Unknown,
        Pod,
        String,
        ActorId,
    };

    struct TActorInfo {
        EActivityType ActivityType;
        TJsonMap Attributes;

        TActorInfo(EActivityType activityType, const TJsonMap& attributes)
            : ActivityType(activityType)
            , Attributes(attributes)
        {
        }

        TActorInfo(const TSchemeBoardMonEvents::TEvRegister& ev)
            : TActorInfo(ev.ActivityType, ev.Attributes)
        {
        }
    };

    void Handle(TSchemeBoardMonEvents::TEvRegister::TPtr& ev) {
        const auto& msg = *ev->Get();

        if (RegisteredActors.emplace(ev->Sender, msg).second) {
            ByActivityType[msg.ActivityType].emplace(ev->Sender);
        }
    }

    void Handle(TSchemeBoardMonEvents::TEvUnregister::TPtr& ev) {
        auto registered = RegisteredActors.find(ev->Sender);
        if (registered == RegisteredActors.end()) {
            return;
        }

        auto byActivity = ByActivityType.find(registered->second.ActivityType);
        Y_ABORT_UNLESS(byActivity != ByActivityType.end());

        byActivity->second.erase(ev->Sender);
        if (byActivity->second.empty()) {
            ByActivityType.erase(byActivity);
        }

        RegisteredActors.erase(registered);
    }

    static ERequestType ParseRequestType(const TStringBuf relPath) {
        if (!relPath || relPath == "/") {
            return ERequestType::Index;
        } else if (relPath.StartsWith("/populator")) {
            return ERequestType::Populator;
        } else if (relPath.StartsWith("/replica_populator")) {
            return ERequestType::ReplicaPopulator;
        } else if (relPath.StartsWith("/replica")) {
            return ERequestType::Replica;
        } else if (relPath.StartsWith("/subscriber")) {
            return ERequestType::Subscriber;
        } else if (relPath.StartsWith("/subscriber_proxy")) {
            return ERequestType::SubscriberProxy;
        } else if (relPath.StartsWith("/replica_subscriber")) {
            return ERequestType::ReplicaSubscriber;
        } else if (relPath.StartsWith("/cache")) {
            return ERequestType::Cache;
        } else if (relPath.StartsWith("/describe")) {
            return ERequestType::Describe;
        } else if (relPath.StartsWith("/resolver")) {
            return ERequestType::Resolver;
        } else if (relPath.StartsWith("/resolve")) {
            return ERequestType::Resolve;
        } else if (relPath.StartsWith("/backup")) {
            return ERequestType::Backup;
        } else if (relPath.StartsWith("/restore")) {
            return ERequestType::Restore;
        } else {
            return ERequestType::Unknown;
        }
    }

    static TString MakeLink(ERequestType requestType, const TStringBuf prefix) {
        TStringBuilder str;

        if (prefix) {
            str << prefix << "/";
        }

        switch (requestType) {
        case ERequestType::Index:
            return str << ROOT;
        case ERequestType::Populator:
            return str << "populator";
        case ERequestType::ReplicaPopulator:
            return str << "replica_populator";
        case ERequestType::Replica:
            return str << "replica";
        case ERequestType::Subscriber:
            return str << "subscriber";
        case ERequestType::SubscriberProxy:
            return str << "subscriber_proxy";
        case ERequestType::ReplicaSubscriber:
            return str << "replica_subscriber";
        case ERequestType::Cache:
            return str << "cache";
        case ERequestType::Describe:
            return str << "describe";
        case ERequestType::Resolver:
            return str << "resolver";
        case ERequestType::Resolve:
            return str << "resolve";
        case ERequestType::Backup:
            return str << "backup";
        case ERequestType::Restore:
            return str << "restore";
        case ERequestType::Unknown:
            return str;
        }
    }

    static EAttributeType ParseAttributeType(const TJsonValue& value) {
        switch (value.GetType()) {
        case JSON_BOOLEAN:
        case JSON_INTEGER:
        case JSON_DOUBLE:
        case JSON_UINTEGER:
            return EAttributeType::Pod;

        case JSON_STRING:
            return EAttributeType::String;

        case JSON_MAP:
        {
            const auto* type = value.GetMapSafe().FindPtr("@type");

            if (!type || type->GetType() != JSON_STRING) {
                return EAttributeType::Unknown;
            }

            if (type->GetStringSafe() == "ACTOR_ID") {
                return EAttributeType::ActorId;
            }
            // can not determine map type, fallback to unknown
            [[fallthrough]];
        }

        default:
            return EAttributeType::Unknown;
        }
    }

    template <typename P, typename D>
    static bool TryGetNext(TStringBuf& s, D delim, P& param) {
        TMaybe<TStringBuf> buf;
        GetNext(s, delim, buf);
        if (!buf) {
            return false;
        }

        return TryFromString(*buf, param);
    }

    static TActorId ParseActorId(TStringBuf str) {
        ui64 x1;
        ui64 x2;

        if (!TryGetNext(str, ':', x1) || !TryGetNext(str, ':', x2)) {
            return {};
        }

        return TActorId(x1, x2);
    }

    static TActorId ParseActorIdFromPath(TStringBuf relPath) {
        if (relPath.EndsWith('/')) {
            relPath.Chop(1);
        }

        auto lastPart = relPath.RNextTok('/');
        if (!lastPart) {
            return {};
        }

        return ParseActorId(lastPart);
    }

    static TActorId ParseActorId(const TJsonValue& value) {
        if (value.GetType() != JSON_STRING) {
            return {};
        }

        return ParseActorId(TStringBuf(value.GetStringSafe()));
    }

    static EActivityType ParseActivityType(const TJsonValue& value) {
        if (value.GetType() != JSON_STRING) {
            return TActivity::OTHER;
        }

        EActivityType result;
        if (!TActivity::EType_Parse(value.GetStringSafe(), &result)) {
            return TActivity::OTHER;
        }

        return result;
    }

    static std::pair<EActivityType, TActorId> ParseActorId(const TJsonValue::TMapType& map) {
        const auto* activityType = map.FindPtr("ActivityType");
        const auto* actorId = map.FindPtr("ActorId");

        if (!activityType || !actorId) {
            return {};
        }

        return std::make_pair(ParseActivityType(*activityType), ParseActorId(*actorId));
    }

    static TPathId PathIdFromProto(const NKikimrSchemeBoardMon::TPathId& proto) {
        return TPathId(proto.GetOwnerId(), proto.GetLocalPathId());
    }

    template <typename T>
    static TString GetPath(const T& proto) {
        if (proto.HasPath()) {
            return proto.GetPath();
        } else if (proto.HasPathId()) {
            return ToString(PathIdFromProto(proto.GetPathId()));
        }

        return {};
    }

    const TJsonValue::TMapType& GetAttrs(const TActorId& actorId) const {
        auto it = RegisteredActors.find(actorId);
        Y_ABORT_UNLESS(it != RegisteredActors.end());

        return it->second.Attributes.GetMapSafe();
    }

    using TRenderer = std::function<void(IOutputStream&)>;

    template <typename T, typename U>
    static void Header(IOutputStream& str, const T& title, const U& subTitile) {
        HTML(str) {
            DIV_CLASS("page-header") {
                TAG(TH3) {
                    str << title;
                    if (subTitile) {
                        SMALL() { str << " " << subTitile; }
                    }
                }
            }
        }
    }

    template <>
    void Header(IOutputStream& str, const TString& activityType, const NActorsProto::TActorId& actorId) {
        Header(str, activityType, ActorIdFromProto(actorId));
    }

    static void Panel(IOutputStream& str, TRenderer title, TRenderer body) {
        HTML(str) {
            DIV_CLASS("panel panel-default") {
                DIV_CLASS("panel-heading") {
                    H4_CLASS("panel-title") {
                        title(str);
                    }
                }
                body(str);
            }
        }
    }

    static void SimplePanel(IOutputStream& str, const TStringBuf title, TRenderer body) {
        auto titleRenderer = [&title](IOutputStream& str) {
            HTML(str) {
                str << title;
            }
        };

        auto bodyRenderer = [body = std::move(body)](IOutputStream& str) {
            HTML(str) {
                DIV_CLASS("panel-body") {
                    body(str);
                }
            }
        };

        Panel(str, titleRenderer, bodyRenderer);
    }

    static void CollapsedPanel(IOutputStream& str, const TStringBuf title, const TStringBuf targetId, TRenderer body) {
        auto titleRenderer = [&title, &targetId](IOutputStream& str) {
            HTML(str) {
                str << "<a data-toggle='collapse' href='#" << targetId << "'>"
                    << title
                << "</a>";
            }
        };

        auto bodyRenderer = [&targetId, body = std::move(body)](IOutputStream& str) {
            HTML(str) {
                str << "<div id='" << targetId << "' class='collapse'>";
                DIV_CLASS("panel-body") {
                    body(str);
                }
                str << "</div>";
            }
        };

        Panel(str, titleRenderer, bodyRenderer);
    }

    static ERequestType ConvertActivityType(EActivityType activityType) {
        static THashMap<EActivityType, ERequestType> activityToRequest = {
            {TActivity::SCHEME_BOARD_POPULATOR_ACTOR, ERequestType::Populator},
            {TActivity::SCHEME_BOARD_REPLICA_POPULATOR_ACTOR, ERequestType::ReplicaPopulator},
            {TActivity::SCHEME_BOARD_REPLICA_ACTOR, ERequestType::Replica},
            {TActivity::SCHEME_BOARD_SUBSCRIBER_ACTOR, ERequestType::Subscriber},
            {TActivity::SCHEME_BOARD_SUBSCRIBER_PROXY_ACTOR, ERequestType::SubscriberProxy},
            {TActivity::SCHEME_BOARD_REPLICA_SUBSCRIBER_ACTOR, ERequestType::ReplicaSubscriber},
            {TActivity::PROXY_SCHEME_CACHE, ERequestType::Cache},
        };

        return activityToRequest.Value(activityType, ERequestType::Unknown);
    }

    static TString ActorIdToStringSafe(const TActorId& actorId) {
        return TStringBuilder() << actorId.RawX1() << ":" << actorId.RawX2();
    }

    template <typename T>
    static void Link(IOutputStream& str, const TStringBuf path, const T& title) {
        HTML(str) {
            HREF(path) {
                str << title;
            }
        }
    }

    static void Link(IOutputStream& str, ERequestType requestType, const TStringBuf title, const TStringBuf prefix = "..") {
        Link(str, MakeLink(requestType, prefix), title);
    }

    static void Link(IOutputStream& str, EActivityType activityType, const TActorId& actorId, const TStringBuf prefix = "..") {
        const TString path = TStringBuilder()
            << MakeLink(ConvertActivityType(activityType), prefix)
            << "/" << ActorIdToStringSafe(actorId);
        Link(str, path, actorId);
    }

    static void Link(IOutputStream& str, EActivityType activityType, const NActorsProto::TActorId& actorId, const TStringBuf prefix = "..") {
        Link(str, activityType, ActorIdFromProto(actorId), prefix);
    }

    template <typename T>
    static void TermDesc(IOutputStream& str, const TStringBuf term, const T& desc) {
        HTML(str) {
            DT() { str << term; }
            DD() { str << desc; }
        }
    }

    template <typename T>
    static void TermDescLink(IOutputStream& str, const TStringBuf term, EActivityType activityType, const T& actorId) {
        HTML(str) {
            DT() { str << term; }
            DD() { Link(str, activityType, actorId); }
        }
    }

    enum EFormType : ui8 {
        ByPath = 1 << 0,
        ByPathId = 1 << 1,
        Both = ByPath | ByPathId,
    };

    static void Form(IOutputStream& str, EFormType formType, ERequestType linkType, const TStringBuf linkPrefix, const TActorId& actorId) {
        HTML(str) {
            FORM_CLASS("form-horizontal") {
                const auto action = MakeLink(linkType, linkPrefix);

                if (formType & EFormType::ByPath) {
                    DIV_CLASS("form-group") {
                        LABEL_CLASS_FOR("col-sm-2 control-label", "path") {
                            str << "Path";
                        }
                        DIV_CLASS("col-sm-8") {
                            str << "<input type='text' id='path' name='path' class='form-control' placeholder='/full/path'>";
                        }
                        DIV_CLASS("col-sm-2") {
                            str << "<button type='submit' name='byPath' formaction='" << action << "' class='btn btn-primary'>"
                                << "Find by path"
                            << "</button>";
                        }
                    }
                }

                if (formType & EFormType::ByPathId) {
                    DIV_CLASS("form-group") {
                        LABEL_CLASS_FOR("col-sm-2 control-label", "pathId") {
                            str << "PathId";
                        }
                        DIV_CLASS("col-sm-4") {
                            str << "<input type='number' id='ownerId' name='ownerId' class='form-control' placeholder='owner id'>";
                        }
                        DIV_CLASS("col-sm-4") {
                            str << "<input type='number' id='pathId' name='pathId' class='form-control' placeholder='local path id'>";
                        }
                        DIV_CLASS("col-sm-2") {
                            str << "<button type='submit' name='byPathId' formaction='" << action << "' class='btn btn-info'>"
                                << "Find by path id"
                            << "</button>";
                        }
                    }
                }

                if (actorId) {
                    str << "<input type='hidden' name='actorId' value='" << ActorIdToStringSafe(actorId) << "'>";
                }

                str << "<pre id='description' class='hidden'/>";

                str << R"(<script>
                $(document).ready(function() {
                    $('button').click(function(e) {
                        e.preventDefault();

                        var btn = this;
                        var form = $('form');

                        $.ajax({
                            type: "GET",
                            url: btn.formAction,
                            data: Object.assign({[btn.name]: 1}, form.serializeArray().reduce(function(obj, cur, _) {
                                obj[cur.name] = cur.value;
                                return obj;
                            }, {})),
                            success: function(data) {
                                $('#description').text(JSON.stringify(data, null, ' ')).removeClass('hidden');
                            },
                            error: function (data) {
                                $('#description').text("Error: " + data).removeClass('hidden');
                            },
                        });
                    });
                });
                </script>)";
            }
        }
    }

    static void ResolveForm(IOutputStream& str) {
        Form(str, EFormType::Both, ERequestType::Resolve, "", {});
    }

    static void DescribeForm(IOutputStream& str, const TActorId& actorId, EFormType type = EFormType::Both) {
        Form(str, type, ERequestType::Describe, "..", actorId);
    }

    static void Navbar(IOutputStream& str, ERequestType originRequestType) {
        static const TVector<std::pair<ERequestType, TStringBuf>> requestTypeToTitle = {
            {ERequestType::Index, "Main"},
            {ERequestType::Resolver, "Resolver"},
            {ERequestType::Backup, "Backup"},
            {ERequestType::Restore, "Restore"},
        };

        const bool isIndex = originRequestType == ERequestType::Index;

        HTML(str) {
            str << "<style>"
                << ".backup-tab, .restore-tab { color: red !important; }"
                << ".backup-tab:hover, .restore-tab:hover { background-color: red !important; color: white !important; }"
                << ".nav-pills > li.active > a.backup-tab, .nav-pills > li.active > a.restore-tab { background-color: red !important; color: white !important; }"
                << ".nav-pills > li.active > a.backup-tab:hover, .nav-pills > li.active > a.restore-tab:hover { background-color: darkred !important; color: white !important; }"
                << "</style>";

            TAG_CLASS(TNav, "navbar") {
                UL_CLASS("nav nav-pills") {
                    for (const auto& [rt, title] : requestTypeToTitle) {
                        const TStringBuf linkPrefix = isIndex
                            ? ROOT
                            : (rt == ERequestType::Index ? ".." : "");

                        TString cssClass;
                        if (rt == ERequestType::Backup || rt == ERequestType::Restore) {
                            cssClass = TStringBuilder() << (rt == ERequestType::Backup ? "backup-tab" : "restore-tab");
                        }

                        if (rt == originRequestType) {
                            LI_CLASS("active") {
                                if (!cssClass.empty()) {
                                    str << "<a href='#' class='" << cssClass << "'>" << title << "</a>";
                                } else {
                                    Link(str, "#", title);
                                }
                            }
                        } else {
                            LI() {
                                if (!cssClass.empty()) {
                                    str << "<a href='" << MakeLink(rt, linkPrefix)
                                        << "' class='" << cssClass << "'>" << title << "</a>";
                                } else {
                                    Link(str, rt, title, linkPrefix);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    TString RenderIndex() const {
        TStringStream str;

        HTML(str) {
            Navbar(str, ERequestType::Index);

            for (const auto& kv : ByActivityType) {
                const auto& activityType = kv.first;
                const auto& actorIds = kv.second;
                const auto activityTypeStr = TActivity::EType_Name(activityType);

                CollapsedPanel(str, activityTypeStr, activityTypeStr, [&](IOutputStream& str) {
                    HTML(str) {
                        TABLE_CLASS("table table-hover") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "#"; }
                                    TABLEH() { str << "Actor"; }

                                    Y_ABORT_UNLESS(!actorIds.empty());
                                    for (const auto& [key, _] : GetAttrs(*actorIds.begin())) {
                                        TABLEH() { str << key; }
                                    }
                                }
                            }
                            TABLEBODY() {
                                int i = 1;
                                for (const auto& actorId : actorIds) {
                                    TABLER() {
                                        TABLED() { str << i++; }
                                        TABLED() { Link(str, activityType, actorId, ROOT); }

                                        for (const auto& [_, value] : GetAttrs(actorId)) {
                                            switch (ParseAttributeType(value)) {
                                            case EAttributeType::Pod:
                                                TABLED() { str << value; }
                                                break;

                                            case EAttributeType::String:
                                                TABLED() { str << value.GetStringSafe(); }
                                                break;

                                            case EAttributeType::ActorId: {
                                                const auto kv = ParseActorId(value.GetMapSafe());
                                                TABLED() { Link(str, kv.first, kv.second, ROOT); }
                                                break;
                                            }

                                            case EAttributeType::Unknown:
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }

        return str.Str();
    }

    static TString RenderResolver(const TList<TActorId>& replicas) {
        TStringStream str;

        HTML(str) {
            Navbar(str, ERequestType::Resolver);
            Header(str, "Replica resolver", "");

            SimplePanel(str, "Resolver", [](IOutputStream& str) {
                ResolveForm(str);
            });

            CollapsedPanel(str, "All replicas", "allReplicas", [&replicas](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Actor"; }
                            }
                        }
                        TABLEBODY() {
                            ui32 i = 0;
                            for (const auto& replica : replicas) {
                                TABLER() {
                                    TABLED() { str << ++i; }
                                    TABLED() { Link(str, TActivity::SCHEME_BOARD_REPLICA_ACTOR, replica, ""); }
                                }
                            }
                        }
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderReplica(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaResponse);
        const auto& response = record.GetReplicaResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());
            if (record.GetTruncated()) {
                Warning(str, "some lists have been truncated.");
            }

            SimplePanel(str, "Descriptions", [&record](IOutputStream& str) {
                const auto& response = record.GetReplicaResponse();

                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(str, "TotalCount", response.GetDescriptions().GetTotalCount());
                        TermDesc(str, "ByPathCount", response.GetDescriptions().GetByPathCount());
                        TermDesc(str, "ByPathIdCount", response.GetDescriptions().GetByPathIdCount());
                    }

                    DescribeForm(str, ActorIdFromProto(record.GetSelf()));
                }
            });

            CollapsedPanel(str, "Populators", "populators", [&response](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Actor"; }
                                TABLEH() { str << "Owner"; }
                                TABLEH() { str << "Generation"; }
                                TABLEH() { str << "PendingGeneration"; }
                            }
                        }
                        TABLEBODY() {
                            for (ui32 i = 0; i < response.PopulatorsSize(); ++i) {
                                const auto& populator = response.GetPopulators(i);

                                TABLER() {
                                    TABLED() { str << (i + 1); }
                                    TABLED() { Link(str, TActivity::SCHEME_BOARD_REPLICA_POPULATOR_ACTOR, populator.GetActorId()); }
                                    TABLED() { str << populator.GetOwner(); }
                                    TABLED() { str << populator.GetGeneration(); }
                                    TABLED() { str << populator.GetPendingGeneration(); }
                                }
                            }
                        }
                    }
                }
            });

            CollapsedPanel(str, "Subscribers", "subscribers", [&response](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Actor"; }
                                TABLEH() { str << "Path"; }
                            }
                        }
                        TABLEBODY() {
                            for (ui32 i = 0; i < response.SubscribersSize(); ++i) {
                                const auto& subscriber = response.GetSubscribers(i);

                                TABLER() {
                                    TABLED() { str << (i + 1); }
                                    TABLED() { Link(str, TActivity::SCHEME_BOARD_REPLICA_SUBSCRIBER_ACTOR, subscriber.GetActorId()); }
                                    TABLED() { str << GetPath(subscriber); }
                                }
                            }
                        }
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderPopulator(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kPopulatorResponse);
        const auto& response = record.GetPopulatorResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());
            if (record.GetTruncated()) {
                Warning(str, "some lists have been truncated.");
            }

            SimplePanel(str, "Info", [&record](IOutputStream& str) {
                const auto& response = record.GetPopulatorResponse();

                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(str, "Owner", response.GetOwner());
                        TermDesc(str, "Generation", response.GetGeneration());
                        TermDesc(str, "DescriptionsCount", response.GetDescriptionsCount());
                        TermDesc(str, "MaxPathId", PathIdFromProto(response.GetMaxPathId()));
                        TermDesc(str, "DelayedUpdatesCount", response.GetDelayedUpdatesCount());
                    }

                    DescribeForm(str, ActorIdFromProto(record.GetSelf()), EFormType::ByPathId);
                }
            });

            CollapsedPanel(str, "ReplicaPopulators", "replicaPopulators", [&response](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Actor"; }
                            }
                        }
                        TABLEBODY() {
                            for (ui32 i = 0; i < response.ReplicaPopulatorsSize(); ++i) {
                                const auto& replicaPopulator = response.GetReplicaPopulators(i);

                                TABLER() {
                                    TABLED() { str << (i + 1); }
                                    TABLED() { Link(str, TActivity::SCHEME_BOARD_REPLICA_POPULATOR_ACTOR, replicaPopulator); }
                                }
                            }
                        }
                    }
                }
            });

            CollapsedPanel(str, "UpdateAcks", "updateAcks", [&response](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Cookie"; }
                                TABLEH() { str << "AckTo"; }
                                TABLEH() { str << "PathId / Version / AcksCount"; }
                            }
                        }
                        TABLEBODY() {
                            for (ui32 i = 0; i < response.UpdateAcksSize(); ++i) {
                                const auto& updateAck = response.GetUpdateAcks(i);

                                TABLER() {
                                    TABLED() { str << (i + 1); }
                                    TABLED() { str << updateAck.GetCookie(); }
                                    TABLED() { str << ActorIdFromProto(updateAck.GetAckTo()); }
                                    TABLED() {
                                        TABLE_CLASS("table table-condensed") {
                                            TABLEBODY() {
                                                for (ui32 i = 0; i < updateAck.PathAcksSize(); ++i) {
                                                    const auto& pathAck = updateAck.GetPathAcks(i);

                                                    TABLER() {
                                                        TABLED() { str << PathIdFromProto(pathAck.GetPathId()); }
                                                        TABLED() { str << pathAck.GetVersion(); }
                                                        TABLED() { str << pathAck.GetAcksCount(); }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderReplicaPopulator(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaPopulatorResponse);
        const auto& response = record.GetReplicaPopulatorResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());
            if (record.GetTruncated()) {
                Warning(str, "some lists have been truncated.");
            }

            SimplePanel(str, "Info", [&response](IOutputStream& str) {
                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDescLink(str, "Parent", TActivity::SCHEME_BOARD_POPULATOR_ACTOR, response.GetParent());
                        TermDescLink(str, "Replica", TActivity::SCHEME_BOARD_REPLICA_ACTOR, response.GetReplica());
                        TermDesc(str, "Owner", response.GetOwner());
                        TermDesc(str, "Generation", response.GetGeneration());
                        TermDesc(str, "CurPathId", PathIdFromProto(response.GetCurPathId()));
                        TermDesc(str, "LastAckedPathId", PathIdFromProto(response.GetLastAckedPathId()));
                        TermDesc(str, "BatchSize", response.GetBatchSize());
                        TermDesc(str, "BatchSizeLimit", response.GetBatchSizeLimit());
                    }
                }
            });

            auto renderUpdates = [&str](const TStringBuf title, const TStringBuf targetId, const auto& updates) {
                CollapsedPanel(str, title, targetId, [&updates](IOutputStream& str) {
                    HTML(str) {
                        TABLE_CLASS("table table-hover") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "#"; }
                                    TABLEH() { str << "PathId"; }
                                    TABLEH() { str << "Version / TxIds"; }
                                }
                            }
                            TABLEBODY() {
                                for (int i = 0; i < updates.size(); ++i) {
                                    const auto& update = updates[i];

                                    TABLER() {
                                        TABLED() { str << (i + 1); }
                                        TABLED() { str << PathIdFromProto(update.GetPathId()); }
                                        TABLED() {
                                            TABLE_CLASS("table table-condensed") {
                                                TABLEBODY() {
                                                    for (ui32 i = 0; i < update.VersionsSize(); ++i) {
                                                        const auto& version = update.GetVersions(i);

                                                        TABLER() {
                                                            TABLED() { str << version.GetVersion(); }
                                                            TABLED() { str << JoinSeq(", ", version.GetTxIds()); }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            };

            renderUpdates("Updates", "updates", response.GetUpdates());
            renderUpdates("UpdatesRequested", "updatesRequested", response.GetUpdatesRequested());
            renderUpdates("UpdatesInFlight", "updatesInFlight", response.GetUpdatesInFlight());
        }

        return str.Str();
    }

    static TString RenderSubscriber(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kSubscriberResponse);
        const auto& response = record.GetSubscriberResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());

            SimplePanel(str, "Info", [&response](IOutputStream& str) {
                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(str, "Owner", ActorIdFromProto(response.GetOwner()));
                        TermDesc(str, "DomainOwnerId", response.GetDomainOwnerId());
                        TermDesc(str, "DelayedSyncRequest", response.GetDelayedSyncRequest());
                        TermDesc(str, "CurrentSyncRequest", response.GetCurrentSyncRequest());
                        TermDesc(str, "Path", GetPath(response));
                    }
                }
            });

            CollapsedPanel(str, "State", "state", [&response](IOutputStream& str) {
                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(str, "Deleted", response.GetState().GetDeleted());
                        TermDesc(str, "Strong", response.GetState().GetStrong());
                        TermDesc(str, "PathId", PathIdFromProto(response.GetState().GetPathId()));
                        TermDesc(str, "Version", response.GetState().GetVersion());
                        TermDesc(str, "DomainId", PathIdFromProto(response.GetState().GetDomainId()));
                        TermDesc(str, "AbandonedSchemeShards", JoinSeq(", ", response.GetState().GetAbandonedSchemeShards()));
                    }
                }
            });

            CollapsedPanel(str, "ProxyStates", "proxyStates", [&response](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Actor"; }
                                TABLEH() { str << "Deleted"; }
                                TABLEH() { str << "Strong"; }
                                TABLEH() { str << "PathId"; }
                                TABLEH() { str << "Version"; }
                                TABLEH() { str << "DomainId"; }
                                TABLEH() { str << "AbandonedSchemeShards"; }
                            }
                        }
                        TABLEBODY() {
                            for (ui32 i = 0; i < response.ProxyStatesSize(); ++i) {
                                const auto& proxy = response.GetProxyStates(i);
                                const auto& state = proxy.GetState();

                                TABLER() {
                                    TABLED() { str << (i + 1); }
                                    TABLED() { Link(str, TActivity::SCHEME_BOARD_SUBSCRIBER_PROXY_ACTOR, proxy.GetProxy()); }
                                    TABLED() { str << state.GetDeleted(); }
                                    TABLED() { str << state.GetStrong(); }
                                    TABLED() { str << PathIdFromProto(state.GetPathId()); }
                                    TABLED() { str << state.GetVersion(); }
                                    TABLED() { str << PathIdFromProto(state.GetDomainId()); }
                                    TABLED() { str << JoinSeq(", ", state.GetAbandonedSchemeShards()); }
                                }
                            }
                        }
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderSubscriberProxy(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kSubscriberProxyResponse);
        const auto& response = record.GetSubscriberProxyResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());

            SimplePanel(str, "Info", [&response](IOutputStream& str) {
                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDescLink(str, "Parent", TActivity::SCHEME_BOARD_SUBSCRIBER_ACTOR, response.GetParent());
                        TermDescLink(str, "Replica", TActivity::SCHEME_BOARD_REPLICA_ACTOR, response.GetReplica());
                        TermDescLink(str, "ReplicaSubscriber", TActivity::SCHEME_BOARD_REPLICA_SUBSCRIBER_ACTOR, response.GetReplicaSubscriber());
                        TermDesc(str, "DomainOwnerId", response.GetDomainOwnerId());
                        TermDesc(str, "CurrentSyncRequest", response.GetCurrentSyncRequest());
                        TermDesc(str, "Path", GetPath(response));
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderReplicaSubscriber(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaSubscriberResponse);
        const auto& response = record.GetReplicaSubscriberResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());

            SimplePanel(str, "Info", [&response](IOutputStream& str) {
                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDescLink(str, "Parent", TActivity::SCHEME_BOARD_SUBSCRIBER_PROXY_ACTOR, response.GetParent());
                        TermDescLink(str, "Replica", TActivity::SCHEME_BOARD_REPLICA_ACTOR, response.GetReplica());
                        TermDesc(str, "DomainOwnerId", response.GetDomainOwnerId());
                        TermDesc(str, "CurrentSyncRequest", response.GetCurrentSyncRequest());
                        TermDesc(str, "Path", GetPath(response));
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderCache(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kCacheResponse);

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());

            SimplePanel(str, "Descriptions", [&record](IOutputStream& str) {
                const auto& response = record.GetCacheResponse();

                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(str, "TotalCount", response.GetItemsTotalCount());
                        TermDesc(str, "ByPathCount", response.GetItemsByPathCount());
                        TermDesc(str, "ByPathIdCount", response.GetItemsByPathIdCount());
                    }

                    DescribeForm(str, ActorIdFromProto(record.GetSelf()));
                }
            });
        }

        return str.Str();
    }

    TString RenderBackup(bool backupStarted, const TString& error = "") {
        TStringStream str;

        HTML(str) {
            Navbar(str, ERequestType::Backup);
            Header(str, "Backup", "Backup path descriptions locally, see <a href='https://ydb.tech/docs' target='_blank'>docs</a>");

            if (backupStarted) {
                Info(str, "Backup started successfully!");
            }
            if (!error.empty()) {
                Danger(str, error);
            }

            DIV_CLASS_ID("alert alert-warning", "backupWarning") {
                if (!BackupProgress.Warning.empty()) {
                    STRONG() { str << "Warning:"; }
                    str << " failed to backup paths:";
                    PRE() { str << BackupProgress.Warning; }
                } else {
                    str << "<script>$('#backupWarning').hide();</script>";
                }
            }

            SimplePanel(str, "Backup Configuration", [&backupProgress = BackupProgress](IOutputStream& str) {
                HTML(str) {
                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "backupPath") {
                                str << "File Path";
                            }
                            DIV_CLASS("col-sm-10") {
                                str << "<input type='text' id='backupPath' name='backupPath' class='form-control' "
                                    << "placeholder='/tmp/scheme_board_backup.jsonl' required>";
                            }
                        }

                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "inFlightLimit") {
                                str << "In-Flight Limit";
                            }
                            DIV_CLASS("col-sm-10") {
                                str << "<input type='number' id='inFlightLimit' name='inFlightLimit' class='form-control' "
                                    << "value='" << BackupLimits.DefaultInFlight << "'>";
                                str << "<small class='form-text text-muted'>Recommended range: "
                                    << BackupLimits.MinInFlight << " - " << BackupLimits.MaxInFlight
                                    << ". Values outside this range may cause performance issues.</small>";
                            }
                        }

                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "requireMajority") {
                                str << "Replica Selection";
                            }
                            DIV_CLASS("col-sm-10") {
                                DIV_CLASS("checkbox") {
                                    LABEL_CLASS_FOR("", "requireMajority") {
                                        str << "<input type='checkbox' id='requireMajority' name='requireMajority' value='true' checked> "
                                            << "Require majority of replicas";
                                    }
                                }
                                str << "<small class='form-text text-muted'>"
                                    "If checked: backup will query enough replicas to form a majority and select the newest version available.<br>"
                                    "If unchecked: backup will query just one randomly chosen replica, which can be faster but less reliable."
                                    "</small>";
                            }
                        }

                        DIV_CLASS("form-group") {
                            DIV_CLASS("col-sm-offset-2 col-sm-10") {
                                const char* state = backupProgress.IsRunning() ? "disabled" : "";
                                str << "<button type='submit' name='startBackup' class='btn btn-primary' " << state << ">"
                                    << "Start Backup"
                                << "</button>";
                            }
                        }
                    }

                    DIV_CLASS_ID("alert alert-info", "backupStatus") {
                        str << "Status: " << backupProgress.StatusToString();
                    }

                    double p = backupProgress.GetProgress();
                    DIV_CLASS_ID("progress", "backupProgress") {
                        TAG_CLASS_STYLE(TDiv, "progress-bar", TStringBuilder() << "width:" << p << "%;") {
                            str << p << "%";
                        }
                    }

                    TAG_ATTRS(TDiv, {{"id", "backupDetails"}}) {
                        str << "Processed: " << backupProgress.ProcessedPaths
                            << " / Total: " << backupProgress.TotalPaths;
                    }

                    str << R"(
                    <script>
                    $(document).ready(function() {
                        $('button[name="startBackup"]').click(function(e) {
                            e.preventDefault();

                            var btn = this;
                            var form = $(btn.form);

                            var inFlightLimit = parseInt($('#inFlightLimit').val());
                            var min = )" << BackupLimits.MinInFlight << R"(;
                            var max = )" << BackupLimits.MaxInFlight << R"(;
                            if (inFlightLimit < min || inFlightLimit > max) {
                                var msg = 'Warning: in-flight limit (' + inFlightLimit +
                                    ') is outside the recommended range (' + min + ' - ' + max + '). Proceed?';
                                if (!confirm(msg))
                                    return;
                            }

                            $.ajax({
                                type: "GET",
                                url: window.location.pathname,
                                data: form.serialize() + '&startBackup=1',
                                success: function(response) {
                                    $('body').html(response);
                                    if (window.history && window.history.replaceState) {
                                        window.history.replaceState(null, '', window.location.pathname);
                                    }
                                },
                                error: function(xhr, status, error) {
                                    $(btn).prop('disabled', false);
                                    alert('Failed to start backup: ' + error);
                                }
                            });
                        });

                        function updateBackupProgress() {
                            $.ajax({
                                url: window.location.pathname + '?backupProgress=1',
                                dataType: 'json',
                                success: function(data) {
                                    var status = data.status.toLowerCase();
                                    var progress = data.progress;
                                    var processed = data.processed;
                                    var total = data.total;
                                    var warning = data.warning;

                                    if (status === 'running' || status === 'starting') {
                                        $('#backupProgress .progress-bar').css('width', progress + '%')
                                            .text(progress.toFixed(1) + '%');
                                        $('#backupStatus').text('Status: ' + status);
                                        $('#backupDetails').text('Processed: ' + processed + ' / Total: ' + total);
                                        if (warning && warning.length > 0) {
                                            $('#backupWarning')
                                                .show()
                                                .html('<strong>Warning:</strong><pre>' + warning + '</pre>');
                                        } else {
                                            $('#backupWarning').hide().empty();
                                        }
                                        setTimeout(updateBackupProgress, 1000);
                                    } else if (status === 'completed') {
                                        $('#backupProgress .progress-bar').css('width', '100%')
                                            .text('100%');
                                        $('#backupStatus').text('Status: backup completed successfully')
                                            .removeClass('alert-info alert-danger').addClass('alert-success');
                                        $('#backupDetails').text('Processed: ' + processed + ' / Total: ' + total);
                                        $('button[name="startBackup"]').prop('disabled', false);
                                        if (warning && warning.length > 0) {
                                            $('#backupWarning')
                                                .show()
                                                .html('<strong>Warning:</strong><pre>' + warning + '</pre>');
                                        } else {
                                            $('#backupWarning').hide().empty();
                                        }
                                    } else if (status.startsWith('error:')) {
                                        $('#backupStatus').text('Status: ' + status)
                                            .removeClass('alert-info alert-success').addClass('alert-danger');
                                        $('button[name="startBackup"]').prop('disabled', false);
                                    } else {
                                        $('button[name="startBackup"]').prop('disabled', false);
                                    }
                                },
                                error: function() {
                                    setTimeout(updateBackupProgress, 1000);
                                }
                            });
                        }

                        )" << (backupProgress.IsRunning() ? "updateBackupProgress();" : "") << R"(
                    });
                    </script>
                    )";
                }
            });
        }

        return str.Str();
    }

    TString RenderRestore(bool restoreStarted, const TString& error = "") {
        TStringStream str;

        HTML(str) {
            Navbar(str, ERequestType::Restore);
            Header(str, "Restore", "Restore path descriptions saved locally, see <a href='https://ydb.tech/docs' target='_blank'>docs</a>");

            Warning(str,
                "Emergency restore will override existing scheme board data. "
                "Use only when the Scheme Shard is unavailable."
            );

            if (restoreStarted) {
                Info(str, "Restore started successfully!");
            }
            if (!error.empty()) {
                Danger(str, error);
            }

            SimplePanel(str, "Restore Configuration", [&restoreProgress = RestoreProgress](IOutputStream& str) {
                HTML(str) {
                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "restorePath") {
                                str << "Backup File Path";
                            }
                            DIV_CLASS("col-sm-10") {
                                str << "<input type='text' id='restorePath' name='restorePath' class='form-control' "
                                    << "placeholder='/tmp/scheme_board_backup.jsonl' required>";
                            }
                        }

                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "schemeShardId") {
                                str << "Scheme Shard Tablet ID";
                            }
                            DIV_CLASS("col-sm-10") {
                                str << "<input type='number' id='schemeShardId' name='schemeShardId' class='form-control' "
                                    << "placeholder='" << TTestTxConfig::SchemeShard << "' required>";
                                str << "<small class='form-text text-muted'>"
                                    << "Only paths owned by this specific SchemeShard will be restored from the backup file"
                                    << "</small>";
                            }
                        }

                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "generation") {
                                str << "Generation";
                            }
                            DIV_CLASS("col-sm-10") {
                                str << "<input type='number' id='generation' name='generation' class='form-control' "
                                    << "value='1' min='1' required>";
                            }
                        }

                        DIV_CLASS("form-group") {
                            DIV_CLASS("col-sm-offset-2 col-sm-10") {
                                const char* state = restoreProgress.IsRunning() ? "disabled" : "";
                                str << "<button type='submit' name='startRestore' class='btn btn-danger' " << state << ">"
                                    << "Start Emergency Restore"
                                << "</button>";
                            }
                        }
                    }

                    DIV_CLASS_ID("alert alert-warning", "restoreStatus") {
                        str << "Status: " << restoreProgress.StatusToString();
                    }

                    double p = restoreProgress.GetProgress();
                    DIV_CLASS_ID("progress", "restoreProgress") {
                        TAG_CLASS_STYLE(TDiv, "progress-bar progress-bar-danger", TStringBuilder() << "width:" << p << "%;") {
                            str << p << "%";
                        }
                    }

                    TAG_ATTRS(TDiv, {{"id", "restoreDetails"}}) {
                        str << "Processed: " << restoreProgress.ProcessedPaths
                            << " / Total: " << restoreProgress.TotalPaths;
                    }

                    str << R"(
                    <script>
                    $(document).ready(function() {
                        $('button[name="startRestore"]').click(function(e) {
                            e.preventDefault();

                            var btn = this;
                            var form = $(btn.form);

                            if (!confirm('Are you sure you want to start emergency restore? This will override existing data!')) {
                                return;
                            }

                            $.ajax({
                                type: "GET",
                                url: window.location.pathname,
                                data: form.serialize() + '&startRestore=1',
                                success: function(response) {
                                    $('body').html(response);
                                    if (window.history && window.history.replaceState) {
                                        window.history.replaceState(null, '', window.location.pathname);
                                    }
                                },
                                error: function(xhr, status, error) {
                                    $(btn).prop('disabled', false);
                                    alert('Failed to start restore: ' + error);
                                }
                            });
                        });

                        function updateRestoreProgress() {
                            $.ajax({
                                url: window.location.pathname + '?restoreProgress=1',
                                dataType: 'json',
                                success: function(data) {
                                    var status = data.status.toLowerCase();
                                    var progress = data.progress;
                                    var processed = data.processed;
                                    var total = data.total;

                                    if (status === 'running' || status === 'starting') {
                                        $('#restoreProgress .progress-bar').css('width', progress + '%')
                                            .text(progress.toFixed(1) + '%');
                                        $('#restoreStatus').text('Status: ' + status);
                                        $('#restoreDetails').text('Processed: ' + processed + ' / Total: ' + total);
                                        setTimeout(updateRestoreProgress, 1000);
                                    } else if (status === 'completed') {
                                        $('#restoreProgress .progress-bar').css('width', '100%')
                                            .text('100%')
                                            .removeClass('progress-bar-danger')
                                            .addClass('progress-bar-success');
                                        $('#restoreStatus').text('Status: restore completed successfully')
                                            .removeClass('alert-warning alert-danger')
                                            .addClass('alert-success');
                                        $('#restoreDetails').text('Processed: ' + processed + ' / Total: ' + total);
                                        $('button[name="startRestore"]').prop('disabled', false);
                                    } else if (status.startsWith('error:')) {
                                        $('#restoreStatus').text('Status: ' + status)
                                            .removeClass('alert-warning alert-success')
                                            .addClass('alert-danger');
                                        $('button[name="startRestore"]').prop('disabled', false);
                                    } else {
                                        $('button[name="startRestore"]').prop('disabled', false);
                                    }
                                },
                                error: function() {
                                    setTimeout(updateRestoreProgress, 1000);
                                }
                            });
                        }

                        )" << (restoreProgress.IsRunning() ? "updateRestoreProgress();" : "") << R"(
                    });
                    </script>
                    )";
                }
            });
        }

        return str.Str();
    }

    template <typename TDerived, typename TEvResponse>
    class TBaseRequester: public TActorBootstrapped<TDerived> {
        static constexpr char HTTPBADGATEWAY[] = "HTTP/1.1 502 Bad Gateway\r\nConnection: Close\r\n\r\nBad Gateway\r\n";
        static constexpr char HTTPUNAVAILABLE[] = "HTTP/1.1 503 Service Unavailable\r\nConnection: Close\r\n\r\nService Unavailable\r\n";
        static constexpr char HTTPTIMEOUT[] = "HTTP/1.1 504 Gateway Timeout\r\nConnection: Close\r\n\r\nGateway Timeout\r\n";

    protected:
        virtual IEventBase* MakeRequest() const = 0;
        virtual void ProcessResponse(typename TEvResponse::TPtr& ev) = 0;

        void Handle(typename TEvResponse::TPtr& ev) {
            ProcessResponse(ev);
        }

        void Reply(const TString& content, EContentType type = EContentType::Html) {
            this->Send(ReplyTo, new NMon::TEvHttpInfoRes(content, 0, type));
            this->PassAway();
        }

        void BadGateway() {
            Reply(HTTPBADGATEWAY, EContentType::Custom);
        }

        void Timeout() {
            Reply(HTTPTIMEOUT, EContentType::Custom);
        }

        void Unavailable() {
            Reply(HTTPUNAVAILABLE, EContentType::Custom);
        }

    public:
        static constexpr auto ActorActivityType() {
            return TActivity::SCHEME_BOARD_INFO_REQUESTER_ACTOR;
        }

        explicit TBaseRequester(const TActorId& requestFrom, const TActorId& replyTo)
            : RequestFrom(requestFrom)
            , ReplyTo(replyTo)
        {
        }

        void Bootstrap() {
            this->Send(RequestFrom, MakeRequest(), IEventHandle::FlagTrackDelivery);
            this->Become(&TDerived::StateWork, TDuration::Seconds(10), new TEvents::TEvWakeup());
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvResponse, Handle);
                cFunc(TEvents::TEvWakeup::EventType, Timeout);
                cFunc(TEvents::TEvUndelivered::EventType, Unavailable);
            }
        }

        using TBase = TBaseRequester<TDerived, TEvResponse>;

    private:
        const TActorId RequestFrom;
        const TActorId ReplyTo;

    }; // TBaseRequester

    class TReplicaEnumerator: public TBaseRequester<TReplicaEnumerator, TEvStateStorage::TEvListSchemeBoardResult> {
        IEventBase* MakeRequest() const override {
            return new TEvStateStorage::TEvListSchemeBoard(false);
        }

        void ProcessResponse(TEvStateStorage::TEvListSchemeBoardResult::TPtr& ev) override {
            const auto& info = ev->Get()->Info;
            if (!info) {
                return BadGateway();
            }

            return Reply(RenderResolver(info->SelectAllReplicas()));
        }

    public:
        explicit TReplicaEnumerator(const TActorId& replyTo)
            : TBase(MakeStateStorageProxyID(), replyTo)
        {
        }

    }; // TReplicaEnumerator

    class TReplicaResolver: public TBaseRequester<TReplicaResolver, TEvStateStorage::TEvResolveReplicasList> {
        IEventBase* MakeRequest() const override {
            switch (Path.index()) {
            case 0:
                return new TEvStateStorage::TEvResolveSchemeBoard(std::get<TString>(Path));
            case 1:
                return new TEvStateStorage::TEvResolveSchemeBoard(std::get<TPathId>(Path));
            default:
                Y_ABORT("unreachable");
            }
        }

        void ProcessResponse(TEvStateStorage::TEvResolveReplicasList::TPtr& ev) override {
            TJsonValue json;

            auto& replicas = json["replicas"];
            replicas.SetType(JSON_ARRAY);

            for (const auto& replica : ev->Get()->GetPlainReplicas()) {
                replicas.AppendValue(ToString(replica));
            }

            Reply(TStringBuilder() << NMonitoring::HTTPOKJSON << WriteJson(json), EContentType::Custom);
        }

    public:
        template <typename T>
        explicit TReplicaResolver(const TActorId& requestFrom, const TActorId& replyTo, const T& path)
            : TBase(requestFrom, replyTo)
            , Path(path)
        {
        }

    private:
        std::variant<TString, TPathId> Path;

    }; // TReplicaResolver

    class TInfoRequester: public TBaseRequester<TInfoRequester, TSchemeBoardMonEvents::TEvInfoResponse> {
        IEventBase* MakeRequest() const override {
            return new TSchemeBoardMonEvents::TEvInfoRequest();
        }

        void ProcessResponse(TSchemeBoardMonEvents::TEvInfoResponse::TPtr& ev) override {
            const auto& record = ev->Get()->Record;

            switch (record.GetResponseCase()) {
            case NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaResponse:
                return Reply(RenderReplica(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kPopulatorResponse:
                return Reply(RenderPopulator(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaPopulatorResponse:
                return Reply(RenderReplicaPopulator(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kSubscriberResponse:
                return Reply(RenderSubscriber(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kSubscriberProxyResponse:
                return Reply(RenderSubscriberProxy(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaSubscriberResponse:
                return Reply(RenderReplicaSubscriber(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kCacheResponse:
                return Reply(RenderCache(record));
            default:
                return BadGateway();
            }
        }

    public:
        using TBase::TBase;

    }; // TInfoRequester

    class TDescriber: public TBaseRequester<TDescriber, TSchemeBoardMonEvents::TEvDescribeResponse> {
        IEventBase* MakeRequest() const override {
            switch (Path.index()) {
            case 0:
                return new TSchemeBoardMonEvents::TEvDescribeRequest(std::get<TString>(Path));
            case 1:
                return new TSchemeBoardMonEvents::TEvDescribeRequest(std::get<TPathId>(Path));
            default:
                Y_ABORT("unreachable");
            }
        }

        void ProcessResponse(TSchemeBoardMonEvents::TEvDescribeResponse::TPtr& ev) override {
            const auto& record = ev->Get()->Record;
            Reply(TStringBuilder() << NMonitoring::HTTPOKJSON << record.GetJson(), EContentType::Custom);
        }

    public:
        template <typename T>
        explicit TDescriber(const TActorId& requestFrom, const TActorId& replyTo, const T& path)
            : TBase(requestFrom, replyTo)
            , Path(path)
        {
        }

    private:
        std::variant<TString, TPathId> Path;

    }; // TDescriber

    template <typename TActionActor>
    bool RunFormAction(const TActorId& requestFrom, const TActorId& replyTo, const TCgiParameters& params) {
        if (params.Has("byPath")) {
            Register(new TActionActor(requestFrom, replyTo, params.Get("path")));
            return true;
        } else if (params.Has("byPathId")) {
            ui64 oid;
            ui64 lpid;
            if (TryFromString(params.Get("ownerId"), oid) && TryFromString(params.Get("pathId"), lpid)) {
                Register(new TActionActor(requestFrom, replyTo, TPathId(oid, lpid)));
                return true;
            }
        }

        return false;
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        const auto& request = ev->Get()->Request;
        const auto& params = request.GetParams();

        switch (ParseRequestType(request.GetPathInfo())) {
        case ERequestType::Index:
            return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderIndex()));

        case ERequestType::Populator:
        case ERequestType::ReplicaPopulator:
        case ERequestType::Replica:
        case ERequestType::Subscriber:
        case ERequestType::SubscriberProxy:
        case ERequestType::ReplicaSubscriber:
        case ERequestType::Cache:
            if (const auto actorId = ParseActorIdFromPath(request.GetPathInfo())) {
                return (void)Register(new TInfoRequester(actorId, ev->Sender));
            }
            break;

        case ERequestType::Describe:
            if (const auto actorId = ParseActorId(TStringBuf(params.Get("actorId")))) {
                if (RunFormAction<TDescriber>(actorId, ev->Sender, params)) {
                    return;
                }
            }
            break;

        case ERequestType::Resolver:
            return (void)Register(new TReplicaEnumerator(ev->Sender));

        case ERequestType::Resolve:
            if (RunFormAction<TReplicaResolver>(MakeStateStorageProxyID(), ev->Sender, params)) {
                return;
            }
            break;

        case ERequestType::Backup:
            if (params.Has("startBackup")) {
                if (BackupProgress.IsRunning()) {
                    return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        RenderBackup(false, "Backup is already running")
                    ));
                }

                const TString filePath = params.Get("backupPath");
                if (filePath.empty()) {
                    return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        RenderBackup(false, "Backup file path is required")
                    ));
                }

                ui32 inFlightLimit = BackupLimits.DefaultInFlight;
                if (params.Has("inFlightLimit")) {
                    if (!TryFromString(params.Get("inFlightLimit"), inFlightLimit)) {
                        return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                            RenderBackup(false, "Invalid in-flight limit value")
                        ));
                    }
                }

                bool requireMajority = true;
                if (params.Has("requireMajority")) {
                    if (!TryFromString(params.Get("requireMajority"), requireMajority)) {
                        return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                            RenderBackup(false, "Invalid require majority toggle value")
                        ));
                    }
                }

                BackupProgress = TBackupProgress();
                BackupProgress.Status = TBackupProgress::EStatus::Starting;

                SBB_LOG_I("Starting backup to file: " << filePath
                    << ", in-flight limit: " << inFlightLimit
                    << ", require majority: " << requireMajority
                );

                Register(CreateSchemeBoardBackuper(filePath, inFlightLimit, requireMajority, SelfId()));

                return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderBackup(true)));
            }

            if (params.Has("backupProgress")) {
                return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                    TStringBuilder() << NMonitoring::HTTPOKJSON << BackupProgress.ToJson(),
                    0, EContentType::Custom
                ));
            }

            return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderBackup(false)));

        case ERequestType::Restore:
            if (params.Has("startRestore")) {
                if (!ev->Get()->UserToken || !IsAdministrator(AppData(), ev->Get()->UserToken)) {
                    return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        RenderRestore(false, "Unauthorized")
                    ));
                }
                if (RestoreProgress.IsRunning()) {
                    return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        RenderRestore(false, "Restore is already running")
                    ));
                }

                const TString filePath = params.Get("restorePath");
                if (filePath.empty()) {
                    return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        RenderRestore(false, "Restore file path is required")
                    ));
                }

                ui64 schemeShardId = 0;
                if (!TryFromString(params.Get("schemeShardId"), schemeShardId)) {
                    return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        RenderRestore(false, "Invalid Scheme Shard ID")
                    ));
                }

                ui64 generation = 1;
                if (params.Has("generation")) {
                    TryFromString(params.Get("generation"), generation);
                }

                RestoreProgress = TRestoreProgress();
                RestoreProgress.Status = TRestoreProgress::EStatus::Starting;

                SBB_LOG_I("Starting restore from " << filePath
                    << " for SchemeShard ID: " << schemeShardId
                    << " of generation: " << generation
                );

                Register(CreateSchemeBoardRestorer(filePath, schemeShardId, generation, SelfId()));

                return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderRestore(true)));
            }

            if (params.Has("restoreProgress")) {
                return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                    TStringBuilder() << NMonitoring::HTTPOKJSON << RestoreProgress.ToJson(),
                    0, EContentType::Custom
                ));
            }

            return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderRestore(false)));

        case ERequestType::Unknown:
            break;
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(NMonitoring::HTTPNOTFOUND, 0, EContentType::Custom));
    }

    template <typename TProgress, typename TEventPtr>
    void Handle(TProgress& progress, TEventPtr& ev) {
        const auto& msg = *ev->Get();
        SBB_LOG_D("Handle " << msg.ToString());
        progress = TProgress(msg);
    }

    void Handle(TSchemeBoardMonEvents::TEvBackupProgress::TPtr& ev) {
        Handle(BackupProgress, ev);
    }

    void Handle(TSchemeBoardMonEvents::TEvBackupResult::TPtr& ev) {
        Handle(BackupProgress, ev);
    }

    void Handle(TSchemeBoardMonEvents::TEvRestoreProgress::TPtr& ev) {
        Handle(RestoreProgress, ev);
    }

    void Handle(TSchemeBoardMonEvents::TEvRestoreResult::TPtr& ev) {
        Handle(RestoreProgress, ev);
    }

public:
    static constexpr auto ActorActivityType() {
        return TActivity::SCHEME_BOARD_MONITORING_ACTOR;
    }

    void Bootstrap() {
        if (auto* mon = AppData()->Mon) {
            auto* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, ROOT, "Scheme Board",
                false, TlsActivationContext->ActorSystem(), SelfId());
        }

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardMonEvents::TEvRegister, Handle);
            hFunc(TSchemeBoardMonEvents::TEvUnregister, Handle);

            hFunc(NMon::TEvHttpInfo, Handle);

            hFunc(TSchemeBoardMonEvents::TEvBackupProgress, Handle);
            hFunc(TSchemeBoardMonEvents::TEvBackupResult, Handle);
            hFunc(TSchemeBoardMonEvents::TEvRestoreProgress, Handle);
            hFunc(TSchemeBoardMonEvents::TEvRestoreResult, Handle);

            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    THashMap<TActorId, TActorInfo> RegisteredActors;
    THashMap<EActivityType, THashSet<TActorId>> ByActivityType;
    TBackupProgress BackupProgress;
    TRestoreProgress RestoreProgress;

}; // TMonitoring

} // NSchemeBoard

IActor* CreateSchemeBoardMonitoring() {
    return new NSchemeBoard::TMonitoring();
}

} // NKikimr
