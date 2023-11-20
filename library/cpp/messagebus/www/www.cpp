#include "www.h"

#include "concat_strings.h"
#include "html_output.h"

#include <library/cpp/messagebus/remote_connection_status.h>
#include <library/cpp/monlib/deprecated/json/writer.h>

#include <library/cpp/http/fetch/httpfsm.h>
#include <library/cpp/http/fetch/httpheader.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/uri/http_url.h>

#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/mutex.h>

#include <utility>

using namespace NBus;
using namespace NBus::NPrivate;
using namespace NActor;
using namespace NActor::NPrivate;

static const char HTTP_OK_JS[] = "HTTP/1.1 200 Ok\r\nContent-Type: text/javascript\r\nConnection: Close\r\n\r\n";
static const char HTTP_OK_JSON[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/json; charset=utf-8\r\nConnection: Close\r\n\r\n";
static const char HTTP_OK_PNG[] = "HTTP/1.1 200 Ok\r\nContent-Type: image/png\r\nConnection: Close\r\n\r\n";
static const char HTTP_OK_BIN[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/octet-stream\r\nConnection: Close\r\n\r\n";
static const char HTTP_OK_HTML[] = "HTTP/1.1 200 Ok\r\nContent-Type: text/html; charset=utf-8\r\nConnection: Close\r\n\r\n";

namespace {
    typedef TIntrusivePtr<TBusModuleInternal> TBusModuleInternalPtr;

    template <typename TValuePtr>
    struct TNamedValues {
        TVector<std::pair<TString, TValuePtr>> Entries;

        TValuePtr FindByName(TStringBuf name) {
            Y_ABORT_UNLESS(!!name);

            for (unsigned i = 0; i < Entries.size(); ++i) {
                if (Entries[i].first == name) {
                    return Entries[i].second;
                }
            }
            return TValuePtr();
        }

        TString FindNameByPtr(TValuePtr value) {
            Y_ABORT_UNLESS(!!value);

            for (unsigned i = 0; i < Entries.size(); ++i) {
                if (Entries[i].second.Get() == value.Get()) {
                    return Entries[i].first;
                }
            }

            Y_ABORT("unregistered");
        }

        void Add(TValuePtr p) {
            Y_ABORT_UNLESS(!!p);

            // Do not add twice
            for (unsigned i = 0; i < Entries.size(); ++i) {
                if (Entries[i].second.Get() == p.Get()) {
                    return;
                }
            }

            if (!!p->GetNameInternal()) {
                TValuePtr current = FindByName(p->GetNameInternal());

                if (!current) {
                    Entries.emplace_back(p->GetNameInternal(), p);
                    return;
                }
            }

            for (unsigned i = 1;; ++i) {
                TString prefix = p->GetNameInternal();
                if (!prefix) {
                    prefix = "unnamed";
                }
                TString name = ConcatStrings(prefix, "-", i);

                TValuePtr current = FindByName(name);

                if (!current) {
                    Entries.emplace_back(name, p);
                    return;
                }
            }
        }

        size_t size() const {
            return Entries.size();
        }

        bool operator!() const {
            return size() == 0;
        }
    };

    template <typename TSessionPtr>
    struct TSessionValues: public TNamedValues<TSessionPtr> {
        typedef TNamedValues<TSessionPtr> TBase;

        TVector<TString> GetNamesForQueue(TBusMessageQueue* queue) {
            TVector<TString> r;
            for (unsigned i = 0; i < TBase::size(); ++i) {
                if (TBase::Entries[i].second->GetQueue() == queue) {
                    r.push_back(TBase::Entries[i].first);
                }
            }
            return r;
        }
    };
}

namespace {
    TString RootHref() {
        return ConcatStrings("?");
    }

    TString QueueHref(TStringBuf name) {
        return ConcatStrings("?q=", name);
    }

    TString ServerSessionHref(TStringBuf name) {
        return ConcatStrings("?ss=", name);
    }

    TString ClientSessionHref(TStringBuf name) {
        return ConcatStrings("?cs=", name);
    }

    TString OldModuleHref(TStringBuf name) {
        return ConcatStrings("?om=", name);
    }

    /*
    static void RootLink() {
        A(RootHref(), "root");
    }
    */

    void QueueLink(TStringBuf name) {
        A(QueueHref(name), name);
    }

    void ServerSessionLink(TStringBuf name) {
        A(ServerSessionHref(name), name);
    }

    void ClientSessionLink(TStringBuf name) {
        A(ClientSessionHref(name), name);
    }

    void OldModuleLink(TStringBuf name) {
        A(OldModuleHref(name), name);
    }

}

struct TBusWww::TImpl {
    // TODO: use weak pointers
    TNamedValues<TBusMessageQueuePtr> Queues;
    TSessionValues<TIntrusivePtr<TBusClientSession>> ClientSessions;
    TSessionValues<TIntrusivePtr<TBusServerSession>> ServerSessions;
    TSessionValues<TBusModuleInternalPtr> Modules;

    TMutex Mutex;

    void RegisterClientSession(TBusClientSessionPtr s) {
        Y_ABORT_UNLESS(!!s);
        TGuard<TMutex> g(Mutex);
        ClientSessions.Add(s.Get());
        Queues.Add(s->GetQueue());
    }

    void RegisterServerSession(TBusServerSessionPtr s) {
        Y_ABORT_UNLESS(!!s);
        TGuard<TMutex> g(Mutex);
        ServerSessions.Add(s.Get());
        Queues.Add(s->GetQueue());
    }

    void RegisterQueue(TBusMessageQueuePtr q) {
        Y_ABORT_UNLESS(!!q);
        TGuard<TMutex> g(Mutex);
        Queues.Add(q);
    }

    void RegisterModule(TBusModule* module) {
        Y_ABORT_UNLESS(!!module);
        TGuard<TMutex> g(Mutex);

        {
            TVector<TBusClientSessionPtr> clientSessions = module->GetInternal()->GetClientSessionsInternal();
            for (unsigned i = 0; i < clientSessions.size(); ++i) {
                RegisterClientSession(clientSessions[i]);
            }
        }

        {
            TVector<TBusServerSessionPtr> serverSessions = module->GetInternal()->GetServerSessionsInternal();
            for (unsigned i = 0; i < serverSessions.size(); ++i) {
                RegisterServerSession(serverSessions[i]);
            }
        }

        Queues.Add(module->GetInternal()->GetQueue());
        Modules.Add(module->GetInternal());
    }

    TString FindQueueNameBySessionName(TStringBuf sessionName, bool client) {
        TIntrusivePtr<TBusClientSession> clientSession;
        TIntrusivePtr<TBusServerSession> serverSession;
        TBusSession* session;
        if (client) {
            clientSession = ClientSessions.FindByName(sessionName);
            session = clientSession.Get();
        } else {
            serverSession = ServerSessions.FindByName(sessionName);
            session = serverSession.Get();
        }
        Y_ABORT_UNLESS(!!session);
        return Queues.FindNameByPtr(session->GetQueue());
    }

    struct TRequest {
        TImpl* const Outer;
        IOutputStream& Os;
        const TCgiParameters& CgiParams;
        const TOptionalParams& Params;

        TRequest(TImpl* outer, IOutputStream& os, const TCgiParameters& cgiParams, const TOptionalParams& params)
            : Outer(outer)
            , Os(os)
            , CgiParams(cgiParams)
            , Params(params)
        {
        }

        void CrumbsParentLinks() {
            for (unsigned i = 0; i < Params.ParentLinks.size(); ++i) {
                const TLink& link = Params.ParentLinks[i];
                TTagGuard li("li");
                A(link.Href, link.Title);
            }
        }

        void Crumb(TStringBuf name, TStringBuf href = "") {
            if (!!href) {
                TTagGuard li("li");
                A(href, name);
            } else {
                LiWithClass("active", name);
            }
        }

        void BreadcrumbRoot() {
            TTagGuard ol("ol", "breadcrumb");
            CrumbsParentLinks();
            Crumb("MessageBus");
        }

        void BreadcrumbQueue(TStringBuf queueName) {
            TTagGuard ol("ol", "breadcrumb");
            CrumbsParentLinks();
            Crumb("MessageBus", RootHref());
            Crumb(ConcatStrings("queue ", queueName));
        }

        void BreadcrumbSession(TStringBuf sessionName, bool client) {
            TString queueName = Outer->FindQueueNameBySessionName(sessionName, client);
            TStringBuf whatSession = client ? "client session" : "server session";

            TTagGuard ol("ol", "breadcrumb");
            CrumbsParentLinks();
            Crumb("MessageBus", RootHref());
            Crumb(ConcatStrings("queue ", queueName), QueueHref(queueName));
            Crumb(ConcatStrings(whatSession, " ", sessionName));
        }

        void ServeSessionsOfQueue(TBusMessageQueuePtr queue, bool includeQueue) {
            TVector<TString> clientNames = Outer->ClientSessions.GetNamesForQueue(queue.Get());
            TVector<TString> serverNames = Outer->ServerSessions.GetNamesForQueue(queue.Get());
            TVector<TString> moduleNames = Outer->Modules.GetNamesForQueue(queue.Get());

            TTagGuard table("table", "table table-condensed table-bordered");

            {
                TTagGuard colgroup("colgroup");
                TagWithClass("col", "col-md-2");
                TagWithClass("col", "col-md-2");
                TagWithClass("col", "col-md-8");
            }

            {
                TTagGuard tr("tr");
                Th("What", "span2");
                Th("Name", "span2");
                Th("Status", "span6");
            }

            if (includeQueue) {
                TTagGuard tr1("tr");
                Td("queue");

                {
                    TTagGuard td("td");
                    QueueLink(Outer->Queues.FindNameByPtr(queue));
                }

                {
                    TTagGuard tr2("td");
                    Pre(queue->GetStatusSingleLine());
                }
            }

            for (unsigned j = 0; j < clientNames.size(); ++j) {
                TTagGuard tr("tr");
                Td("client session");

                {
                    TTagGuard td("td");
                    ClientSessionLink(clientNames[j]);
                }

                {
                    TTagGuard td("td");
                    Pre(Outer->ClientSessions.FindByName(clientNames[j])->GetStatusSingleLine());
                }
            }

            for (unsigned j = 0; j < serverNames.size(); ++j) {
                TTagGuard tr("tr");
                Td("server session");

                {
                    TTagGuard td("td");
                    ServerSessionLink(serverNames[j]);
                }

                {
                    TTagGuard td("td");
                    Pre(Outer->ServerSessions.FindByName(serverNames[j])->GetStatusSingleLine());
                }
            }

            for (unsigned j = 0; j < moduleNames.size(); ++j) {
                TTagGuard tr("tr");
                Td("module");

                {
                    TTagGuard td("td");
                    if (false) {
                        OldModuleLink(moduleNames[j]);
                    } else {
                        // TODO
                        Text(moduleNames[j]);
                    }
                }

                {
                    TTagGuard td("td");
                    Pre(Outer->Modules.FindByName(moduleNames[j])->GetStatusSingleLine());
                }
            }
        }

        void ServeQueue(const TString& name) {
            TBusMessageQueuePtr queue = Outer->Queues.FindByName(name);

            if (!queue) {
                BootstrapError(ConcatStrings("queue not found by name: ", name));
                return;
            }

            BreadcrumbQueue(name);

            TDivGuard container("container");

            H1(ConcatStrings("MessageBus queue ", '"', name, '"'));

            TBusMessageQueueStatus status = queue->GetStatusRecordInternal();

            Pre(status.PrintToString());

            ServeSessionsOfQueue(queue, false);

            HnWithSmall(3, "Peak queue size", "(stored for an hour)");

            {
                TDivGuard div;
                TDivGuard div2(TAttr("id", "queue-size-graph"), TAttr("style", "height: 300px"));
            }

            {
                TScriptFunctionGuard script;

                NJsonWriter::TBuf data(NJsonWriter::HEM_ESCAPE_HTML);
                NJsonWriter::TBuf ticks(NJsonWriter::HEM_ESCAPE_HTML);

                const TExecutorHistory& history = status.ExecutorStatus.History;

                data.BeginList();
                ticks.BeginList();
                for (unsigned i = 0; i < history.HistoryRecords.size(); ++i) {
                    ui64 secondOfMinute = (history.FirstHistoryRecordSecond() + i) % 60;
                    ui64 minuteOfHour = (history.FirstHistoryRecordSecond() + i) / 60 % 60;

                    unsigned printEach;

                    if (history.HistoryRecords.size() <= 500) {
                        printEach = 1;
                    } else if (history.HistoryRecords.size() <= 1000) {
                        printEach = 2;
                    } else if (history.HistoryRecords.size() <= 3000) {
                        printEach = 6;
                    } else {
                        printEach = 12;
                    }

                    if (secondOfMinute % printEach != 0) {
                        continue;
                    }

                    ui32 max = 0;
                    for (unsigned j = 0; j < printEach; ++j) {
                        if (i < j) {
                            continue;
                        }
                        max = Max<ui32>(max, history.HistoryRecords[i - j].MaxQueueSize);
                    }

                    data.BeginList();
                    data.WriteString(ToString(i));
                    data.WriteInt(max);
                    data.EndList();

                    // TODO: can be done with flot time plugin
                    if (history.HistoryRecords.size() <= 20) {
                        ticks.BeginList();
                        ticks.WriteInt(i);
                        ticks.WriteString(ToString(secondOfMinute));
                        ticks.EndList();
                    } else if (history.HistoryRecords.size() <= 60) {
                        if (secondOfMinute % 5 == 0) {
                            ticks.BeginList();
                            ticks.WriteInt(i);
                            ticks.WriteString(ToString(secondOfMinute));
                            ticks.EndList();
                        }
                    } else {
                        bool needTick;
                        if (history.HistoryRecords.size() <= 3 * 60) {
                            needTick = secondOfMinute % 15 == 0;
                        } else if (history.HistoryRecords.size() <= 7 * 60) {
                            needTick = secondOfMinute % 30 == 0;
                        } else if (history.HistoryRecords.size() <= 20 * 60) {
                            needTick = secondOfMinute == 0;
                        } else {
                            needTick = secondOfMinute == 0 && minuteOfHour % 5 == 0;
                        }
                        if (needTick) {
                            ticks.BeginList();
                            ticks.WriteInt(i);
                            ticks.WriteString(Sprintf(":%02u:%02u", (unsigned)minuteOfHour, (unsigned)secondOfMinute));
                            ticks.EndList();
                        }
                    }
                }
                ticks.EndList();
                data.EndList();

                HtmlOutputStream() << "    var data = " << data.Str() << ";\n";
                HtmlOutputStream() << "    var ticks = " << ticks.Str() << ";\n";
                HtmlOutputStream() << "    plotQueueSize('#queue-size-graph', data, ticks);\n";
            }
        }

        void ServeSession(TStringBuf name, bool client) {
            TIntrusivePtr<TBusClientSession> clientSession;
            TIntrusivePtr<TBusServerSession> serverSession;
            TBusSession* session;
            TStringBuf whatSession;
            if (client) {
                whatSession = "client session";
                clientSession = Outer->ClientSessions.FindByName(name);
                session = clientSession.Get();
            } else {
                whatSession = "server session";
                serverSession = Outer->ServerSessions.FindByName(name);
                session = serverSession.Get();
            }
            if (!session) {
                BootstrapError(ConcatStrings(whatSession, " not found by name: ", name));
                return;
            }

            TSessionDumpStatus dumpStatus = session->GetStatusRecordInternal();

            TBusMessageQueuePtr queue = session->GetQueue();
            TString queueName = Outer->Queues.FindNameByPtr(session->GetQueue());

            BreadcrumbSession(name, client);

            TDivGuard container("container");

            H1(ConcatStrings("MessageBus ", whatSession, " ", '"', name, '"'));

            TBusMessageQueueStatus queueStatus = queue->GetStatusRecordInternal();

            {
                H3(ConcatStrings("queue ", queueName));
                Pre(queueStatus.PrintToString());
            }

            TSessionDumpStatus status = session->GetStatusRecordInternal();

            if (status.Shutdown) {
                BootstrapError("Session shut down");
                return;
            }

            H3("Basic");
            Pre(status.Head);

            if (status.ConnectionStatusSummary.Server) {
                H3("Acceptors");
                Pre(status.Acceptors);
            }

            H3("Connections");
            Pre(status.ConnectionsSummary);

            {
                TDivGuard div;
                TTagGuard button("button",
                                 TAttr("type", "button"),
                                 TAttr("class", "btn"),
                                 TAttr("data-toggle", "collapse"),
                                 TAttr("data-target", "#connections"));
                Text("Show connection details");
            }
            {
                TDivGuard div(TAttr("id", "connections"), TAttr("class", "collapse"));
                Pre(status.Connections);
            }

            H3("TBusSessionConfig");
            Pre(status.Config.PrintToString());

            if (!client) {
                H3("Message process time histogram");

                const TDurationHistogram& h =
                    dumpStatus.ConnectionStatusSummary.WriterStatus.Incremental.ProcessDurationHistogram;

                {
                    TDivGuard div;
                    TDivGuard div2(TAttr("id", "h"), TAttr("style", "height: 300px"));
                }

                {
                    TScriptFunctionGuard script;

                    NJsonWriter::TBuf buf(NJsonWriter::HEM_ESCAPE_HTML);
                    buf.BeginList();
                    for (unsigned i = 0; i < h.Times.size(); ++i) {
                        TString label = TDurationHistogram::LabelBefore(i);
                        buf.BeginList();
                        buf.WriteString(label);
                        buf.WriteLongLong(h.Times[i]);
                        buf.EndList();
                    }
                    buf.EndList();

                    HtmlOutputStream() << "    var hist = " << buf.Str() << ";\n";
                    HtmlOutputStream() << "    plotHist('#h', hist);\n";
                }
            }
        }

        void ServeDefault() {
            if (!Outer->Queues) {
                BootstrapError("no queues");
                return;
            }

            BreadcrumbRoot();

            TDivGuard container("container");

            H1("MessageBus queues");

            for (unsigned i = 0; i < Outer->Queues.size(); ++i) {
                TString queueName = Outer->Queues.Entries[i].first;
                TBusMessageQueuePtr queue = Outer->Queues.Entries[i].second;

                HnWithSmall(3, queueName, "(queue)");

                ServeSessionsOfQueue(queue, true);
            }
        }

        void WriteQueueSensors(NMonitoring::TDeprecatedJsonWriter& sj, TStringBuf queueName, TBusMessageQueue* queue) {
            auto status = queue->GetStatusRecordInternal();
            sj.OpenMetric();
            sj.WriteLabels("mb_queue", queueName, "sensor", "WorkQueueSize");
            sj.WriteValue(status.ExecutorStatus.WorkQueueSize);
            sj.CloseMetric();
        }

        void WriteMessageCounterSensors(NMonitoring::TDeprecatedJsonWriter& sj,
                                        TStringBuf labelName, TStringBuf sessionName, bool read, const TMessageCounter& counter) {
            TStringBuf readOrWrite = read ? "read" : "write";

            sj.OpenMetric();
            sj.WriteLabels(labelName, sessionName, "mb_dir", readOrWrite, "sensor", "MessageBytes");
            sj.WriteValue(counter.BytesData);
            sj.WriteModeDeriv();
            sj.CloseMetric();

            sj.OpenMetric();
            sj.WriteLabels(labelName, sessionName, "mb_dir", readOrWrite, "sensor", "MessageCount");
            sj.WriteValue(counter.Count);
            sj.WriteModeDeriv();
            sj.CloseMetric();
        }

        void WriteSessionStatus(NMonitoring::TDeprecatedJsonWriter& sj, TStringBuf sessionName, bool client,
                                TBusSession* session) {
            TStringBuf labelName = client ? "mb_client_session" : "mb_server_session";

            auto status = session->GetStatusRecordInternal();

            sj.OpenMetric();
            sj.WriteLabels(labelName, sessionName, "sensor", "InFlightCount");
            sj.WriteValue(status.Status.InFlightCount);
            sj.CloseMetric();

            sj.OpenMetric();
            sj.WriteLabels(labelName, sessionName, "sensor", "InFlightSize");
            sj.WriteValue(status.Status.InFlightSize);
            sj.CloseMetric();

            sj.OpenMetric();
            sj.WriteLabels(labelName, sessionName, "sensor", "SendQueueSize");
            sj.WriteValue(status.ConnectionStatusSummary.WriterStatus.SendQueueSize);
            sj.CloseMetric();

            if (client) {
                sj.OpenMetric();
                sj.WriteLabels(labelName, sessionName, "sensor", "AckMessagesSize");
                sj.WriteValue(status.ConnectionStatusSummary.WriterStatus.AckMessagesSize);
                sj.CloseMetric();
            }

            WriteMessageCounterSensors(sj, labelName, sessionName, false,
                                       status.ConnectionStatusSummary.WriterStatus.Incremental.MessageCounter);
            WriteMessageCounterSensors(sj, labelName, sessionName, true,
                                       status.ConnectionStatusSummary.ReaderStatus.Incremental.MessageCounter);
        }

        void ServeSolomonJson(const TString& q, const TString& cs, const TString& ss) {
            Y_UNUSED(q);
            Y_UNUSED(cs);
            Y_UNUSED(ss);
            bool all = q == "" && cs == "" && ss == "";

            NMonitoring::TDeprecatedJsonWriter sj(&Os);

            sj.OpenDocument();
            sj.OpenMetrics();

            for (unsigned i = 0; i < Outer->Queues.size(); ++i) {
                TString queueName = Outer->Queues.Entries[i].first;
                TBusMessageQueuePtr queue = Outer->Queues.Entries[i].second;
                if (all || q == queueName) {
                    WriteQueueSensors(sj, queueName, &*queue);
                }

                TVector<TString> clientNames = Outer->ClientSessions.GetNamesForQueue(queue.Get());
                TVector<TString> serverNames = Outer->ServerSessions.GetNamesForQueue(queue.Get());
                TVector<TString> moduleNames = Outer->Modules.GetNamesForQueue(queue.Get());
                for (auto& sessionName : clientNames) {
                    if (all || cs == sessionName) {
                        auto session = Outer->ClientSessions.FindByName(sessionName);
                        WriteSessionStatus(sj, sessionName, true, &*session);
                    }
                }

                for (auto& sessionName : serverNames) {
                    if (all || ss == sessionName) {
                        auto session = Outer->ServerSessions.FindByName(sessionName);
                        WriteSessionStatus(sj, sessionName, false, &*session);
                    }
                }
            }

            sj.CloseMetrics();
            sj.CloseDocument();
        }

        void ServeStatic(IOutputStream& os, TStringBuf path) {
            if (path.EndsWith(".js")) {
                os << HTTP_OK_JS;
            } else if (path.EndsWith(".png")) {
                os << HTTP_OK_PNG;
            } else {
                os << HTTP_OK_BIN;
            }
            auto blob = NResource::Find(TString("/") + TString(path));
            os.Write(blob.Data(), blob.Size());
        }

        void HeaderJsCss() {
            LinkStylesheet("//yandex.st/bootstrap/3.0.2/css/bootstrap.css");
            LinkFavicon("?file=bus-ico.png");
            ScriptHref("//yandex.st/jquery/2.0.3/jquery.js");
            ScriptHref("//yandex.st/bootstrap/3.0.2/js/bootstrap.js");
            ScriptHref("//cdnjs.cloudflare.com/ajax/libs/flot/0.8.1/jquery.flot.min.js");
            ScriptHref("//cdnjs.cloudflare.com/ajax/libs/flot/0.8.1/jquery.flot.categories.min.js");
            ScriptHref("?file=messagebus.js");
        }

        void Serve() {
            THtmlOutputStreamPushPop pp(&Os);

            TCgiParameters::const_iterator file = CgiParams.Find("file");
            if (file != CgiParams.end()) {
                ServeStatic(Os, file->second);
                return;
            }

            bool solomonJson = false;
            TCgiParameters::const_iterator fmt = CgiParams.Find("fmt");
            if (fmt != CgiParams.end()) {
                if (fmt->second == "solomon-json") {
                    solomonJson = true;
                }
            }

            TCgiParameters::const_iterator cs = CgiParams.Find("cs");
            TCgiParameters::const_iterator ss = CgiParams.Find("ss");
            TCgiParameters::const_iterator q = CgiParams.Find("q");

            if (solomonJson) {
                Os << HTTP_OK_JSON;

                TString qp = q != CgiParams.end() ? q->first : "";
                TString csp = cs != CgiParams.end() ? cs->first : "";
                TString ssp = ss != CgiParams.end() ? ss->first : "";
                ServeSolomonJson(qp, csp, ssp);
            } else {
                Os << HTTP_OK_HTML;

                Doctype();

                TTagGuard html("html");
                {
                    TTagGuard head("head");

                    HeaderJsCss();
                    // &#x2709; &#x1f68c;
                    Title(TChars("MessageBus", false));
                }

                TTagGuard body("body");

                if (cs != CgiParams.end()) {
                    ServeSession(cs->second, true);
                } else if (ss != CgiParams.end()) {
                    ServeSession(ss->second, false);
                } else if (q != CgiParams.end()) {
                    ServeQueue(q->second);
                } else {
                    ServeDefault();
                }
            }
        }
    };

    void ServeHttp(IOutputStream& os, const TCgiParameters& queryArgs, const TBusWww::TOptionalParams& params) {
        TGuard<TMutex> g(Mutex);

        TRequest request(this, os, queryArgs, params);

        request.Serve();
    }
};

NBus::TBusWww::TBusWww()
    : Impl(new TImpl)
{
}

NBus::TBusWww::~TBusWww() {
}

void NBus::TBusWww::RegisterClientSession(TBusClientSessionPtr s) {
    Impl->RegisterClientSession(s);
}

void TBusWww::RegisterServerSession(TBusServerSessionPtr s) {
    Impl->RegisterServerSession(s);
}

void TBusWww::RegisterQueue(TBusMessageQueuePtr q) {
    Impl->RegisterQueue(q);
}

void TBusWww::RegisterModule(TBusModule* module) {
    Impl->RegisterModule(module);
}

void TBusWww::ServeHttp(IOutputStream& httpOutputStream,
                        const TCgiParameters& queryArgs,
                        const TBusWww::TOptionalParams& params) {
    Impl->ServeHttp(httpOutputStream, queryArgs, params);
}

struct TBusWwwHttpServer::TImpl: public THttpServer::ICallBack {
    TIntrusivePtr<TBusWww> Www;
    THttpServer HttpServer;

    static THttpServer::TOptions MakeHttpServerOptions(unsigned port) {
        Y_ABORT_UNLESS(port > 0);
        THttpServer::TOptions r;
        r.Port = port;
        return r;
    }

    TImpl(TIntrusivePtr<TBusWww> www, unsigned port)
        : Www(www)
        , HttpServer(this, MakeHttpServerOptions(port))
    {
        HttpServer.Start();
    }

    struct TClientRequestImpl: public TClientRequest {
        TBusWwwHttpServer::TImpl* const Outer;

        TClientRequestImpl(TBusWwwHttpServer::TImpl* outer)
            : Outer(outer)
        {
        }

        bool Reply(void*) override {
            Outer->ServeRequest(Input(), Output());
            return true;
        }
    };

    TString MakeSimpleResponse(unsigned code, TString text, TString content = "") {
        if (!content) {
            TStringStream contentSs;
            contentSs << code << " " << text;
            content = contentSs.Str();
        }
        TStringStream ss;
        ss << "HTTP/1.1 "
           << code << " " << text << "\r\nConnection: Close\r\n\r\n"
           << content;
        return ss.Str();
    }

    void ServeRequest(THttpInput& input, THttpOutput& output) {
        TCgiParameters cgiParams;
        try {
            THttpRequestHeader header;
            THttpHeaderParser parser;
            parser.Init(&header);
            if (parser.Execute(input.FirstLine()) < 0) {
                HtmlOutputStream() << MakeSimpleResponse(400, "Bad request");
                return;
            }
            THttpURL url;
            if (url.Parse(header.GetUrl()) != THttpURL::ParsedOK) {
                HtmlOutputStream() << MakeSimpleResponse(400, "Invalid url");
                return;
            }
            cgiParams.Scan(url.Get(THttpURL::FieldQuery));

            TBusWww::TOptionalParams params;
            //params.ParentLinks.emplace_back();
            //params.ParentLinks.back().Title = "temp";
            //params.ParentLinks.back().Href = "http://wiki.yandex-team.ru/";

            Www->ServeHttp(output, cgiParams, params);
        } catch (...) {
            output << MakeSimpleResponse(500, "Exception",
                                         TString() + "Exception: " + CurrentExceptionMessage());
        }
    }

    TClientRequest* CreateClient() override {
        return new TClientRequestImpl(this);
    }

    ~TImpl() override {
        HttpServer.Stop();
    }
};

NBus::TBusWwwHttpServer::TBusWwwHttpServer(TIntrusivePtr<TBusWww> www, unsigned port)
    : Impl(new TImpl(www, port))
{
}

NBus::TBusWwwHttpServer::~TBusWwwHttpServer() {
}
