#include "tablet_flat_executed.h"
#include "flat_dbase_scheme.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/protos/actors.pb.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/http/fetch/httpheader.h>
#include <library/cpp/monlib/service/pages/mon_page.h>

#include <util/stream/str.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NKikimr::NTabletFlatExecutor {
namespace {

constexpr size_t MaxProgramBytes = 256 << 10;

TString PostParam(const NMon::TEvRemoteHttpInfo& ev, TStringBuf key) {
    if (ev.ExtendedQuery) {
        for (const auto& param : ev.ExtendedQuery->GetPostParams()) {
            if (param.GetKey() == key) {
                return param.GetValue();
            }
        }
    }
    return {};
}

TString RenderSchema(const NTable::TScheme& scheme) {
    TVector<std::pair<TString, ui32>> tables;
    tables.reserve(scheme.TableNames.size());
    for (const auto& [name, id] : scheme.TableNames) {
        tables.emplace_back(name, id);
    }
    Sort(tables);

    TStringBuilder html;
    html << "<h3>Local tables</h3>";
    if (tables.empty()) {
        html << "<p>This tablet has no local tables.</p>";
        return html;
    }
    for (const auto& [name, id] : tables) {
        const auto* info = scheme.GetTableInfo(id);
        html << "<p><b>" << EncodeHtmlPcdata(name) << "</b> (" << id << ")</p><ul>";
        if (!info) {
            html << "<li>missing scheme</li></ul>";
            continue;
        }
        THashSet<ui32> keys;
        for (ui32 columnId : info->KeyColumns) {
            keys.insert(columnId);
        }
        auto columnText = [&](ui32 columnId, bool key) {
            const auto* column = info->Columns.FindPtr(columnId);
            if (!column) {
                html << "<li>" << columnId << "</li>";
                return;
            }
            html << "<li>" << EncodeHtmlPcdata(column->Name) << " "
                 << EncodeHtmlPcdata(NScheme::TypeName(column->PType, column->PTypeMod));
            if (key) {
                html << " key";
            }
            if (column->NotNull) {
                html << " not null";
            }
            html << "</li>";
        };
        for (ui32 columnId : info->KeyColumns) {
            columnText(columnId, true);
        }
        TVector<std::pair<TString, ui32>> rest;
        for (const auto& [columnId, column] : info->Columns) {
            if (!keys.contains(columnId)) {
                rest.emplace_back(column.Name, columnId);
            }
        }
        Sort(rest);
        for (const auto& [name, columnId] : rest) {
            Y_UNUSED(name);
            columnText(columnId, false);
        }
        html << "</ul>";
    }
    return html;
}

void AppendIssues(IOutputStream& out, const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& issues) {
    for (const auto& issue : issues) {
        if (issue.Getposition().Getrow() || issue.Getposition().Getcolumn()) {
            out << issue.Getposition().Getrow() << ":" << issue.Getposition().Getcolumn() << " ";
        }
        out << EncodeHtmlPcdata(issue.Getmessage()) << "<br>";
        AppendIssues(out, issue.Getissues());
    }
}

TString RenderQueryForm(ui64 tabletId, const TString& schemaHtml, const TString& program, const TString& params, bool compileOnly, const TString& resultHtml) {
    TStringBuilder html;
    html << "<h2>Local query</h2>"
         << "<p>SQL, or a MiniKQL s-expression when the text starts with <code>(</code>. "
         << "A write changes the local database and does not update the tablet's memory. Restart the tablet after a write.</p>"
         << schemaHtml
         << "<form method='POST' action='?TabletID=" << tabletId << "'>"
         << "<input type='hidden' name='TabletID' value='" << tabletId << "'>"
         << "<div><textarea name='program' rows='14' style='width:100%;font-family:monospace' placeholder='SELECT value FROM table WHERE key = 1'>"
         << EncodeHtmlPcdata(program) << "</textarea></div>"
         << "<div style='margin-top:8px'>Parameters, s-expression</div>"
         << "<div><textarea name='params' rows='4' style='width:100%;font-family:monospace'>"
         << EncodeHtmlPcdata(params) << "</textarea></div>"
         << "<div style='margin-top:8px'><label><input type='checkbox' name='compile' value='1'"
         << (compileOnly ? " checked" : "") << "> Compile only</label></div>"
         << "<div style='margin-top:8px'><button type='submit'>Run</button></div>"
         << "</form>";
    if (resultHtml) {
        html << "<h3>Result</h3>" << resultHtml;
    }
    return html;
}

TString RenderResponse(const NKikimrTabletTxBase::TEvLocalMKQLResponse& record) {
    TStringBuilder html;
    const auto status = static_cast<NKikimrProto::EReplyStatus>(record.GetStatus());
    html << "<p>Local query status: " << NKikimrProto::EReplyStatus_Name(status) << "</p>";
    if (record.HasCompileResults()) {
        TStringStream issues;
        AppendIssues(issues, record.GetCompileResults().GetProgramCompileErrors());
        AppendIssues(issues, record.GetCompileResults().GetParamsCompileErrors());
        if (issues.Str()) {
            html << "<pre>" << issues.Str() << "</pre>";
        } else if (status == NKikimrProto::OK && !record.HasExecutionEngineEvaluatedResponse()) {
            html << "<p>Compiled.</p>";
        }
    }
    if (record.GetMiniKQLErrors()) {
        html << "<pre>" << EncodeHtmlPcdata(record.GetMiniKQLErrors()) << "</pre>";
    }
    if (record.HasExecutionEngineEvaluatedResponse()) {
        html << "<pre>" << EncodeHtmlPcdata(record.GetExecutionEngineEvaluatedResponse().DebugString()) << "</pre>";
    }
    return html;
}

class TLocalQueryReply : public TActorBootstrapped<TLocalQueryReply> {
public:
    TLocalQueryReply(TActorId httpReplyTo, TActorId tablet, ui64 tabletId, TString program, TString params, bool compileOnly, TString schemaHtml)
        : HttpReplyTo(httpReplyTo)
        , Tablet(tablet)
        , TabletId(tabletId)
        , Program(std::move(program))
        , Params(std::move(params))
        , CompileOnly(compileOnly)
        , SchemaHtml(std::move(schemaHtml))
    {}

    void Bootstrap(const TActorContext& ctx) {
        auto request = std::make_unique<TEvTablet::TEvLocalMKQL>();
        auto* transaction = request->Record.MutableProgram();
        transaction->MutableProgram()->SetText(Program);
        if (Params) {
            transaction->MutableParams()->SetText(Params);
        }
        if (CompileOnly) {
            transaction->SetMode(NKikimrTxUserProxy::TMiniKQLTransaction_EMode_COMPILE);
        }
        ctx.Send(Tablet, request.release());
        ctx.Schedule(TDuration::Seconds(55), new TEvents::TEvWakeup);
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvLocalMKQLResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Reply(const TActorContext& ctx, const TString& resultHtml) {
        if (Replied) {
            return;
        }
        Replied = true;
        ctx.Send(HttpReplyTo, new NMon::TEvRemoteHttpInfoRes(
            RenderQueryForm(TabletId, SchemaHtml, Program, Params, CompileOnly, resultHtml)));
        Die(ctx);
    }

    void Handle(TEvTablet::TEvLocalMKQLResponse::TPtr& ev, const TActorContext& ctx) {
        Reply(ctx, RenderResponse(ev->Get()->Record));
    }

    void HandleTimeout(const TActorContext& ctx) {
        Reply(ctx, "<p>Local query status: TIMEOUT</p>");
    }

    const TActorId HttpReplyTo;
    const TActorId Tablet;
    const ui64 TabletId;
    const TString Program;
    const TString Params;
    const bool CompileOnly;
    const TString SchemaHtml;
    bool Replied = false;
};

} // namespace

void RenderTabletQueryPage(
        NMon::TEvRemoteHttpInfo::TPtr& ev,
        const TActorContext& ctx,
        IMiniKQLFactory* factory,
        const NTable::TScheme& scheme,
        ui64 tabletId,
        bool follower)
{
    if (!IsAdministrator(AppData(ctx), ev->Get()->GetUserToken())) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPFORBIDDEN));
        return;
    }

    const TString schemaHtml = RenderSchema(scheme);
    const bool post = ev->Get()->GetMethod() == HTTP_METHOD_POST;
    const TString program = post ? PostParam(*ev->Get(), "program") : TString();
    const TString params = post ? PostParam(*ev->Get(), "params") : TString();
    const bool compileOnly = post && PostParam(*ev->Get(), "compile") == "1";

    if (!post || !program) {
        const TString note = post ? "<p>Local query status: ERROR</p><pre>program is empty</pre>" : TString();
        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(RenderQueryForm(tabletId, schemaHtml, program, params, compileOnly, note)));
        return;
    }
    if (program.size() > MaxProgramBytes || params.size() > MaxProgramBytes) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(RenderQueryForm(
            tabletId, schemaHtml, program, params, compileOnly,
            "<p>Local query status: ERROR</p><pre>program is too large</pre>")));
        return;
    }
    if (follower) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(RenderQueryForm(
            tabletId, schemaHtml, program, params, compileOnly,
            "<p>Local query status: ERROR</p><pre>open the leader tablet to run a query</pre>")));
        return;
    }
    if (!factory) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(RenderQueryForm(
            tabletId, schemaHtml, program, params, compileOnly,
            "<p>Local query status: ERROR</p><pre>this tablet has no local MiniKQL engine</pre>")));
        return;
    }

    ctx.Register(new TLocalQueryReply(ev->Sender, ctx.SelfID, tabletId, program, params, compileOnly, schemaHtml));
}

} // namespace NKikimr::NTabletFlatExecutor
