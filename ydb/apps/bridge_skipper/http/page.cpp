#include "page.h"

#include <library/cpp/json/json_writer.h>
#include <util/stream/str.h>
#include <util/string/cast.h>

namespace NBridgeSkipper {
namespace {

using TJson = NJson::TJsonValue;

TString Escape(TStringBuf value) {
    TString result;
    for (const char c : value) {
        switch (c) {
            case '&': result += "&amp;"; break;
            case '<': result += "&lt;"; break;
            case '>': result += "&gt;"; break;
            case '"': result += "&quot;"; break;
            case '\'': result += "&#39;"; break;
            default: result += c; break;
        }
    }
    return result;
}

void Action(TStringStream& html, const TJson& card, TStringBuf action, size_t index, bool main) {
    const auto& availability = card["actions"][action];
    const TString reasonId = "reason-" + ::ToString(index) + "-" + TString(action) + (main ? "-main" : "");
    const bool available = availability["available"].GetBoolean();
    html << "<span class=\"action-wrap\"" << (available ? "" : " tabindex=\"0\"")
        << " title=\"" << Escape(availability["reason"].GetString()) << "\">"
        << "<button type=\"button\" data-action=\"" << Escape(action) << "\" data-pile-name=\""
        << Escape(card["name"].GetString()) << "\" aria-describedby=\"" << reasonId << "\""
        << (available ? "" : " disabled") << (main ? " class=\"primary\"" : "") << ">"
        << Escape(availability["label"].GetString()) << "</button><small class=\"action-reason\" id=\""
        << reasonId << "\">" << (available ? "" : Escape(availability["reason"].GetString())) << "</small></span>";
}

void Card(TStringStream& html, const TJson& card, size_t index) {
    const auto state = card["state"].GetString();
    html << "<article class=\"pile-card" << (card["incident"].GetBoolean() ? " failed" : "")
        << "\" data-testid=\"pile-card\" data-pile-name=\"" << Escape(card["name"].GetString()) << "\">"
        << "<div class=\"card-heading\"><span class=\"alert-icon\" aria-hidden=\"true\"" << (card["incident"].GetBoolean() ? "" : " hidden") << ">!</span><h2 class=\"pile-name\">" << Escape(card["display_name"].GetString())
        << "</h2><span class=\"state-badge" << (state == "NOT_SYNCHRONIZED" ? " syncing" : "") << "\">"
        << Escape(state) << "</span></div><p class=\"health\">" << Escape(card["health_text"].GetString()) << "</p>"
        << "<p class=\"last-known metadata\">";
    if (card["assessment"].GetString() != "available" && !card["last_known_verdict"].IsNull()) {
        html << "Last known: " << Escape(card["last_known_verdict"].GetString());
    }
    html << "</p><p class=\"evidence-age metadata\">" << Escape(card["evidence_age_text"].GetString())
        << "</p><p class=\"proposed metadata\">" << Escape(card["proposed"].GetString())
        << "</p><div class=\"card-actions\"><span class=\"context-action\">";
    if (!card["context_action"].IsNull()) {
        Action(html, card, card["context_action"].GetString(), index, true);
    }
    html << "</span><button type=\"button\" data-details=\"" << Escape(card["name"].GetString())
        << "\">Details</button><details class=\"more\"><summary>More</summary><div class=\"more-menu\">";
    for (const TStringBuf action : {"switchover", "failover", "take_offline", "rejoin"}) {
        Action(html, card, action, index, false);
    }
    html << "</div></details></div></article>";
}

TString RetentionCount(const TJson& logs, TStringBuf field, TStringBuf label) {
    const auto& value = logs[TString(field)];
    if (value.IsUInteger() || (value.IsInteger() && value.GetInteger() >= 0)) {
        return TString(label) + " " + ::ToString(value.GetUInteger());
    }
    return TString(label) + " count unavailable";
}

TString ScriptJson(const TJson& value) {
    // JSON is embedded as inert data. Escape HTML-significant characters even
    // inside quoted JSON strings so a pile name cannot terminate the script.
    TString result;
    for (const char c : NJson::WriteJson(value, false, true)) {
        if (c == '<') {
            result += "\\u003c";
        } else if (c == '>') {
            result += "\\u003e";
        } else if (c == '&') {
            result += "\\u0026";
        } else {
            result += c;
        }
    }
    return result;
}

constexpr TStringBuf Css = R"css(
:root{color-scheme:light dark;--bg:#f5f7fa;--surface:#fff;--text:#172033;--muted:#536078;--border:#dde3ec;--action:#2457d6;--red:#c73545;--incident:#fdecee;--incident-text:#9f2030;--soft:#eef3fc;--shadow:0 8px 30px #17203320;font:14px/1.4 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
@media(prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#111827;--surface:#1b2435;--text:#e7edf8;--muted:#acb9cf;--border:#39465d;--action:#8db1ff;--red:#f17a86;--incident:#38222c;--incident-text:#ffabb4;--soft:#26334d;--shadow:0 8px 30px #0008}}
:root[data-theme=light]{color-scheme:light}
:root[data-theme=dark]{color-scheme:dark;--bg:#111827;--surface:#1b2435;--text:#e7edf8;--muted:#acb9cf;--border:#39465d;--action:#8db1ff;--red:#f17a86;--incident:#38222c;--incident-text:#ffabb4;--soft:#26334d;--shadow:0 8px 30px #0008}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text)}button,input,select,summary{font:inherit}button,select,input{color:var(--text)}button,select{border:1px solid var(--border);border-radius:6px;padding:6px 10px;background:var(--surface)}button,summary,select{cursor:pointer}button:hover:enabled,summary:hover{border-color:var(--action)}button:disabled{cursor:not-allowed;opacity:.55}button.primary{background:var(--action);color:var(--surface);border-color:var(--action)}a{color:var(--action)}:focus-visible{outline:3px solid var(--action);outline-offset:3px}.container{width:calc(100% - 40px);max-width:1520px;margin:auto;min-width:0}.page-header{background:var(--surface);border-bottom:1px solid var(--border)}.header-inner{padding:14px 0;display:flex;gap:16px;align-items:center;flex-wrap:wrap}.brand{flex:1;min-width:180px}.brand h1{font-size:20px;margin:0}.brand p{margin:2px 0 0;color:var(--muted);overflow-wrap:anywhere}.mode-controls{display:flex;gap:18px;align-items:center;flex-wrap:wrap}.mode-controls label{display:flex;gap:6px;align-items:center}.mode-controls input{accent-color:var(--action);width:18px;height:18px}.effective-mode{flex-basis:100%;font-size:12px;color:var(--muted);margin:0}.observation-row{display:flex;align-items:center;gap:8px 18px;flex-wrap:wrap;padding:12px 0;color:var(--muted);font-size:12px}.observation-row .spacer{flex:1}.incident-strip{padding:12px 16px;border:1px solid var(--border);border-radius:8px;background:var(--surface);margin-bottom:14px;overflow-wrap:anywhere}.incident-strip.incident{border-color:#d55b68;background:var(--incident)}.incident-strip h2{font-size:15px;margin:0 0 3px}.incident-strip.incident h2{color:var(--incident-text)}.incident-strip p{margin:3px 0}.strip-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}.status-message{overflow-wrap:anywhere;margin:8px 0}.status-message:empty{display:none}.error{color:var(--incident-text);background:var(--incident);border:1px solid var(--red);border-radius:6px;padding:10px}.pile-grid{display:grid;grid-template-columns:minmax(0,1fr);gap:14px}.pile-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;min-width:0;padding:14px 16px;position:relative}.pile-card.failed{border:2px solid var(--red);padding:13px 15px}.card-heading{display:flex;align-items:flex-start;gap:10px;flex-wrap:wrap}.alert-icon{color:var(--red);border:1px solid currentColor;border-radius:50%;width:18px;height:18px;text-align:center;font-size:12px;font-weight:700;flex-shrink:0}.alert-icon[hidden]{display:none}.pile-name{white-space:pre-wrap;font-size:20px;line-height:1.2;margin:0;flex:1;overflow-wrap:anywhere;min-width:0}.state-badge{font-size:11px;letter-spacing:.03em;padding:3px 7px;background:var(--soft);border-radius:4px;overflow-wrap:anywhere;max-width:100%}.state-badge.syncing{color:#8d5a09;background:#fff0ce}.health{font-weight:600;margin:8px 0 3px}.failed .health{color:var(--red)}.metadata{font-size:12px;color:var(--muted);margin:2px 0;overflow-wrap:anywhere}.metadata:empty{display:none}.proposed{color:var(--action)}.card-actions{display:flex;align-items:flex-start;gap:8px;flex-wrap:wrap;margin-top:10px}.context-action:empty{display:none}.action-wrap[hidden]{display:none}.action-wrap{display:inline-flex;flex-direction:column;min-width:0}.action-reason{font-size:11px;color:var(--muted);max-width:240px;overflow-wrap:anywhere}.action-reason:empty{display:none}.context-action .action-reason{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap}.more{position:relative}.more summary{border:1px solid var(--border);padding:6px 10px;border-radius:6px;list-style:none}.more summary::-webkit-details-marker{display:none}.more-menu{z-index:3;position:absolute;right:0;top:100%;margin-top:5px;min-width:230px;max-width:calc(100vw - 64px);padding:10px;display:grid;gap:10px;background:var(--surface);border:1px solid var(--border);border-radius:8px;box-shadow:var(--shadow)}section.history,section.logs{margin-top:18px}section h2{font-size:16px;margin:0}section .section-heading{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:7px}.section-heading h2{flex:1}.history-table{width:100%;border-collapse:collapse;font-size:12px;table-layout:fixed}.history-table th{text-align:left;color:var(--muted);font-weight:500}.history-table th,.history-table td{padding:6px 8px;border-bottom:1px solid var(--border);overflow-wrap:anywhere;vertical-align:top}.history-table th:first-child{width:21%}.history-table th:nth-child(2){width:18%}.history-table th:nth-child(4){width:15%}.history-empty{font-size:12px;color:var(--muted)}.logs{padding-bottom:24px}.logs-toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.logs-toolbar input[type=search]{background:var(--surface);border:1px solid var(--border);padding:6px 8px;border-radius:6px;min-width:80px;max-width:100%;flex:1}.logs-toolbar label{display:flex;align-items:center;gap:5px}.logs-tail{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 10px;height:180px;overflow:auto;overflow-wrap:anywhere;margin-top:8px;font:12px/1.5 ui-monospace,SFMono-Regular,Consolas,monospace;white-space:pre-wrap}.logs-tail.expanded{height:60vh}.log-row{padding:2px 0}.log-row time{color:var(--muted)}.log-row summary{display:inline;white-space:pre-wrap}.log-row pre{white-space:pre-wrap;overflow-wrap:anywhere}.log-level{font-weight:600}.coverage{font-size:12px;color:var(--muted);margin:5px 0}.warning{color:var(--incident-text)}.new-logs{font-size:12px}.new-logs[hidden]{display:none}dialog{color:var(--text);background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:0;max-width:min(760px,calc(100vw - 32px));max-height:calc(100dvh - 32px);width:760px;box-shadow:var(--shadow)}dialog::backdrop{background:#10182875}.dialog-content{padding:20px;overflow-wrap:anywhere}.dialog-content h2{margin:0 0 10px;font-size:20px}.dialog-content h3{font-size:15px}.dialog-actions{display:flex;justify-content:flex-end;gap:8px;flex-wrap:wrap;margin-top:18px}.scroll-region{overflow:auto;max-width:100%}pre{font:12px/1.5 ui-monospace,SFMono-Regular,Consolas,monospace}.exact-update{overflow:auto;max-width:100%;padding:10px;background:var(--bg);border:1px solid var(--border);border-radius:5px}.review-vector{border-collapse:collapse;width:100%;font-size:12px}.review-vector th{text-align:left}.review-vector td,.review-vector th{padding:6px;border-bottom:1px solid var(--border);overflow-wrap:anywhere}.review-vector td:first-child,.drawer h2{white-space:pre-wrap}.review-vector tr.changed{background:var(--soft)}.drawer{margin:0 0 0 auto;height:100dvh;max-height:100dvh;max-width:min(640px,100vw);border-radius:0;width:640px}.drawer pre{white-space:pre-wrap;overflow-wrap:anywhere}.drawer .close{float:right}.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}footer{font-size:12px;color:var(--muted);padding-bottom:18px}@media(min-width:760px){.pile-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(min-width:1440px){.pile-grid{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(max-width:759px){.container{width:calc(100% - 32px)}.header-inner{gap:10px}.mode-controls{gap:12px}.history-table th:first-child{width:26%}.history-table th:nth-child(2){width:20%}.dialog-content{padding:16px}.logs-toolbar{align-items:flex-start}.observation-row{gap:7px 12px}}
)css";

constexpr TStringBuf Javascript = R"js(
'use strict';
(()=>{
    const $=s=>document.querySelector(s), $$=s=>Array.from(document.querySelectorAll(s));
    let state=JSON.parse($('#initial-state').textContent), received=performance.now(), readBusy=false, readAgain=false, postBusy=0, readSequence=0, acceptedSequence=0;
    // A cancelled preview may still receive a reply; only its own dialog generation can consume it.
    let review=null, reviewFocus=null, reviewPosting=false, reviewGeneration=0, zone='UTC', logCursor='', historyExpanded=false, logNew=0, logsBusy=false;
    const cards=new Map(), logs=new Map(), timeFormats=new Map();
    let historyGeneration=0, historyBusy=false, historyReadPending=false, historyRendered=null;
    const logLimit=5000;
    let logQueryGeneration=0, logReadPending=false, logLocalGap=false, logRemoteGap=false, logPaused=false, logWindow=null;
    try{
        zone=Intl.DateTimeFormat().resolvedOptions().timeZone||'UTC'
    }catch{
    }
    const titleName=n=>n===''?'Empty pile name':n;
    const supportedActions=['switchover','failover','take_offline','rejoin'];
    const relative=ms=>{
        const n=Math.max(0,Math.floor(ms/1000));
        return n<60?n+' s ago':n<3600?Math.floor(n/60)+' min ago':Math.floor(n/3600)+' h ago'
    };
    const elapsed=()=>Math.max(0,performance.now()-received);
    const jsonText=x=>JSON.stringify(x,null,2);
    const text=(el,value)=>{
        const v=value==null?'':String(value);
        if(el.textContent!==v)el.textContent=v
    };
    const node=(tag,cls,content)=>{
        const el=document.createElement(tag);
        if(cls)el.className=cls;
        if(content!=null)el.textContent=content;
        return el
    };
    const time=raw=>{
        const el=node('time');
        if(raw){
            el.dateTime=raw;
            formatTime(el)
        }else el.textContent='Unavailable';
        return el
    };
    function zoneFormats(){
        if(!timeFormats.has(zone)){
            timeFormats.set(zone,{
                short:new Intl.DateTimeFormat(undefined,{
                    timeZone:zone,month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit',timeZoneName:'shortOffset'
                }),
                full:new Intl.DateTimeFormat(undefined,{
                    timeZone:zone,dateStyle:'full',timeStyle:'long'
                }),
                offset:new Intl.DateTimeFormat('en',{
                    timeZone:zone,timeZoneName:'longOffset'
                })
            });
        }
        return timeFormats.get(zone);
    }
    function formatTime(el){
        try{
            const date=new Date(el.dateTime),formats=zoneFormats();
            el.textContent=formats.short.format(date);
            el.title=formats.full.format(date)+' · '+zone
        }catch{
            el.textContent=el.dateTime+' UTC'
        }
    }
    function timezone(){
        try{
            const parts=zoneFormats().offset.formatToParts(new Date(state.now));
            const offset=parts.find(x=>x.type==='timeZoneName')?.value||'UTC';
            text($('#timezone'),zone+' · '+offset.replace('GMT','UTC'));
            text($('.history-table th'),'Time ['+zone+']')
        }catch{
            zone='UTC';
            text($('#timezone'),'UTC')
        }
    }
    function status(message,error=false){
        text($('#command-status'),message);
        $('#command-status').className='status-message'+(error?' error':'')
    }
    const ageAdvanced=age=>age!=null&&Math.floor((age+elapsed())/1000)!==Math.floor(age/1000);
    const evidenceExpired=e=>e?.age_ms!=null&&e.ttl_ms!=null&&e.age_ms+elapsed()>=e.ttl_ms;
    function exclusionExpired(card){
        return card.assessment==='not_assessed'&&card.state==='NOT_SYNCHRONIZED'
            &&(evidenceExpired(card.state_evidence)||evidenceExpired(card.participation_evidence));
    }
    function cardIncident(card){
        return !!card.incident||exclusionExpired(card)&&['FAILED','LOST'].includes(card.last_known_verdict);
    }
    function refreshIncident(){
        const restored=(state.cards||[]).filter(card=>exclusionExpired(card)&&cardIncident(card));
        const active=!!state.incident?.active||restored.length>0;
        const piles=[...new Set([...(state.incident?.piles||[]),...restored.map(card=>card.name)])];
        $('#incident-strip').classList.toggle('incident',active);
        text($('#incident-piles'),active?'Affected piles: '+piles.map(titleName).join(', '):'');
    }
    function healthText(card){
        const e=card.evidence||{},age=e.age_ms,ttl=e.ttl_ms,delta=elapsed();
        const exclusion=exclusionExpired(card);
        if(card.assessment==='not_assessed'&&!exclusion){
            return card.health_text;
        }
        if(age==null||ttl==null||age+delta<ttl){
            return exclusion||card.assessment==='available'&&evidenceExpired(card.state_evidence)
                ?'Assessment unavailable':card.health_text;
        }
        // Canonical copy remains unchanged on receipt. Only the clock-driven
        // expiry transition and its relative age need local wording.
        const overdue=Math.max(0,age+delta-ttl);
        if(!exclusion&&age>=ttl&&Math.floor(overdue/1000)===Math.floor((age-ttl)/1000)){
            return card.health_text;
        }
        return 'Health unknown · '+(overdue<1000?'Expired just now':'expired '+relative(overdue));
    }
    function evidenceAgeText(card){
        const age=card.evidence?.age_ms;
        return ageAdvanced(age)?'Last sample '+relative(age+elapsed()):card.evidence_age_text;
    }
    function makeAction(action,card,main=false){
        const wrap=node('span','action-wrap');
        const b=node('button',main?'primary':'',card.actions?.[action]?.label||'');
        b.type='button';
        b.dataset.action=action;
        b.dataset.pileName=card.name;
        const reason=node('small','action-reason');
        reason.id='reason-js-'+Math.random().toString(36).slice(2);
        b.setAttribute('aria-describedby',reason.id);
        wrap.append(b,reason);
        return wrap
    }
    function createCard(card){
        const el=node('article','pile-card');
        el.dataset.testid='pile-card';
        el.dataset.pileName=card.name;
        const heading=node('div','card-heading');
        const alert=node('span','alert-icon','!');
        alert.setAttribute('aria-hidden','true');
        heading.append(alert,node('h2','pile-name'),node('span','state-badge'));
        el.append(heading,node('p','health'),node('p','last-known metadata'),node('p','evidence-age metadata'),node('p','proposed metadata'));
        const actions=node('div','card-actions');
        actions.append(node('span','context-action'));
        const d=node('button',null,'Details');
        d.type='button';
        d.dataset.details=card.name;
        actions.append(d);
        const more=node('details','more');
        more.append(node('summary',null,'More'));
        const menu=node('div','more-menu');
        for(const a of supportedActions)menu.append(makeAction(a,card));
        more.append(menu);
        actions.append(more);
        el.append(actions);
        return el
    }
    function updateCard(el,card){
        const incident=cardIncident(card);
        el.classList.toggle('failed',incident);
        el.querySelector('.alert-icon').hidden=!incident;
        text(el.querySelector('.pile-name'),titleName(card.name));
        text(el.querySelector('.state-badge'),card.state);
        el.querySelector('.state-badge').classList.toggle('syncing',card.state==='NOT_SYNCHRONIZED');
        text(el.querySelector('.health'),healthText(card));
        const e=card.evidence||{
        },stale=card.assessment!=='available'||evidenceExpired(e)||evidenceExpired(card.state_evidence);
        text(el.querySelector('.last-known'),stale&&card.last_known_verdict?'Last known: '+card.last_known_verdict:'');
        text(el.querySelector('.evidence-age'),evidenceAgeText(card));
        text(el.querySelector('.proposed'),card.proposed);
        const context=el.querySelector('.context-action');
        const primary=supportedActions.includes(card.context_action)?card.context_action:null;
        if((context.firstElementChild?.querySelector('button')?.dataset.action||null)!==primary){
            context.replaceChildren();
            if(primary)context.append(makeAction(primary,card,true))
        }for(const b of el.querySelectorAll('[data-action]')){
            const a=(card.actions||{
            })[b.dataset.action]||{
                available:false,reason:'Assessment unavailable'
            };
            // Rejoin uses current participant evidence, not the disconnected target's retained health.
            const expired=a.available&&(evidenceExpired(card.state_evidence)
                ||b.dataset.action!=='rejoin'&&evidenceExpired(e));
            b.disabled=!a.available||expired;
            text(b,a.label||'');
            const reason=b.parentNode.querySelector('.action-reason');
            text(reason,b.disabled?(expired?'Fresh coherent observations required':a.reason):'');
            b.parentNode.title=b.disabled?reason.textContent:'';
            if(b.disabled)b.parentNode.tabIndex=0;
            else b.parentNode.removeAttribute('tabindex')
        }
    }
    function updateCards(){
        $('#pile-empty').hidden=!!(state.cards||[]).length;
        const names=new Set();
        for(const card of state.cards||[]){
            names.add(card.name);
            let el=cards.get(card.name);
            if(!el){
                el=createCard(card);
                cards.set(card.name,el);
                $('#pile-grid').append(el)
            }updateCard(el,card)
        }for(const [name,el]of cards)if(!names.has(name)){
            el.remove();
            cards.delete(name)
        }const ordered=(state.cards||[]).map(card=>card.name);
        for(let i=0;i<ordered.length;i++){
            const el=cards.get(ordered[i]);
            if($('#pile-grid').children[i]!==el)$('#pile-grid').insertBefore(el,$('#pile-grid').children[i]||null)
        }
    }
    function refreshFailoverAction(){
        const action=state.failover_action;
        const expired=action.available&&evidenceExpired((state.view||state.last_observed_view)?.evidence);
        const disabled=!action.available||expired;
        const reason=disabled?(expired?'Fresh coherent observations required':action.reason):'';
        $('#review-failover').disabled=disabled;
        text($('#review-failover'),action.label);
        text($('#review-failover-reason'),reason);
        $('#review-failover-action').title=reason;
        if(disabled){
            $('#review-failover-action').tabIndex=0;
        }else{
            $('#review-failover-action').removeAttribute('tabindex');
        }
    }
    function refreshAges(){
        const view=state.view||state.last_observed_view;
        const age=view?.age_ms;
        const ttl=view?.evidence?.ttl_ms||state.evidence_ttl_ms||30000;
        const viewExpired=age!=null&&age<ttl&&age+elapsed()>=ttl;
        text($('#state-age'),ageAdvanced(age)||viewExpired?(age+elapsed()>=ttl?'State stale · ':state.view?'State: ':'Last state: ')+relative(age+elapsed()):state.state_age_text);
        text($('#loop-age'),ageAdvanced(state.loop_age_ms)?'Loop: '+relative(state.loop_age_ms+elapsed()):state.loop_age_text);
        for(const card of state.cards||[])if(cards.has(card.name))updateCard(cards.get(card.name),card);
        refreshIncident();
        refreshFailoverAction();
        if(review?.plan&&review.expiresMs!=null&&performance.now()>=review.expiresMs){
            $('#review-execute').disabled=true;
            text($('#review-status'),'This review expired. Refresh the plan before executing.')
        }
    }
    function renderHistory(data){
        if(!data)return;
        const rows=data.rows||[],boot=data.boot_id||state.controller_boot_id;
        const key=boot&&typeof data.next_cursor==='string'&&data.next_cursor
            ?JSON.stringify([boot,data.next_cursor,rows.length,!!data.coverage_gap,historyExpanded,zone,state.started_at]):null;
        if(key&&key===historyRendered)return;
        historyRendered=key;
        const table=$('#history-rows');
        table.replaceChildren();
        for(const row of historyExpanded?rows:rows.slice(-3)){
            const tr=node('tr');
            const stamp=node('td');
            stamp.append(time(row.at||row.timestamp));
            tr.append(stamp,node('td',null,row.generation||row.expected_generation||row.details?.generation||'—'),node('td',null,row.summary||''),node('td',null,row.origin_label||''),node('td',null,row.status||row.details?.status||''));
            table.append(tr)
        }$('#history-empty').hidden=!!rows.length;
        text($('#history-coverage'),data.coverage_gap?'History coverage gap: older or previous-boot events are unavailable.':'');
        if(!rows.length&&state.started_at){
            $('#history-empty').replaceChildren(document.createTextNode('No transitions recorded since '),time(state.started_at),document.createTextNode('.'))
        }
    }
    async function fetchHistory(){
        if(!historyExpanded)return;
        if(historyBusy){
            historyReadPending=true;
            return;
        }
        historyBusy=true;
        historyReadPending=false;
        const generation=historyGeneration,boot=state.controller_boot_id;
        try{
            const response=await fetch('/api/history',{cache:'no-store'});
            if(!response.ok)return;
            const data=await response.json();
            if(!historyExpanded||generation!==historyGeneration||boot!==state.controller_boot_id
                ||data.boot_id&&data.boot_id!==boot)return;
            renderHistory(data);
        }catch{
        }finally{
            historyBusy=false;
            if(historyExpanded&&historyReadPending)fetchHistory();
        }
    }
    function pinnedLogRows(){
        const tail=$('#logs-tail'), pinned=new Set();
        const selection=window.getSelection(), ranges=[];
        if(selection&&!selection.isCollapsed){
            for(let index=0;index<selection.rangeCount;index++){
                const range=selection.getRangeAt(index);
                if(range.intersectsNode(tail))ranges.push(range);
            }
        }
        for(const element of logs.values()){
            if(ranges.some(range=>range.intersectsNode(element)))pinned.add(element);
        }
        const focused=document.activeElement?.closest('.log-row');
        if(focused&&tail.contains(focused))pinned.add(focused);
        return pinned;
    }
    function logCursorParts(value){
        const cursor=String(value||''),split=cursor.lastIndexOf(':'),suffix=cursor.slice(split+1);
        return split>=0&&/^\d+$/.test(suffix)?{boot:cursor.slice(0,split),sequence:BigInt(suffix)}:null;
    }
    function advanceLogWindow(data){
        if(!Number.isSafeInteger(data.retained_count)||data.retained_count<0)return;
        const next=logCursorParts(data.next_cursor),boot=data.boot_id||state.controller_boot_id;
        const sequence=data.retained_count===0?0n:next?.boot===boot?next.sequence:null;
        if(sequence!==null&&sequence>=BigInt(data.retained_count)){
            logWindow={boot,oldest:sequence-BigInt(data.retained_count)+1n};
        }
    }
    function inLogWindow(key){
        const cursor=logCursorParts(key);
        return !logWindow||!cursor||cursor.boot===logWindow.boot&&cursor.sequence>=logWindow.oldest;
    }
    function logRetention(data){
        const counts=[['retained_count','Retained'],['dropped_count','dropped'],['total_count','total']].map(([field,label])=>{
            const count=data[field];
            return Number.isInteger(count)&&count>=0?label+' '+count.toLocaleString():label+' count unavailable';
        });
        const line=$('#log-retention');
        line.replaceChildren(document.createTextNode('Captured ring: '+counts.join(' · ')+' · '));
        if(data.oldest_at)line.append(document.createTextNode('oldest '),time(data.oldest_at));
        else line.append(document.createTextNode('Oldest time unavailable'));
    }
    function logCoverage(){
        const messages=[];
        if(logRemoteGap)messages.push('Log coverage gap: some older or previous-boot records are unavailable.');
        if(logLocalGap)messages.push('Browser coverage gap: some retained records are omitted by the 5,000-row display limit.');
        if(Array.from(logs).some(([key,element])=>!element.hidden&&!inLogWindow(key)))messages.push('Browser coverage gap: pinned older records remain visible outside the server retained-log export window.');
        if(logPaused)messages.push('Log display paused: all 5,000 retained rows are selected or focused. Clear the selection to resume; new rows remain available in retained-log export.');
        text($('#log-coverage'),messages.join(' '));
    }
    function createLogRow(row){
        const element=node('div','log-row');
        element.append(time(row.at||row.timestamp),document.createTextNode(' '),
            node('span','log-level',(row.level||'info').toUpperCase()),document.createTextNode(' '));
        const details=node('details');
        const label=row.summary??row.message??'Log event';
        details.append(node('summary',null,label),node('pre',null,jsonText(row)));
        element.append(details);
        element._record=row;
        return element;
    }
    function appendLogs(data,full=false){
        if(!data)return true;
        if(data.boot_id&&data.boot_id!==state.controller_boot_id)return false;
        advanceLogWindow(data);
        const tail=$('#logs-tail'), pinned=pinnedLogRows(), incoming=new Map();
        for(const row of data.rows||[]){
            const key=String(row.id||row.cursor||'');
            if(key&&inLogWindow(key))incoming.set(key,row);
        }
        const ordered=[],orderedKeys=new Set();
        for(const element of tail.children){
            const key=element.dataset.logId;
            if(pinned.has(element)||!full&&!element.hidden&&inLogWindow(key)){
                if(!full||!incoming.has(key)){
                    ordered.push({key,element,row:element._record});
                    orderedKeys.add(key);
                }
            }
        }
        for(const [key,row]of incoming){
            const element=logs.get(key);
            if(element)element.hidden=false;
            if(full||!orderedKeys.has(key))ordered.push({key,element,row});
        }
        const retained=new Set();
        for(const entry of ordered)if(entry.element&&pinned.has(entry.element))retained.add(entry.key);
        for(let index=ordered.length-1;index>=0&&retained.size<logLimit;index--)retained.add(ordered[index].key);
        const blocked=pinned.size>=logLimit&&Array.from(incoming.keys()).some(key=>!retained.has(key));
        if(ordered.length>retained.size)logLocalGap=true;
        if(blocked)logPaused=true;
        for(const [key,element]of logs){
            if(!retained.has(key)){
                element.remove();
                logs.delete(key);
            }
        }
        // Reuse ordered server rows around unchanged selected/focused anchors.
        let sibling=null,appended=0;
        for(let index=ordered.length-1;index>=0;index--){
            const entry=ordered[index];
            if(!retained.has(entry.key))continue;
            let element=entry.element;
            if(!element){
                element=createLogRow(entry.row);
                element.dataset.logId=entry.key;
                tail.insertBefore(element,sibling);
                logs.set(entry.key,element);
                appended++;
            }
            sibling=element;
        }
        if($('#log-follow').checked)tail.scrollTop=tail.scrollHeight;
        else if(appended){
            logNew+=appended;
            $('#new-logs').hidden=false;
            text($('#new-log-count'),logNew+' new entries');
        }
        logRemoteGap ||= !!data.coverage_gap;
        logRetention(data);
        logCoverage();
        const capture=data.capture_level||'info';
        text($('#capture-level'),'Capture: '+capture+'.'+(['debug','trace'].includes(capture)?'':' Debug filtering unavailable; restart with debug capture to collect Debug.'));
        $('#log-level option[value=debug]').disabled=!['debug','trace'].includes(capture);
        return !blocked;
    }
    function apply(next,sequence){
        if(sequence<acceptedSequence)return;
        if(next.controller_boot_id===state.controller_boot_id&&BigInt(next.mode_revision||'0')<BigInt(state.mode_revision||'0'))return;
        acceptedSequence=sequence;
        const bootChanged=next.controller_boot_id!==state.controller_boot_id;
        state=next;
        if(bootChanged){
            logWindow=null;
            resetLogQuery({discardPrevious:true});
            fetchLogs();
            ++historyGeneration;
            historyReadPending=historyExpanded;
        }
        received=performance.now();
        $('#manual-mode').checked=state.mode==='manual';
        $('#auto-mode').checked=state.mode==='auto';
        text($('#effective-mode'),state.mode_text);
        text($('#cluster-id'),state.cluster_id);
        text($('#controller-status'),state.controller_ready?'Controller connected':'Controller unavailable');
        text($('#generation'),'Generation '+((state.view||state.last_observed_view)?.generation||'unavailable'));
        $('#receipt-time').replaceChildren(document.createTextNode('Last receipt '),time(state.now));
        text($('#decision-title'),state.decision_text);
        text($('#decision-reason'),state.decision.reason);
        text($('#pending-status'),state.pending_mutation?state.pending_mutation.message+' '+(state.retry_message||''):'');
        text($('#operation-status'),state.operation_message||'');
        $('#operation-details').hidden=!state.last_mutation&&!state.pending_mutation;
        text($('#operation-json'),jsonText(state.operation_details));
        text($('#save-warning'),state.save_warning||'');
        $('#save-warning').hidden=!state.save_warning;
        $('#review-failover-action').hidden=!state.decision.plan;
        $('#inspect-plan').hidden=!state.decision.plan;
        updateCards();
        if(historyExpanded)fetchHistory();
        else renderHistory(state.history);
        timezone();
        refreshAges();
        if(review&&(review.boot!==state.controller_boot_id||review.modeRevision!==state.mode_revision
            ||review.mode!=='manual'&&review.snapshot!==state.snapshot_revision)){
            review.stale=true;
            $('#review-execute').disabled=true;
            text($('#review-status'),review.failure||'Relevant state changed. Refresh the plan before executing.')
        }
    }
    async function readState(){
        if(readBusy){
            readAgain=true;
            return
        }readBusy=true;
        const seq=++readSequence;
        try{
            const r=await fetch('/api/state',{
                cache:'no-store'
            });
            if(!r.ok)throw Error('State request failed ('+r.status+')');
            apply(await r.json(),seq)
        }catch(e){
            text($('#controller-status'),'Connection unavailable: '+e.message)
        }finally{
            readBusy=false;
            if(readAgain){
                readAgain=false;
                readState()
            }
        }
    }
    async function post(path,body){
        postBusy++;
        try{
            const r=await fetch(path,{
                method:'POST',headers:{
                    'Content-Type':'application/json'
                },body:JSON.stringify(body)
            });
            let data;
            try{
                data=await r.json()
            }catch{
                data={
                    error:'INVALID_RESPONSE',reason:'Command response unavailable'
                }
            }if(!r.ok){
                const error=Error((data.error||data.code||'COMMAND_FAILED')+': '+(data.reason||data.message||'Reconcile current state before another reviewed command.'));
                error.data=data;
                throw error
            }return data
        }finally{
            postBusy--;
            await readState()
        }
    }
    const requestId=()=>crypto.randomUUID?crypto.randomUUID():state.controller_boot_id+'-'+Date.now()+'-'+Math.random().toString(36).slice(2);
    function openDialog(){
        const dialog=$('#review-dialog');
        if(!dialog.open){
            reviewFocus=document.activeElement;
            dialog.showModal();
        }
        $('#review-cancel').focus()
    }
    function closeReview(){
        if(reviewPosting)return;
        ++reviewGeneration;
        $('#review-dialog').close();
        review=null;
        reviewFocus?.focus()
    }
    function reviewTable(before,after){
        const table=node('table','review-vector');
        const head=node('tr');
        for(const h of['Pile (exact name)','Before','Submitted / after overlay'])head.append(node('th',null,h));
        table.append(head);
        for(const name of Object.keys(before||{
        }).sort()){
            const tr=node('tr',before[name]!==after?.[name]?'changed':'');
            tr.append(node('td',null,titleName(name)),node('td',null,before[name]),node('td',null,after?.[name]??before[name]));
            table.append(tr)
        }return table
    }
    function reviewHeader(title){
        ++reviewGeneration;
        review=null;
        text($('#review-title'),title);
        $('#review-body').replaceChildren();
        text($('#review-status'),'');
        $('#review-execute').disabled=false;
        text($('#review-execute'),'Execute');
        $('#refresh-plan').hidden=true
    }
    function modeReview(mode){
        if(mode===state.mode){
            closeReview();
            status(mode[0].toUpperCase()+mode.slice(1)+' mode is already active.');
            return;
        }
        reviewHeader(mode==='auto'?'Enable Auto mode':'Switch to Manual mode');
        review={
            mode,snapshot:state.snapshot_revision,modeRevision:state.mode_revision,boot:state.controller_boot_id
        };
        const body=$('#review-body');
        body.append(node('p',null,mode==='auto'?state.auto_activation_text:'Switch modes atomically. Each Manual approval authorizes one handoff.'));
        body.append(node('p',null,state.decision.kind+' · '+state.decision.reason));
        if(state.decision.plan){
            body.append(node('h3',null,'Current Ready plan'),node('pre','exact-update',jsonText(state.decision.plan)))
        }
        if(state.pending_mutation)body.append(node('p',null,state.pending_mutation.message));
        text($('#review-execute'),mode==='auto'?'Enable Auto':'Enable Manual');
        $('#refresh-plan').hidden=false;
        openDialog();
    }
    async function changeMode(mode){
        if(mode===state.mode)return;
        $('#manual-mode').checked=state.mode==='manual';
        $('#auto-mode').checked=state.mode==='auto';
        if(mode==='auto'||mode!=='observe'&&state.mode!=='observe'){
            modeReview(mode);
            return;
        }
        await sendMode(mode,state.snapshot_revision,state.mode_revision,state.controller_boot_id)
    }
    async function sendMode(mode,snapshot,revision,boot){
        try{
            await post('/api/mode',{
                mode,expected_boot_id:boot,expected_mode_revision:revision,reviewed_snapshot_revision:snapshot,request_id:requestId()
            });
            status('Mode command acknowledged. Remote requests already handed off may still complete.');
            return null
        }catch(e){
            status(e.message+' No automatic repeat was sent.',true);
            return e
        }
    }
    async function createReview(action,target){
        reviewHeader('Review '+(action==='take_offline'?'take offline':action));
        const generation=reviewGeneration;
        $('#review-execute').disabled=true;
        $('#review-body').append(node('p',null,'Preparing a fresh reviewed plan…'));
        openDialog();
        try{
            const snapshot=state.snapshot_revision;
            const body={
                action,snapshot_revision:snapshot
            };
            if(target!=null)body.target_pile=target;
            const data=await post('/api/plans',body);
            if(generation!==reviewGeneration||!$('#review-dialog').open)return;
            review={
                plan:data,action,target,snapshot,modeRevision:data.mode_revision||state.mode_revision,boot:state.controller_boot_id,stale:state.snapshot_revision!==snapshot
            };
            const expires=Date.parse(data.expires_at)-Date.parse(state.now);
            review.expiresMs=Number.isFinite(expires)?performance.now()+Math.max(0,expires):performance.now();
            const content=$('#review-body');
            content.replaceChildren();
            content.append(node('p',null,'Cluster '+(data.cluster_id||state.cluster_id)+' · expected generation '+data.plan.expected_generation));
            content.append(node('p',null,data.reason||state.decision.reason));
            const tableWrap=node('div','scroll-region');
            tableWrap.append(reviewTable(data.before||{
            },data.after||{
            }));
            content.append(tableWrap);
            content.append(node('p',null,'Endpoint: '+data.plan.endpoint));
            content.append(node('p',null,'Zero data loss. Application availability is not guaranteed. Verify it after the observed transition.'));
            let availability='Alternate synchronized-pile availability unavailable.';
            if(Array.isArray(data.alternate_synchronized_pile_names)){
                const alternates=data.alternate_synchronized_pile_names.map(titleName);
                availability=alternates.length?'Alternate synchronized piles after this change: '+alternates.join(', '):'After this change: no alternate synchronized pile remains for failover.';
            }
            content.append(node('p',null,availability));
            content.append(node('p',null,action==='failover'?'All failed piles are included in one guarded request. Disconnected piles leave data and config participation; unlisted piles retain their exact internal state.':action==='switchover'?'The synchronized target is submitted as PROMOTED; participation is unchanged. YDB later commits PRIMARY and demotes the old primary to SYNCHRONIZED.':action==='take_offline'?'The target leaves data participation as SUSPENDED but remains required for config commits until the YDB follow-up disconnects it.':'Rejoin submits NOT_SYNCHRONIZED at stage 1. The pile starts outside config and data participation; YDB controls later synchronization stages.'));
            if(action!=='failover')content.append(node('p',null,'Role changes can restart confirmation and BSC health warm-up. This unresolved lifecycle request blocks incompatible failover at its generation.'));
            if(state.last_mutation)content.append(node('p',null,'Latest recorded attempt status: '+(state.last_mutation.status||'in flight')+' · '+(state.last_mutation.outcome||'awaiting outcome')));
            if(data.pending_mutation)content.append(node('p',null,'Pending interaction: '+jsonText(data.pending_mutation)));
            const evidence=node('details');
            evidence.append(node('summary',null,'Evidence and reasons'),node('pre','exact-update',jsonText(data.evidence||{
                decision:state.decision,piles:state.piles
            })));
            content.append(evidence);
            const exact=node('details');
            exact.append(node('summary',null,'Exact update / Copy CLI'),node('pre','exact-update',jsonText(data.plan)));
            if(data.cli_command){
                exact.append(node('pre','exact-update',data.cli_command));
                const copy=node('button',null,'Copy CLI');
                copy.type='button';
                copy.addEventListener('click',()=>copyText(data.cli_command));
                exact.append(copy,node('p','metadata','Use exclusive administration with automatic dispatch off before external CLI execution. CLI execution is outside Skipper pending tracking.'))
            }content.append(exact);
            const expiry=node('p','metadata');
            expiry.append(document.createTextNode('Review expires '),time(data.expires_at));
            content.append(expiry);
            $('#review-execute').disabled=review.stale||state.mode!=='manual';
            text($('#review-execute'),'Execute '+(action==='take_offline'?'take offline':action));
            $('#refresh-plan').hidden=false;
            if(review.stale)text($('#review-status'),'Relevant state changed. Refresh the plan before executing.')
        }catch(e){
            if(generation!==reviewGeneration||!$('#review-dialog').open)return;
            text($('#review-status'),e.message);
            $('#review-execute').disabled=true;
            review=null
        }
    }
    async function executeReview(){
        if(!review||review.stale||reviewPosting)return;
        const executing=review;
        const generation=reviewGeneration;
        const stillCurrent=()=>generation===reviewGeneration&&review===executing&&$('#review-dialog').open;
        reviewPosting=true;
        $('#review-execute').disabled=true;
        $('#refresh-plan').disabled=true;
        try{
            if(executing.mode){
                const error=await sendMode(executing.mode,executing.snapshot,executing.modeRevision,executing.boot);
                if(error){
                    throw error
                }
                if(stillCurrent()){
                    ++reviewGeneration;
                    $('#review-dialog').close();
                    review=null;
                }
            }else{
                const data=await post('/api/operations',{
                    plan_id:executing.plan.plan_id,request_id:requestId()
                });
                status('Request handed off as '+data.operation_id+'. It may still complete.');
                if(stillCurrent()){
                    text($('#review-status'),'Handed off as '+data.operation_id+'. This is not RPC success or observed completion.');
                    ++reviewGeneration;
                    $('#review-dialog').close();
                    review=null;
                }
            }
        }catch(e){
            if(stillCurrent()){
                executing.stale=true;
                executing.failure=e.message+' No automatic repeat was sent. Obtain a new review before another attempt.';
                text($('#review-status'),executing.failure);
            }
            status(e.message,true);
        }finally{
            reviewPosting=false;
            $('#refresh-plan').disabled=false;
        }
    }
    async function copyText(value){
        try{
            await navigator.clipboard.writeText(value);
            status('Copied.')
        }catch{
            const el=node('textarea');
            el.value=value;
            document.body.append(el);
            el.select();
            document.execCommand('copy');
            el.remove();
            status('Copied.')
        }
    }
    function showDetails(name){
        const card=(state.cards||[]).find(c=>c.name===name);
        if(!card)return;
        const currentPile=(state.piles||[]).find(pile=>pile.name===name)||null;
        const lastKnownPile=(state.last_known_piles||[]).find(pile=>pile.name===name)||null;
        const endpointUris=new Set(card.endpoint_uris||[]);
        const endpoints=(state.endpoints||[]).filter(endpoint=>endpointUris.has(endpoint.uri));
        reviewFocus=document.activeElement;
        const content=$('#evidence-content');
        content.replaceChildren();
        content.append(node('h2',null,titleName(name)),node('p',null,card.state+' · '+healthText(card)));
        const assessment=(label,value)=>{
            content.append(node('h3',null,label));
            if(!value){
                content.append(node('p',null,'Assessment unavailable'));
                return;
            }
            content.append(node('p',null,value.verdict+' · '+value.assessment));
            for(const reason of value.reasons||[])content.append(node('p',null,reason));
            for(const [label,raw]of[['Sample sent ',value.evidence?.sample_at],['Evidence expires ',value.evidence?.expires_at]]){
                const line=node('p');
                line.append(document.createTextNode(label),time(raw));
                content.append(line);
            }
            content.append(node('p','metadata',value.evidence?.age_ms==null?'Sample time unavailable':'Last sample '+relative(value.evidence.age_ms+elapsed())));
        };
        assessment('Current assessment',card.current_assessment);
        if(lastKnownPile)assessment('Last known assessment',card.last_known_assessment);
        content.append(node('p',null,'Connectivity is a reporter snapshot, not an independent storage vote. BSC/Fallback belongs to each SelfCheck reply.'));
        content.append(node('h3',null,'Endpoint observations'));
        for(const endpoint of endpoints){
            content.append(node('h4',null,endpoint.uri),node('p',null,'Identity: '+endpoint.identity+(endpoint.ignored?' · ignored':'')));
            if(endpoint.last_error)content.append(node('p','warning',endpoint.last_error));
            content.append(node('p',null,endpoint.selfcheck_mode?'SelfCheck '+endpoint.selfcheck_mode+' · sent '+relative((endpoint.selfcheck_age_ms||0)+elapsed())+(endpoint.truncated?' · truncated':''):'SelfCheck unavailable'));
            for(const note of endpoint.notes||[])content.append(node('p','metadata',note));
        }
        for(const [label,pile]of[['Current connectivity reporters',currentPile],['Last known connectivity reporters',lastKnownPile]]){
            if(!pile)continue;
            content.append(node('h3',null,label));
            for(const report of pile.connectivity||[]){
                content.append(node('p',null,'Reporter '+report.reporter_node_id+' in '+titleName(report.vantage_pile)+' · generation '+report.generation+' · '+(report.reporter_is_root?'root':'non-root')+' · '+(report.binding_in_progress?'binding in progress':'settled')));
                content.append(node('p','metadata','Static nodes '+report.connected_static_nodes+'/'+report.static_nodes+' · node quorum '+report.has_node_quorum+' · config participation '+report.in_config_quorum+' · data participation '+report.in_data_quorum+' · sent '+relative((report.age_ms||0)+elapsed())));
            }
        }
        const raw=node('details');
        raw.append(node('summary',null,'Raw diagnostic JSON (UTC)'),node('pre',null,jsonText({card,current_pile:currentPile,last_known_pile:lastKnownPile,endpoints})));
        content.append(raw);
        $('#evidence-drawer').showModal();
        $('#close-evidence').focus()
    }
    function inspectPlan(){
        reviewHeader('Inspect Ready failover');
        review=null;
        $('#review-body').append(node('pre','exact-update',jsonText(state.decision.plan)));
        if(state.decision.cli_command){
            $('#review-body').append(node('pre','exact-update',state.decision.cli_command));
            const b=node('button',null,'Copy CLI');
            b.type='button';
            b.addEventListener('click',()=>copyText(state.decision.cli_command));
            $('#review-body').append(b,node('p','metadata','Use exclusive administration with automatic dispatch off before external CLI execution. CLI execution is outside Skipper pending tracking.'))
        }$('#review-execute').disabled=true;
        openDialog()
    }
    function resetLogQuery({hidePrevious=false,discardPrevious=false}={}){
        ++logQueryGeneration;
        logCursor='';
        logReadPending=true;
        const pinned=pinnedLogRows();
        for(const [key,element]of logs){
            if(hidePrevious)element.hidden=true;
            if(!pinned.has(element)&&(hidePrevious||discardPrevious||!inLogWindow(key))){
                element.remove();
                logs.delete(key);
            }
        }
    }
    function filters(){
        const query=new URLSearchParams({
            level:$('#log-level').value,search:$('#log-search').value,pile:$('#log-pile').value
        });
        $('#download-logs').href='/api/logs/export?'+query;
        resetLogQuery({hidePrevious:true});
        fetchLogs();
    }
    async function fetchLogs(){
        if(logsBusy)return;
        if(logPaused&&logCursor!==''&&pinnedLogRows().size<logLimit)resetLogQuery();
        logsBusy=true;
        logReadPending=false;
        const generation=logQueryGeneration,after=logCursor,boot=state.controller_boot_id;
        try{
            const query=new URLSearchParams({
                after,level:$('#log-level').value,search:$('#log-search').value,pile:$('#log-pile').value
            });
            const response=await fetch('/api/logs?'+query,{cache:'no-store'});
            if(!response.ok)return;
            const data=await response.json();
            if(generation!==logQueryGeneration||boot!==state.controller_boot_id||data.boot_id&&data.boot_id!==boot)return;
            const complete=appendLogs(data,after===''||!!data.coverage_gap);
            if(complete){
                logCursor=data.next_cursor||after;
                if(after===''){
                    logPaused=false;
                    if(pinnedLogRows().size===0)logLocalGap=false;
                    logCoverage();
                }
            }
        }catch{
        }finally{
            logsBusy=false;
            if(logReadPending)fetchLogs();
        }
    }
    for(const el of $$('[data-testid=pile-card]'))cards.set(el.dataset.pileName,el);
    for(const el of $$('[data-log-id]')){
        const record=(state.logs?.rows||[]).find(r=>(r.id||r.cursor)===el.dataset.logId);
        if(record){
            el._record=record;
            logs.set(el.dataset.logId,el)
        }
    }
    document.addEventListener('click',e=>{
        const action=e.target.closest('[data-action]');
        if(action&&!action.disabled){
            createReview(action.dataset.action,action.dataset.pileName);
            return
        }const details=e.target.closest('[data-details]');
        if(details)showDetails(details.dataset.details)
    });
    $('#manual-mode').addEventListener('change',e=>changeMode(e.target.checked?'manual':'observe'));
    $('#auto-mode').addEventListener('change',e=>changeMode(e.target.checked?'auto':'observe'));
    $('#refresh').addEventListener('click',readState);
    $('#review-failover').addEventListener('click',()=>createReview('failover'));
    $('#inspect-plan').addEventListener('click',inspectPlan);
    $('#view-evidence').addEventListener('click',()=>{
        const names=state.incident?.piles||[];
        const name=names.length?names[0]:(state.cards||[])[0]?.name;
        if(name!=null)showDetails(name)
    });
    $('#copy-snapshot').addEventListener('click',()=>copyText(jsonText(state)));
    $('#review-cancel').addEventListener('click',closeReview);
    $('#review-dialog').addEventListener('cancel',e=>{
        if(reviewPosting)e.preventDefault();
        else{
            ++reviewGeneration;
            review=null;
            reviewFocus?.focus()
        }
    });
    $('#review-execute').addEventListener('click',executeReview);
    $('#review-off').addEventListener('click',()=>{
        if(state.mode!=='observe')sendMode('observe',state.snapshot_revision,state.mode_revision,state.controller_boot_id)
    });
    $('#refresh-plan').addEventListener('click',()=>{
        if(reviewPosting)return;
        if(review?.mode)modeReview(review.mode);
        else if(review?.action)createReview(review.action,review.target)
    });
    $('#close-evidence').addEventListener('click',()=>{
        $('#evidence-drawer').close();
        reviewFocus?.focus()
    });
    $('#evidence-drawer').addEventListener('cancel',()=>reviewFocus?.focus());
    $('#history-expand').addEventListener('click',()=>{
        historyExpanded=!historyExpanded;
        ++historyGeneration;
        historyReadPending=historyExpanded;
        text($('#history-expand'),historyExpanded?'Show three':'Show more');
        if(historyExpanded)fetchHistory();
        else renderHistory(state.history);
    });
    $('#logs-tail').addEventListener('scroll',()=>{
        const el=$('#logs-tail');
        if(el.scrollTop+el.clientHeight<el.scrollHeight-12)$('#log-follow').checked=false
    });
    $('#log-follow').addEventListener('change',()=>{
        if($('#log-follow').checked){
            $('#logs-tail').scrollTop=$('#logs-tail').scrollHeight;
            logNew=0;
            $('#new-logs').hidden=true
        }
    });
    $('#jump-latest').addEventListener('click',()=>{
        const selection=window.getSelection(),tail=$('#logs-tail');
        if(selection){
            for(let index=0;index<selection.rangeCount;index++){
                if(selection.getRangeAt(index).intersectsNode(tail)){
                    selection.removeAllRanges();
                    break;
                }
            }
        }
        $('#log-follow').checked=true;
        tail.scrollTop=tail.scrollHeight;
        logNew=0;
        $('#new-logs').hidden=true;
        resetLogQuery();
        fetchLogs();
    });
    for(const id of['log-level','log-pile','log-search'])$('#'+id).addEventListener('input',filters);
    $('#logs-expand').addEventListener('click',()=>{
        $('#logs-tail').classList.toggle('expanded');
        text($('#logs-expand'),$('#logs-tail').classList.contains('expanded')?'Collapse':'Expand')
    });
    function theme(value){
        if(value==='system')document.documentElement.removeAttribute('data-theme');
        else document.documentElement.dataset.theme=value;
        try{
            localStorage.setItem('skipper-theme',value)
        }catch{
        }
    }
    $('#theme-select').addEventListener('change',e=>theme(e.target.value));
    try{
        $('#theme-select').value=localStorage.getItem('skipper-theme')||'system'
    }catch{
    }theme($('#theme-select').value);
    for(const el of $$('time[datetime]'))if(el.dateTime)formatTime(el);
    appendLogs(state.logs,true);
    apply(state,0);
    setInterval(()=>{
        readState();
        fetchLogs();
        refreshAges()
    },1000);
})();
)js";

} // namespace

TString RenderPage(const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow) {
    const auto state = BuildPresentation(snapshot, now, wallNow);
    TStringStream html;
    html << "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        << "<title>Bridge Skipper · " << Escape(snapshot.ClusterId) << "</title><style>" << Css << "</style></head><body>"
        << "<header class=\"page-header\"><div class=\"container header-inner\"><div class=\"brand\"><h1>Bridge Skipper</h1><p id=\"cluster-id\">"
        << Escape(snapshot.ClusterId) << "</p></div><div class=\"mode-controls\"><label><input type=\"checkbox\" role=\"switch\" id=\"manual-mode\" data-testid=\"manual-mode\""
        << (snapshot.Mode == EMode::Manual ? " checked" : "") << ">Manual mode</label><label><input type=\"checkbox\" role=\"switch\" id=\"auto-mode\" data-testid=\"auto-mode\""
        << (snapshot.Mode == EMode::Auto ? " checked" : "") << ">Auto mode</label></div><p class=\"effective-mode\" id=\"effective-mode\" data-testid=\"effective-mode\">"
        << Escape(state["mode_text"].GetString()) << "</p></div></header><main class=\"container\"><div class=\"observation-row\"><span id=\"controller-status\">"
        << (snapshot.ControllerReady && !snapshot.Stopping ? "Controller connected" : "Controller unavailable")
        << "</span><span id=\"generation\">Generation ";
    const auto& observed = state["last_observed_view"];
    html << (observed.IsNull() ? "unavailable" : Escape(observed["generation"].GetString()))
        << "</span><span id=\"state-age\">" << Escape(state["state_age_text"].GetString())
        << "</span><span id=\"loop-age\">" << Escape(state["loop_age_text"].GetString())
        << "</span><span id=\"receipt-time\">Last receipt <time datetime=\"" << Escape(state["now"].GetString()) << "\">"
        << Escape(state["now"].GetString()) << " UTC</time></span><span class=\"spacer\"></span><button type=\"button\" id=\"refresh\">Refresh</button>"
        << "<span id=\"timezone\">UTC</span><label>Theme <select id=\"theme-select\" data-testid=\"theme-select\"><option value=\"system\">System</option><option value=\"light\">Light</option><option value=\"dark\">Dark</option></select></label></div>"
        << "<p id=\"command-status\" class=\"status-message\" role=\"status\" aria-live=\"polite\"></p><p id=\"save-warning\" class=\"error\""
        << (state["save_warning"].IsNull() ? " hidden" : "") << ">" << Escape(state["save_warning"].GetString()) << "</p>"
        << "<section class=\"incident-strip" << (state["incident"]["active"].GetBoolean() ? " incident" : "")
        << "\" id=\"incident-strip\" data-testid=\"incident-strip\" aria-label=\"Current decision\"><h2 id=\"decision-title\">"
        << Escape(state["decision_text"].GetString()) << "</h2><p id=\"incident-piles\">";
    if (state["incident"]["active"].GetBoolean()) {
        html << "Affected piles: ";
        bool first = true;
        for (const auto& pile : state["incident"]["piles"].GetArray()) {
            if (!first) {
                html << ", ";
            }
            first = false;
            html << Escape(pile.GetString().empty() ? TString("Empty pile name") : pile.GetString());
        }
    }
    html << "</p><p class=\"metadata\" id=\"decision-reason\">" << Escape(snapshot.Decision.Reason)
        << "</p><p id=\"pending-status\" class=\"metadata\">" << (state["pending_mutation"].IsNull() ? TString()
            : Escape(state["pending_mutation"]["message"].GetString() + " " + state["retry_message"].GetString()))
        << "</p><p id=\"operation-status\" class=\"metadata\">" << Escape(state["operation_message"].GetString())
        << "</p><details id=\"operation-details\"" << (state["last_mutation"].IsNull() && state["pending_mutation"].IsNull() ? " hidden" : "")
        << "><summary>Exact request and attempt status</summary><pre id=\"operation-json\" class=\"exact-update\">"
        << Escape(NJson::WriteJson(state["operation_details"], true, true)) << "</pre></details><div class=\"strip-actions\"><span class=\"action-wrap\" id=\"review-failover-action\""
        << (snapshot.Decision.Plan ? "" : " hidden") << (state["failover_action"]["available"].GetBoolean() ? "" : " tabindex=\"0\"")
        << " title=\"" << Escape(state["failover_action"]["reason"].GetString()) << "\"><button type=\"button\" id=\"review-failover\" class=\"primary\""
        << (state["failover_action"]["available"].GetBoolean() ? "" : " disabled") << " aria-describedby=\"review-failover-reason\">" << Escape(state["failover_action"]["label"].GetString()) << "</button>"
        << "<small id=\"review-failover-reason\" class=\"action-reason\">" << (state["failover_action"]["available"].GetBoolean() ? TString() : Escape(state["failover_action"]["reason"].GetString()))
        << "</small></span><button type=\"button\" id=\"inspect-plan\"" << (snapshot.Decision.Plan ? "" : " hidden")
        << ">Inspect plan / Copy CLI</button><button type=\"button\" id=\"view-evidence\">View evidence</button><button type=\"button\" id=\"copy-snapshot\">Copy snapshot</button>"
        << "<a href=\"https://github.com/ydb-platform/ydb/blob/main/ydb/apps/bridge_skipper/docs/operations.md\" target=\"_blank\" rel=\"noopener\">Recovery runbook</a></div></section>"
        << "<p id=\"pile-empty\" class=\"metadata\"" << (state["cards"].GetArray().empty() ? "" : " hidden") << ">Awaiting the first coherent pile observation.</p><div class=\"pile-grid\" id=\"pile-grid\" aria-label=\"Observed piles\">";
    size_t index = 0;
    for (const auto& card : state["cards"].GetArray()) {
        Card(html, card, index++);
    }
    html << "</div><section class=\"history\" aria-label=\"Transition history\"><div class=\"section-heading\"><h2>Transition history</h2><button type=\"button\" id=\"history-expand\">Show more</button></div>"
        << "<p id=\"history-coverage\" class=\"coverage\">"
        << (state["history"]["coverage_gap"].GetBoolean() ? "History coverage gap: older or previous-boot events are unavailable." : "")
        << "</p><p id=\"history-empty\" class=\"history-empty\""
        << (state["history"]["rows"].GetArray().empty() ? "" : " hidden") << ">No transitions recorded since <time datetime=\""
        << Escape(state["started_at"].GetString()) << "\">" << Escape(state["started_at"].GetString()) << " UTC</time>.</p>"
        << "<table class=\"history-table\"><thead><tr><th>Time [UTC]</th><th>Generation/context</th><th>Change</th><th>Origin</th><th>Status</th></tr></thead><tbody id=\"history-rows\">";
    for (const auto& row : state["history"]["rows"].GetArray()) {
        html << "<tr><td><time datetime=\"" << Escape(row["at"].GetString()) << "\">" << Escape(row["at"].GetString())
            << " UTC</time></td><td>" << Escape((row["generation"].IsString() ? row["generation"] : row["expected_generation"]).GetString()) << "</td><td>" << Escape(row["summary"].GetString())
            << "</td><td>" << Escape(row["origin_label"].GetString()) << "</td><td>" << Escape(row["status"].GetString()) << "</td></tr>";
    }
    html << "</tbody></table></section><section class=\"logs\" aria-label=\"Logs\"><div class=\"section-heading\"><h2>Logs</h2><button type=\"button\" id=\"logs-expand\">Expand</button></div>"
        << "<div class=\"logs-toolbar\"><label>Level <select id=\"log-level\"><option value=\"info\">Info+</option><option value=\"debug\">Debug+</option><option value=\"warn\">Warn+</option><option value=\"error\">Error+</option></select></label>"
        << "<label class=\"sr-only\" for=\"log-pile\">Filter logs by pile</label><input type=\"search\" id=\"log-pile\" placeholder=\"Pile filter\">"
        << "<label class=\"sr-only\" for=\"log-search\">Filter logs by text</label><input type=\"search\" id=\"log-search\" placeholder=\"Search retained logs\">"
        << "<label><input type=\"checkbox\" id=\"log-follow\" checked>Follow</label><a id=\"download-logs\" href=\"/api/logs/export?level=info\" download>Download filtered</a></div>"
        << "<p id=\"capture-level\" class=\"coverage\">Capture: " << Escape(state["logs"]["capture_level"].GetString())
        << "</p><p id=\"log-retention\" class=\"coverage\">Captured ring: "
        << Escape(RetentionCount(state["logs"], "retained_count", "Retained")) << " · "
        << Escape(RetentionCount(state["logs"], "dropped_count", "dropped")) << " · "
        << Escape(RetentionCount(state["logs"], "total_count", "total")) << " · ";
    if (const auto& oldest = state["logs"]["oldest_at"]; oldest.IsString()) {
        html << "oldest <time datetime=\"" << Escape(oldest.GetString()) << "\">" << Escape(oldest.GetString()) << " UTC</time>";
    } else {
        html << "Oldest time unavailable";
    }
    html << "</p><p id=\"log-coverage\" class=\"coverage\">"
        << (state["logs"]["coverage_gap"].GetBoolean() ? "Log coverage gap: some older or previous-boot records are unavailable." : "")
        << "</p><div class=\"logs-tail\" id=\"logs-tail\" data-testid=\"logs-tail\" tabindex=\"0\" role=\"region\" aria-label=\"Retained logs\">";
    for (const auto& row : state["logs"]["rows"].GetArray()) {
        html << "<div class=\"log-row\" data-log-id=\"" << Escape((row["id"].IsString() ? row["id"] : row["cursor"]).GetString()) << "\"><time datetime=\"" << Escape(row["at"].GetString()) << "\">"
            << Escape(row["at"].GetString()) << " UTC</time> <strong>" << Escape(row["level"].GetString()) << "</strong> "
            << "<details><summary>" << Escape(row["summary"].GetString()) << "</summary><pre>" << Escape(NJson::WriteJson(row, true, true)) << "</pre></details></div>";
    }
    html << "</div><div id=\"new-logs\" class=\"new-logs\" hidden><span id=\"new-log-count\"></span> · <button type=\"button\" id=\"jump-latest\">Jump to latest</button></div></section>"
        << "<footer>One active controller is required. Switching modes off does not cancel remote work.</footer></main>"
        << "<dialog id=\"review-dialog\" data-testid=\"review-dialog\" aria-labelledby=\"review-title\"><div class=\"dialog-content\"><h2 id=\"review-title\">Review operation</h2>"
        << "<div id=\"review-body\"></div><p id=\"review-status\" role=\"status\" aria-live=\"polite\"></p><div class=\"dialog-actions\"><button type=\"button\" id=\"refresh-plan\" hidden>Refresh plan</button>"
        << "<button type=\"button\" id=\"review-off\">Switch dispatch off</button><button type=\"button\" id=\"review-cancel\" data-testid=\"review-cancel\">Cancel</button><button type=\"button\" class=\"primary\" id=\"review-execute\" data-testid=\"review-execute\">Execute</button></div></div></dialog>"
        << "<dialog class=\"drawer\" id=\"evidence-drawer\" data-testid=\"evidence-drawer\" aria-label=\"Pile evidence\"><div class=\"dialog-content\"><button type=\"button\" class=\"close\" id=\"close-evidence\">Close</button><div id=\"evidence-content\"></div></div></dialog>"
        << "<script type=\"application/json\" id=\"initial-state\">" << ScriptJson(state) << "</script><script>" << Javascript << "</script></body></html>";
    return html.Str();
}

} // namespace NBridgeSkipper
