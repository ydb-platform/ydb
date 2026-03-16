import './async-DWBXLqlH.js';
import { c as bind_props, g as attr_style, d as attr_class, j as stringify, e as ensure_array_like, a as attr } from './index-D1re1cuM.js';
import './2-BbOIMXxe.js';
import { t as tick } from './index-server-D4bj_1UO.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import './Code.svelte_svelte_type_style_lang-B4LFIer5.js';
import { K } from './Index.svelte_svelte_type_style_lang2-unMJJ7aH.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './prism-python-B3XC7jWw.js';
import './utils.svelte-BHoyPsmo.js';
import './Check-BB0k-IQt.js';
import './Copy-Cs57vtBT.js';
import './MarkdownCode-BTyfjBma.js';
import './index35-C1d7OdX7.js';
import 'path';
import 'url';
import 'fs';
import './IconButton-C2_XRZp7.js';
import './IconButtonWrapper-BI5v6wo4.js';

/* empty css                                      */function N(_,l){_.component(p=>{let v=l.app,c=l.root,m="",u=350,f="chat",x="",h=[];const E=(t="smooth")=>{};let b=[];(async()=>v.post_data(`${c}/gradio_api/vibe-starter-queries/`,{}).then(async([e,a])=>{if(a!==200)throw new Error(`Error: ${a}`);b=e.starter_queries;}).catch(async e=>{console.error("Failed to fetch starter queries:",e);}))();const q=async()=>{try{const t=await fetch(`${c}/gradio_api/vibe-code/`,{method:"GET",headers:{"Content-Type":"application/json"}});t.ok&&(x=(await t.json()).code);}catch(t){console.error("Failed to fetch code:",t);}};q(),tick().then(()=>E("auto"));let g=true,d;function M(t){t.push(`<div class="vibe-editor svelte-1s2fnws"${attr_style(`width: ${stringify(u)}px;`)}><button class="resize-handle svelte-1s2fnws" aria-label="Resize sidebar"></button> <div class="tab-header svelte-1s2fnws"><button${attr_class("tab-button svelte-1s2fnws",void 0,{active:f==="chat"})}>Chat</button> <button${attr_class("tab-button svelte-1s2fnws",void 0,{active:f==="code"})}>Code `),t.push("<!--[!-->"),t.push('<!--]--></button></div> <div class="tab-content svelte-1s2fnws">');{t.push("<!--[-->"),t.push('<div class="message-history svelte-1s2fnws"><!--[-->');const a=ensure_array_like(h);for(let o=0,n=a.length;o<n;o++){let i=a[o];t.push(`<div${attr_class("message-item svelte-1s2fnws",void 0,{"bot-message":i.isBot,"user-message":!i.isBot})}><div class="message-content svelte-1s2fnws"><span class="message-text svelte-1s2fnws">`),K(t,{value:i.text,latex_delimiters:[],theme_mode:"system"}),t.push("<!----></span> "),!i.isBot&&i.hash&&!i.isPending?(t.push("<!--[-->"),t.push('<button class="undo-button svelte-1s2fnws" title="Undo this change">Undo</button>')):t.push("<!--[!-->"),t.push("<!--]--></div></div>");}if(t.push("<!--]--> "),h.length===0?(t.push("<!--[-->"),t.push('<div class="no-messages svelte-1s2fnws">No messages yet</div>')):t.push("<!--[!-->"),t.push("<!--]--> "),h.length===0){t.push("<!--[-->"),t.push('<div class="starter-queries-container svelte-1s2fnws"><div class="starter-queries svelte-1s2fnws"><!--[-->');const o=ensure_array_like(b);for(let n=0,i=o.length;n<i;n++){let W=o[n];t.push(`<button class="starter-query-button svelte-1s2fnws">${escape_html(W)}</button>`);}t.push("<!--]--></div></div>");}else t.push("<!--[!-->");t.push("<!--]--></div>");}t.push('<!--]--></div> <div class="input-section svelte-1s2fnws"><div class="powered-by svelte-1s2fnws">Powered by: <code>gpt-oss</code></div> <textarea placeholder="What can I add or change?" class="prompt-input svelte-1s2fnws">');const e=escape_html(m);e&&t.push(`${e}`),t.push(`</textarea> <button class="submit-button svelte-1s2fnws"${attr("disabled",m.trim()==="",true)}>Send</button></div></div>`);}do g=true,d=p.copy(),M(d);while(!g);p.subsume(d),bind_props(l,{app:v,root:c});});}

export { N as default };
//# sourceMappingURL=Index21-_StiL91I.js.map
