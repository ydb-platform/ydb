import { f as fallback } from './async-DWBXLqlH.js';
import { b as spread_props, d as attr_class, e as ensure_array_like, j as stringify, c as bind_props } from './index-D1re1cuM.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { G } from './Block-DAfEyy2Q.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { R } from './index3-BRDKiycc.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './prism-python-B3XC7jWw.js';
import './spring-D6sHki8W.js';
import './IconButton-C2_XRZp7.js';
import './Clear-CTfKb9Id.js';

function v(j,i){j.component(s$1=>{let o=i.json,_=fallback(i.depth,1/0),c=fallback(i._cur,0),a=fallback(i._last,true),h=[],l=false,u=["",""],y=false;function f(p){return p===null?"null":typeof p}function m(p){return JSON.stringify(p)}function x(p){switch(f(p)){case "function":return "f () {...}";case "symbol":return p.toString();default:return m(p)}}if(h=f(o)==="object"?Object.keys(o):[],l=Array.isArray(o),u=l?["[","]"]:["{","}"],y=_<c,!h.length)s$1.push("<!--[-->"),s$1.push(`<span${attr_class("_jsonBkt empty svelte-zxj9ye",void 0,{isArray:l})}>${escape_html(u[0])}${escape_html(u[1])}</span>`),a?s$1.push("<!--[!-->"):(s$1.push("<!--[-->"),s$1.push('<span class="_jsonSep svelte-zxj9ye">,</span>')),s$1.push("<!--]-->");else {if(s$1.push("<!--[!-->"),y)s$1.push("<!--[-->"),s$1.push(`<span${attr_class("_jsonBkt svelte-zxj9ye",void 0,{isArray:l})} role="button" tabindex="0">${escape_html(u[0])}...${escape_html(u[1])}</span>`),!a&&y?(s$1.push("<!--[-->"),s$1.push('<span class="_jsonSep svelte-zxj9ye">,</span>')):s$1.push("<!--[!-->"),s$1.push("<!--]-->");else {s$1.push("<!--[!-->"),s$1.push(`<span${attr_class("_jsonBkt svelte-zxj9ye",void 0,{isArray:l})} role="button" tabindex="0">${escape_html(u[0])}</span> <ul class="_jsonList svelte-zxj9ye"><!--[-->`);const p=ensure_array_like(h);for(let n=0,b=p.length;n<b;n++){let e=p[n];s$1.push("<li>"),l?s$1.push("<!--[!-->"):(s$1.push("<!--[-->"),s$1.push(`<span class="_jsonKey svelte-zxj9ye">${escape_html(m(e))}</span><span class="_jsonSep svelte-zxj9ye">:</span>`)),s$1.push("<!--]--> "),f(o[e])==="object"?(s$1.push("<!--[-->"),v(s$1,{json:o[e],depth:_,_cur:c+1,_last:n===h.length-1}),s$1.push("<!---->")):(s$1.push("<!--[!-->"),s$1.push(`<span${attr_class(`_jsonVal ${stringify(f(o[e]))}`,"svelte-zxj9ye")}>${escape_html(x(o[e]))}</span>`),n<h.length-1?(s$1.push("<!--[-->"),s$1.push('<span class="_jsonSep svelte-zxj9ye">,</span>')):s$1.push("<!--[!-->"),s$1.push("<!--]-->")),s$1.push("<!--]--></li>");}s$1.push(`<!--]--></ul> <span${attr_class("_jsonBkt svelte-zxj9ye",void 0,{isArray:l})} role="button" tabindex="0">${escape_html(u[1])}</span>`),a?s$1.push("<!--[!-->"):(s$1.push("<!--[-->"),s$1.push('<span class="_jsonSep svelte-zxj9ye">,</span>')),s$1.push("<!--]-->");}s$1.push("<!--]-->");}s$1.push("<!--]-->"),bind_props(i,{json:o,depth:_,_cur:c,_last:a});});}function N(j,i){j.component(s$1=>{const{$$slots:o,$$events:_,...c}=i,a=new S(c);G(s$1,{visible:a.shared.visible,elem_id:a.shared.elem_id,elem_classes:a.shared.elem_classes,container:true,scale:a.shared.scale,min_width:a.shared.min_width,children:l=>{a.shared.loading_status?(l.push("<!--[-->"),R(l,spread_props([{autoscroll:a.shared.autoscroll,i18n:a.i18n},a.shared.loading_status,{on_clear_status:()=>a.dispatch("clear_status",a.shared.loading_status)}]))):l.push("<!--[!-->"),l.push("<!--]--> "),v(l,{json:a.props.value}),l.push("<!---->");},$$slots:{default:true}});});}

export { N as default };
//# sourceMappingURL=Index43-CSZMZlMm.js.map
