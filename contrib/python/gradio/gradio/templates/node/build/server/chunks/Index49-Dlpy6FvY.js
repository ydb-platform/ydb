import './async-DWBXLqlH.js';
import { d as attr_class, a as attr } from './index-D1re1cuM.js';
import { G } from './Block-DAfEyy2Q.js';
import { c } from './BlockTitle-CtCsgYQk.js';
import { o as onDestroy } from './index-server-D4bj_1UO.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { y } from './IconButtonWrapper-BI5v6wo4.js';
export { default as BaseExample } from './Example3-DjEQYz7e.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './Info-CpWGSVPL.js';
import './MarkdownCode-BTyfjBma.js';
import './index35-C1d7OdX7.js';
import 'path';
import 'url';
import 'fs';
import './prism-python-B3XC7jWw.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';

function M(o){o.push('<svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 24 24"><rect x="2" y="4" width="20" height="18" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" fill="none"></rect><line x1="2" y1="9" x2="22" y2="9" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" fill="none"></line><line x1="7" y1="2" x2="7" y2="6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" fill="none"></line><line x1="17" y1="2" x2="17" y2="6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" fill="none"></line></svg>');}const Y=(o,a)=>{if(o==null||o==="")return  true;const n=a?/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/:/^\d{4}-\d{2}-\d{2}$/,s=o.match(n)!==null,l=o.match(/^(?:\s*now\s*(?:-\s*\d+\s*[dmhs])?)?\s*$/)!==null;return s||l},H=(o,a)=>{if(!o||o===""){const s=new Date;return {selected_date:s,current_year:s.getFullYear(),current_month:s.getMonth(),selected_hour:s.getHours(),selected_minute:s.getMinutes(),selected_second:s.getSeconds(),is_pm:s.getHours()>=12}}try{let s=o;!a&&o.match(/^\d{4}-\d{2}-\d{2}$/)&&(s+=" 00:00:00");const l=new Date(s.replace(" ","T"));if(!isNaN(l.getTime()))return {selected_date:l,current_year:l.getFullYear(),current_month:l.getMonth(),selected_hour:l.getHours(),selected_minute:l.getMinutes(),selected_second:l.getSeconds(),is_pm:l.getHours()>=12}}catch{}const n=new Date;return {selected_date:n,current_year:n.getFullYear(),current_month:n.getMonth(),selected_hour:n.getHours(),selected_minute:n.getMinutes(),selected_second:n.getSeconds(),is_pm:n.getHours()>=12}};function L(o,a){o.component(n=>{const{$$slots:s$1,$$events:l,..._}=a,e=new S(_);e.props.value;let c$1=e.props.value;new Date().getFullYear(),new Date().getMonth(),new Date().getHours(),new Date().getMinutes(),new Date().getSeconds();let m=Y(c$1,e.props.include_time),p=!e.shared.interactive;const w=()=>{const i=H(c$1,e.props.include_time);i.selected_date,i.current_year,i.current_month,i.selected_hour,i.selected_minute,i.selected_second,i.is_pm;},g=i=>{},f=()=>{};onDestroy(()=>{typeof window<"u"&&(window.removeEventListener("click",g),window.removeEventListener("scroll",f,true));}),w();let h=true,d;function v(i){G(i,{visible:e.shared.visible,elem_id:e.shared.elem_id,elem_classes:e.shared.elem_classes,scale:e.shared.scale,min_width:e.shared.min_width,allow_overflow:false,padding:true,children:t=>{t.push('<div class="label-content svelte-16sct4k">'),e.shared.show_label&&e.props.buttons&&e.props.buttons.length>0?(t.push("<!--[-->"),y(t,{buttons:e.props.buttons,on_custom_button_click:u=>{e.dispatch("custom_button_click",{id:u});}})):t.push("<!--[!-->"),t.push("<!--]--> "),c(t,{show_label:e.shared.show_label,info:e.props.info,children:u=>{u.push(`<!---->${escape_html(e.shared.label||"Date")}`);},$$slots:{default:true}}),t.push(`<!----></div> <div class="timebox svelte-16sct4k"><input${attr_class("time svelte-16sct4k",void 0,{invalid:!m})}${attr("value",c$1)}${attr("disabled",p,true)}${attr("placeholder",e.props.include_time?"YYYY-MM-DD HH:MM:SS":"YYYY-MM-DD")}/> `),e.shared.interactive?(t.push("<!--[-->"),t.push(`<button class="calendar svelte-16sct4k"${attr("disabled",p,true)}>`),M(t),t.push("<!----></button>")):t.push("<!--[!-->"),t.push("<!--]--></div> "),t.push("<!--[!-->"),t.push("<!--]-->");},$$slots:{default:true}});}do h=true,d=n.copy(),v(d);while(!h);n.subsume(d);});}

export { L as default };
//# sourceMappingURL=Index49-Dlpy6FvY.js.map
