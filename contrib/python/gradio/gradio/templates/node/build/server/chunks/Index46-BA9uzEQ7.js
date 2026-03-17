import './async-DWBXLqlH.js';
import { d as attr_class, e as ensure_array_like, a as attr, b as spread_props } from './index-D1re1cuM.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { c } from './BlockTitle-CtCsgYQk.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { C, F, S as S$1 } from './Dropdown-B4Fi0D5R.js';
import { y } from './IconButtonWrapper-BI5v6wo4.js';
import { G } from './Block-DAfEyy2Q.js';
import { R } from './index3-BRDKiycc.js';
export { default as BaseExample } from './Example4-De5uyqW4.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './Info-CpWGSVPL.js';
import './MarkdownCode-BTyfjBma.js';
import './index35-C1d7OdX7.js';
import 'path';
import 'url';
import 'fs';
import './prism-python-B3XC7jWw.js';
import './spring-D6sHki8W.js';
import './IconButton-C2_XRZp7.js';
import './Clear-CTfKb9Id.js';

function k(c){c.push('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="100%" height="100%"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"></path></svg>');}function W(c,n){return c.reduce((e,m,f)=>((!n||m[0].toLowerCase().includes(n.toLowerCase()))&&e.push(f),e),[])}function j(c$1,n){c$1.component(e=>{const{$$slots:m,$$events:f,..._}=n,s$1=_.gradio;let h,u="",b=s$1.shared.label||"Multiselect",r=s$1.props.buttons,i=s$1.props.choices.map(t=>t[0]),l=s$1.props.choices.map(t=>t[1]),v=!s$1.shared.interactive,w=false,[B,g]=(()=>{const t=W(s$1.props.choices,u);return [t,t.length>0&&!s$1.props.allow_custom_value?t[0]:null]})();function D(){return s$1.props.value===void 0?[]:Array.isArray(s$1.props.value)?s$1.props.value.map(t=>{const o=l.indexOf(t);if(o!==-1)return o;if(s$1.props.allow_custom_value)return t}).filter(t=>t!==void 0):[]}let p=D();function x(t){p=p.filter(o=>o!==t),s$1.props.value=p.map(o=>typeof o=="number"?l[o]:o),s$1.dispatch("input"),s$1.dispatch("select",{index:typeof t=="number"?t:-1,value:typeof t=="number"?l[t]:t,selected:false});}function I(t){(s$1.props.max_choices==null||p.length<s$1.props.max_choices)&&(p.push(t),s$1.dispatch("select",{index:typeof t=="number"?t:-1,value:typeof t=="number"?l[t]:t,selected:true})),p.length===s$1.props.max_choices&&(w=false,g=null,h.blur()),s$1.props.value=p.map(o=>typeof o=="number"?l[o]:o);}function M(t){const o=parseInt(t);A(o);}function A(t){p.includes(t)?x(t):I(t),u="",g=null,s$1.dispatch("input");}s$1.props.value;function L(t){s$1.dispatch("custom_button_click",{id:t});}e.push(`<div${attr_class("svelte-1dv2vbb",void 0,{container:s$1.shared.container})}>`),s$1.shared.show_label&&r&&r.length>0?(e.push("<!--[-->"),y(e,{buttons:r,on_custom_button_click:L})):e.push("<!--[!-->"),e.push("<!--]--> "),c(e,{show_label:s$1.shared.show_label,info:s$1.props.info,children:t=>{t.push(`<!---->${escape_html(b)}`);},$$slots:{default:true}}),e.push(`<!----> <div class="wrap svelte-1dv2vbb"><div${attr_class("wrap-inner svelte-1dv2vbb",void 0,{show_options:w})}><!--[-->`);const y$1=ensure_array_like(p);for(let t=0,o=y$1.length;t<o;t++){let d=y$1[t];e.push('<div class="token svelte-1dv2vbb"><span class="svelte-1dv2vbb">'),typeof d=="number"?(e.push("<!--[-->"),e.push(`${escape_html(i[d])}`)):(e.push("<!--[!-->"),e.push(`${escape_html(d)}`)),e.push("<!--]--></span> "),v?e.push("<!--[!-->"):(e.push("<!--[-->"),e.push(`<div class="token-remove svelte-1dv2vbb" role="button" tabindex="0"${attr("title",s$1.i18n("common.remove")+" "+d)}>`),k(e),e.push("<!----></div>")),e.push("<!--]--></div>");}e.push(`<!--]--> <div class="secondary-wrap svelte-1dv2vbb"><input${attr_class("border-none svelte-1dv2vbb",void 0,{subdued:!i.includes(u)&&!s$1.props.allow_custom_value||p.length===s$1.props.max_choices})}${attr("disabled",v,true)} autocomplete="off"${attr("value",u)}${attr("readonly",!s$1.props.filterable,true)}/> `),v?e.push("<!--[!-->"):(e.push("<!--[-->"),p.length>0?(e.push("<!--[-->"),e.push(`<div role="button" tabindex="0" class="token-remove remove-all svelte-1dv2vbb"${attr("title",s$1.i18n("common.clear"))}>`),k(e),e.push("<!----></div>")):e.push("<!--[!-->"),e.push('<!--]--> <span class="icon-wrap svelte-1dv2vbb">'),C(e),e.push("<!----></span>")),e.push("<!--]--></div></div> "),F(e,{show_options:w,choices:s$1.props.choices,filtered_indices:B,disabled:v,selected_indices:p,active_index:g,remember_scroll:true,onchange:M}),e.push("<!----></div></div>");});}function ss(c,n){c.component(e=>{let{$$slots:m,$$events:f,..._}=n;const s$1=new S(_);let h=true,u;function b(r){G(r,{visible:s$1.shared.visible,elem_id:s$1.shared.elem_id,elem_classes:s$1.shared.elem_classes,padding:s$1.shared.container,allow_overflow:false,scale:s$1.shared.scale,min_width:s$1.shared.min_width,children:i=>{R(i,spread_props([{autoscroll:s$1.shared.autoscroll,i18n:s$1.i18n},s$1.shared.loading_status,{on_clear_status:()=>s$1.dispatch("clear_status",loading_status)}])),i.push("<!----> "),s$1.props.multiselect?(i.push("<!--[-->"),j(i,{gradio:s$1})):(i.push("<!--[!-->"),S$1(i,{label:s$1.shared.label,info:s$1.props.info,choices:s$1.props.choices,interactive:s$1.shared.interactive,show_label:s$1.shared.show_label,container:s$1.shared.container,allow_custom_value:s$1.props.allow_custom_value,filterable:s$1.props.filterable,buttons:s$1.props.buttons,oncustom_button_click:l=>{s$1.dispatch("custom_button_click",{id:l});},on_change:()=>s$1.dispatch("change"),on_input:()=>s$1.dispatch("input"),on_select:l=>s$1.dispatch("select",l),on_focus:()=>s$1.dispatch("focus"),on_blur:()=>s$1.dispatch("blur"),on_key_up:l=>s$1.dispatch("key_up",l),get value(){return s$1.props.value},set value(l){s$1.props.value=l,h=false;}})),i.push("<!--]-->");},$$slots:{default:true}});}do h=true,u=e.copy(),b(u);while(!h);e.subsume(u);});}

export { S$1 as BaseDropdown, F as BaseDropdownOptions, j as BaseMultiselect, ss as default };
//# sourceMappingURL=Index46-BA9uzEQ7.js.map
