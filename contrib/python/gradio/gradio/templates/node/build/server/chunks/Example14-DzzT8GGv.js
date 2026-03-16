import { f as fallback } from './async-DWBXLqlH.js';
import { d as attr_class, e as ensure_array_like, c as bind_props } from './index-D1re1cuM.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';

function q(y,a){y.component(l=>{let s$1=a.value,h=a.type,p=fallback(a.selected,false),i=a.index,e=Array.isArray(s$1),b=e&&(s$1.length===0||s$1[0].length===0);if(e){if(l.push("<!--[-->"),l.push(`<div${attr_class("svelte-wcwkqi",void 0,{table:h==="table",gallery:h==="gallery",selected:p})}>`),typeof s$1=="string")l.push("<!--[-->"),l.push(`${escape_html(s$1)}`);else {if(l.push("<!--[!-->"),b)l.push("<!--[-->"),l.push('<table class="svelte-wcwkqi"><tbody><tr class="svelte-wcwkqi"><td class="svelte-wcwkqi">Empty</td></tr></tbody></table>');else {l.push("<!--[!-->"),l.push('<table class="svelte-wcwkqi"><tbody><!--[-->');const o=ensure_array_like(s$1.slice(0,3));for(let c=0,f=o.length;c<f;c++){let v=o[c];l.push('<tr class="svelte-wcwkqi"><!--[-->');const w=ensure_array_like(v.slice(0,3));for(let u=0,g=w.length;u<g;u++){let k=w[u];l.push(`<td class="svelte-wcwkqi">${escape_html(k)}</td>`);}l.push("<!--]-->"),v.length>3?(l.push("<!--[-->"),l.push('<td class="svelte-wcwkqi">…</td>')):l.push("<!--[!-->"),l.push("<!--]--></tr>");}l.push("<!--]--></tbody></table> "),s$1.length>3?(l.push("<!--[-->"),l.push(`<div${attr_class("overlay svelte-wcwkqi",void 0,{odd:i%2!=0,even:i%2==0,button:h==="gallery"})}></div>`)):l.push("<!--[!-->"),l.push("<!--]-->");}l.push("<!--]-->");}l.push("<!--]--></div>");}else l.push("<!--[!-->");l.push("<!--]-->"),bind_props(a,{value:s$1,type:h,selected:p,index:i});});}

export { q as default };
//# sourceMappingURL=Example14-DzzT8GGv.js.map
