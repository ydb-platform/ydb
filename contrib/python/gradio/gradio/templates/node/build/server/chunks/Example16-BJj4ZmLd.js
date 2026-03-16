import './async-DWBXLqlH.js';
import { d as attr_class, e as ensure_array_like } from './index-D1re1cuM.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';

function f(i,u){i.component(l=>{let{value:s$1,type:p,selected:c=false}=u;if(l.push(`<ul${attr_class("svelte-14aa7hi",void 0,{table:p==="table",gallery:p==="gallery",selected:c})}>`),s$1){l.push("<!--[-->"),l.push("<!--[-->");const h=ensure_array_like(Array.isArray(s$1)?s$1.slice(0,3):[s$1]);for(let a=0,e=h.length;a<e;a++){let o=h[a];l.push(`<li><code>./${escape_html(o)}</code></li>`);}l.push("<!--]--> "),Array.isArray(s$1)&&s$1.length>3?(l.push("<!--[-->"),l.push('<li class="extra svelte-14aa7hi">...</li>')):l.push("<!--[!-->"),l.push("<!--]-->");}else l.push("<!--[!-->");l.push("<!--]--></ul>");});}

export { f as default };
//# sourceMappingURL=Example16-BJj4ZmLd.js.map
