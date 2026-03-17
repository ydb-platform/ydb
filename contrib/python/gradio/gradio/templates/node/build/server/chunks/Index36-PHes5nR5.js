import './async-DWBXLqlH.js';
import { b as spread_props, d as attr_class, g as attr_style, i as slot, c as bind_props } from './index-D1re1cuM.js';
import { G } from './Block-DAfEyy2Q.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { R } from './index3-BRDKiycc.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import { y } from './Index.svelte_svelte_type_style_lang-C-Is37XG.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './prism-python-B3XC7jWw.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './spring-D6sHki8W.js';
import './IconButton-C2_XRZp7.js';
import './Clear-CTfKb9Id.js';

function v(i,a){i.component(l=>{let{open:e=true,label:d="",onexpand:c,oncollapse:u}=a;l.push(`<button${attr_class("label-wrap svelte-e5lyqv",void 0,{open:e})}><span class="svelte-e5lyqv">${escape_html(d)}</span> <span class="icon svelte-e5lyqv"${attr_style("",{transform:e?"rotate(0)":"rotate(90deg)"})}>▼</span></button> <div${attr_style("",{display:e?"block":"none"})}><!--[-->`),slot(l,a,"default",{},null),l.push("<!--]--></div>"),bind_props(a,{open:e});});}function I(i,a){i.component(l=>{let{$$slots:e,$$events:d,...c}=a;class u extends S{set_data(p){"open"in p&&p.open&&this.dispatch("gradio_expand"),super.set_data(p),this.shared.loading_status.status="complete";}}const s$1=new u(c);let m=s$1.shared.label||"";G(l,{elem_id:s$1.shared.elem_id,elem_classes:s$1.shared.elem_classes,visible:s$1.shared.visible,children:o=>{s$1.shared.loading_status?(o.push("<!--[-->"),R(o,spread_props([{autoscroll:s$1.shared.autoscroll,i18n:s$1.i18n},s$1.shared.loading_status]))):o.push("<!--[!-->"),o.push("<!--]--> "),v(o,{label:m,open:s$1.props.open,onexpand:()=>{s$1.dispatch("expand"),s$1.dispatch("gradio_expand");},oncollapse:()=>s$1.dispatch("collapse"),children:p=>{y(p,{children:n=>{n.push("<!--[-->"),slot(n,a,"default",{},null),n.push("<!--]-->");},$$slots:{default:true}});},$$slots:{default:true}}),o.push("<!---->");},$$slots:{default:true}});});}

export { I as default };
//# sourceMappingURL=Index36-PHes5nR5.js.map
