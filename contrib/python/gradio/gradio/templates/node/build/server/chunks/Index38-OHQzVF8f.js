import './async-DWBXLqlH.js';
import { d as attr_class, g as attr_style, e as ensure_array_like, a as attr, j as stringify, b as spread_props } from './index-D1re1cuM.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { e } from './LineChart-C9BQ7Ob5.js';
import { G } from './Block-DAfEyy2Q.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { k as k$1 } from './BlockLabel-B2_AkSr2.js';
import { p } from './Empty-Cuu1wIXQ.js';
import { y } from './IconButtonWrapper-BI5v6wo4.js';
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

function k(c,n){c.component(l=>{let{value:i,color:h=void 0,selectable:p=false,show_heading:s$1=true,onselect:u}=n;function t(e){return e.replace(/\s/g,"-")}if(l.push('<div class="container svelte-g2cwl3">'),s$1||!i.confidences?(l.push("<!--[-->"),l.push(`<h2${attr_class("output-class svelte-g2cwl3",void 0,{"no-confidence":!("confidences"in i)})} data-testid="label-output-value"${attr_style("",{"background-color":h||"transparent"})}>${escape_html(i.label)}</h2>`)):l.push("<!--[!-->"),l.push("<!--]--> "),typeof i=="object"&&i.confidences){l.push("<!--[-->"),l.push("<!--[-->");const e=ensure_array_like(i.confidences);for(let d=0,b=e.length;d<b;d++){let o=e[d];l.push(`<button${attr_class("confidence-set group svelte-g2cwl3",void 0,{selectable:p})}${attr("data-testid",`${o.label}-confidence-set`)}><div class="inner-wrap svelte-g2cwl3"><meter${attr("aria-labelledby",t(`meter-text-${o.label}`))}${attr("aria-label",o.label)}${attr("aria-valuenow",Math.round(o.confidence*100))} aria-valuemin="0" aria-valuemax="100" class="bar svelte-g2cwl3" min="0" max="1"${attr("value",o.confidence)}${attr_style(`width: ${stringify(o.confidence*100)}%; background: var(--stat-background-fill); `)}></meter> <dl class="label svelte-g2cwl3"><dt${attr("id",t(`meter-text-${o.label}`))} class="text svelte-g2cwl3">${escape_html(o.label)}</dt> <div class="line svelte-g2cwl3"></div> <dd class="confidence svelte-g2cwl3">${escape_html(Math.round(o.confidence*100))}%</dd></dl></div></button>`);}l.push("<!--]-->");}else l.push("<!--[!-->");l.push("<!--]--></div>");});}function D(c,n){c.component(l=>{const{$$slots:i,$$events:h,...p$1}=n,s$1=new S(p$1);s$1.props.value;let u=s$1.props.value.label;G(l,{test_id:"label",visible:s$1.shared.visible,elem_id:s$1.shared.elem_id,elem_classes:s$1.shared.elem_classes,container:s$1.shared.container,scale:s$1.shared.scale,min_width:s$1.shared.min_width,padding:false,children:t=>{R(t,spread_props([{autoscroll:s$1.shared.autoscroll,i18n:s$1.i18n},s$1.shared.loading_status,{on_clear_status:()=>s$1.dispatch("clear_status",s$1.shared.loading_status)}])),t.push("<!----> "),s$1.shared.show_label&&s$1.props.buttons&&s$1.props.buttons.length>0?(t.push("<!--[-->"),y(t,{buttons:s$1.props.buttons,on_custom_button_click:e=>{s$1.dispatch("custom_button_click",{id:e});}})):t.push("<!--[!-->"),t.push("<!--]--> "),s$1.shared.show_label?(t.push("<!--[-->"),k$1(t,{Icon:e,label:s$1.shared.label||s$1.i18n("label.label"),disable:s$1.shared.container===false,float:s$1.props.show_heading===true})):t.push("<!--[!-->"),t.push("<!--]--> "),u!=null?(t.push("<!--[-->"),k(t,{onselect:e=>s$1.dispatch("select",e),selectable:s$1.props._selectable,value:s$1.props.value,color:s$1.props.color,show_heading:s$1.props.show_heading})):(t.push("<!--[!-->"),p(t,{unpadded_box:true,children:e$1=>{e(e$1);},$$slots:{default:true}})),t.push("<!--]-->");},$$slots:{default:true}});});}

export { k as BaseLabel, D as default };
//# sourceMappingURL=Index38-OHQzVF8f.js.map
