import './async-DWBXLqlH.js';
import { b as spread_props, d as attr_class, a as attr } from './index-D1re1cuM.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { G } from './Block-DAfEyy2Q.js';
import { c } from './BlockTitle-CtCsgYQk.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { y } from './IconButtonWrapper-BI5v6wo4.js';
import { R } from './index3-BRDKiycc.js';
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

function T(l,i){l.component(e=>{const{$$slots:c$1,$$events:_,...r}=i,s$1=new S(r);s$1.props.value??=0,s$1.props.value;const p=!s$1.shared.interactive;G(e,{visible:s$1.shared.visible,elem_id:s$1.shared.elem_id,elem_classes:s$1.shared.elem_classes,padding:s$1.shared.container,allow_overflow:false,scale:s$1.shared.scale,min_width:s$1.shared.min_width,children:t=>{R(t,spread_props([{autoscroll:s$1.shared.autoscroll,i18n:s$1.i18n},s$1.shared.loading_status,{show_validation_error:false,on_clear_status:()=>{s$1.dispatch("clear_status",s$1.shared.loading_status);}}])),t.push(`<!----> <label${attr_class("block svelte-16ty2ow",void 0,{container:s$1.shared.container})}>`),s$1.shared.show_label&&s$1.props.buttons&&s$1.props.buttons.length>0?(t.push("<!--[-->"),y(t,{buttons:s$1.props.buttons,on_custom_button_click:o=>{s$1.dispatch("custom_button_click",{id:o});}})):t.push("<!--[!-->"),t.push("<!--]--> "),c(t,{show_label:s$1.shared.show_label,info:s$1.props.info,children:o=>{o.push(`<!---->${escape_html(s$1.shared.label||"Number")} `),s$1.shared.loading_status?.validation_error?(o.push("<!--[-->"),o.push(`<div class="validation-error svelte-16ty2ow">${escape_html(s$1.shared.loading_status?.validation_error)}</div>`)):o.push("<!--[!-->"),o.push("<!--]-->");},$$slots:{default:true}}),t.push(`<!----> <input${attr("aria-label",s$1.shared.label||"Number")} type="number"${attr("value",s$1.props.value)}${attr("min",s$1.props.minimum)}${attr("max",s$1.props.maximum)}${attr("step",s$1.props.step)}${attr("placeholder",s$1.props.placeholder)}${attr("disabled",p,true)}${attr_class("svelte-16ty2ow",void 0,{"validation-error":s$1.shared.loading_status?.validation_error})}/></label>`);},$$slots:{default:true}});});}

export { T as default };
//# sourceMappingURL=Index31-L_0S1FQx.js.map
