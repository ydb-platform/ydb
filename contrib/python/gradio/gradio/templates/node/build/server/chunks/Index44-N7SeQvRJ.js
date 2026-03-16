import './async-DWBXLqlH.js';
import { a as attr, g as attr_style, c as bind_props, b as spread_props } from './index-D1re1cuM.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { s } from './tinycolor-DfhFic3A.js';
import { c } from './BlockTitle-CtCsgYQk.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { G } from './Block-DAfEyy2Q.js';
import { R } from './index3-BRDKiycc.js';
export { default as BaseExample } from './Example13-CjUlezuZ.js';
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

function w(e,a){return s(e).toHexString()}function B(e,a){e.component(t=>{let{value:i=void 0,label:c$1,info:u,disabled:o,show_label:p,on_input:r=()=>{},on_submit:n=()=>{},on_blur:d=()=>{},on_focus:m=()=>{}}=a;w(i),c(t,{show_label:p,info:u,children:s$1=>{s$1.push(`<!---->${escape_html(c$1)}`);},$$slots:{default:true}}),t.push(`<!----> <button class="dialog-button svelte-nbn1m9"${attr("disabled",o,true)}${attr_style("",{background:i})}></button> `),t.push("<!--[!-->"),t.push("<!--]-->"),bind_props(a,{value:i});});}function q(e,a){e.component(t=>{let{$$slots:i,$$events:c,...u}=a;const o=new S(u,{value:"#000000"});o.props.value;let p=o.shared.label||o.i18n("color_picker.color_picker"),r=true,n;function d(m){G(m,{visible:o.shared.visible,elem_id:o.shared.elem_id,elem_classes:o.shared.elem_classes,container:o.shared.container,scale:o.shared.scale,min_width:o.shared.min_width,children:s$1=>{R(s$1,spread_props([{autoscroll:o.shared.autoscroll,i18n:o.i18n},o.shared.loading_status,{on_clear_status:()=>o.dispatch("clear_status",o.shared.loading_status)}])),s$1.push("<!----> "),B(s$1,{label:p,info:o.props.info,show_label:o.shared.show_label,disabled:!o.shared.interactive,on_input:()=>o.dispatch("input"),on_submit:()=>o.dispatch("submit"),on_blur:()=>o.dispatch("blur"),on_focus:()=>o.dispatch("focus"),get value(){return o.props.value},set value(h){o.props.value=h,r=false;}}),s$1.push("<!---->");},$$slots:{default:true}});}do r=true,n=t.copy(),d(n);while(!r);t.subsume(n);});}

export { B as BaseColorPicker, q as default };
//# sourceMappingURL=Index44-N7SeQvRJ.js.map
