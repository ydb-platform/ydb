import './async-DWBXLqlH.js';
import { b as spread_props } from './index-D1re1cuM.js';
import { t as tick } from './index-server-D4bj_1UO.js';
import { p as pt } from './Textbox-DE8qzELL.js';
import { R } from './index3-BRDKiycc.js';
import { G } from './Block-DAfEyy2Q.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
export { default as BaseExample } from './Example2-SVmhqJ3c.js';
import './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './BlockTitle-CtCsgYQk.js';
import './Info-CpWGSVPL.js';
import './MarkdownCode-BTyfjBma.js';
import './index35-C1d7OdX7.js';
import 'path';
import 'url';
import 'fs';
import './IconButton-C2_XRZp7.js';
import './Check-BB0k-IQt.js';
import './Copy-Cs57vtBT.js';
import './Send-sAzByYjt.js';
import './Square-DwNteyXt.js';
import './IconButtonWrapper-BI5v6wo4.js';
import './spring-D6sHki8W.js';
import './Clear-CTfKb9Id.js';
import './prism-python-B3XC7jWw.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';

function A(p,i){p.component(l=>{let{$$slots:g,$$events:w,...n}=i;const s$1=new S(n);let u=s$1.shared.label||"Textbox";s$1.props.value=s$1.props.value??"",s$1.props.value;async function d(a){!s$1.shared||!s$1.props||(s$1.props.validation_error=null,s$1.props.value=a,await tick(),s$1.dispatch("input"));}function c(a){!s$1.shared||!s$1.props||(s$1.props.validation_error=null,s$1.props.value=a);}let e=true,r;function h(a){G(a,{visible:s$1.shared.visible,elem_id:s$1.shared.elem_id,elem_classes:s$1.shared.elem_classes,scale:s$1.shared.scale,min_width:s$1.shared.min_width,allow_overflow:false,padding:s$1.shared.container,rtl:s$1.props.rtl,children:o=>{s$1.shared.loading_status?(o.push("<!--[-->"),R(o,spread_props([{autoscroll:s$1.shared.autoscroll,i18n:s$1.i18n},s$1.shared.loading_status,{show_validation_error:false,on_clear_status:()=>s$1.dispatch("clear_status",s$1.shared.loading_status)}]))):o.push("<!--[!-->"),o.push("<!--]--> "),pt(o,{label:u,info:s$1.props.info,show_label:s$1.shared.show_label,lines:s$1.props.lines,type:s$1.props.type,rtl:s$1.props.rtl,text_align:s$1.props.text_align,max_lines:s$1.props.max_lines,placeholder:s$1.props.placeholder,submit_btn:s$1.props.submit_btn,stop_btn:s$1.props.stop_btn,buttons:s$1.props.buttons,autofocus:s$1.props.autofocus,container:s$1.shared.container,autoscroll:s$1.shared.autoscroll,max_length:s$1.props.max_length,html_attributes:s$1.props.html_attributes,validation_error:s$1.shared?.loading_status?.validation_error||s$1.shared?.validation_error,onchange:c,oninput:d,onsubmit:()=>{s$1.shared.validation_error=null,s$1.dispatch("submit");},onblur:()=>s$1.dispatch("blur"),onselect:t=>s$1.dispatch("select",t),onfocus:()=>s$1.dispatch("focus"),onstop:()=>s$1.dispatch("stop"),oncopy:t=>s$1.dispatch("copy",t),oncustombuttonclick:t=>{s$1.dispatch("custom_button_click",{id:t});},disabled:!s$1.shared.interactive,get value(){return s$1.props.value},set value(t){s$1.props.value=t,e=false;}}),o.push("<!---->");},$$slots:{default:true}});}do e=true,r=l.copy(),h(r);while(!e);l.subsume(r);});}

export { pt as BaseTextbox, A as default };
//# sourceMappingURL=Index18-vSRbweJ8.js.map
