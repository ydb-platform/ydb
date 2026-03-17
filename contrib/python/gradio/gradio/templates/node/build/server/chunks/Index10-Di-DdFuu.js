import './async-DWBXLqlH.js';
import { b as spread_props } from './index-D1re1cuM.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { G } from './Block-DAfEyy2Q.js';
import { n } from './Info-CpWGSVPL.js';
import { y } from './IconButtonWrapper-BI5v6wo4.js';
import { R } from './index3-BRDKiycc.js';
import { x } from './Checkbox-CRqna9Jy.js';
import './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './MarkdownCode-BTyfjBma.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import './prism-python-B3XC7jWw.js';
import './index35-C1d7OdX7.js';
import 'path';
import 'url';
import 'fs';
import './spring-D6sHki8W.js';
import './IconButton-C2_XRZp7.js';
import './Clear-CTfKb9Id.js';

function A(e,l){e.component(i=>{let{$$slots:v,$$events:g,...u}=l;const s$1=new S(u);let a=true,p;function c(h){G(h,{visible:s$1.shared.visible,elem_id:s$1.shared.elem_id,elem_classes:s$1.shared.elem_classes,children:t=>{R(t,spread_props([{autoscroll:s$1.shared.autoscroll,i18n:s$1.i18n},s$1.shared.loading_status,{on_clear_status:()=>s$1.dispatch("clear_status",s$1.shared.loading_status)}])),t.push("<!----> "),s$1.shared.show_label&&s$1.props.buttons&&s$1.props.buttons.length>0?(t.push("<!--[-->"),y(t,{buttons:s$1.props.buttons,on_custom_button_click:o=>{s$1.dispatch("custom_button_click",{id:o});}})):t.push("<!--[!-->"),t.push("<!--]--> "),x(t,{label:s$1.shared.label||s$1.i18n("checkbox.checkbox"),interactive:s$1.shared.interactive,show_label:s$1.shared.show_label,on_change:o=>s$1.dispatch("change",o),on_input:()=>s$1.dispatch("input"),on_select:o=>s$1.dispatch("select",o),get value(){return s$1.props.value},set value(o){s$1.props.value=o,a=false;}}),t.push("<!----> "),s$1.props.info?(t.push("<!--[-->"),n(t,{info:s$1.props.info})):t.push("<!--[!-->"),t.push("<!--]-->");},$$slots:{default:true}});}do a=true,p=i.copy(),c(p);while(!a);i.subsume(p);});}

export { x as BaseCheckbox, A as default };
//# sourceMappingURL=Index10-Di-DdFuu.js.map
