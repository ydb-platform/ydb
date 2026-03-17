import './async-DWBXLqlH.js';
import { b as spread_props, d as attr_class, g as attr_style } from './index-D1re1cuM.js';
import { p as ps } from './2-BbOIMXxe.js';
import { S, T } from './utils.svelte-BHoyPsmo.js';
import pr from './HTML-2-DiYRRt.js';
import { R } from './index3-BRDKiycc.js';
import { i } from './Code-JPvLawBI.js';
import { G } from './Block-DAfEyy2Q.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { k } from './BlockLabel-B2_AkSr2.js';
import { y } from './IconButtonWrapper-BI5v6wo4.js';
export { default as BaseExample } from './Example18-Cul-0cb7.js';
import './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './_commonjs-dynamic-modules-DX_xVkta.js';
import 'fs';
import './spring-D6sHki8W.js';
import './IconButton-C2_XRZp7.js';
import './Clear-CTfKb9Id.js';
import './prism-python-B3XC7jWw.js';

function q(i$1,r){i$1.component(n=>{let{$$slots:x,$$events:B,...l}=r,d=l.children;const s$1=new S(l);let h={value:s$1.props.value||"",label:s$1.shared.label,visible:s$1.shared.visible,...s$1.props.props};s$1.props.value;async function m(a){try{const t=await ps([a]),e=await s$1.shared.client.upload(t,s$1.shared.root,void 0,s$1.shared.max_file_size??void 0);if(e&&e[0])return {path:e[0].path,url:e[0].url};throw new Error("Upload failed")}catch(t){throw s$1.dispatch("error",t instanceof Error?t.message:String(t)),t}}G(n,{visible:s$1.shared.visible,elem_id:s$1.shared.elem_id,elem_classes:s$1.shared.elem_classes,container:s$1.shared.container,padding:s$1.props.padding!==false,overflow_behavior:"visible",children:a=>{s$1.shared.show_label&&s$1.props.buttons&&s$1.props.buttons.length>0?(a.push("<!--[-->"),y(a,{buttons:s$1.props.buttons,on_custom_button_click:t=>{s$1.dispatch("custom_button_click",{id:t});}})):a.push("<!--[!-->"),a.push("<!--]--> "),s$1.shared.show_label?(a.push("<!--[-->"),k(a,{Icon:i,show_label:s$1.shared.show_label,label:s$1.shared.label,float:true})):a.push("<!--[!-->"),a.push("<!--]--> "),R(a,spread_props([{autoscroll:s$1.shared.autoscroll,i18n:s$1.i18n},s$1.shared.loading_status,{variant:"center",on_clear_status:()=>s$1.dispatch("clear_status",s$1.shared.loading_status)}])),a.push(`<!----> <div${attr_class("html-container svelte-1jts93g",void 0,{pending:s$1.shared.loading_status?.status==="pending"&&s$1.shared.loading_status?.show_progress!=="hidden","label-padding":s$1.shared.show_label??void 0})}${attr_style("",{"min-height":s$1.props.min_height&&s$1.shared.loading_status?.status!=="pending"?T(s$1.props.min_height):void 0,"max-height":s$1.props.max_height?T(s$1.props.max_height):void 0,"overflow-y":s$1.props.max_height?"auto":void 0})}>`),pr(a,{props:h,html_template:s$1.props.html_template,css_template:s$1.props.css_template,js_on_load:s$1.props.js_on_load,elem_classes:s$1.shared.elem_classes,visible:s$1.shared.visible,autoscroll:s$1.shared.autoscroll,apply_default_css:s$1.props.apply_default_css,component_class_name:s$1.props.component_class_name,upload:m,server:s$1.shared.server,children:t=>{d?.(t);}}),a.push("<!----></div>");},$$slots:{default:true}});});}

export { pr as BaseHTML, q as default };
//# sourceMappingURL=Index28-CtYAkrwx.js.map
