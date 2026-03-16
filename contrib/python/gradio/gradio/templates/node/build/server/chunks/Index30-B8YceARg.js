import './async-DWBXLqlH.js';
import { b as spread_props } from './index-D1re1cuM.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { G } from './Block-DAfEyy2Q.js';
import { c } from './BlockTitle-CtCsgYQk.js';
import { w } from './IconButton-C2_XRZp7.js';
import { p } from './Empty-Cuu1wIXQ.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { l } from './Download-DQCZIsve.js';
import { e } from './LineChart-C9BQ7Ob5.js';
import { y } from './IconButtonWrapper-BI5v6wo4.js';
import { v } from './FullscreenButton-CJEwZ820.js';
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
import './Maximize-DWTTqpgt.js';
import './spring-D6sHki8W.js';
import './Clear-CTfKb9Id.js';

function Z(m,d){m.component(c$1=>{let{$$slots:G$1,$$events:L,...h}=d;const s$1=new S(h);(()=>{if(!s$1.props.color||!s$1.props.value||s$1.props.value.datatypes[s$1.props.color]!=="nominal")return [];const p=s$1.props.value.columns.indexOf(s$1.props.color);return p===-1?[]:Array.from(new Set(s$1.props.value.data.map(o=>o[p])))})();let l$1=s$1.props.x_lim||null,i=s$1.props.y_lim||null;l$1?.[0]!==null&&l$1?.[0],l$1?.[1]!==null&&l$1?.[1],i?.[0]!==null&&i?.[0],i?.[1]!==null&&i?.[1];let n=false;function _(p){if(p==="x")return "ascending";if(p==="-x")return "descending";if(p==="y")return {field:s$1.props.y,order:"ascending"};if(p==="-y")return {field:s$1.props.y,order:"descending"};if(p===null)return null;if(Array.isArray(p))return p}_(s$1.props.sort),s$1.props.value&&s$1.props.value.datatypes[s$1.props.x];const g={s:1,m:60,h:3600,d:1440*60};let u=s$1.props.x_bin?typeof s$1.props.x_bin=="string"?1e3*parseInt(s$1.props.x_bin.substring(0,s$1.props.x_bin.length-1))*g[s$1.props.x_bin[s$1.props.x_bin.length-1]]:s$1.props.x_bin:void 0;(()=>{if(s$1.props.value)if(s$1.props.value.mark==="point"){const p=u!==void 0;return s$1.props.y_aggregate||p?"sum":void 0}else return s$1.props.y_aggregate?s$1.props.y_aggregate:"sum"})(),s$1.props.value&&(s$1.props.value.mark==="point"||u!==void 0||s$1.props.value.datatypes[s$1.props.x]),s$1.props.value;const b=typeof window<"u";function v$1(){}JSON.stringify(s$1.props.color_map);let a=true,r;function x(p$1){G(p$1,{visible:s$1.shared.visible,elem_id:s$1.shared.elem_id,elem_classes:s$1.shared.elem_classes,scale:s$1.shared.scale,min_width:s$1.shared.min_width,allow_overflow:false,padding:true,height:s$1.props.height,get fullscreen(){return n},set fullscreen(o){n=o,a=false;},children:o=>{s$1.shared.loading_status?(o.push("<!--[-->"),R(o,spread_props([{autoscroll:s$1.shared.autoscroll,i18n:s$1.i18n},s$1.shared.loading_status,{on_clear_status:()=>s$1.dispatch("clear_status",s$1.shared.loading_status)}]))):o.push("<!--[!-->"),o.push("<!--]--> "),s$1.props.buttons?.length?(o.push("<!--[-->"),y(o,{buttons:s$1.props.buttons,on_custom_button_click:t=>{s$1.dispatch("custom_button_click",{id:t});},children:t=>{s$1.props.buttons?.some(e=>typeof e=="string"&&e==="export")?(t.push("<!--[-->"),w(t,{Icon:l,label:"Export",onclick:v$1})):t.push("<!--[!-->"),t.push("<!--]--> "),s$1.props.buttons?.some(e=>typeof e=="string"&&e==="fullscreen")?(t.push("<!--[-->"),v(t,{fullscreen:n})):t.push("<!--[!-->"),t.push("<!--]-->");}})):o.push("<!--[!-->"),o.push("<!--]--> "),c(o,{show_label:s$1.shared.show_label,info:void 0,children:t=>{t.push(`<!---->${escape_html(s$1.shared.label)}`);},$$slots:{default:true}}),o.push("<!----> "),s$1.props.value&&b?(o.push("<!--[-->"),o.push('<div class="svelte-19utvcn"></div> '),s$1.props.caption?(o.push("<!--[-->"),o.push(`<p class="caption svelte-19utvcn">${escape_html(s$1.props.caption)}</p>`)):o.push("<!--[!-->"),o.push("<!--]-->")):(o.push("<!--[!-->"),p(o,{unpadded_box:true,children:t=>{e(t);},$$slots:{default:true}})),o.push("<!--]-->");},$$slots:{default:true}});}do a=true,r=c$1.copy(),x(r);while(!a);c$1.subsume(r);});}

export { Z as default };
//# sourceMappingURL=Index30-B8YceARg.js.map
