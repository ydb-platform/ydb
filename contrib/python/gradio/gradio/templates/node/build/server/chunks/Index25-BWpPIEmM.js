import './async-DWBXLqlH.js';
import { b as spread_props } from './index-D1re1cuM.js';
import './2-BbOIMXxe.js';
import { S as S$1 } from './utils.svelte-BHoyPsmo.js';
import { t as tick } from './index-server-D4bj_1UO.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { k } from './BlockLabel-B2_AkSr2.js';
import { p } from './Empty-Cuu1wIXQ.js';
import { i } from './File-B6n6D8FG.js';
import { y } from './IconButtonWrapper-BI5v6wo4.js';
import { S, u as ul } from './FileUpload-v4zGpqMw.js';
import { G } from './Block-DAfEyy2Q.js';
import { k as k$1 } from './UploadText-CL6YR3zU.js';
import { R } from './index3-BRDKiycc.js';
export { default as BaseExample } from './Example5-Bb_boRc2.js';
import './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './prism-python-B3XC7jWw.js';
import './Upload2-9VoUdsIo.js';
import './IconButton-C2_XRZp7.js';
import './Clear-CTfKb9Id.js';
import './Upload-CBOKnugH.js';
import './DownloadLink-YXQWXYPr.js';
import './spring-D6sHki8W.js';

function z(d,c){d.component(t=>{let{value:o,label:_,show_label:p$1,selectable:l,i18n:r,height:h,buttons:s=null,on_custom_button_click:i$1=null,on_select:n,on_download:m}=c;p$1&&s&&s.length>0?(t.push("<!--[-->"),y(t,{buttons:s,on_custom_button_click:i$1})):t.push("<!--[!-->"),t.push("<!--]--> "),k(t,{show_label:p$1,float:o===null,Icon:i,label:_||"File"}),t.push("<!----> "),o&&(!Array.isArray(o)||o.length>0)?(t.push("<!--[-->"),S(t,{i18n:r,selectable:l,value:o,height:h})):(t.push("<!--[!-->"),p(t,{unpadded_box:true,size:"large",children:u=>{i(u);},$$slots:{default:true}})),t.push("<!--]-->");});}function O(d,c){d.component(t=>{const{$$slots:o,$$events:_,...p}=c;let l=null,r=false;class h extends S$1{async get_data(){return l&&(await l,await tick()),await super.get_data()}}const s$1=new h(p);s$1.props.value;let i=true,n;function m(u){G(u,{visible:s$1.shared.visible,variant:s$1.props.value?"solid":"dashed",border_mode:r?"focus":"base",padding:false,elem_id:s$1.shared.elem_id,elem_classes:s$1.shared.elem_classes,container:s$1.shared.container,scale:s$1.shared.scale,min_width:s$1.shared.min_width,allow_overflow:false,children:e=>{R(e,spread_props([{autoscroll:s$1.shared.autoscroll,i18n:s$1.i18n},s$1.shared.loading_status,{status:s$1.shared.loading_status?.status||"complete",on_clear_status:()=>s$1.dispatch("clear_status",s$1.shared.loading_status)}])),e.push("<!----> "),s$1.shared.interactive?(e.push("<!--[!-->"),ul(e,{upload:(...a)=>s$1.shared.client.upload(...a),stream_handler:(...a)=>s$1.shared.client.stream(...a),label:s$1.shared.label,show_label:s$1.shared.show_label,value:s$1.props.value,file_count:s$1.props.file_count,file_types:s$1.props.file_types,selectable:s$1.props._selectable,height:s$1.props.height??void 0,root:s$1.shared.root,allow_reordering:s$1.props.allow_reordering,max_file_size:s$1.shared.max_file_size,buttons:s$1.props.buttons,on_custom_button_click:a=>{s$1.dispatch("custom_button_click",{id:a});},onchange:a=>{s$1.props.value=a;},ondrag:a=>r=a,onclear:()=>s$1.dispatch("clear"),onselect:a=>s$1.dispatch("select",a),onupload:()=>s$1.dispatch("upload"),onerror:a=>{s$1.shared.loading_status=s$1.shared.loading_status||{},s$1.shared.loading_status.status="error",s$1.dispatch("error",a);},ondelete:a=>{s$1.dispatch("delete",a);},i18n:s$1.i18n,get upload_promise(){return l},set upload_promise(a){l=a,i=false;},children:a=>{k$1(a,{i18n:s$1.i18n,type:"file"});},$$slots:{default:true}})):(e.push("<!--[-->"),z(e,{on_select:({detail:a})=>s$1.dispatch("select",a),on_download:({detail:a})=>s$1.dispatch("download",a),selectable:s$1.props._selectable,value:s$1.props.value,label:s$1.shared.label,show_label:s$1.shared.show_label,height:s$1.props.height,i18n:s$1.i18n,buttons:s$1.props.buttons,on_custom_button_click:a=>{s$1.dispatch("custom_button_click",{id:a});}})),e.push("<!--]-->");},$$slots:{default:true}});}do i=true,n=t.copy(),m(n);while(!i);t.subsume(n);});}

export { z as BaseFile, ul as BaseFileUpload, S as FilePreview, O as default };
//# sourceMappingURL=Index25-BWpPIEmM.js.map
