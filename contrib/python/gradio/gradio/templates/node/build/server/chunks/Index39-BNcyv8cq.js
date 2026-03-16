import './async-DWBXLqlH.js';
import { c as bind_props, b as spread_props, i as slot } from './index-D1re1cuM.js';
import { t as tick } from './index-server-D4bj_1UO.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import A from './Model3D-Dr8wwzw6.js';
import { e as ee } from './Upload2-9VoUdsIo.js';
import { z } from './ModifyUpload-CHrOyHY7.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { k } from './BlockLabel-B2_AkSr2.js';
import { i } from './File-B6n6D8FG.js';
import { G } from './Block-DAfEyy2Q.js';
import { p } from './Empty-Cuu1wIXQ.js';
import { k as k$1 } from './UploadText-CL6YR3zU.js';
import { y } from './IconButtonWrapper-BI5v6wo4.js';
import { R } from './index3-BRDKiycc.js';
export { default as BaseExample } from './Example22-B5opjjSX.js';
import './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './IconButton-C2_XRZp7.js';
import './Download-DQCZIsve.js';
import './Undo-BWigpY9w.js';
import './prism-python-B3XC7jWw.js';
import './DownloadLink-YXQWXYPr.js';
import './Clear-CTfKb9Id.js';
import './Edit-DnE7yQ6U.js';
import './Upload-CBOKnugH.js';
import './spring-D6sHki8W.js';

function P(b,n){b.component(u=>{let{value:i$1=void 0,display_mode:j="solid",clear_color:F=[0,0,0,0],label:v="",show_label:a,root:f,i18n:d,zoom_speed:w=1,pan_speed:r=1,max_file_size:y=null,uploading:h=void 0,upload_promise:c=void 0,camera_position:D=[null,null,null],upload:x,stream_handler:z$1,onchange:p,onclear:_,ondrag:k$1,onload:l,onerror:o}=n,s$1=false;async function S(e){i$1=e,await tick(),p?.(i$1),l?.(i$1);}async function L(){i$1=null,await tick(),_?.(),p?.(null);}async function T(){}function W(e){o?.(e);}let m=true,M;function q(e){k(e,{show_label:a,Icon:i,label:v||"3D Model"}),e.push("<!----> "),i$1==null?(e.push("<!--[-->"),ee(e,{upload:x,stream_handler:z$1,onload:S,root:f,max_file_size:y,filetype:[".stl",".obj",".gltf",".glb","model/obj",".splat",".ply"],onerror:W,aria_label:d("model3d.drop_to_upload"),get upload_promise(){return c},set upload_promise(t){c=t,m=false;},get dragging(){return s$1},set dragging(t){s$1=t,m=false;},get uploading(){return h},set uploading(t){h=t,m=false;},children:t=>{t.push("<!--[-->"),slot(t,n,"default",{},null),t.push("<!--]-->");},$$slots:{default:true}})):(e.push("<!--[!-->"),e.push('<div class="input-model svelte-18wa0f8">'),z(e,{undoable:true,onclear:L,i18n:d,onundo:T}),e.push("<!----> "),e.push("<!--[!-->"),e.push("<!---->"),e.push("<!---->"),e.push("<!--]--></div>")),e.push("<!--]-->");}do m=true,M=u.copy(),q(M);while(!m);u.subsume(M),bind_props(n,{value:i$1,uploading:h,upload_promise:c});});}function ca(b,n){b.component(u=>{let i$1 = class i extends S{async get_data(){return r&&(await r,await tick()),await super.get_data()}};const{$$slots:j,$$events:F,...v}=n,a=new i$1(v);a.props.value;let f=false,d=false,w=false,r;const y$1=typeof window<"u";function h(l){a.props.value=l,a.dispatch("change",l),w=true;}function c(l){d=l;}function D(){a.props.value=null,a.dispatch("clear");}function x(l){a.props.value=l,a.dispatch("upload");}function z(l){a.shared.loading_status&&(a.shared.loading_status.status="error"),a.dispatch("error",l);}let p$1=true,_;function k$2(l){a.shared.interactive?(l.push("<!--[!-->"),G(l,{visible:a.shared.visible,variant:a.props.value===null?"dashed":"solid",border_mode:d?"focus":"base",padding:false,elem_id:a.shared.elem_id,elem_classes:a.shared.elem_classes,container:a.shared.container,scale:a.shared.scale,min_width:a.shared.min_width,height:a.props.height,children:o=>{R(o,spread_props([{autoscroll:a.shared.autoscroll,i18n:a.i18n},a.shared.loading_status,{on_clear_status:()=>a.dispatch("clear_status",a.shared.loading_status)}])),o.push("<!----> "),P(o,{label:a.shared.label,show_label:a.shared.show_label,root:a.shared.root,display_mode:a.props.display_mode,clear_color:a.props.clear_color,camera_position:a.props.camera_position,zoom_speed:a.props.zoom_speed,onchange:h,ondrag:c,onclear:D,onload:x,onerror:z,i18n:a.i18n,max_file_size:a.shared.max_file_size,upload:(...s)=>a.shared.client.upload(...s),stream_handler:(...s)=>a.shared.client.stream(...s),get upload_promise(){return r},set upload_promise(s){r=s,p$1=false;},get value(){return a.props.value},set value(s){a.props.value=s,p$1=false;},get uploading(){return f},set uploading(s){f=s,p$1=false;},children:s=>{k$1(s,{i18n:a.i18n,type:"file"});},$$slots:{default:true}}),o.push("<!---->");},$$slots:{default:true}})):(l.push("<!--[-->"),G(l,{visible:a.shared.visible,variant:a.props.value===null?"dashed":"solid",border_mode:d?"focus":"base",padding:false,elem_id:a.shared.elem_id,elem_classes:a.shared.elem_classes,container:a.shared.container,scale:a.shared.scale,min_width:a.shared.min_width,height:a.props.height,children:o=>{R(o,spread_props([{autoscroll:a.shared.autoscroll,i18n:a.i18n},a.shared.loading_status,{on_clear_status:()=>a.dispatch("clear_status",a.shared.loading_status)}])),o.push("<!----> "),a.props.value&&y$1?(o.push("<!--[-->"),A(o,{value:a.props.value,i18n:a.i18n,display_mode:a.props.display_mode,clear_color:a.props.clear_color,label:a.shared.label,show_label:a.shared.show_label,camera_position:a.props.camera_position,zoom_speed:a.props.zoom_speed,has_change_history:w})):(o.push("<!--[!-->"),a.shared.show_label&&a.props.buttons&&a.props.buttons.length>0?(o.push("<!--[-->"),y(o,{buttons:a.props.buttons,on_custom_button_click:s=>{a.dispatch("custom_button_click",{id:s});}})):o.push("<!--[!-->"),o.push("<!--]--> "),k(o,{show_label:a.shared.show_label,Icon:i,label:a.shared.label||"3D Model"}),o.push("<!----> "),p(o,{unpadded_box:true,size:"large",children:s=>{i(s);},$$slots:{default:true}}),o.push("<!---->")),o.push("<!--]-->");},$$slots:{default:true}})),l.push("<!--]-->");}do p$1=true,_=u.copy(),k$2(_);while(!p$1);u.subsume(_);});}

export { A as BaseModel3D, P as BaseModel3DUpload, ca as default };
//# sourceMappingURL=Index39-BNcyv8cq.js.map
