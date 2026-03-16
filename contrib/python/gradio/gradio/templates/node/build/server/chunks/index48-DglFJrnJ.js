import './async-DWBXLqlH.js';
import { c as bind_props, b as spread_props } from './index-D1re1cuM.js';
import { e as ee } from './Upload2-9VoUdsIo.js';
import { t as tick } from './index-server-D4bj_1UO.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { k } from './BlockLabel-B2_AkSr2.js';
import { r } from './Video-BJvaeIU2.js';
import './2-BbOIMXxe.js';
import { n } from './SelectSource-DO1h5mRf.js';
import { $ } from './Webcam2-BtNL1kyZ.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import { P as Mt, p as te, V as Ft } from './VideoPreview-GsGpXz96.js';
export { l as loaded, a as playable } from './VideoPreview-GsGpXz96.js';
export { default as BaseExample } from './Example28-DwA2Gw6Q.js';
import { G } from './Block-DAfEyy2Q.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { k as k$1 } from './UploadText-CL6YR3zU.js';
import { R } from './index3-BRDKiycc.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './prism-python-B3XC7jWw.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './Upload-CBOKnugH.js';
import './Microphone-DE3HcBil.js';
import './Webcam-yHK4vsGx.js';
import './StreamingBar-5tkurVhZ.js';
import './DownloadLink-YXQWXYPr.js';
import './IconButton-C2_XRZp7.js';
import './Empty-Cuu1wIXQ.js';
import './ShareButton-CBu2I3ry.js';
import './Download-DQCZIsve.js';
import './IconButtonWrapper-BI5v6wo4.js';
import './Maximize-DWTTqpgt.js';
import './VolumeLevels-DVpBNR5e.js';
import './Play-Cq5X6dSn.js';
import './Undo-BWigpY9w.js';
import './Video2-BN0RGMYm.js';
import './ModifyUpload-CHrOyHY7.js';
import './Clear-CTfKb9Id.js';
import './Edit-DnE7yQ6U.js';
import './spring-D6sHki8W.js';

function es(w,g){w.component(h=>{let{value:i=null,subtitle:P=null,sources:y=["webcam","upload"],label:u=void 0,show_download_button:k$1=false,show_label:s$1=true,webcam_options:c,include_audio:_,autoplay:m,root:n$1,i18n:f,active_source:x="webcam",handle_reset_value:z=()=>{},max_file_size:p=null,upload:r$1,stream_handler:B,loop:l,uploading:t=void 0,upload_promise:o=void 0,playback_position:V=void 0,buttons:ts=null,on_custom_button_click:ls=null,onchange:S,onclear:q,onplay:A,onpause:C,onend:D,ondrag:is,onerror:U,onupload:F,onstart_recording:ps,onstop_recording:us,onstop:H,children:G}=g,W=false,b=x??"webcam";function J(a){i=a,S?.(a),a&&F?.(a);}function E(){i=null,S?.(null),q?.();}function K(a){W=true,S?.(a);}let L=false,d=true,I;function M(a){k(a,{show_label:s$1,Icon:r,label:u||"Video"}),a.push('<!----> <div data-testid="video" class="video-container svelte-ey25pz">'),i===null||i?.url===void 0?(a.push("<!--[-->"),a.push('<div class="upload-container svelte-ey25pz">'),b==="upload"?(a.push("<!--[-->"),ee(a,{filetype:"video/x-m4v,video/*",onload:J,max_file_size:p,onerror:e=>U?.(e),root:n$1,upload:r$1,stream_handler:B,aria_label:f("video.drop_to_upload"),get upload_promise(){return o},set upload_promise(e){o=e,d=false;},get dragging(){return L},set dragging(e){L=e,d=false;},get uploading(){return t},set uploading(e){t=e,d=false;},children:e=>{G?(e.push("<!--[-->"),G(e),e.push("<!---->")):e.push("<!--[!-->"),e.push("<!--]-->");},$$slots:{default:true}})):(a.push("<!--[!-->"),b==="webcam"?(a.push("<!--[-->"),$(a,{root:n$1,mirror_webcam:c.mirror,webcam_constraints:c.constraints,include_audio:_,mode:"video",i18n:f,upload:r$1,stream_every:1})):a.push("<!--[!-->"),a.push("<!--]-->")),a.push("<!--]--></div>")):(a.push("<!--[!-->"),i?.url?(a.push("<!--[-->"),a.push("<!---->"),Mt(a,{upload:r$1,root:n$1,interactive:true,autoplay:m,src:i.url,subtitle:P?.url,is_stream:false,onplay:()=>A?.(),onpause:()=>C?.(),onstop:()=>H?.(),onend:()=>D?.(),onerror:e=>U?.(e),mirror:c.mirror&&b==="webcam",label:u,handle_change:K,handle_reset_value:z,loop:l,value:i,i18n:f,show_download_button:k$1,handle_clear:E,has_change_history:W,get playback_position(){return V},set playback_position(e){V=e,d=false;}}),a.push("<!---->")):(a.push("<!--[!-->"),i.size?(a.push("<!--[-->"),a.push(`<div class="file-name svelte-ey25pz">${escape_html(i.orig_name||i.url)}</div> <div class="file-size svelte-ey25pz">${escape_html(te(i.size))}</div>`)):a.push("<!--[!-->"),a.push("<!--]-->")),a.push("<!--]-->")),a.push("<!--]--> "),n(a,{sources:y,handle_clear:E,get active_source(){return b},set active_source(e){b=e,d=false;}}),a.push("<!----></div>");}do d=true,I=h.copy(),M(I);while(!d);h.subsume(I),bind_props(g,{value:i,uploading:t,upload_promise:o,playback_position:V});});}function Ws(w,g){w.component(h=>{const{$$slots:i,$$events:P,...y}=g;let u;class k extends S{async get_data(){return u&&(await u,await tick()),await super.get_data()}}const s$1=new k(y);s$1.props.value;let c=false,_=false,m=s$1.props.sources?s$1.props.sources[0]:void 0,n=s$1.props.value;const f=()=>{n===null||s$1.props.value===n||(s$1.props.value=n);};function x(l){l!=null?s$1.props.value=l:s$1.props.value=null;}function z(l){const[t,o]=l.includes("Invalid file type")?["warning","complete"]:["error","error"];s$1.shared.loading_status.status=o,s$1.shared.loading_status.message=l,s$1.dispatch(t,l);}let p=true,r;function B(l){s$1.shared.interactive?(l.push("<!--[!-->"),G(l,{visible:s$1.shared.visible,variant:s$1.props.value===null&&m==="upload"?"dashed":"solid",border_mode:_?"focus":"base",padding:false,elem_id:s$1.shared.elem_id,elem_classes:s$1.shared.elem_classes,height:s$1.props.height||void 0,width:s$1.props.width,container:s$1.shared.container,scale:s$1.shared.scale,min_width:s$1.shared.min_width,allow_overflow:false,children:t=>{R(t,spread_props([{autoscroll:s$1.shared.autoscroll,i18n:s$1.i18n},s$1.shared.loading_status,{on_clear_status:()=>s$1.dispatch("clear_status",s$1.shared.loading_status)}])),t.push("<!----> "),es(t,{value:s$1.props.value,subtitle:s$1.props.subtitles,onchange:x,ondrag:o=>_=o,onerror:z,label:s$1.shared.label,show_label:s$1.shared.show_label,buttons:s$1.props.buttons??["download","share"],on_custom_button_click:o=>{s$1.dispatch("custom_button_click",{id:o});},sources:s$1.props.sources,active_source:m,webcam_options:s$1.props.webcam_options,include_audio:s$1.props.include_audio,autoplay:s$1.props.autoplay,root:s$1.shared.root,loop:s$1.props.loop,handle_reset_value:f,onclear:()=>{s$1.props.value=null,s$1.dispatch("clear"),s$1.dispatch("input");},onplay:()=>s$1.dispatch("play"),onpause:()=>s$1.dispatch("pause"),onupload:()=>{s$1.dispatch("upload"),s$1.dispatch("input");},onstop:()=>s$1.dispatch("stop"),onend:()=>s$1.dispatch("end"),onstart_recording:()=>s$1.dispatch("start_recording"),onstop_recording:()=>s$1.dispatch("stop_recording"),i18n:s$1.i18n,max_file_size:s$1.shared.max_file_size,upload:(...o)=>s$1.shared.client.upload(...o),stream_handler:(...o)=>s$1.shared.client.stream(...o),get upload_promise(){return u},set upload_promise(o){u=o,p=false;},get uploading(){return c},set uploading(o){c=o,p=false;},get playback_position(){return s$1.props.playback_position},set playback_position(o){s$1.props.playback_position=o,p=false;},children:o=>{k$1(o,{i18n:s$1.i18n,type:"video"});},$$slots:{default:true}}),t.push("<!---->");},$$slots:{default:true}})):(l.push("<!--[-->"),G(l,{visible:s$1.shared.visible,variant:s$1.props.value===null&&m==="upload"?"dashed":"solid",border_mode:_?"focus":"base",padding:false,elem_id:s$1.shared.elem_id,elem_classes:s$1.shared.elem_classes,height:s$1.props.height||void 0,width:s$1.props.width,container:s$1.shared.container,scale:s$1.shared.scale,min_width:s$1.shared.min_width,allow_overflow:false,children:t=>{R(t,spread_props([{autoscroll:s$1.shared.autoscroll,i18n:s$1.i18n},s$1.shared.loading_status,{on_clear_status:()=>s$1.dispatch("clear_status",s$1.shared.loading_status)}])),t.push("<!----> "),Ft(t,{value:s$1.props.value,subtitle:s$1.props.subtitles,label:s$1.shared.label,show_label:s$1.shared.show_label,autoplay:s$1.props.autoplay,loop:s$1.props.loop,buttons:s$1.props.buttons??["download","share"],on_custom_button_click:o=>{s$1.dispatch("custom_button_click",{id:o});},onplay:()=>s$1.dispatch("play"),onpause:()=>s$1.dispatch("pause"),onstop:()=>s$1.dispatch("stop"),onend:()=>s$1.dispatch("end"),onshare:o=>s$1.dispatch("share",o),onerror:o=>s$1.dispatch("error",o),i18n:s$1.i18n,upload:(...o)=>s$1.shared.client.upload(...o),get playback_position(){return s$1.props.playback_position},set playback_position(o){s$1.props.playback_position=o,p=false;}}),t.push("<!---->");},$$slots:{default:true}})),l.push("<!--]-->");}do p=true,r=h.copy(),B(r);while(!p);h.subsume(r);});}

export { es as BaseInteractiveVideo, Mt as BasePlayer, Ft as BaseStaticVideo, Ws as default, te as prettyBytes };
//# sourceMappingURL=index48-DglFJrnJ.js.map
