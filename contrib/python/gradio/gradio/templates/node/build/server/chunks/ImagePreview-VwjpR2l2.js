import { f as fallback } from './async-DWBXLqlH.js';
import { d as attr_class, c as bind_props } from './index-D1re1cuM.js';
import './2-BbOIMXxe.js';
import { O } from './utils.svelte-BHoyPsmo.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { k } from './BlockLabel-B2_AkSr2.js';
import { u } from './DownloadLink-YXQWXYPr.js';
import { w } from './IconButton-C2_XRZp7.js';
import { p } from './Empty-Cuu1wIXQ.js';
import { y as y$1 } from './ShareButton-CBu2I3ry.js';
import { l } from './Download-DQCZIsve.js';
import { i } from './Image2-BjKIvzAW.js';
import { y } from './IconButtonWrapper-BI5v6wo4.js';
import { v } from './FullscreenButton-CJEwZ820.js';
import { c } from './Image-Dl2g0mq0.js';
import './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './prism-python-B3XC7jWw.js';
import './index-server-D4bj_1UO.js';
import './Maximize-DWTTqpgt.js';

/* empty css                                          */function Q(g,o){g.component(a=>{let u$1=o.value,e=fallback(o.label,void 0),c$1=o.show_label,i$1=fallback(o.buttons,()=>[],true),f=fallback(o.on_custom_button_click,null),n=fallback(o.selectable,false),m=o.i18n,p$1=fallback(o.display_icon_button_wrapper_top_corner,false),b=fallback(o.fullscreen,false),_=fallback(o.show_button_background,true);k(a,{show_label:c$1,Icon:i,label:c$1?e||m("image.image"):""}),a.push("<!----> "),u$1==null||!u$1?.url?(a.push("<!--[-->"),p(a,{unpadded_box:true,size:"large",children:l=>{i(l);},$$slots:{default:true}})):(a.push("<!--[!-->"),a.push('<div class="image-container svelte-12vrxzd">'),y(a,{display_top_corner:p$1,show_background:_,buttons:i$1,on_custom_button_click:f,children:l$1=>{i$1.some(t=>typeof t=="string"&&t==="fullscreen")?(l$1.push("<!--[-->"),v(l$1,{fullscreen:b})):l$1.push("<!--[!-->"),l$1.push("<!--]--> "),i$1.some(t=>typeof t=="string"&&t==="download")?(l$1.push("<!--[-->"),u(l$1,{href:u$1.url,download:u$1.orig_name||"image",children:t=>{w(t,{Icon:l,label:m("common.download")});},$$slots:{default:true}})):l$1.push("<!--[!-->"),l$1.push("<!--]--> "),i$1.some(t=>typeof t=="string"&&t==="share")?(l$1.push("<!--[-->"),y$1(l$1,{i18n:m,formatter:async t=>t?`<img src="${await O(t)}" />`:"",value:u$1})):l$1.push("<!--[!-->"),l$1.push("<!--]-->");}}),a.push(`<!----> <button class="svelte-12vrxzd"><div${attr_class("image-frame svelte-12vrxzd",void 0,{selectable:n})}>`),c(a,{src:u$1.url,restProps:{loading:"lazy",alt:""}}),a.push("<!----></div></button></div>")),a.push("<!--]-->"),bind_props(o,{value:u$1,label:e,show_label:c$1,buttons:i$1,on_custom_button_click:f,selectable:n,i18n:m,display_icon_button_wrapper_top_corner:p$1,fullscreen:b,show_button_background:_});});}

export { Q as default };
//# sourceMappingURL=ImagePreview-VwjpR2l2.js.map
