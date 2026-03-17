import './async-DWBXLqlH.js';
import { b as spread_props, d as attr_class, a as attr, e as ensure_array_like, g as attr_style, j as stringify } from './index-D1re1cuM.js';
import { G } from './Block-DAfEyy2Q.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import { k } from './BlockLabel-B2_AkSr2.js';
import { p } from './Empty-Cuu1wIXQ.js';
import { i } from './Image2-BjKIvzAW.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { y } from './IconButtonWrapper-BI5v6wo4.js';
import { v } from './FullscreenButton-CJEwZ820.js';
import { R } from './index3-BRDKiycc.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './prism-python-B3XC7jWw.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './IconButton-C2_XRZp7.js';
import './Maximize-DWTTqpgt.js';
import './spring-D6sHki8W.js';
import './Clear-CTfKb9Id.js';

function Q(f,_){f.component(c=>{const{$$slots:E,$$events:F,...d}=_,t=new S(d);t.props.value;let n=null,i$1=false,g=t.shared.label||t.i18n("annotated_image.annotated_image"),u=true,h;function v$1(b){G(b,{visible:t.shared.visible,elem_id:t.shared.elem_id,elem_classes:t.shared.elem_classes,padding:false,height:t.props.height,width:t.props.width,allow_overflow:false,container:t.shared.container,scale:t.shared.scale,min_width:t.shared.min_width,get fullscreen(){return i$1},set fullscreen(s){i$1=s,u=false;},children:s$1=>{if(R(s$1,spread_props([{autoscroll:t.shared.autoscroll,i18n:t.i18n},t.shared.loading_status])),s$1.push("<!----> "),k(s$1,{show_label:t.shared.show_label,Icon:i,label:g}),s$1.push('<!----> <div class="container svelte-1oizopk">'),t.props.value==null)s$1.push("<!--[-->"),p(s$1,{size:"large",unpadded_box:true,children:p=>{i(p);},$$slots:{default:true}});else {s$1.push("<!--[!-->"),s$1.push('<div class="image-container svelte-1oizopk">'),y(s$1,{buttons:t.props.buttons||[],on_custom_button_click:o=>{t.dispatch("custom_button_click",{id:o});},children:o=>{(t.props.buttons||[]).some(a=>typeof a=="string"&&a==="fullscreen")?(o.push("<!--[-->"),v(o,{fullscreen:i$1})):o.push("<!--[!-->"),o.push("<!--]-->");}}),s$1.push(`<!----> <img${attr_class("base-image svelte-1oizopk",void 0,{"fit-height":t.props.height&&!i$1})}${attr("src",t.props.value?t.props.value.image.url:null)} alt="the base file that is annotated"/> <!--[-->`);const p=ensure_array_like(t.props.value?t.props.value.annotations:[]);for(let o=0,a=p.length;o<a;o++){let e=p[o];s$1.push(`<img${attr("alt",`segmentation mask identifying ${stringify(t.shared.label)} within the uploaded file`)}${attr_class("mask fit-height svelte-1oizopk",void 0,{"fit-height":!i$1,active:n==e.label,inactive:n!=e.label&&n!=null})}${attr("src",e.image.url)}${attr_style(t.props.color_map&&e.label in t.props.color_map?null:`filter: hue-rotate(${Math.round(o*360/(t.props.value?.annotations.length??1))}deg);`)}/>`);}if(s$1.push("<!--]--></div> "),t.props.show_legend&&t.props.value){s$1.push("<!--[-->"),s$1.push('<div class="legend svelte-1oizopk"><!--[-->');const o=ensure_array_like(t.props.value.annotations);for(let a=0,e=o.length;a<e;a++){let r=o[a];s$1.push(`<button class="legend-item svelte-1oizopk"${attr_style(`background-color: ${stringify(t.props.color_map&&r.label in t.props.color_map?t.props.color_map[r.label]+"88":`hsla(${Math.round(a*360/t.props.value.annotations.length)}, 100%, 50%, 0.3)`)}`)}>${escape_html(r.label)}</button>`);}s$1.push("<!--]--></div>");}else s$1.push("<!--[!-->");s$1.push("<!--]-->");}s$1.push("<!--]--></div>");},$$slots:{default:true}});}do u=true,h=c.copy(),v$1(h);while(!u);c.subsume(h);});}

export { Q as default };
//# sourceMappingURL=Index22-JBRCr2Ov.js.map
