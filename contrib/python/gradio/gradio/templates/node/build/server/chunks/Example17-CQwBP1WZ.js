import { f as fallback } from './async-DWBXLqlH.js';
import { d as attr_class, e as ensure_array_like, a as attr, c as bind_props } from './index-D1re1cuM.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import './context-BZS6UlnY.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';

function m(o,i){o.component(s$1=>{let l=i.value,p=i.type,c=fallback(i.selected,false);if(s$1.push(`<div${attr_class("container svelte-xds4q5",void 0,{table:p==="table",gallery:p==="gallery",selected:c})}>`),l&&l.length>0){s$1.push("<!--[-->"),s$1.push('<div class="images-wrapper svelte-xds4q5"><!--[-->');const h=ensure_array_like(l.slice(0,3));for(let u=0,v=h.length;u<v;u++){let a=h[u];"image"in a&&a.image?(s$1.push("<!--[-->"),s$1.push(`<div class="image-container svelte-xds4q5"><img${attr("src",a.image.url)}${attr("alt",a.caption||"")} class="svelte-xds4q5"/> `),a.caption?(s$1.push("<!--[-->"),s$1.push(`<span class="caption svelte-xds4q5">${escape_html(a.caption)}</span>`)):s$1.push("<!--[!-->"),s$1.push("<!--]--></div>")):(s$1.push("<!--[!-->"),"video"in a&&a.video?(s$1.push("<!--[-->"),s$1.push(`<div class="image-container svelte-xds4q5"><video${attr("src",a.video.url)}${attr("controls",false,true)} muted preload="metadata" class="svelte-xds4q5"></video> `),a.caption?(s$1.push("<!--[-->"),s$1.push(`<span class="caption svelte-xds4q5">${escape_html(a.caption)}</span>`)):s$1.push("<!--[!-->"),s$1.push("<!--]--></div>")):s$1.push("<!--[!-->"),s$1.push("<!--]-->")),s$1.push("<!--]-->");}s$1.push("<!--]--> "),l.length>3?(s$1.push("<!--[-->"),s$1.push('<div class="more-indicator svelte-xds4q5">…</div>')):s$1.push("<!--[!-->"),s$1.push("<!--]--></div>");}else s$1.push("<!--[!-->");s$1.push("<!--]--></div>"),bind_props(i,{value:l,type:p,selected:c});});}

export { m as default };
//# sourceMappingURL=Example17-CQwBP1WZ.js.map
