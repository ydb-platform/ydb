import './async-DWBXLqlH.js';
import { d as attr_class, e as ensure_array_like, a as attr, c as bind_props } from './index-D1re1cuM.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';

/* empty css                                        */function v(l,u){l.component(t=>{let o=u.button,p=u.on_click;t.push(`<button class="custom-button svelte-gnx6f7"${attr("title",o.value||"")}${attr("aria-label",o.value||"Custom action")}>`),o.value?(t.push("<!--[-->"),t.push(`<span class="custom-button-label svelte-gnx6f7">${escape_html(o.value)}</span>`)):t.push("<!--[!-->"),t.push("<!--]--></button>"),bind_props(u,{button:o,on_click:p});});}function y(l,u){l.component(t=>{let{top_panel:o=true,display_top_corner:p=false,show_background:_=true,buttons:n=null,on_custom_button_click:c=null,children:i}=u;if(t.push(`<div${attr_class(`icon-button-wrapper ${o?"top-panel":""} ${p?"display-top-corner":"hide-top-corner"} ${_?"":"no-background"}`,"svelte-1pnho82")}>`),i?(t.push("<!--[-->"),i(t),t.push("<!---->")):t.push("<!--[!-->"),t.push("<!--]--> "),n){t.push("<!--[-->"),t.push("<!--[-->");const h=ensure_array_like(n);for(let a=0,f=h.length;a<f;a++){let b=h[a];typeof b!="string"?(t.push("<!--[-->"),v(t,{button:b,on_click:m=>{c&&c(m);}})):t.push("<!--[!-->"),t.push("<!--]-->");}t.push("<!--]-->");}else t.push("<!--[!-->");t.push("<!--]--></div>");});}

export { v, y };
//# sourceMappingURL=IconButtonWrapper-BI5v6wo4.js.map
