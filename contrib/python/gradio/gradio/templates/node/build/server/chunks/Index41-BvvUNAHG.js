import { f as fallback } from './async-DWBXLqlH.js';
import { s as store_get, a as attr, d as attr_class, g as attr_style, j as stringify, i as slot, u as unsubscribe_stores, c as bind_props } from './index-D1re1cuM.js';
import './2-BbOIMXxe.js';
import { S } from './utils.svelte-BHoyPsmo.js';
import { t as tick, c as createEventDispatcher } from './index-server-D4bj_1UO.js';
import { O } from './Walkthrough.svelte_svelte_type_style_lang-DPfou4O_.js';
import { y } from './Index.svelte_svelte_type_style_lang-C-Is37XG.js';
import { g as getContext } from './context-BZS6UlnY.js';
import './escaping-CBnpiEl5.js';
import './uneval-ZBzcyJ66.js';
import './clone-CubQhOZi.js';
import './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';
import './index3-BRDKiycc.js';
import './spring-D6sHki8W.js';
import './MarkdownCode.svelte_svelte_type_style_lang-CSnoIIp_.js';
import './prism-python-B3XC7jWw.js';
import './IconButton-C2_XRZp7.js';
import './Clear-CTfKb9Id.js';

function N(d,e){d.component(n=>{var r;let m,i=fallback(e.elem_id,""),l=fallback(e.elem_classes,()=>[],true),s$1=e.label,c=fallback(e.id,()=>({}),true),_=e.visible,f=e.interactive,u=e.order,o=e.scale,p=e.component_id;const h=createEventDispatcher(),{register_tab:v,unregister_tab:g,selected_tab:x,selected_tab_index:k}=getContext(O);let b;function y$1(a,B){return a=JSON.parse(a),v(a,B)}m=JSON.stringify({label:s$1,id:c,elem_id:i,visible:_,interactive:f,scale:o,component_id:p}),b=y$1(m,u),store_get(r??={},"$selected_tab_index",k)===b&&tick().then(()=>h("select",{value:s$1,index:b})),n.push(`<div${attr("id",i)}${attr_class(`tabitem ${stringify(l.join(" "))}`,"svelte-dmtrd3",{"grow-children":o>=1})} role="tabpanel"${attr_style("",{display:store_get(r??={},"$selected_tab",x)===c&&_!==false?"flex":"none","flex-grow":o})}>`),y(n,{scale:o>=1?o:null,children:a=>{a.push("<!--[-->"),slot(a,e,"default",{},null),a.push("<!--]-->");},$$slots:{default:true}}),n.push("<!----></div>"),r&&unsubscribe_stores(r),bind_props(e,{elem_id:i,elem_classes:l,label:s$1,id:c,visible:_,interactive:f,order:u,scale:o,component_id:p});});}function z(d,e){d.component(n=>{let{$$slots:r,$$events:m,...i}=e;const l=new S(i);N(n,{elem_id:l.shared.elem_id,elem_classes:l.shared.elem_classes,label:l.shared.label,visible:l.shared.visible,interactive:l.shared.interactive,id:l.props.id,order:l.props.order,scale:l.props.scale,component_id:l.props.component_id,children:s$1=>{s$1.push("<!--[-->"),slot(s$1,e,"default",{},null),s$1.push("<!--]-->");},$$slots:{default:true}});});}

export { N as BaseTabItem, z as default };
//# sourceMappingURL=Index41-BvvUNAHG.js.map
