import './async-DWBXLqlH.js';
import { a as attr, d as attr_class, g as attr_style, j as stringify, b as spread_props, i as slot } from './index-D1re1cuM.js';
import { R } from './index3-BRDKiycc.js';

function y(e,a){e.component(s$1=>{let{$$slots:h,$$events:d,...t}=a,p=t.scale??null,n=t.min_width??0,m=t.elem_id??"",u=t.elem_classes??[],c=t.visible??true,o=t.variant??"default",i=t.loading_status;t.show_progress,s$1.push(`<div${attr("id",m)}${attr_class(`column ${stringify(u.join(" "))}`,"svelte-siq5d6",{compact:o==="compact",panel:o==="panel",hide:!c})}${attr_style("",{"flex-grow":p,"min-width":`calc(min(${stringify(n)}px, 100%))`})}>`),i&&i.show_progress?(s$1.push("<!--[-->"),R(s$1,spread_props([{autoscroll:t.autoscroll,i18n:t.i18n},i,{status:i?i.status=="pending"?"generating":i.status:null}]))):s$1.push("<!--[!-->"),s$1.push("<!--]--> <!--[-->"),slot(s$1,a,"default",{},null),s$1.push("<!--]--></div>");});}

export { y };
//# sourceMappingURL=Index.svelte_svelte_type_style_lang-C-Is37XG.js.map
