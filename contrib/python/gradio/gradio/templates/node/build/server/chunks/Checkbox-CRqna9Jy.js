import './async-DWBXLqlH.js';
import { d as attr_class, a as attr, c as bind_props } from './index-D1re1cuM.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';

function x(c,s$1){c.component(t=>{let{label:o="Checkbox",value:a=void 0,interactive:p=true,show_label:i=true,on_change:b,on_input:h,on_select:n}=s$1,l=!p;t.push(`<label${attr_class("checkbox-container svelte-1q8xtp9",void 0,{disabled:l})}><input${attr("checked",a,true)}${attr("disabled",l,true)} type="checkbox" name="test" data-testid="checkbox" class="svelte-1q8xtp9"/> `),i?(t.push("<!--[-->"),t.push(`<span class="label-text svelte-1q8xtp9">${escape_html(o)}</span>`)):t.push("<!--[!-->"),t.push("<!--]--></label>"),bind_props(s$1,{value:a});});}

export { x };
//# sourceMappingURL=Checkbox-CRqna9Jy.js.map
