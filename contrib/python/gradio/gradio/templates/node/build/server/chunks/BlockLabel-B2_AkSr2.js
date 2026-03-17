import { f as fallback } from './async-DWBXLqlH.js';
import { a as attr, d as attr_class, c as bind_props } from './index-D1re1cuM.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';

/* empty css                                        */function k(t,a){let b=fallback(a.label,null),s$1=a.Icon,e=fallback(a.show_label,true),c=fallback(a.disable,false),f=fallback(a.float,true),o=fallback(a.rtl,false);t.push(`<label for="" data-testid="block-label"${attr("dir",o?"rtl":"ltr")}${attr_class("svelte-19djge9",void 0,{hide:!e,"sr-only":!e,float:f,"hide-label":c})}><span class="svelte-19djge9">`),s$1(t,{}),t.push(`<!----></span> ${escape_html(b)}</label>`),bind_props(a,{label:b,Icon:s$1,show_label:e,disable:c,float:f,rtl:o});}

export { k };
//# sourceMappingURL=BlockLabel-B2_AkSr2.js.map
