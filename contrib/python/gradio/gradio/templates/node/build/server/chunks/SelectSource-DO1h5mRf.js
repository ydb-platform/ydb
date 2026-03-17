import './async-DWBXLqlH.js';
import { d as attr_class, c as bind_props } from './index-D1re1cuM.js';
import { h, e } from './Upload-CBOKnugH.js';
import { i } from './Microphone-DE3HcBil.js';
import { r } from './Video-BJvaeIU2.js';
import { h as h$1 } from './Webcam-yHK4vsGx.js';

function n(c,u){c.component(s$1=>{let{sources:t,active_source:o=void 0,handle_clear:b=()=>{},handle_select:v=()=>{}}=u;[...new Set(t)].length>1?(s$1.push("<!--[-->"),s$1.push('<span class="source-selection svelte-exvkcd" data-testid="source-select">'),t.includes("upload")?(s$1.push("<!--[-->"),s$1.push(`<button${attr_class("icon svelte-exvkcd",void 0,{selected:o==="upload"||!o})} aria-label="Upload file">`),h(s$1),s$1.push("<!----></button>")):s$1.push("<!--[!-->"),s$1.push("<!--]--> "),t.includes("microphone")?(s$1.push("<!--[-->"),s$1.push(`<button${attr_class("icon svelte-exvkcd",void 0,{selected:o==="microphone"})} aria-label="Record audio">`),i(s$1),s$1.push("<!----></button>")):s$1.push("<!--[!-->"),s$1.push("<!--]--> "),t.includes("webcam")?(s$1.push("<!--[-->"),s$1.push(`<button${attr_class("icon svelte-exvkcd",void 0,{selected:o==="webcam"})} aria-label="Capture from camera">`),h$1(s$1),s$1.push("<!----></button>")):s$1.push("<!--[!-->"),s$1.push("<!--]--> "),t.includes("webcam-video")?(s$1.push("<!--[-->"),s$1.push(`<button${attr_class("icon svelte-exvkcd",void 0,{selected:o==="webcam-video"})} aria-label="Record video from camera">`),r(s$1),s$1.push("<!----></button>")):s$1.push("<!--[!-->"),s$1.push("<!--]--> "),t.includes("clipboard")?(s$1.push("<!--[-->"),s$1.push(`<button${attr_class("icon svelte-exvkcd",void 0,{selected:o==="clipboard"})} aria-label="Paste from clipboard">`),e(s$1),s$1.push("<!----></button>")):s$1.push("<!--[!-->"),s$1.push("<!--]--></span>")):s$1.push("<!--[!-->"),s$1.push("<!--]-->"),bind_props(u,{active_source:o});});}

export { n };
//# sourceMappingURL=SelectSource-DO1h5mRf.js.map
