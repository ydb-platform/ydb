import './async-DWBXLqlH.js';
import { a as attr } from './index-D1re1cuM.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import { o as onDestroy } from './index-server-D4bj_1UO.js';
import './2-BbOIMXxe.js';
import { R } from './utils.svelte-BHoyPsmo.js';

function Z(c,t){c.component(e=>{let{value:i,label:s$1,loop:n=false}=t,r=0;i.url;onDestroy(()=>{}),e.push(`<div class="minimal-audio-player svelte-15unlcf"${attr("aria-label",s$1||"Audio")}${attr("data-testid",s$1&&typeof s$1=="string"&&s$1.trim()?"waveform-"+s$1:"unlabelled-audio")}><button class="play-btn svelte-15unlcf"${attr("aria-label","Play")}>`),e.push("<!--[!-->"),e.push('<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" class="svelte-15unlcf"><path d="M8 5.74537C8 5.06444 8.77346 4.64713 9.35139 5.02248L18.0227 10.2771C18.5518 10.6219 18.5518 11.3781 18.0227 11.7229L9.35139 16.9775C8.77346 17.3529 8 16.9356 8 16.2546V5.74537Z" fill="currentColor"></path></svg>'),e.push(`<!--]--></button> <div class="waveform-wrapper svelte-15unlcf"></div> <div class="timestamp svelte-15unlcf">${escape_html(R(r))}</div></div>`);});}

export { Z };
//# sourceMappingURL=MinimalAudioPlayer-CXGP--cm.js.map
