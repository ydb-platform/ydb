import './async-DWBXLqlH.js';
import { c as bind_props } from './index-D1re1cuM.js';
import { w } from './IconButton-C2_XRZp7.js';
import { c as createEventDispatcher } from './index-server-D4bj_1UO.js';
import './2-BbOIMXxe.js';
import { h } from './utils.svelte-BHoyPsmo.js';

function u(e){e.push('<svg id="icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" width="100%" height="100%"><path d="M23,20a5,5,0,0,0-3.89,1.89L11.8,17.32a4.46,4.46,0,0,0,0-2.64l7.31-4.57A5,5,0,1,0,18,7a4.79,4.79,0,0,0,.2,1.32l-7.31,4.57a5,5,0,1,0,0,6.22l7.31,4.57A4.79,4.79,0,0,0,18,25a5,5,0,1,0,5-5ZM23,4a3,3,0,1,1-3,3A3,3,0,0,1,23,4ZM7,19a3,3,0,1,1,3-3A3,3,0,0,1,7,19Zm16,9a3,3,0,1,1,3-3A3,3,0,0,1,23,28Z" fill="currentColor"></path></svg>');}function y(e,a){e.component(l=>{const r=createEventDispatcher();let n=a.formatter,i=a.value,s$1=a.i18n,o=false;w(l,{Icon:u,label:s$1("common.share"),pending:o,onclick:async()=>{try{o=!0;const t=await n(i);r("share",{description:t});}catch(t){console.error(t);t instanceof h?t.message:"Share failed.";}finally{o=false;}}}),bind_props(a,{formatter:n,value:i,i18n:s$1});});}

export { u, y };
//# sourceMappingURL=ShareButton-CBu2I3ry.js.map
