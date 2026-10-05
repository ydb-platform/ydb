// The monitoring UI owns React, ChartKit and Yagr. Load its shared entry assets,
// rather than bundling another copy of these libraries into the metrics viewer.
let loading;
export function loadChartKit() {
 if(window.YdbMetricChartKit)return Promise.resolve(window.YdbMetricChartKit);
 if(loading)return loading;
 loading=(async()=>{
  const base=new URL('../../monitoring/',import.meta.url);
  const response=await fetch(new URL('static/js/metric-chart-assets.json',base));
  if(!response.ok)throw new Error('Unable to load monitoring chart assets');
  const assets=await response.json();
  if(!Array.isArray(assets.scripts)||!assets.scripts.length||!Array.isArray(assets.styles))throw new Error('Invalid monitoring chart assets');
  const assetUrl=path=>{
   if(typeof path!=='string'||!path.startsWith('static/')||path.includes('..'))throw new Error('Invalid monitoring asset path');
   return new URL(path,base).href;
  };
  await Promise.all(assets.styles.map(path=>new Promise((resolve,reject)=>{
   const href=assetUrl(path);
   if([...document.styleSheets].some(sheet=>sheet.href===href)){resolve();return;}
   const link=document.createElement('link');link.rel='stylesheet';link.href=href;
   link.onload=resolve;link.onerror=()=>{link.remove();reject(new Error('Unable to load monitoring chart styles'));};
   document.head.append(link);
  })));
  for(const path of assets.scripts)await new Promise((resolve,reject)=>{
   const script=document.createElement('script');script.src=assetUrl(path);script.async=false;
   script.onload=resolve;script.onerror=()=>{script.remove();reject(new Error('Unable to load monitoring chart script'));};
   document.head.append(script);
  });
  if(!window.YdbMetricChartKit)throw new Error('Monitoring chart entry did not initialize');
  return window.YdbMetricChartKit;
 })().catch(error=>{loading=undefined;throw error;});
 return loading;
}
