import{_ as l,s as S,g as k,q as R,p as E,a as F,b as I,H as D,y as G,D as f,E as C,F as P,l as z,K as H}from"./mermaid.core-vMMZVCDT-DDcWQHpZ.js";import{p as V}from"./chunk-4BX2VUAB-DiOwmOep-CeXk0lKM.js";import{p as W}from"./treemap-KMMF4GRG-BLLAjc28-Bj1F9L_c.js";import"./index-xGeN4i2A.js";import"./svelte/svelte_svelte.js";import"./svelte/svelte_animate.js";import"./svelte/svelte_attachments.js";import"./svelte/svelte_easing.js";import"./svelte/svelte_internal_client.js";import"./svelte/svelte_internal_flags_async.js";import"./svelte/svelte_internal_flags_legacy.js";import"./svelte/svelte_internal_flags_tracing.js";import"./svelte/svelte_internal_server.js";import"./svelte/svelte_legacy.js";import"./svelte/svelte_motion.js";import"./svelte/svelte_reactivity.js";import"./svelte/svelte_reactivity_window.js";import"./svelte/svelte_server.js";import"./svelte/svelte_store.js";import"./svelte/svelte_transition.js";import"./svelte/svelte_events.js";import"./Index-D1ls8qWH.js";import"./i18n-CeTBXmsd.js";import"./utils.svelte-D2llzkGs.js";import"./index-tFQomdd2.js";import"./dsv-DB8NKgIY.js";import"./index-CnqicUFC.js";import"./ModifyUpload-DPJLPIwz.js";import"./ScrollFade-B34EcSMi.js";import"./MarkdownCode-CEyGWpPm.js";import"./prism-python-BSSAvU0Z.js";import"./Index-60Uec2ns.js";import"./StreamingBar-m3Ywvutz.js";import"./FullscreenButton-DrvnVVOX.js";import"./Example-BRN12iqL.js";import"./min-BP3TZd4l-CQnIHz4D.js";import"./_baseUniq-CrYdfo_J-DsRTJFct.js";var h={showLegend:!0,ticks:5,max:null,min:0,graticule:"circle"},w={axes:[],curves:[],options:h},g=structuredClone(w),B=P.radar,j=l(()=>f({...B,...C().radar}),"getConfig"),b=l(()=>g.axes,"getAxes"),q=l(()=>g.curves,"getCurves"),K=l(()=>g.options,"getOptions"),N=l(r=>{g.axes=r.map(t=>({name:t.name,label:t.label??t.name}))},"setAxes"),U=l(r=>{g.curves=r.map(t=>({name:t.name,label:t.label??t.name,entries:X(t.entries)}))},"setCurves"),X=l(r=>{if(r[0].axis==null)return r.map(e=>e.value);const t=b();if(t.length===0)throw new Error("Axes must be populated before curves for reference entries");return t.map(e=>{const a=r.find(o=>{var s;return((s=o.axis)==null?void 0:s.$refText)===e.name});if(a===void 0)throw new Error("Missing entry for axis "+e.label);return a.value})},"computeCurveEntries"),Y=l(r=>{var t,e,a,o,s;const i=r.reduce((n,c)=>(n[c.name]=c,n),{});g.options={showLegend:((t=i.showLegend)==null?void 0:t.value)??h.showLegend,ticks:((e=i.ticks)==null?void 0:e.value)??h.ticks,max:((a=i.max)==null?void 0:a.value)??h.max,min:((o=i.min)==null?void 0:o.value)??h.min,graticule:((s=i.graticule)==null?void 0:s.value)??h.graticule}},"setOptions"),Z=l(()=>{G(),g=structuredClone(w)},"clear"),$={getAxes:b,getCurves:q,getOptions:K,setAxes:N,setCurves:U,setOptions:Y,getConfig:j,clear:Z,setAccTitle:I,getAccTitle:F,setDiagramTitle:E,getDiagramTitle:R,getAccDescription:k,setAccDescription:S},J=l(r=>{V(r,$);const{axes:t,curves:e,options:a}=r;$.setAxes(t),$.setCurves(e),$.setOptions(a)},"populate"),Q={parse:l(async r=>{const t=await W("radar",r);z.debug(t),J(t)},"parse")},tt=l((r,t,e,a)=>{const o=a.db,s=o.getAxes(),i=o.getCurves(),n=o.getOptions(),c=o.getConfig(),p=o.getDiagramTitle(),m=D(t),d=et(m,c),u=n.max??Math.max(...i.map(y=>Math.max(...y.entries))),x=n.min,v=Math.min(c.width,c.height)/2;rt(d,s,v,n.ticks,n.graticule),at(d,s,v,c),M(d,s,i,x,u,n.graticule,c),T(d,i,n.showLegend,c),d.append("text").attr("class","radarTitle").text(p).attr("x",0).attr("y",-c.height/2-c.marginTop)},"draw"),et=l((r,t)=>{const e=t.width+t.marginLeft+t.marginRight,a=t.height+t.marginTop+t.marginBottom,o={x:t.marginLeft+t.width/2,y:t.marginTop+t.height/2};return r.attr("viewbox",`0 0 ${e} ${a}`).attr("width",e).attr("height",a),r.append("g").attr("transform",`translate(${o.x}, ${o.y})`)},"drawFrame"),rt=l((r,t,e,a,o)=>{if(o==="circle")for(let s=0;s<a;s++){const i=e*(s+1)/a;r.append("circle").attr("r",i).attr("class","radarGraticule")}else if(o==="polygon"){const s=t.length;for(let i=0;i<a;i++){const n=e*(i+1)/a,c=t.map((p,m)=>{const d=2*m*Math.PI/s-Math.PI/2,u=n*Math.cos(d),x=n*Math.sin(d);return`${u},${x}`}).join(" ");r.append("polygon").attr("points",c).attr("class","radarGraticule")}}},"drawGraticule"),at=l((r,t,e,a)=>{const o=t.length;for(let s=0;s<o;s++){const i=t[s].label,n=2*s*Math.PI/o-Math.PI/2;r.append("line").attr("x1",0).attr("y1",0).attr("x2",e*a.axisScaleFactor*Math.cos(n)).attr("y2",e*a.axisScaleFactor*Math.sin(n)).attr("class","radarAxisLine"),r.append("text").text(i).attr("x",e*a.axisLabelFactor*Math.cos(n)).attr("y",e*a.axisLabelFactor*Math.sin(n)).attr("class","radarAxisLabel")}},"drawAxes");function M(r,t,e,a,o,s,i){const n=t.length,c=Math.min(i.width,i.height)/2;e.forEach((p,m)=>{if(p.entries.length!==n)return;const d=p.entries.map((u,x)=>{const v=2*Math.PI*x/n-Math.PI/2,y=A(u,a,o,c),_=y*Math.cos(v),O=y*Math.sin(v);return{x:_,y:O}});s==="circle"?r.append("path").attr("d",L(d,i.curveTension)).attr("class",`radarCurve-${m}`):s==="polygon"&&r.append("polygon").attr("points",d.map(u=>`${u.x},${u.y}`).join(" ")).attr("class",`radarCurve-${m}`)})}l(M,"drawCurves");function A(r,t,e,a){const o=Math.min(Math.max(r,t),e);return a*(o-t)/(e-t)}l(A,"relativeRadius");function L(r,t){const e=r.length;let a=`M${r[0].x},${r[0].y}`;for(let o=0;o<e;o++){const s=r[(o-1+e)%e],i=r[o],n=r[(o+1)%e],c=r[(o+2)%e],p={x:i.x+(n.x-s.x)*t,y:i.y+(n.y-s.y)*t},m={x:n.x-(c.x-i.x)*t,y:n.y-(c.y-i.y)*t};a+=` C${p.x},${p.y} ${m.x},${m.y} ${n.x},${n.y}`}return`${a} Z`}l(L,"closedRoundCurve");function T(r,t,e,a){if(!e)return;const o=(a.width/2+a.marginRight)*3/4,s=-(a.height/2+a.marginTop)*3/4,i=20;t.forEach((n,c)=>{const p=r.append("g").attr("transform",`translate(${o}, ${s+c*i})`);p.append("rect").attr("width",12).attr("height",12).attr("class",`radarLegendBox-${c}`),p.append("text").attr("x",16).attr("y",0).attr("class","radarLegendText").text(n.label)})}l(T,"drawLegend");var ot={draw:tt},nt=l((r,t)=>{let e="";for(let a=0;a<r.THEME_COLOR_LIMIT;a++){const o=r[`cScale${a}`];e+=`
		.radarCurve-${a} {
			color: ${o};
			fill: ${o};
			fill-opacity: ${t.curveOpacity};
			stroke: ${o};
			stroke-width: ${t.curveStrokeWidth};
		}
		.radarLegendBox-${a} {
			fill: ${o};
			fill-opacity: ${t.curveOpacity};
			stroke: ${o};
		}
		`}return e},"genIndexStyles"),st=l(r=>{const t=H(),e=C(),a=f(t,e.themeVariables),o=f(a.radar,r);return{themeVariables:a,radarOptions:o}},"buildRadarStyleOptions"),it=l(({radar:r}={})=>{const{themeVariables:t,radarOptions:e}=st(r);return`
	.radarTitle {
		font-size: ${t.fontSize};
		color: ${t.titleColor};
		dominant-baseline: hanging;
		text-anchor: middle;
	}
	.radarAxisLine {
		stroke: ${e.axisColor};
		stroke-width: ${e.axisStrokeWidth};
	}
	.radarAxisLabel {
		dominant-baseline: middle;
		text-anchor: middle;
		font-size: ${e.axisLabelFontSize}px;
		color: ${e.axisColor};
	}
	.radarGraticule {
		fill: ${e.graticuleColor};
		fill-opacity: ${e.graticuleOpacity};
		stroke: ${e.graticuleColor};
		stroke-width: ${e.graticuleStrokeWidth};
	}
	.radarLegendText {
		text-anchor: start;
		font-size: ${e.legendFontSize}px;
		dominant-baseline: hanging;
	}
	${nt(t,e)}
	`},"styles"),qt={parser:Q,db:$,renderer:ot,styles:it};export{qt as diagram};
