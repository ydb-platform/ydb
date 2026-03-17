import{_ as u,g as j,s as q,a as Z,b as H,q as J,p as K,l as F,c as Q,E as X,I as Y,O as tt,d as et,y as rt,G as at}from"./mermaid.core-dCZaVwzf.js";import{p as it}from"./chunk-4BX2VUAB-C6pX-7OU.js";import{p as nt}from"./mermaid-parser.core-Cm3oO3zm.js";import{d as R}from"./arc-CSo4ybba.js";import{o as ot}from"./ordinal-BeghXfj9.js";import{a as S,t as z,n as st}from"./step-ChYay-CR.js";import"./index-xGeN4i2A.js";import"./svelte/svelte_svelte.js";import"./svelte/svelte_animate.js";import"./svelte/svelte_attachments.js";import"./svelte/svelte_easing.js";import"./svelte/svelte_internal_client.js";import"./svelte/svelte_internal_flags_async.js";import"./svelte/svelte_internal_flags_legacy.js";import"./svelte/svelte_internal_flags_tracing.js";import"./svelte/svelte_internal_server.js";import"./svelte/svelte_legacy.js";import"./svelte/svelte_motion.js";import"./svelte/svelte_reactivity.js";import"./svelte/svelte_reactivity_window.js";import"./svelte/svelte_server.js";import"./svelte/svelte_store.js";import"./svelte/svelte_transition.js";import"./svelte/svelte_events.js";import"./i18n-CeTBXmsd.js";import"./dispatch-kxCwF96_.js";import"./select-BigU4G0v.js";import"./min-Cb8b3dsT.js";import"./_baseUniq-BweWlAmD.js";import"./init-Dmth1JHB.js";function lt(t,r){return r<t?-1:r>t?1:r>=t?0:NaN}function ct(t){return t}function pt(){var t=ct,r=lt,m=null,y=S(0),o=S(z),l=S(0);function s(e){var i,c=(e=st(e)).length,d,x,h=0,p=new Array(c),n=new Array(c),v=+y.apply(this,arguments),w=Math.min(z,Math.max(-z,o.apply(this,arguments)-v)),f,C=Math.min(Math.abs(w)/c,l.apply(this,arguments)),$=C*(w<0?-1:1),g;for(i=0;i<c;++i)(g=n[p[i]=i]=+t(e[i],i,e))>0&&(h+=g);for(r!=null?p.sort(function(A,D){return r(n[A],n[D])}):m!=null&&p.sort(function(A,D){return m(e[A],e[D])}),i=0,x=h?(w-c*$)/h:0;i<c;++i,v=f)d=p[i],g=n[d],f=v+(g>0?g*x:0)+$,n[d]={data:e[d],index:i,value:g,startAngle:v,endAngle:f,padAngle:C};return n}return s.value=function(e){return arguments.length?(t=typeof e=="function"?e:S(+e),s):t},s.sortValues=function(e){return arguments.length?(r=e,m=null,s):r},s.sort=function(e){return arguments.length?(m=e,r=null,s):m},s.startAngle=function(e){return arguments.length?(y=typeof e=="function"?e:S(+e),s):y},s.endAngle=function(e){return arguments.length?(o=typeof e=="function"?e:S(+e),s):o},s.padAngle=function(e){return arguments.length?(l=typeof e=="function"?e:S(+e),s):l},s}var ut=at.pie,G={sections:new Map,showData:!1},T=G.sections,N=G.showData,dt=structuredClone(ut),gt=u(()=>structuredClone(dt),"getConfig"),mt=u(()=>{T=new Map,N=G.showData,rt()},"clear"),ft=u(({label:t,value:r})=>{if(r<0)throw new Error(`"${t}" has invalid value: ${r}. Negative values are not allowed in pie charts. All slice values must be >= 0.`);T.has(t)||(T.set(t,r),F.debug(`added new section: ${t}, with value: ${r}`))},"addSection"),ht=u(()=>T,"getSections"),vt=u(t=>{N=t},"setShowData"),St=u(()=>N,"getShowData"),L={getConfig:gt,clear:mt,setDiagramTitle:K,getDiagramTitle:J,setAccTitle:H,getAccTitle:Z,setAccDescription:q,getAccDescription:j,addSection:ft,getSections:ht,setShowData:vt,getShowData:St},yt=u((t,r)=>{it(t,r),r.setShowData(t.showData),t.sections.map(r.addSection)},"populateDb"),xt={parse:u(async t=>{const r=await nt("pie",t);F.debug(r),yt(r,L)},"parse")},wt=u(t=>`
  .pieCircle{
    stroke: ${t.pieStrokeColor};
    stroke-width : ${t.pieStrokeWidth};
    opacity : ${t.pieOpacity};
  }
  .pieOuterCircle{
    stroke: ${t.pieOuterStrokeColor};
    stroke-width: ${t.pieOuterStrokeWidth};
    fill: none;
  }
  .pieTitleText {
    text-anchor: middle;
    font-size: ${t.pieTitleTextSize};
    fill: ${t.pieTitleTextColor};
    font-family: ${t.fontFamily};
  }
  .slice {
    font-family: ${t.fontFamily};
    fill: ${t.pieSectionTextColor};
    font-size:${t.pieSectionTextSize};
    // fill: white;
  }
  .legend text {
    fill: ${t.pieLegendTextColor};
    font-family: ${t.fontFamily};
    font-size: ${t.pieLegendTextSize};
  }
`,"getStyles"),At=wt,Dt=u(t=>{const r=[...t.values()].reduce((o,l)=>o+l,0),m=[...t.entries()].map(([o,l])=>({label:o,value:l})).filter(o=>o.value/r*100>=1).sort((o,l)=>l.value-o.value);return pt().value(o=>o.value)(m)},"createPieArcs"),Ct=u((t,r,m,y)=>{F.debug(`rendering pie chart
`+t);const o=y.db,l=Q(),s=X(o.getConfig(),l.pie),e=40,i=18,c=4,d=450,x=d,h=Y(r),p=h.append("g");p.attr("transform","translate("+x/2+","+d/2+")");const{themeVariables:n}=l;let[v]=tt(n.pieOuterStrokeWidth);v??=2;const w=s.textPosition,f=Math.min(x,d)/2-e,C=R().innerRadius(0).outerRadius(f),$=R().innerRadius(f*w).outerRadius(f*w);p.append("circle").attr("cx",0).attr("cy",0).attr("r",f+v/2).attr("class","pieOuterCircle");const g=o.getSections(),A=Dt(g),D=[n.pie1,n.pie2,n.pie3,n.pie4,n.pie5,n.pie6,n.pie7,n.pie8,n.pie9,n.pie10,n.pie11,n.pie12];let E=0;g.forEach(a=>{E+=a});const O=A.filter(a=>(a.data.value/E*100).toFixed(0)!=="0"),b=ot(D);p.selectAll("mySlices").data(O).enter().append("path").attr("d",C).attr("fill",a=>b(a.data.label)).attr("class","pieCircle"),p.selectAll("mySlices").data(O).enter().append("text").text(a=>(a.data.value/E*100).toFixed(0)+"%").attr("transform",a=>"translate("+$.centroid(a)+")").style("text-anchor","middle").attr("class","slice"),p.append("text").text(o.getDiagramTitle()).attr("x",0).attr("y",-400/2).attr("class","pieTitleText");const W=[...g.entries()].map(([a,M])=>({label:a,value:M})),k=p.selectAll(".legend").data(W).enter().append("g").attr("class","legend").attr("transform",(a,M)=>{const P=i+c,B=P*W.length/2,V=12*i,U=M*P-B;return"translate("+V+","+U+")"});k.append("rect").attr("width",i).attr("height",i).style("fill",a=>b(a.label)).style("stroke",a=>b(a.label)),k.append("text").attr("x",i+c).attr("y",i-c).text(a=>o.getShowData()?`${a.label} [${a.value}]`:a.label);const _=Math.max(...k.selectAll("text").nodes().map(a=>a?.getBoundingClientRect().width??0)),I=x+e+i+c+_;h.attr("viewBox",`0 0 ${I} ${d}`),et(h,d,I,s.useMaxWidth)},"draw"),$t={draw:Ct},re={parser:xt,db:L,renderer:$t,styles:At};export{re as diagram};
