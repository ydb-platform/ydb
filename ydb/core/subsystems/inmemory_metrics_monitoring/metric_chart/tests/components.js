import {createAllocationBar} from '../allocation.js';
import {createMetricChart,createMetricChartCursorGroup,formatMetricValue} from '../chart.js';

const fixtures=document.getElementById('fixtures'),results=[];
function check(condition,message){if(!condition)throw Error(message);results.push('PASS '+message);}
function host(parent=fixtures){const node=document.createElement('div');parent.append(node);return node;}
function move(svg,x=.6){const r=svg.getBoundingClientRect();svg.dispatchEvent(new PointerEvent('pointermove',{clientX:r.left+r.width*x,clientY:r.top+80}));}
async function plot(host){
    for(let i=0;i<200;i++){const p=host.querySelector('.u-over');if(p&&p.querySelector('[data-cursor]'))return p;await new Promise(r=>setTimeout(r,25));}
    throw Error('ChartKit did not render: '+host.textContent);
}
try{
    check(formatMetricValue(2048,{unit:'bytesPerSecond',precision:0})==='2 KiB/s','throughput uses IEC bytes/s');
    check(formatMetricValue(8,{unit:'iops',precision:0})==='8 ops/s','IOPS units');
    let selected=0;
    const autoHost=host(),auto=createAllocationBar(autoHost,{maxSegments:2,onSelect:()=>selected++});
    auto.setData({capacity:100,free:10,segments:[{key:'a',label:'A',value:10},{key:'b',label:'B',value:30},{key:'c',label:'C',value:20}]});
    check([...autoHost.querySelectorAll('[data-segment-key]')].map(n=>n.dataset.segmentKey).join(',')==='b,c,others,unattributed,free','default top-N, Other, unattributed and free');
    check(autoHost.querySelector('.ymc-allocation-summary')&&autoHost.querySelector('.ymc-allocation-legend'),'default summary and legend retained');
    check(auto.getColor('a')==='#8995a5','omitted owner resolves to aggregate color');
    auto.highlight('a');check(autoHost.querySelector('[data-segment-key="others"]').style.opacity==='1','omitted owner highlights Other');
    autoHost.querySelector('[data-segment-key="b"]').click();
    check(selected===0&&document.querySelector('.ymc-tooltip-pinned'),'default allocation click still pins instead of selecting');
    document.querySelector('.ymc-tooltip-pinned button').click();
    const preparedHost=host();let hover=null;
    const prepared=createAllocationBar(preparedHost,{mode:'prepared',summary:false,legend:false,title:'Tablet IOPS',entityLabel:'Tablet',segmentLabels:true,onHover:s=>hover=s?.key,formatValue:v=>v+' ops/s'});
    const segments=Array.from({length:90},(_,i)=>({key:String(i),label:'Tablet '+i,value:i+1,color:`hsl(${i*4} 60% 45%)`,href:'?tablet='+i}));
    segments.push({key:'others',label:'Other / 10 tablets',value:100,color:'#d5d8de',pattern:'striped',href:'?other=iops'});
    prepared.setData({segments});
    check(preparedHost.querySelectorAll('[data-segment-key]').length===91,'prepared mode retains all 90 owners and explicit Other');
    check(preparedHost.querySelector('[data-segment-key]').dataset.segmentKey==='0','prepared input order retained');
    check(!preparedHost.querySelector('.ymc-allocation-summary')&&!preparedHost.querySelector('.ymc-allocation-legend'),'summary and legend can be hidden');
    const part=preparedHost.querySelector('[data-segment-key="89"]');part.dispatchEvent(new PointerEvent('pointerenter'));
    check(hover==='89'&&part.getAttribute('href')==='?tablet=89','hover event and normal navigation');
    part.dispatchEvent(new PointerEvent('pointermove',{clientX:400,clientY:200}));
    const tip=document.querySelector('.ymc-tooltip:not([hidden])');
    check(tip.textContent.includes('Tablet IOPS')&&tip.textContent.includes('90 ops/s'),'custom tooltip title and formatting');
    check(tip.querySelector('.ymc-tooltip-nearest').textContent.includes('Tablet 89'),'hovered row retained beyond tooltip row limit');
    part.dispatchEvent(new PointerEvent('pointerleave'));check(hover===undefined,'hover clears');
    const zeroHost=host(),zero=createAllocationBar(zeroHost,{mode:'prepared'});
    zero.setData({segments:[{key:'zero',label:'Zero',value:0},{key:'reserve',label:'Reserve',value:4,pattern:'striped',color:'#ddd'}]});
    check(zeroHost.querySelector('.ymc-allocation-legend').textContent.includes('Zero'),'zero categories retained in prepared legend');
    check(zeroHost.querySelector('[data-segment-key="reserve"]').classList.contains('ymc-striped'),'striped reserve segment');
    zero.setData({segments:[{key:'bad',label:'<img src=x>',value:1,href:'javascript:alert(1)'}]});
    check(!zeroHost.querySelector('a')&&!zeroHost.querySelector('img'),'unsafe links rejected and labels remain text');
    const grid=host();grid.className='charts';const left=host(grid),right=host(grid);
    const points=[{time:0,raw:'2',value:2},{time:1000,raw:'3',value:3},{time:2000,raw:null,value:null},{time:3000,raw:'1',value:1},{time:4000,raw:'2',value:2}];
    const series=[{key:'read',display:'Read',color:'#377bba',points},{key:'sync',display:'Sync',color:'#5caaa5',points:points.map(p=>({...p,value:p.value===null?null:p.value*2,raw:p.value===null?null:String(p.value*2)}))}];
    const cursorGroup=createMetricChartCursorGroup();
    const chart2=createMetricChart(right,{cursorGroup,settings:{unit:'bytesPerSecond',height:220,fill:true}});
    const chart=createMetricChart(left,{settings:{unit:'iops',height:220,fill:true},tooltipTotal:false,tooltipOrder:'series',cursorGroup});
    const data={series,begin:0,end:4000,title:'Requests'};chart.setData(data);chart2.setData({...data,title:'Throughput'});
    await plot(left);await plot(right);check(left.querySelector('canvas')&&right.querySelector('canvas'),'both charts use Canvas');
    move(left.querySelector('.u-over'),.35);
    const chartTip=document.querySelector('.ymc-tooltip:not([hidden])');
    check(!chartTip.querySelector('tfoot'),'optional tooltip total hidden');
    check(chartTip.querySelector('tbody tr').textContent.includes('Read'),'supplied tooltip order retained');
    check(!right.querySelector('[data-cursor]').hidden,'cursor synchronizes across charts');
    left.querySelector('.u-over').dispatchEvent(new PointerEvent('pointerleave'));
    move(right.querySelector('.u-over'),.35);
    check(document.querySelector('.ymc-tooltip:not([hidden]) tfoot').textContent.includes('Sum'),'default tooltip sum retained');
    right.querySelector('.u-over').dispatchEvent(new PointerEvent('pointerleave'));
    const stackHost=host(),stack=createMetricChart(stackHost,{settings:{type:'area',unit:'bytes',height:220},tooltipTotal:'Allocated',tooltipOrder:'series'});
    stack.setData({...data,series:series.map(s=>({...s,step:true}))});
    await plot(stackHost);check(stackHost.querySelector('canvas'),'stacked areas use ChartKit');
    move(stackHost.querySelector('.u-over'),.35);
    check(document.querySelector('.ymc-tooltip:not([hidden]) tfoot').textContent.includes('Allocated'),'custom tooltip total label');
    stackHost.querySelector('.u-over').dispatchEvent(new PointerEvent('pointerleave'));
    const isolatedHost=host(),isolated=createMetricChart(isolatedHost);isolated.setData({...data,series:[{...series[0],points:[points[0],{time:1000,raw:null,value:null},points[3]]}]});
    await plot(isolatedHost);
    isolated.destroy();
    const emptyHost=host(),empty=createAllocationBar(emptyHost,{mode:'prepared',summary:false,emptyText:'No I/O activity'});empty.setData({segments:[]});
    check(emptyHost.textContent==='No I/O activity','custom empty state');
    const disposable=host(),destroyed=createMetricChart(disposable);destroyed.setData(data);await plot(disposable);const count=document.querySelectorAll('.ymc-tooltip-layer').length;destroyed.destroy();
    check(!disposable.children.length&&document.querySelectorAll('.ymc-tooltip-layer').length===count-1,'destroy releases chart and tooltip');
    document.getElementById('results').textContent=results.join('\n')+'\nALL PASSED ('+results.length+')';
}catch(error){document.getElementById('results').textContent=results.join('\n')+'\nFAILED '+error.stack;throw error;}
