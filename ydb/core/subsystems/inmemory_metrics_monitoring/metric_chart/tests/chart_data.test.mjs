import {test} from 'node:test';
import assert from 'node:assert/strict';
import {prepareChartKitSeries, formatSeriesName, seriesStats} from '../chart.js';
const point=(time,value)=>({time,value,raw:String(value)});
const line=(key,values,extra={})=>({key,display:key,color:'#2678bc',points:values.map(([t,v])=>point(t,v)),...extra});

test('align sampled and on-change lines without inventing values outside retention',()=>{
 const data=prepareChartKitSeries([
  line('sample',[[10,2],[20,4]]),
  line('step',[[0,5],[15,7]],{step:true}),
 ],5,25);
 assert.deepEqual(data.timeline,[5,5,10,10,15,15,20,20,25,25]);
 assert.deepEqual(data.graphs[0].data,[null,null,2,2,3,3,4,4,null,null]);
 assert.deepEqual(data.graphs[1].data,[5,5,5,5,5,7,7,7,7,7]);
});
test('explicit nulls and closed on-change history remain gaps',()=>{
 const data=prepareChartKitSeries([line('state',[[0,1],[10,null],[20,2],[30,3]],{step:true,closed:true})],0,40);
 assert.deepEqual(data.graphs[0].data,[null,1,1,null,null,2,2,3,null,null]);
});
test('area thickness is the original value with separate positive and negative stacks',()=>{
 const data=prepareChartKitSeries([
  line('a',[[0,2],[10,-3]],{type:'area',step:true}),
  line('b',[[0,4],[10,-5]],{type:'area',step:true}),
 ],0,20);
 assert.equal(data.graphs[4].data[1],6);
 assert.equal(data.graphs[5].data[1],2);
 assert.equal(data.graphs[6].data[3],-8);
 assert.equal(data.graphs[7].data[3],-3);
 assert.deepEqual(data.bands[2].series,[4,3]);
 assert.deepEqual(data.bands[3].series,[1,2]);
});
test('ordinary filled lines do not join the area stack',()=>{
 const data=prepareChartKitSeries([line('filled',[[0,2],[10,3]],{fill:true}),line('plain',[[0,4],[10,5]])],0,10);
 assert.equal(data.graphs[0].type,'area');
 assert.equal(data.graphs[0].color,'#2678bc2e');
 assert.equal(data.graphs[1].type,'line');
 assert.deepEqual(data.bands,[]);
});
test('exact text and name templates are independent of aligned plotting data',()=>{
 const s=line('counter',[[0,1],[10,2]],{metric:'counter',labelValues:[{name:'pool',value:'Batch'}]});s.points[1].raw='9007199254740993';
 assert.equal(formatSeriesName(s,'{metric}: {pool}'),'counter: Batch');
 assert.equal(seriesStats(s,0,10).last,'9007199254740993');
});

test('an isolated sampled point stays visible as a dot',()=>{
 const data=prepareChartKitSeries([line('single',[[5,2]])],0,10);
 assert.equal(data.graphs[0].type,'dots');
 assert.deepEqual(data.extent,[0,2]);
});
test('disjoint histories cannot multiply aligned data without a bound',()=>{
 const input=Array.from({length:64},(_,s)=>line('s'+s,Array.from({length:1000},(_,i)=>[i*64+s,i===500?null:1]),{type:'area'}));
 const data=prepareChartKitSeries(input,0,64000);
 assert.equal(data.sampled,true);
 assert.ok(data.graphs.reduce((n,g)=>n+g.data.length,0)<=1000000);
 assert.ok(data.graphs[0].data.some((v,i)=>v===null&&data.timeline[i]>31900&&data.timeline[i]<32200));
});
