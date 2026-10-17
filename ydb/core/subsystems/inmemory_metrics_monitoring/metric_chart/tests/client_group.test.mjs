import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createInMemoryMetricsClient} from '../client.js';
test('same metric in different participants remains separate and filters by participant labels',async()=>{
 globalThis.location={href:'http://localhost/actors/metrics'};
 const fields=['System','User'].map(pool=>({name:'cpu',labels:[{name:'pool',value:pool}]}));
 const catalog={common_labels:[],lines:[{id:1,name:'pools',labels:[],fields,frontend:'group',readable:true}]};
 let requests=0;globalThis.fetch=async()=>{requests++;return {ok:true,json:async()=>({timestamp_ms:100,lines:[{points:[{timestamp_ms:10,values:['1.23','0.57']}]}]})};};
 const client=createInMemoryMetricsClient({endpoint:'/actors/metrics'});
 const all=await client.queryMany([{id:'a',metric:'cpu',filters:[]}],{catalog});
 assert.equal(all.series.length,2);assert.notEqual(all.series[0].key,all.series[1].key);
 assert.equal(all.series[0].points[0].raw,'1.23');assert.equal(all.series[1].points[0].raw,'0.57');assert.equal(requests,1);
 const user=await client.queryMany([{id:'a',metric:'cpu',filters:[{label:'pool',op:'==',value:'User'}]}],{catalog});
 assert.equal(user.series.length,1);assert.equal(user.series[0].points[0].raw,'0.57');
});
