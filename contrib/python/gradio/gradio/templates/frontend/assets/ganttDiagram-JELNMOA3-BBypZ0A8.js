import{_ as c,g as ve,s as xe,q as Te,p as be,a as we,b as _e,c as lt,d as De,as as G,l as nt,j as Se,i as Me,y as Ce,u as Ee}from"./mermaid.core-dCZaVwzf.js";import{j as Et}from"./i18n-CeTBXmsd.js";import{t as Ie,m as Ae,a as Fe,i as $e,b as Ut,c as Zt,d as Ye,e as Le,f as We,g as Oe,h as Pe,j as Ve,k as ze,l as Qt,n as Kt,o as Jt,s as te,p as ee}from"./time-BeeRGM3E.js";import{l as Ne}from"./linear-Bc_1ZEPy.js";import{s as gt}from"./select-BigU4G0v.js";import"./index-xGeN4i2A.js";import"./svelte/svelte_svelte.js";import"./svelte/svelte_animate.js";import"./svelte/svelte_attachments.js";import"./svelte/svelte_easing.js";import"./svelte/svelte_internal_client.js";import"./svelte/svelte_internal_flags_async.js";import"./svelte/svelte_internal_flags_legacy.js";import"./svelte/svelte_internal_flags_tracing.js";import"./svelte/svelte_internal_server.js";import"./svelte/svelte_legacy.js";import"./svelte/svelte_motion.js";import"./svelte/svelte_reactivity.js";import"./svelte/svelte_reactivity_window.js";import"./svelte/svelte_server.js";import"./svelte/svelte_store.js";import"./svelte/svelte_transition.js";import"./svelte/svelte_events.js";import"./step-ChYay-CR.js";import"./dispatch-kxCwF96_.js";import"./init-Dmth1JHB.js";import"./defaultLocale-CzDG__Ur.js";function Re(t){return t}var xt=1,At=2,$t=3,vt=4,re=1e-6;function He(t){return"translate("+t+",0)"}function Be(t){return"translate(0,"+t+")"}function qe(t){return i=>+t(i)}function je(t,i){return i=Math.max(0,t.bandwidth()-i*2)/2,t.round()&&(i=Math.round(i)),n=>+t(n)+i}function Ge(){return!this.__axis}function ue(t,i){var n=[],s=null,a=null,m=6,h=6,b=3,E=typeof window<"u"&&window.devicePixelRatio>1?0:.5,F=t===xt||t===vt?-1:1,T=t===vt||t===At?"x":"y",L=t===xt||t===$t?He:Be;function M(D){var N=s??(i.ticks?i.ticks.apply(i,n):i.domain()),I=a??(i.tickFormat?i.tickFormat.apply(i,n):Re),S=Math.max(m,0)+b,C=i.range(),O=+C[0]+E,Y=+C[C.length-1]+E,R=(i.bandwidth?je:qe)(i.copy(),E),H=D.selection?D.selection():D,A=H.selectAll(".domain").data([null]),g=H.selectAll(".tick").data(N,i).order(),d=g.exit(),u=g.enter().append("g").attr("class","tick"),x=g.select("line"),v=g.select("text");A=A.merge(A.enter().insert("path",".tick").attr("class","domain").attr("stroke","currentColor")),g=g.merge(u),x=x.merge(u.append("line").attr("stroke","currentColor").attr(T+"2",F*m)),v=v.merge(u.append("text").attr("fill","currentColor").attr(T,F*S).attr("dy",t===xt?"0em":t===$t?"0.71em":"0.32em")),D!==H&&(A=A.transition(D),g=g.transition(D),x=x.transition(D),v=v.transition(D),d=d.transition(D).attr("opacity",re).attr("transform",function(k){return isFinite(k=R(k))?L(k+E):this.getAttribute("transform")}),u.attr("opacity",re).attr("transform",function(k){var f=this.parentNode.__axis;return L((f&&isFinite(f=f(k))?f:R(k))+E)})),d.remove(),A.attr("d",t===vt||t===At?h?"M"+F*h+","+O+"H"+E+"V"+Y+"H"+F*h:"M"+E+","+O+"V"+Y:h?"M"+O+","+F*h+"V"+E+"H"+Y+"V"+F*h:"M"+O+","+E+"H"+Y),g.attr("opacity",1).attr("transform",function(k){return L(R(k)+E)}),x.attr(T+"2",F*m),v.attr(T,F*S).text(I),H.filter(Ge).attr("fill","none").attr("font-size",10).attr("font-family","sans-serif").attr("text-anchor",t===At?"start":t===vt?"end":"middle"),H.each(function(){this.__axis=R})}return M.scale=function(D){return arguments.length?(i=D,M):i},M.ticks=function(){return n=Array.from(arguments),M},M.tickArguments=function(D){return arguments.length?(n=D==null?[]:Array.from(D),M):n.slice()},M.tickValues=function(D){return arguments.length?(s=D==null?null:Array.from(D),M):s&&s.slice()},M.tickFormat=function(D){return arguments.length?(a=D,M):a},M.tickSize=function(D){return arguments.length?(m=h=+D,M):m},M.tickSizeInner=function(D){return arguments.length?(m=+D,M):m},M.tickSizeOuter=function(D){return arguments.length?(h=+D,M):h},M.tickPadding=function(D){return arguments.length?(b=+D,M):b},M.offset=function(D){return arguments.length?(E=+D,M):E},M}function Xe(t){return ue(xt,t)}function Ue(t){return ue($t,t)}var Tt={exports:{}},Ze=Tt.exports,ie;function Qe(){return ie||(ie=1,(function(t,i){(function(n,s){t.exports=s()})(Ze,(function(){var n="day";return function(s,a,m){var h=function(F){return F.add(4-F.isoWeekday(),n)},b=a.prototype;b.isoWeekYear=function(){return h(this).year()},b.isoWeek=function(F){if(!this.$utils().u(F))return this.add(7*(F-this.isoWeek()),n);var T,L,M,D,N=h(this),I=(T=this.isoWeekYear(),L=this.$u,M=(L?m.utc:m)().year(T).startOf("year"),D=4-M.isoWeekday(),M.isoWeekday()>4&&(D+=7),M.add(D,n));return N.diff(I,"week")+1},b.isoWeekday=function(F){return this.$utils().u(F)?this.day()||7:this.day(this.day()%7?F:F-7)};var E=b.startOf;b.startOf=function(F,T){var L=this.$utils(),M=!!L.u(T)||T;return L.p(F)==="isoweek"?M?this.date(this.date()-(this.isoWeekday()-1)).startOf("day"):this.date(this.date()-1-(this.isoWeekday()-1)+7).endOf("day"):E.bind(this)(F,T)}}}))})(Tt)),Tt.exports}var Ke=Qe();const Je=Et(Ke);var bt={exports:{}},tr=bt.exports,se;function er(){return se||(se=1,(function(t,i){(function(n,s){t.exports=s()})(tr,(function(){var n={LTS:"h:mm:ss A",LT:"h:mm A",L:"MM/DD/YYYY",LL:"MMMM D, YYYY",LLL:"MMMM D, YYYY h:mm A",LLLL:"dddd, MMMM D, YYYY h:mm A"},s=/(\[[^[]*\])|([-_:/.,()\s]+)|(A|a|Q|YYYY|YY?|ww?|MM?M?M?|Do|DD?|hh?|HH?|mm?|ss?|S{1,3}|z|ZZ?)/g,a=/\d/,m=/\d\d/,h=/\d\d?/,b=/\d*[^-_:/,()\s\d]+/,E={},F=function(S){return(S=+S)+(S>68?1900:2e3)},T=function(S){return function(C){this[S]=+C}},L=[/[+-]\d\d:?(\d\d)?|Z/,function(S){(this.zone||(this.zone={})).offset=(function(C){if(!C||C==="Z")return 0;var O=C.match(/([+-]|\d\d)/g),Y=60*O[1]+(+O[2]||0);return Y===0?0:O[0]==="+"?-Y:Y})(S)}],M=function(S){var C=E[S];return C&&(C.indexOf?C:C.s.concat(C.f))},D=function(S,C){var O,Y=E.meridiem;if(Y){for(var R=1;R<=24;R+=1)if(S.indexOf(Y(R,0,C))>-1){O=R>12;break}}else O=S===(C?"pm":"PM");return O},N={A:[b,function(S){this.afternoon=D(S,!1)}],a:[b,function(S){this.afternoon=D(S,!0)}],Q:[a,function(S){this.month=3*(S-1)+1}],S:[a,function(S){this.milliseconds=100*+S}],SS:[m,function(S){this.milliseconds=10*+S}],SSS:[/\d{3}/,function(S){this.milliseconds=+S}],s:[h,T("seconds")],ss:[h,T("seconds")],m:[h,T("minutes")],mm:[h,T("minutes")],H:[h,T("hours")],h:[h,T("hours")],HH:[h,T("hours")],hh:[h,T("hours")],D:[h,T("day")],DD:[m,T("day")],Do:[b,function(S){var C=E.ordinal,O=S.match(/\d+/);if(this.day=O[0],C)for(var Y=1;Y<=31;Y+=1)C(Y).replace(/\[|\]/g,"")===S&&(this.day=Y)}],w:[h,T("week")],ww:[m,T("week")],M:[h,T("month")],MM:[m,T("month")],MMM:[b,function(S){var C=M("months"),O=(M("monthsShort")||C.map((function(Y){return Y.slice(0,3)}))).indexOf(S)+1;if(O<1)throw new Error;this.month=O%12||O}],MMMM:[b,function(S){var C=M("months").indexOf(S)+1;if(C<1)throw new Error;this.month=C%12||C}],Y:[/[+-]?\d+/,T("year")],YY:[m,function(S){this.year=F(S)}],YYYY:[/\d{4}/,T("year")],Z:L,ZZ:L};function I(S){var C,O;C=S,O=E&&E.formats;for(var Y=(S=C.replace(/(\[[^\]]+])|(LTS?|l{1,4}|L{1,4})/g,(function(x,v,k){var f=k&&k.toUpperCase();return v||O[k]||n[k]||O[f].replace(/(\[[^\]]+])|(MMMM|MM|DD|dddd)/g,(function(o,l,y){return l||y.slice(1)}))}))).match(s),R=Y.length,H=0;H<R;H+=1){var A=Y[H],g=N[A],d=g&&g[0],u=g&&g[1];Y[H]=u?{regex:d,parser:u}:A.replace(/^\[|\]$/g,"")}return function(x){for(var v={},k=0,f=0;k<R;k+=1){var o=Y[k];if(typeof o=="string")f+=o.length;else{var l=o.regex,y=o.parser,p=x.slice(f),w=l.exec(p)[0];y.call(v,w),x=x.replace(w,"")}}return(function(r){var z=r.afternoon;if(z!==void 0){var e=r.hours;z?e<12&&(r.hours+=12):e===12&&(r.hours=0),delete r.afternoon}})(v),v}}return function(S,C,O){O.p.customParseFormat=!0,S&&S.parseTwoDigitYear&&(F=S.parseTwoDigitYear);var Y=C.prototype,R=Y.parse;Y.parse=function(H){var A=H.date,g=H.utc,d=H.args;this.$u=g;var u=d[1];if(typeof u=="string"){var x=d[2]===!0,v=d[3]===!0,k=x||v,f=d[2];v&&(f=d[2]),E=this.$locale(),!x&&f&&(E=O.Ls[f]),this.$d=(function(p,w,r,z){try{if(["x","X"].indexOf(w)>-1)return new Date((w==="X"?1e3:1)*p);var e=I(w)(p),_=e.year,V=e.month,P=e.day,W=e.hours,j=e.minutes,$=e.seconds,Q=e.milliseconds,rt=e.zone,ot=e.week,ft=new Date,ht=P||(_||V?1:ft.getDate()),ct=_||ft.getFullYear(),B=0;_&&!V||(B=V>0?V-1:ft.getMonth());var Z,X=W||0,st=j||0,K=$||0,it=Q||0;return rt?new Date(Date.UTC(ct,B,ht,X,st,K,it+60*rt.offset*1e3)):r?new Date(Date.UTC(ct,B,ht,X,st,K,it)):(Z=new Date(ct,B,ht,X,st,K,it),ot&&(Z=z(Z).week(ot).toDate()),Z)}catch{return new Date("")}})(A,u,g,O),this.init(),f&&f!==!0&&(this.$L=this.locale(f).$L),k&&A!=this.format(u)&&(this.$d=new Date("")),E={}}else if(u instanceof Array)for(var o=u.length,l=1;l<=o;l+=1){d[1]=u[l-1];var y=O.apply(this,d);if(y.isValid()){this.$d=y.$d,this.$L=y.$L,this.init();break}l===o&&(this.$d=new Date(""))}else R.call(this,H)}}}))})(bt)),bt.exports}var rr=er();const ir=Et(rr);var wt={exports:{}},sr=wt.exports,ne;function nr(){return ne||(ne=1,(function(t,i){(function(n,s){t.exports=s()})(sr,(function(){return function(n,s){var a=s.prototype,m=a.format;a.format=function(h){var b=this,E=this.$locale();if(!this.isValid())return m.bind(this)(h);var F=this.$utils(),T=(h||"YYYY-MM-DDTHH:mm:ssZ").replace(/\[([^\]]+)]|Q|wo|ww|w|WW|W|zzz|z|gggg|GGGG|Do|X|x|k{1,2}|S/g,(function(L){switch(L){case"Q":return Math.ceil((b.$M+1)/3);case"Do":return E.ordinal(b.$D);case"gggg":return b.weekYear();case"GGGG":return b.isoWeekYear();case"wo":return E.ordinal(b.week(),"W");case"w":case"ww":return F.s(b.week(),L==="w"?1:2,"0");case"W":case"WW":return F.s(b.isoWeek(),L==="W"?1:2,"0");case"k":case"kk":return F.s(String(b.$H===0?24:b.$H),L==="k"?1:2,"0");case"X":return Math.floor(b.$d.getTime()/1e3);case"x":return b.$d.getTime();case"z":return"["+b.offsetName()+"]";case"zzz":return"["+b.offsetName("long")+"]";default:return L}}));return m.bind(this)(T)}}}))})(wt)),wt.exports}var ar=nr();const or=Et(ar);var _t={exports:{}},cr=_t.exports,ae;function lr(){return ae||(ae=1,(function(t,i){(function(n,s){t.exports=s()})(cr,(function(){var n,s,a=1e3,m=6e4,h=36e5,b=864e5,E=/\[([^\]]+)]|Y{1,4}|M{1,4}|D{1,2}|d{1,4}|H{1,2}|h{1,2}|a|A|m{1,2}|s{1,2}|Z{1,2}|SSS/g,F=31536e6,T=2628e6,L=/^(-|\+)?P(?:([-+]?[0-9,.]*)Y)?(?:([-+]?[0-9,.]*)M)?(?:([-+]?[0-9,.]*)W)?(?:([-+]?[0-9,.]*)D)?(?:T(?:([-+]?[0-9,.]*)H)?(?:([-+]?[0-9,.]*)M)?(?:([-+]?[0-9,.]*)S)?)?$/,M={years:F,months:T,days:b,hours:h,minutes:m,seconds:a,milliseconds:1,weeks:6048e5},D=function(A){return A instanceof R},N=function(A,g,d){return new R(A,d,g.$l)},I=function(A){return s.p(A)+"s"},S=function(A){return A<0},C=function(A){return S(A)?Math.ceil(A):Math.floor(A)},O=function(A){return Math.abs(A)},Y=function(A,g){return A?S(A)?{negative:!0,format:""+O(A)+g}:{negative:!1,format:""+A+g}:{negative:!1,format:""}},R=(function(){function A(d,u,x){var v=this;if(this.$d={},this.$l=x,d===void 0&&(this.$ms=0,this.parseFromMilliseconds()),u)return N(d*M[I(u)],this);if(typeof d=="number")return this.$ms=d,this.parseFromMilliseconds(),this;if(typeof d=="object")return Object.keys(d).forEach((function(o){v.$d[I(o)]=d[o]})),this.calMilliseconds(),this;if(typeof d=="string"){var k=d.match(L);if(k){var f=k.slice(2).map((function(o){return o!=null?Number(o):0}));return this.$d.years=f[0],this.$d.months=f[1],this.$d.weeks=f[2],this.$d.days=f[3],this.$d.hours=f[4],this.$d.minutes=f[5],this.$d.seconds=f[6],this.calMilliseconds(),this}}return this}var g=A.prototype;return g.calMilliseconds=function(){var d=this;this.$ms=Object.keys(this.$d).reduce((function(u,x){return u+(d.$d[x]||0)*M[x]}),0)},g.parseFromMilliseconds=function(){var d=this.$ms;this.$d.years=C(d/F),d%=F,this.$d.months=C(d/T),d%=T,this.$d.days=C(d/b),d%=b,this.$d.hours=C(d/h),d%=h,this.$d.minutes=C(d/m),d%=m,this.$d.seconds=C(d/a),d%=a,this.$d.milliseconds=d},g.toISOString=function(){var d=Y(this.$d.years,"Y"),u=Y(this.$d.months,"M"),x=+this.$d.days||0;this.$d.weeks&&(x+=7*this.$d.weeks);var v=Y(x,"D"),k=Y(this.$d.hours,"H"),f=Y(this.$d.minutes,"M"),o=this.$d.seconds||0;this.$d.milliseconds&&(o+=this.$d.milliseconds/1e3,o=Math.round(1e3*o)/1e3);var l=Y(o,"S"),y=d.negative||u.negative||v.negative||k.negative||f.negative||l.negative,p=k.format||f.format||l.format?"T":"",w=(y?"-":"")+"P"+d.format+u.format+v.format+p+k.format+f.format+l.format;return w==="P"||w==="-P"?"P0D":w},g.toJSON=function(){return this.toISOString()},g.format=function(d){var u=d||"YYYY-MM-DDTHH:mm:ss",x={Y:this.$d.years,YY:s.s(this.$d.years,2,"0"),YYYY:s.s(this.$d.years,4,"0"),M:this.$d.months,MM:s.s(this.$d.months,2,"0"),D:this.$d.days,DD:s.s(this.$d.days,2,"0"),H:this.$d.hours,HH:s.s(this.$d.hours,2,"0"),m:this.$d.minutes,mm:s.s(this.$d.minutes,2,"0"),s:this.$d.seconds,ss:s.s(this.$d.seconds,2,"0"),SSS:s.s(this.$d.milliseconds,3,"0")};return u.replace(E,(function(v,k){return k||String(x[v])}))},g.as=function(d){return this.$ms/M[I(d)]},g.get=function(d){var u=this.$ms,x=I(d);return x==="milliseconds"?u%=1e3:u=x==="weeks"?C(u/M[x]):this.$d[x],u||0},g.add=function(d,u,x){var v;return v=u?d*M[I(u)]:D(d)?d.$ms:N(d,this).$ms,N(this.$ms+v*(x?-1:1),this)},g.subtract=function(d,u){return this.add(d,u,!0)},g.locale=function(d){var u=this.clone();return u.$l=d,u},g.clone=function(){return N(this.$ms,this)},g.humanize=function(d){return n().add(this.$ms,"ms").locale(this.$l).fromNow(!d)},g.valueOf=function(){return this.asMilliseconds()},g.milliseconds=function(){return this.get("milliseconds")},g.asMilliseconds=function(){return this.as("milliseconds")},g.seconds=function(){return this.get("seconds")},g.asSeconds=function(){return this.as("seconds")},g.minutes=function(){return this.get("minutes")},g.asMinutes=function(){return this.as("minutes")},g.hours=function(){return this.get("hours")},g.asHours=function(){return this.as("hours")},g.days=function(){return this.get("days")},g.asDays=function(){return this.as("days")},g.weeks=function(){return this.get("weeks")},g.asWeeks=function(){return this.as("weeks")},g.months=function(){return this.get("months")},g.asMonths=function(){return this.as("months")},g.years=function(){return this.get("years")},g.asYears=function(){return this.as("years")},A})(),H=function(A,g,d){return A.add(g.years()*d,"y").add(g.months()*d,"M").add(g.days()*d,"d").add(g.hours()*d,"h").add(g.minutes()*d,"m").add(g.seconds()*d,"s").add(g.milliseconds()*d,"ms")};return function(A,g,d){n=d,s=d().$utils(),d.duration=function(v,k){var f=d.locale();return N(v,{$l:f},k)},d.isDuration=D;var u=g.prototype.add,x=g.prototype.subtract;g.prototype.add=function(v,k){return D(v)?H(this,v,1):u.bind(this)(v,k)},g.prototype.subtract=function(v,k){return D(v)?H(this,v,-1):x.bind(this)(v,k)}}}))})(_t)),_t.exports}var ur=lr();const dr=Et(ur);var Yt=(function(){var t=c(function(f,o,l,y){for(l=l||{},y=f.length;y--;l[f[y]]=o);return l},"o"),i=[6,8,10,12,13,14,15,16,17,18,20,21,22,23,24,25,26,27,28,29,30,31,33,35,36,38,40],n=[1,26],s=[1,27],a=[1,28],m=[1,29],h=[1,30],b=[1,31],E=[1,32],F=[1,33],T=[1,34],L=[1,9],M=[1,10],D=[1,11],N=[1,12],I=[1,13],S=[1,14],C=[1,15],O=[1,16],Y=[1,19],R=[1,20],H=[1,21],A=[1,22],g=[1,23],d=[1,25],u=[1,35],x={trace:c(function(){},"trace"),yy:{},symbols_:{error:2,start:3,gantt:4,document:5,EOF:6,line:7,SPACE:8,statement:9,NL:10,weekday:11,weekday_monday:12,weekday_tuesday:13,weekday_wednesday:14,weekday_thursday:15,weekday_friday:16,weekday_saturday:17,weekday_sunday:18,weekend:19,weekend_friday:20,weekend_saturday:21,dateFormat:22,inclusiveEndDates:23,topAxis:24,axisFormat:25,tickInterval:26,excludes:27,includes:28,todayMarker:29,title:30,acc_title:31,acc_title_value:32,acc_descr:33,acc_descr_value:34,acc_descr_multiline_value:35,section:36,clickStatement:37,taskTxt:38,taskData:39,click:40,callbackname:41,callbackargs:42,href:43,clickStatementDebug:44,$accept:0,$end:1},terminals_:{2:"error",4:"gantt",6:"EOF",8:"SPACE",10:"NL",12:"weekday_monday",13:"weekday_tuesday",14:"weekday_wednesday",15:"weekday_thursday",16:"weekday_friday",17:"weekday_saturday",18:"weekday_sunday",20:"weekend_friday",21:"weekend_saturday",22:"dateFormat",23:"inclusiveEndDates",24:"topAxis",25:"axisFormat",26:"tickInterval",27:"excludes",28:"includes",29:"todayMarker",30:"title",31:"acc_title",32:"acc_title_value",33:"acc_descr",34:"acc_descr_value",35:"acc_descr_multiline_value",36:"section",38:"taskTxt",39:"taskData",40:"click",41:"callbackname",42:"callbackargs",43:"href"},productions_:[0,[3,3],[5,0],[5,2],[7,2],[7,1],[7,1],[7,1],[11,1],[11,1],[11,1],[11,1],[11,1],[11,1],[11,1],[19,1],[19,1],[9,1],[9,1],[9,1],[9,1],[9,1],[9,1],[9,1],[9,1],[9,1],[9,1],[9,1],[9,2],[9,2],[9,1],[9,1],[9,1],[9,2],[37,2],[37,3],[37,3],[37,4],[37,3],[37,4],[37,2],[44,2],[44,3],[44,3],[44,4],[44,3],[44,4],[44,2]],performAction:c(function(o,l,y,p,w,r,z){var e=r.length-1;switch(w){case 1:return r[e-1];case 2:this.$=[];break;case 3:r[e-1].push(r[e]),this.$=r[e-1];break;case 4:case 5:this.$=r[e];break;case 6:case 7:this.$=[];break;case 8:p.setWeekday("monday");break;case 9:p.setWeekday("tuesday");break;case 10:p.setWeekday("wednesday");break;case 11:p.setWeekday("thursday");break;case 12:p.setWeekday("friday");break;case 13:p.setWeekday("saturday");break;case 14:p.setWeekday("sunday");break;case 15:p.setWeekend("friday");break;case 16:p.setWeekend("saturday");break;case 17:p.setDateFormat(r[e].substr(11)),this.$=r[e].substr(11);break;case 18:p.enableInclusiveEndDates(),this.$=r[e].substr(18);break;case 19:p.TopAxis(),this.$=r[e].substr(8);break;case 20:p.setAxisFormat(r[e].substr(11)),this.$=r[e].substr(11);break;case 21:p.setTickInterval(r[e].substr(13)),this.$=r[e].substr(13);break;case 22:p.setExcludes(r[e].substr(9)),this.$=r[e].substr(9);break;case 23:p.setIncludes(r[e].substr(9)),this.$=r[e].substr(9);break;case 24:p.setTodayMarker(r[e].substr(12)),this.$=r[e].substr(12);break;case 27:p.setDiagramTitle(r[e].substr(6)),this.$=r[e].substr(6);break;case 28:this.$=r[e].trim(),p.setAccTitle(this.$);break;case 29:case 30:this.$=r[e].trim(),p.setAccDescription(this.$);break;case 31:p.addSection(r[e].substr(8)),this.$=r[e].substr(8);break;case 33:p.addTask(r[e-1],r[e]),this.$="task";break;case 34:this.$=r[e-1],p.setClickEvent(r[e-1],r[e],null);break;case 35:this.$=r[e-2],p.setClickEvent(r[e-2],r[e-1],r[e]);break;case 36:this.$=r[e-2],p.setClickEvent(r[e-2],r[e-1],null),p.setLink(r[e-2],r[e]);break;case 37:this.$=r[e-3],p.setClickEvent(r[e-3],r[e-2],r[e-1]),p.setLink(r[e-3],r[e]);break;case 38:this.$=r[e-2],p.setClickEvent(r[e-2],r[e],null),p.setLink(r[e-2],r[e-1]);break;case 39:this.$=r[e-3],p.setClickEvent(r[e-3],r[e-1],r[e]),p.setLink(r[e-3],r[e-2]);break;case 40:this.$=r[e-1],p.setLink(r[e-1],r[e]);break;case 41:case 47:this.$=r[e-1]+" "+r[e];break;case 42:case 43:case 45:this.$=r[e-2]+" "+r[e-1]+" "+r[e];break;case 44:case 46:this.$=r[e-3]+" "+r[e-2]+" "+r[e-1]+" "+r[e];break}},"anonymous"),table:[{3:1,4:[1,2]},{1:[3]},t(i,[2,2],{5:3}),{6:[1,4],7:5,8:[1,6],9:7,10:[1,8],11:17,12:n,13:s,14:a,15:m,16:h,17:b,18:E,19:18,20:F,21:T,22:L,23:M,24:D,25:N,26:I,27:S,28:C,29:O,30:Y,31:R,33:H,35:A,36:g,37:24,38:d,40:u},t(i,[2,7],{1:[2,1]}),t(i,[2,3]),{9:36,11:17,12:n,13:s,14:a,15:m,16:h,17:b,18:E,19:18,20:F,21:T,22:L,23:M,24:D,25:N,26:I,27:S,28:C,29:O,30:Y,31:R,33:H,35:A,36:g,37:24,38:d,40:u},t(i,[2,5]),t(i,[2,6]),t(i,[2,17]),t(i,[2,18]),t(i,[2,19]),t(i,[2,20]),t(i,[2,21]),t(i,[2,22]),t(i,[2,23]),t(i,[2,24]),t(i,[2,25]),t(i,[2,26]),t(i,[2,27]),{32:[1,37]},{34:[1,38]},t(i,[2,30]),t(i,[2,31]),t(i,[2,32]),{39:[1,39]},t(i,[2,8]),t(i,[2,9]),t(i,[2,10]),t(i,[2,11]),t(i,[2,12]),t(i,[2,13]),t(i,[2,14]),t(i,[2,15]),t(i,[2,16]),{41:[1,40],43:[1,41]},t(i,[2,4]),t(i,[2,28]),t(i,[2,29]),t(i,[2,33]),t(i,[2,34],{42:[1,42],43:[1,43]}),t(i,[2,40],{41:[1,44]}),t(i,[2,35],{43:[1,45]}),t(i,[2,36]),t(i,[2,38],{42:[1,46]}),t(i,[2,37]),t(i,[2,39])],defaultActions:{},parseError:c(function(o,l){if(l.recoverable)this.trace(o);else{var y=new Error(o);throw y.hash=l,y}},"parseError"),parse:c(function(o){var l=this,y=[0],p=[],w=[null],r=[],z=this.table,e="",_=0,V=0,P=2,W=1,j=r.slice.call(arguments,1),$=Object.create(this.lexer),Q={yy:{}};for(var rt in this.yy)Object.prototype.hasOwnProperty.call(this.yy,rt)&&(Q.yy[rt]=this.yy[rt]);$.setInput(o,Q.yy),Q.yy.lexer=$,Q.yy.parser=this,typeof $.yylloc>"u"&&($.yylloc={});var ot=$.yylloc;r.push(ot);var ft=$.options&&$.options.ranges;typeof Q.yy.parseError=="function"?this.parseError=Q.yy.parseError:this.parseError=Object.getPrototypeOf(this).parseError;function ht(U){y.length=y.length-2*U,w.length=w.length-U,r.length=r.length-U}c(ht,"popStack");function ct(){var U;return U=p.pop()||$.lex()||W,typeof U!="number"&&(U instanceof Array&&(p=U,U=p.pop()),U=l.symbols_[U]||U),U}c(ct,"lex");for(var B,Z,X,st,K={},it,J,Xt,pt;;){if(Z=y[y.length-1],this.defaultActions[Z]?X=this.defaultActions[Z]:((B===null||typeof B>"u")&&(B=ct()),X=z[Z]&&z[Z][B]),typeof X>"u"||!X.length||!X[0]){var It="";pt=[];for(it in z[Z])this.terminals_[it]&&it>P&&pt.push("'"+this.terminals_[it]+"'");$.showPosition?It="Parse error on line "+(_+1)+`:
`+$.showPosition()+`
Expecting `+pt.join(", ")+", got '"+(this.terminals_[B]||B)+"'":It="Parse error on line "+(_+1)+": Unexpected "+(B==W?"end of input":"'"+(this.terminals_[B]||B)+"'"),this.parseError(It,{text:$.match,token:this.terminals_[B]||B,line:$.yylineno,loc:ot,expected:pt})}if(X[0]instanceof Array&&X.length>1)throw new Error("Parse Error: multiple actions possible at state: "+Z+", token: "+B);switch(X[0]){case 1:y.push(B),w.push($.yytext),r.push($.yylloc),y.push(X[1]),B=null,V=$.yyleng,e=$.yytext,_=$.yylineno,ot=$.yylloc;break;case 2:if(J=this.productions_[X[1]][1],K.$=w[w.length-J],K._$={first_line:r[r.length-(J||1)].first_line,last_line:r[r.length-1].last_line,first_column:r[r.length-(J||1)].first_column,last_column:r[r.length-1].last_column},ft&&(K._$.range=[r[r.length-(J||1)].range[0],r[r.length-1].range[1]]),st=this.performAction.apply(K,[e,V,_,Q.yy,X[1],w,r].concat(j)),typeof st<"u")return st;J&&(y=y.slice(0,-1*J*2),w=w.slice(0,-1*J),r=r.slice(0,-1*J)),y.push(this.productions_[X[1]][0]),w.push(K.$),r.push(K._$),Xt=z[y[y.length-2]][y[y.length-1]],y.push(Xt);break;case 3:return!0}}return!0},"parse")},v=(function(){var f={EOF:1,parseError:c(function(l,y){if(this.yy.parser)this.yy.parser.parseError(l,y);else throw new Error(l)},"parseError"),setInput:c(function(o,l){return this.yy=l||this.yy||{},this._input=o,this._more=this._backtrack=this.done=!1,this.yylineno=this.yyleng=0,this.yytext=this.matched=this.match="",this.conditionStack=["INITIAL"],this.yylloc={first_line:1,first_column:0,last_line:1,last_column:0},this.options.ranges&&(this.yylloc.range=[0,0]),this.offset=0,this},"setInput"),input:c(function(){var o=this._input[0];this.yytext+=o,this.yyleng++,this.offset++,this.match+=o,this.matched+=o;var l=o.match(/(?:\r\n?|\n).*/g);return l?(this.yylineno++,this.yylloc.last_line++):this.yylloc.last_column++,this.options.ranges&&this.yylloc.range[1]++,this._input=this._input.slice(1),o},"input"),unput:c(function(o){var l=o.length,y=o.split(/(?:\r\n?|\n)/g);this._input=o+this._input,this.yytext=this.yytext.substr(0,this.yytext.length-l),this.offset-=l;var p=this.match.split(/(?:\r\n?|\n)/g);this.match=this.match.substr(0,this.match.length-1),this.matched=this.matched.substr(0,this.matched.length-1),y.length-1&&(this.yylineno-=y.length-1);var w=this.yylloc.range;return this.yylloc={first_line:this.yylloc.first_line,last_line:this.yylineno+1,first_column:this.yylloc.first_column,last_column:y?(y.length===p.length?this.yylloc.first_column:0)+p[p.length-y.length].length-y[0].length:this.yylloc.first_column-l},this.options.ranges&&(this.yylloc.range=[w[0],w[0]+this.yyleng-l]),this.yyleng=this.yytext.length,this},"unput"),more:c(function(){return this._more=!0,this},"more"),reject:c(function(){if(this.options.backtrack_lexer)this._backtrack=!0;else return this.parseError("Lexical error on line "+(this.yylineno+1)+`. You can only invoke reject() in the lexer when the lexer is of the backtracking persuasion (options.backtrack_lexer = true).
`+this.showPosition(),{text:"",token:null,line:this.yylineno});return this},"reject"),less:c(function(o){this.unput(this.match.slice(o))},"less"),pastInput:c(function(){var o=this.matched.substr(0,this.matched.length-this.match.length);return(o.length>20?"...":"")+o.substr(-20).replace(/\n/g,"")},"pastInput"),upcomingInput:c(function(){var o=this.match;return o.length<20&&(o+=this._input.substr(0,20-o.length)),(o.substr(0,20)+(o.length>20?"...":"")).replace(/\n/g,"")},"upcomingInput"),showPosition:c(function(){var o=this.pastInput(),l=new Array(o.length+1).join("-");return o+this.upcomingInput()+`
`+l+"^"},"showPosition"),test_match:c(function(o,l){var y,p,w;if(this.options.backtrack_lexer&&(w={yylineno:this.yylineno,yylloc:{first_line:this.yylloc.first_line,last_line:this.last_line,first_column:this.yylloc.first_column,last_column:this.yylloc.last_column},yytext:this.yytext,match:this.match,matches:this.matches,matched:this.matched,yyleng:this.yyleng,offset:this.offset,_more:this._more,_input:this._input,yy:this.yy,conditionStack:this.conditionStack.slice(0),done:this.done},this.options.ranges&&(w.yylloc.range=this.yylloc.range.slice(0))),p=o[0].match(/(?:\r\n?|\n).*/g),p&&(this.yylineno+=p.length),this.yylloc={first_line:this.yylloc.last_line,last_line:this.yylineno+1,first_column:this.yylloc.last_column,last_column:p?p[p.length-1].length-p[p.length-1].match(/\r?\n?/)[0].length:this.yylloc.last_column+o[0].length},this.yytext+=o[0],this.match+=o[0],this.matches=o,this.yyleng=this.yytext.length,this.options.ranges&&(this.yylloc.range=[this.offset,this.offset+=this.yyleng]),this._more=!1,this._backtrack=!1,this._input=this._input.slice(o[0].length),this.matched+=o[0],y=this.performAction.call(this,this.yy,this,l,this.conditionStack[this.conditionStack.length-1]),this.done&&this._input&&(this.done=!1),y)return y;if(this._backtrack){for(var r in w)this[r]=w[r];return!1}return!1},"test_match"),next:c(function(){if(this.done)return this.EOF;this._input||(this.done=!0);var o,l,y,p;this._more||(this.yytext="",this.match="");for(var w=this._currentRules(),r=0;r<w.length;r++)if(y=this._input.match(this.rules[w[r]]),y&&(!l||y[0].length>l[0].length)){if(l=y,p=r,this.options.backtrack_lexer){if(o=this.test_match(y,w[r]),o!==!1)return o;if(this._backtrack){l=!1;continue}else return!1}else if(!this.options.flex)break}return l?(o=this.test_match(l,w[p]),o!==!1?o:!1):this._input===""?this.EOF:this.parseError("Lexical error on line "+(this.yylineno+1)+`. Unrecognized text.
`+this.showPosition(),{text:"",token:null,line:this.yylineno})},"next"),lex:c(function(){var l=this.next();return l||this.lex()},"lex"),begin:c(function(l){this.conditionStack.push(l)},"begin"),popState:c(function(){var l=this.conditionStack.length-1;return l>0?this.conditionStack.pop():this.conditionStack[0]},"popState"),_currentRules:c(function(){return this.conditionStack.length&&this.conditionStack[this.conditionStack.length-1]?this.conditions[this.conditionStack[this.conditionStack.length-1]].rules:this.conditions.INITIAL.rules},"_currentRules"),topState:c(function(l){return l=this.conditionStack.length-1-Math.abs(l||0),l>=0?this.conditionStack[l]:"INITIAL"},"topState"),pushState:c(function(l){this.begin(l)},"pushState"),stateStackSize:c(function(){return this.conditionStack.length},"stateStackSize"),options:{"case-insensitive":!0},performAction:c(function(l,y,p,w){switch(p){case 0:return this.begin("open_directive"),"open_directive";case 1:return this.begin("acc_title"),31;case 2:return this.popState(),"acc_title_value";case 3:return this.begin("acc_descr"),33;case 4:return this.popState(),"acc_descr_value";case 5:this.begin("acc_descr_multiline");break;case 6:this.popState();break;case 7:return"acc_descr_multiline_value";case 8:break;case 9:break;case 10:break;case 11:return 10;case 12:break;case 13:break;case 14:this.begin("href");break;case 15:this.popState();break;case 16:return 43;case 17:this.begin("callbackname");break;case 18:this.popState();break;case 19:this.popState(),this.begin("callbackargs");break;case 20:return 41;case 21:this.popState();break;case 22:return 42;case 23:this.begin("click");break;case 24:this.popState();break;case 25:return 40;case 26:return 4;case 27:return 22;case 28:return 23;case 29:return 24;case 30:return 25;case 31:return 26;case 32:return 28;case 33:return 27;case 34:return 29;case 35:return 12;case 36:return 13;case 37:return 14;case 38:return 15;case 39:return 16;case 40:return 17;case 41:return 18;case 42:return 20;case 43:return 21;case 44:return"date";case 45:return 30;case 46:return"accDescription";case 47:return 36;case 48:return 38;case 49:return 39;case 50:return":";case 51:return 6;case 52:return"INVALID"}},"anonymous"),rules:[/^(?:%%\{)/i,/^(?:accTitle\s*:\s*)/i,/^(?:(?!\n||)*[^\n]*)/i,/^(?:accDescr\s*:\s*)/i,/^(?:(?!\n||)*[^\n]*)/i,/^(?:accDescr\s*\{\s*)/i,/^(?:[\}])/i,/^(?:[^\}]*)/i,/^(?:%%(?!\{)*[^\n]*)/i,/^(?:[^\}]%%*[^\n]*)/i,/^(?:%%*[^\n]*[\n]*)/i,/^(?:[\n]+)/i,/^(?:\s+)/i,/^(?:%[^\n]*)/i,/^(?:href[\s]+["])/i,/^(?:["])/i,/^(?:[^"]*)/i,/^(?:call[\s]+)/i,/^(?:\([\s]*\))/i,/^(?:\()/i,/^(?:[^(]*)/i,/^(?:\))/i,/^(?:[^)]*)/i,/^(?:click[\s]+)/i,/^(?:[\s\n])/i,/^(?:[^\s\n]*)/i,/^(?:gantt\b)/i,/^(?:dateFormat\s[^#\n;]+)/i,/^(?:inclusiveEndDates\b)/i,/^(?:topAxis\b)/i,/^(?:axisFormat\s[^#\n;]+)/i,/^(?:tickInterval\s[^#\n;]+)/i,/^(?:includes\s[^#\n;]+)/i,/^(?:excludes\s[^#\n;]+)/i,/^(?:todayMarker\s[^\n;]+)/i,/^(?:weekday\s+monday\b)/i,/^(?:weekday\s+tuesday\b)/i,/^(?:weekday\s+wednesday\b)/i,/^(?:weekday\s+thursday\b)/i,/^(?:weekday\s+friday\b)/i,/^(?:weekday\s+saturday\b)/i,/^(?:weekday\s+sunday\b)/i,/^(?:weekend\s+friday\b)/i,/^(?:weekend\s+saturday\b)/i,/^(?:\d\d\d\d-\d\d-\d\d\b)/i,/^(?:title\s[^\n]+)/i,/^(?:accDescription\s[^#\n;]+)/i,/^(?:section\s[^\n]+)/i,/^(?:[^:\n]+)/i,/^(?::[^#\n;]+)/i,/^(?::)/i,/^(?:$)/i,/^(?:.)/i],conditions:{acc_descr_multiline:{rules:[6,7],inclusive:!1},acc_descr:{rules:[4],inclusive:!1},acc_title:{rules:[2],inclusive:!1},callbackargs:{rules:[21,22],inclusive:!1},callbackname:{rules:[18,19,20],inclusive:!1},href:{rules:[15,16],inclusive:!1},click:{rules:[24,25],inclusive:!1},INITIAL:{rules:[0,1,3,5,8,9,10,11,12,13,14,17,23,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52],inclusive:!0}}};return f})();x.lexer=v;function k(){this.yy={}}return c(k,"Parser"),k.prototype=x,x.Parser=k,new k})();Yt.parser=Yt;var fr=Yt;G.extend(Je);G.extend(ir);G.extend(or);var oe={friday:5,saturday:6},tt="",Pt="",Vt=void 0,zt="",mt=[],kt=[],Nt=new Map,Rt=[],Mt=[],dt="",Ht="",de=["active","done","crit","milestone","vert"],Bt=[],yt=!1,qt=!1,jt="sunday",Ct="saturday",Lt=0,hr=c(function(){Rt=[],Mt=[],dt="",Bt=[],Dt=0,Ot=void 0,St=void 0,q=[],tt="",Pt="",Ht="",Vt=void 0,zt="",mt=[],kt=[],yt=!1,qt=!1,Lt=0,Nt=new Map,Ce(),jt="sunday",Ct="saturday"},"clear"),mr=c(function(t){Pt=t},"setAxisFormat"),kr=c(function(){return Pt},"getAxisFormat"),yr=c(function(t){Vt=t},"setTickInterval"),pr=c(function(){return Vt},"getTickInterval"),gr=c(function(t){zt=t},"setTodayMarker"),vr=c(function(){return zt},"getTodayMarker"),xr=c(function(t){tt=t},"setDateFormat"),Tr=c(function(){yt=!0},"enableInclusiveEndDates"),br=c(function(){return yt},"endDatesAreInclusive"),wr=c(function(){qt=!0},"enableTopAxis"),_r=c(function(){return qt},"topAxisEnabled"),Dr=c(function(t){Ht=t},"setDisplayMode"),Sr=c(function(){return Ht},"getDisplayMode"),Mr=c(function(){return tt},"getDateFormat"),Cr=c(function(t){mt=t.toLowerCase().split(/[\s,]+/)},"setIncludes"),Er=c(function(){return mt},"getIncludes"),Ir=c(function(t){kt=t.toLowerCase().split(/[\s,]+/)},"setExcludes"),Ar=c(function(){return kt},"getExcludes"),Fr=c(function(){return Nt},"getLinks"),$r=c(function(t){dt=t,Rt.push(t)},"addSection"),Yr=c(function(){return Rt},"getSections"),Lr=c(function(){let t=ce();const i=10;let n=0;for(;!t&&n<i;)t=ce(),n++;return Mt=q,Mt},"getTasks"),fe=c(function(t,i,n,s){const a=t.format(i.trim()),m=t.format("YYYY-MM-DD");return s.includes(a)||s.includes(m)?!1:n.includes("weekends")&&(t.isoWeekday()===oe[Ct]||t.isoWeekday()===oe[Ct]+1)||n.includes(t.format("dddd").toLowerCase())?!0:n.includes(a)||n.includes(m)},"isInvalidDate"),Wr=c(function(t){jt=t},"setWeekday"),Or=c(function(){return jt},"getWeekday"),Pr=c(function(t){Ct=t},"setWeekend"),he=c(function(t,i,n,s){if(!n.length||t.manualEndTime)return;let a;t.startTime instanceof Date?a=G(t.startTime):a=G(t.startTime,i,!0),a=a.add(1,"d");let m;t.endTime instanceof Date?m=G(t.endTime):m=G(t.endTime,i,!0);const[h,b]=Vr(a,m,i,n,s);t.endTime=h.toDate(),t.renderEndTime=b},"checkTaskDates"),Vr=c(function(t,i,n,s,a){let m=!1,h=null;for(;t<=i;)m||(h=i.toDate()),m=fe(t,n,s,a),m&&(i=i.add(1,"d")),t=t.add(1,"d");return[i,h]},"fixTaskDates"),Wt=c(function(t,i,n){if(n=n.trim(),c(b=>{const E=b.trim();return E==="x"||E==="X"},"isTimestampFormat")(i)&&/^\d+$/.test(n))return new Date(Number(n));const m=/^after\s+(?<ids>[\d\w- ]+)/.exec(n);if(m!==null){let b=null;for(const F of m.groups.ids.split(" ")){let T=at(F);T!==void 0&&(!b||T.endTime>b.endTime)&&(b=T)}if(b)return b.endTime;const E=new Date;return E.setHours(0,0,0,0),E}let h=G(n,i.trim(),!0);if(h.isValid())return h.toDate();{nt.debug("Invalid date:"+n),nt.debug("With date format:"+i.trim());const b=new Date(n);if(b===void 0||isNaN(b.getTime())||b.getFullYear()<-1e4||b.getFullYear()>1e4)throw new Error("Invalid date:"+n);return b}},"getStartDate"),me=c(function(t){const i=/^(\d+(?:\.\d+)?)([Mdhmswy]|ms)$/.exec(t.trim());return i!==null?[Number.parseFloat(i[1]),i[2]]:[NaN,"ms"]},"parseDuration"),ke=c(function(t,i,n,s=!1){n=n.trim();const m=/^until\s+(?<ids>[\d\w- ]+)/.exec(n);if(m!==null){let T=null;for(const M of m.groups.ids.split(" ")){let D=at(M);D!==void 0&&(!T||D.startTime<T.startTime)&&(T=D)}if(T)return T.startTime;const L=new Date;return L.setHours(0,0,0,0),L}let h=G(n,i.trim(),!0);if(h.isValid())return s&&(h=h.add(1,"d")),h.toDate();let b=G(t);const[E,F]=me(n);if(!Number.isNaN(E)){const T=b.add(E,F);T.isValid()&&(b=T)}return b.toDate()},"getEndDate"),Dt=0,ut=c(function(t){return t===void 0?(Dt=Dt+1,"task"+Dt):t},"parseId"),zr=c(function(t,i){let n;i.substr(0,1)===":"?n=i.substr(1,i.length):n=i;const s=n.split(","),a={};Gt(s,a,de);for(let h=0;h<s.length;h++)s[h]=s[h].trim();let m="";switch(s.length){case 1:a.id=ut(),a.startTime=t.endTime,m=s[0];break;case 2:a.id=ut(),a.startTime=Wt(void 0,tt,s[0]),m=s[1];break;case 3:a.id=ut(s[0]),a.startTime=Wt(void 0,tt,s[1]),m=s[2];break}return m&&(a.endTime=ke(a.startTime,tt,m,yt),a.manualEndTime=G(m,"YYYY-MM-DD",!0).isValid(),he(a,tt,kt,mt)),a},"compileData"),Nr=c(function(t,i){let n;i.substr(0,1)===":"?n=i.substr(1,i.length):n=i;const s=n.split(","),a={};Gt(s,a,de);for(let m=0;m<s.length;m++)s[m]=s[m].trim();switch(s.length){case 1:a.id=ut(),a.startTime={type:"prevTaskEnd",id:t},a.endTime={data:s[0]};break;case 2:a.id=ut(),a.startTime={type:"getStartDate",startData:s[0]},a.endTime={data:s[1]};break;case 3:a.id=ut(s[0]),a.startTime={type:"getStartDate",startData:s[1]},a.endTime={data:s[2]};break}return a},"parseData"),Ot,St,q=[],ye={},Rr=c(function(t,i){const n={section:dt,type:dt,processed:!1,manualEndTime:!1,renderEndTime:null,raw:{data:i},task:t,classes:[]},s=Nr(St,i);n.raw.startTime=s.startTime,n.raw.endTime=s.endTime,n.id=s.id,n.prevTaskId=St,n.active=s.active,n.done=s.done,n.crit=s.crit,n.milestone=s.milestone,n.vert=s.vert,n.order=Lt,Lt++;const a=q.push(n);St=n.id,ye[n.id]=a-1},"addTask"),at=c(function(t){const i=ye[t];return q[i]},"findTaskById"),Hr=c(function(t,i){const n={section:dt,type:dt,description:t,task:t,classes:[]},s=zr(Ot,i);n.startTime=s.startTime,n.endTime=s.endTime,n.id=s.id,n.active=s.active,n.done=s.done,n.crit=s.crit,n.milestone=s.milestone,n.vert=s.vert,Ot=n,Mt.push(n)},"addTaskOrg"),ce=c(function(){const t=c(function(n){const s=q[n];let a="";switch(q[n].raw.startTime.type){case"prevTaskEnd":{const m=at(s.prevTaskId);s.startTime=m.endTime;break}case"getStartDate":a=Wt(void 0,tt,q[n].raw.startTime.startData),a&&(q[n].startTime=a);break}return q[n].startTime&&(q[n].endTime=ke(q[n].startTime,tt,q[n].raw.endTime.data,yt),q[n].endTime&&(q[n].processed=!0,q[n].manualEndTime=G(q[n].raw.endTime.data,"YYYY-MM-DD",!0).isValid(),he(q[n],tt,kt,mt))),q[n].processed},"compileTask");let i=!0;for(const[n,s]of q.entries())t(n),i=i&&s.processed;return i},"compileTasks"),Br=c(function(t,i){let n=i;lt().securityLevel!=="loose"&&(n=Me.sanitizeUrl(i)),t.split(",").forEach(function(s){at(s)!==void 0&&(ge(s,()=>{window.open(n,"_self")}),Nt.set(s,n))}),pe(t,"clickable")},"setLink"),pe=c(function(t,i){t.split(",").forEach(function(n){let s=at(n);s!==void 0&&s.classes.push(i)})},"setClass"),qr=c(function(t,i,n){if(lt().securityLevel!=="loose"||i===void 0)return;let s=[];if(typeof n=="string"){s=n.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/);for(let m=0;m<s.length;m++){let h=s[m].trim();h.startsWith('"')&&h.endsWith('"')&&(h=h.substr(1,h.length-2)),s[m]=h}}s.length===0&&s.push(t),at(t)!==void 0&&ge(t,()=>{Ee.runFunc(i,...s)})},"setClickFun"),ge=c(function(t,i){Bt.push(function(){const n=document.querySelector(`[id="${t}"]`);n!==null&&n.addEventListener("click",function(){i()})},function(){const n=document.querySelector(`[id="${t}-text"]`);n!==null&&n.addEventListener("click",function(){i()})})},"pushFun"),jr=c(function(t,i,n){t.split(",").forEach(function(s){qr(s,i,n)}),pe(t,"clickable")},"setClickEvent"),Gr=c(function(t){Bt.forEach(function(i){i(t)})},"bindFunctions"),Xr={getConfig:c(()=>lt().gantt,"getConfig"),clear:hr,setDateFormat:xr,getDateFormat:Mr,enableInclusiveEndDates:Tr,endDatesAreInclusive:br,enableTopAxis:wr,topAxisEnabled:_r,setAxisFormat:mr,getAxisFormat:kr,setTickInterval:yr,getTickInterval:pr,setTodayMarker:gr,getTodayMarker:vr,setAccTitle:_e,getAccTitle:we,setDiagramTitle:be,getDiagramTitle:Te,setDisplayMode:Dr,getDisplayMode:Sr,setAccDescription:xe,getAccDescription:ve,addSection:$r,getSections:Yr,getTasks:Lr,addTask:Rr,findTaskById:at,addTaskOrg:Hr,setIncludes:Cr,getIncludes:Er,setExcludes:Ir,getExcludes:Ar,setClickEvent:jr,setLink:Br,getLinks:Fr,bindFunctions:Gr,parseDuration:me,isInvalidDate:fe,setWeekday:Wr,getWeekday:Or,setWeekend:Pr};function Gt(t,i,n){let s=!0;for(;s;)s=!1,n.forEach(function(a){const m="^\\s*"+a+"\\s*$",h=new RegExp(m);t[0].match(h)&&(i[a]=!0,t.shift(1),s=!0)})}c(Gt,"getTaskTags");G.extend(dr);var Ur=c(function(){nt.debug("Something is calling, setConf, remove the call")},"setConf"),le={monday:ze,tuesday:Ve,wednesday:Pe,thursday:Oe,friday:We,saturday:Le,sunday:Ye},Zr=c((t,i)=>{let n=[...t].map(()=>-1/0),s=[...t].sort((m,h)=>m.startTime-h.startTime||m.order-h.order),a=0;for(const m of s)for(let h=0;h<n.length;h++)if(m.startTime>=n[h]){n[h]=m.endTime,m.order=h+i,h>a&&(a=h);break}return a},"getMaxIntersections"),et,Ft=1e4,Qr=c(function(t,i,n,s){const a=lt().gantt,m=lt().securityLevel;let h;m==="sandbox"&&(h=gt("#i"+i));const b=m==="sandbox"?gt(h.nodes()[0].contentDocument.body):gt("body"),E=m==="sandbox"?h.nodes()[0].contentDocument:document,F=E.getElementById(i);et=F.parentElement.offsetWidth,et===void 0&&(et=1200),a.useWidth!==void 0&&(et=a.useWidth);const T=s.db.getTasks();let L=[];for(const u of T)L.push(u.type);L=d(L);const M={};let D=2*a.topPadding;if(s.db.getDisplayMode()==="compact"||a.displayMode==="compact"){const u={};for(const v of T)u[v.section]===void 0?u[v.section]=[v]:u[v.section].push(v);let x=0;for(const v of Object.keys(u)){const k=Zr(u[v],x)+1;x+=k,D+=k*(a.barHeight+a.barGap),M[v]=k}}else{D+=T.length*(a.barHeight+a.barGap);for(const u of L)M[u]=T.filter(x=>x.type===u).length}F.setAttribute("viewBox","0 0 "+et+" "+D);const N=b.select(`[id="${i}"]`),I=Ie().domain([Ae(T,function(u){return u.startTime}),Fe(T,function(u){return u.endTime})]).rangeRound([0,et-a.leftPadding-a.rightPadding]);function S(u,x){const v=u.startTime,k=x.startTime;let f=0;return v>k?f=1:v<k&&(f=-1),f}c(S,"taskCompare"),T.sort(S),C(T,et,D),De(N,D,et,a.useMaxWidth),N.append("text").text(s.db.getDiagramTitle()).attr("x",et/2).attr("y",a.titleTopMargin).attr("class","titleText");function C(u,x,v){const k=a.barHeight,f=k+a.barGap,o=a.topPadding,l=a.leftPadding,y=Ne().domain([0,L.length]).range(["#00B9FA","#F95002"]).interpolate($e);Y(f,o,l,x,v,u,s.db.getExcludes(),s.db.getIncludes()),H(l,o,x,v),O(u,f,o,l,k,y,x),A(f,o),g(l,o,x,v)}c(C,"makeGantt");function O(u,x,v,k,f,o,l){u.sort((e,_)=>e.vert===_.vert?0:e.vert?1:-1);const p=[...new Set(u.map(e=>e.order))].map(e=>u.find(_=>_.order===e));N.append("g").selectAll("rect").data(p).enter().append("rect").attr("x",0).attr("y",function(e,_){return _=e.order,_*x+v-2}).attr("width",function(){return l-a.rightPadding/2}).attr("height",x).attr("class",function(e){for(const[_,V]of L.entries())if(e.type===V)return"section section"+_%a.numberSectionStyles;return"section section0"}).enter();const w=N.append("g").selectAll("rect").data(u).enter(),r=s.db.getLinks();if(w.append("rect").attr("id",function(e){return e.id}).attr("rx",3).attr("ry",3).attr("x",function(e){return e.milestone?I(e.startTime)+k+.5*(I(e.endTime)-I(e.startTime))-.5*f:I(e.startTime)+k}).attr("y",function(e,_){return _=e.order,e.vert?a.gridLineStartPadding:_*x+v}).attr("width",function(e){return e.milestone?f:e.vert?.08*f:I(e.renderEndTime||e.endTime)-I(e.startTime)}).attr("height",function(e){return e.vert?T.length*(a.barHeight+a.barGap)+a.barHeight*2:f}).attr("transform-origin",function(e,_){return _=e.order,(I(e.startTime)+k+.5*(I(e.endTime)-I(e.startTime))).toString()+"px "+(_*x+v+.5*f).toString()+"px"}).attr("class",function(e){const _="task";let V="";e.classes.length>0&&(V=e.classes.join(" "));let P=0;for(const[j,$]of L.entries())e.type===$&&(P=j%a.numberSectionStyles);let W="";return e.active?e.crit?W+=" activeCrit":W=" active":e.done?e.crit?W=" doneCrit":W=" done":e.crit&&(W+=" crit"),W.length===0&&(W=" task"),e.milestone&&(W=" milestone "+W),e.vert&&(W=" vert "+W),W+=P,W+=" "+V,_+W}),w.append("text").attr("id",function(e){return e.id+"-text"}).text(function(e){return e.task}).attr("font-size",a.fontSize).attr("x",function(e){let _=I(e.startTime),V=I(e.renderEndTime||e.endTime);if(e.milestone&&(_+=.5*(I(e.endTime)-I(e.startTime))-.5*f,V=_+f),e.vert)return I(e.startTime)+k;const P=this.getBBox().width;return P>V-_?V+P+1.5*a.leftPadding>l?_+k-5:V+k+5:(V-_)/2+_+k}).attr("y",function(e,_){return e.vert?a.gridLineStartPadding+T.length*(a.barHeight+a.barGap)+60:(_=e.order,_*x+a.barHeight/2+(a.fontSize/2-2)+v)}).attr("text-height",f).attr("class",function(e){const _=I(e.startTime);let V=I(e.endTime);e.milestone&&(V=_+f);const P=this.getBBox().width;let W="";e.classes.length>0&&(W=e.classes.join(" "));let j=0;for(const[Q,rt]of L.entries())e.type===rt&&(j=Q%a.numberSectionStyles);let $="";return e.active&&(e.crit?$="activeCritText"+j:$="activeText"+j),e.done?e.crit?$=$+" doneCritText"+j:$=$+" doneText"+j:e.crit&&($=$+" critText"+j),e.milestone&&($+=" milestoneText"),e.vert&&($+=" vertText"),P>V-_?V+P+1.5*a.leftPadding>l?W+" taskTextOutsideLeft taskTextOutside"+j+" "+$:W+" taskTextOutsideRight taskTextOutside"+j+" "+$+" width-"+P:W+" taskText taskText"+j+" "+$+" width-"+P}),lt().securityLevel==="sandbox"){let e;e=gt("#i"+i);const _=e.nodes()[0].contentDocument;w.filter(function(V){return r.has(V.id)}).each(function(V){var P=_.querySelector("#"+V.id),W=_.querySelector("#"+V.id+"-text");const j=P.parentNode;var $=_.createElement("a");$.setAttribute("xlink:href",r.get(V.id)),$.setAttribute("target","_top"),j.appendChild($),$.appendChild(P),$.appendChild(W)})}}c(O,"drawRects");function Y(u,x,v,k,f,o,l,y){if(l.length===0&&y.length===0)return;let p,w;for(const{startTime:P,endTime:W}of o)(p===void 0||P<p)&&(p=P),(w===void 0||W>w)&&(w=W);if(!p||!w)return;if(G(w).diff(G(p),"year")>5){nt.warn("The difference between the min and max time is more than 5 years. This will cause performance issues. Skipping drawing exclude days.");return}const r=s.db.getDateFormat(),z=[];let e=null,_=G(p);for(;_.valueOf()<=w;)s.db.isInvalidDate(_,r,l,y)?e?e.end=_:e={start:_,end:_}:e&&(z.push(e),e=null),_=_.add(1,"d");N.append("g").selectAll("rect").data(z).enter().append("rect").attr("id",P=>"exclude-"+P.start.format("YYYY-MM-DD")).attr("x",P=>I(P.start.startOf("day"))+v).attr("y",a.gridLineStartPadding).attr("width",P=>I(P.end.endOf("day"))-I(P.start.startOf("day"))).attr("height",f-x-a.gridLineStartPadding).attr("transform-origin",function(P,W){return(I(P.start)+v+.5*(I(P.end)-I(P.start))).toString()+"px "+(W*u+.5*f).toString()+"px"}).attr("class","exclude-range")}c(Y,"drawExcludeDays");function R(u,x,v,k){if(v<=0||u>x)return 1/0;const f=x-u,o=G.duration({[k??"day"]:v}).asMilliseconds();return o<=0?1/0:Math.ceil(f/o)}c(R,"getEstimatedTickCount");function H(u,x,v,k){const f=s.db.getDateFormat(),o=s.db.getAxisFormat();let l;o?l=o:f==="D"?l="%d":l=a.axisFormat??"%Y-%m-%d";let y=Ue(I).tickSize(-k+x+a.gridLineStartPadding).tickFormat(Ut(l));const w=/^([1-9]\d*)(millisecond|second|minute|hour|day|week|month)$/.exec(s.db.getTickInterval()||a.tickInterval);if(w!==null){const r=parseInt(w[1],10);if(isNaN(r)||r<=0)nt.warn(`Invalid tick interval value: "${w[1]}". Skipping custom tick interval.`);else{const z=w[2],e=s.db.getWeekday()||a.weekday,_=I.domain(),V=_[0],P=_[1],W=R(V,P,r,z);if(W>Ft)nt.warn(`The tick interval "${r}${z}" would generate ${W} ticks, which exceeds the maximum allowed (${Ft}). This may indicate an invalid date or time range. Skipping custom tick interval.`);else switch(z){case"millisecond":y.ticks(ee.every(r));break;case"second":y.ticks(te.every(r));break;case"minute":y.ticks(Jt.every(r));break;case"hour":y.ticks(Kt.every(r));break;case"day":y.ticks(Qt.every(r));break;case"week":y.ticks(le[e].every(r));break;case"month":y.ticks(Zt.every(r));break}}}if(N.append("g").attr("class","grid").attr("transform","translate("+u+", "+(k-50)+")").call(y).selectAll("text").style("text-anchor","middle").attr("fill","#000").attr("stroke","none").attr("font-size",10).attr("dy","1em"),s.db.topAxisEnabled()||a.topAxis){let r=Xe(I).tickSize(-k+x+a.gridLineStartPadding).tickFormat(Ut(l));if(w!==null){const z=parseInt(w[1],10);if(isNaN(z)||z<=0)nt.warn(`Invalid tick interval value: "${w[1]}". Skipping custom tick interval.`);else{const e=w[2],_=s.db.getWeekday()||a.weekday,V=I.domain(),P=V[0],W=V[1];if(R(P,W,z,e)<=Ft)switch(e){case"millisecond":r.ticks(ee.every(z));break;case"second":r.ticks(te.every(z));break;case"minute":r.ticks(Jt.every(z));break;case"hour":r.ticks(Kt.every(z));break;case"day":r.ticks(Qt.every(z));break;case"week":r.ticks(le[_].every(z));break;case"month":r.ticks(Zt.every(z));break}}}N.append("g").attr("class","grid").attr("transform","translate("+u+", "+x+")").call(r).selectAll("text").style("text-anchor","middle").attr("fill","#000").attr("stroke","none").attr("font-size",10)}}c(H,"makeGrid");function A(u,x){let v=0;const k=Object.keys(M).map(f=>[f,M[f]]);N.append("g").selectAll("text").data(k).enter().append(function(f){const o=f[0].split(Se.lineBreakRegex),l=-(o.length-1)/2,y=E.createElementNS("http://www.w3.org/2000/svg","text");y.setAttribute("dy",l+"em");for(const[p,w]of o.entries()){const r=E.createElementNS("http://www.w3.org/2000/svg","tspan");r.setAttribute("alignment-baseline","central"),r.setAttribute("x","10"),p>0&&r.setAttribute("dy","1em"),r.textContent=w,y.appendChild(r)}return y}).attr("x",10).attr("y",function(f,o){if(o>0)for(let l=0;l<o;l++)return v+=k[o-1][1],f[1]*u/2+v*u+x;else return f[1]*u/2+x}).attr("font-size",a.sectionFontSize).attr("class",function(f){for(const[o,l]of L.entries())if(f[0]===l)return"sectionTitle sectionTitle"+o%a.numberSectionStyles;return"sectionTitle"})}c(A,"vertLabels");function g(u,x,v,k){const f=s.db.getTodayMarker();if(f==="off")return;const o=N.append("g").attr("class","today"),l=new Date,y=o.append("line");y.attr("x1",I(l)+u).attr("x2",I(l)+u).attr("y1",a.titleTopMargin).attr("y2",k-a.titleTopMargin).attr("class","today"),f!==""&&y.attr("style",f.replace(/,/g,";"))}c(g,"drawToday");function d(u){const x={},v=[];for(let k=0,f=u.length;k<f;++k)Object.prototype.hasOwnProperty.call(x,u[k])||(x[u[k]]=!0,v.push(u[k]));return v}c(d,"checkUnique")},"draw"),Kr={setConf:Ur,draw:Qr},Jr=c(t=>`
  .mermaid-main-font {
        font-family: ${t.fontFamily};
  }

  .exclude-range {
    fill: ${t.excludeBkgColor};
  }

  .section {
    stroke: none;
    opacity: 0.2;
  }

  .section0 {
    fill: ${t.sectionBkgColor};
  }

  .section2 {
    fill: ${t.sectionBkgColor2};
  }

  .section1,
  .section3 {
    fill: ${t.altSectionBkgColor};
    opacity: 0.2;
  }

  .sectionTitle0 {
    fill: ${t.titleColor};
  }

  .sectionTitle1 {
    fill: ${t.titleColor};
  }

  .sectionTitle2 {
    fill: ${t.titleColor};
  }

  .sectionTitle3 {
    fill: ${t.titleColor};
  }

  .sectionTitle {
    text-anchor: start;
    font-family: ${t.fontFamily};
  }


  /* Grid and axis */

  .grid .tick {
    stroke: ${t.gridColor};
    opacity: 0.8;
    shape-rendering: crispEdges;
  }

  .grid .tick text {
    font-family: ${t.fontFamily};
    fill: ${t.textColor};
  }

  .grid path {
    stroke-width: 0;
  }


  /* Today line */

  .today {
    fill: none;
    stroke: ${t.todayLineColor};
    stroke-width: 2px;
  }


  /* Task styling */

  /* Default task */

  .task {
    stroke-width: 2;
  }

  .taskText {
    text-anchor: middle;
    font-family: ${t.fontFamily};
  }

  .taskTextOutsideRight {
    fill: ${t.taskTextDarkColor};
    text-anchor: start;
    font-family: ${t.fontFamily};
  }

  .taskTextOutsideLeft {
    fill: ${t.taskTextDarkColor};
    text-anchor: end;
  }


  /* Special case clickable */

  .task.clickable {
    cursor: pointer;
  }

  .taskText.clickable {
    cursor: pointer;
    fill: ${t.taskTextClickableColor} !important;
    font-weight: bold;
  }

  .taskTextOutsideLeft.clickable {
    cursor: pointer;
    fill: ${t.taskTextClickableColor} !important;
    font-weight: bold;
  }

  .taskTextOutsideRight.clickable {
    cursor: pointer;
    fill: ${t.taskTextClickableColor} !important;
    font-weight: bold;
  }


  /* Specific task settings for the sections*/

  .taskText0,
  .taskText1,
  .taskText2,
  .taskText3 {
    fill: ${t.taskTextColor};
  }

  .task0,
  .task1,
  .task2,
  .task3 {
    fill: ${t.taskBkgColor};
    stroke: ${t.taskBorderColor};
  }

  .taskTextOutside0,
  .taskTextOutside2
  {
    fill: ${t.taskTextOutsideColor};
  }

  .taskTextOutside1,
  .taskTextOutside3 {
    fill: ${t.taskTextOutsideColor};
  }


  /* Active task */

  .active0,
  .active1,
  .active2,
  .active3 {
    fill: ${t.activeTaskBkgColor};
    stroke: ${t.activeTaskBorderColor};
  }

  .activeText0,
  .activeText1,
  .activeText2,
  .activeText3 {
    fill: ${t.taskTextDarkColor} !important;
  }


  /* Completed task */

  .done0,
  .done1,
  .done2,
  .done3 {
    stroke: ${t.doneTaskBorderColor};
    fill: ${t.doneTaskBkgColor};
    stroke-width: 2;
  }

  .doneText0,
  .doneText1,
  .doneText2,
  .doneText3 {
    fill: ${t.taskTextDarkColor} !important;
  }


  /* Tasks on the critical line */

  .crit0,
  .crit1,
  .crit2,
  .crit3 {
    stroke: ${t.critBorderColor};
    fill: ${t.critBkgColor};
    stroke-width: 2;
  }

  .activeCrit0,
  .activeCrit1,
  .activeCrit2,
  .activeCrit3 {
    stroke: ${t.critBorderColor};
    fill: ${t.activeTaskBkgColor};
    stroke-width: 2;
  }

  .doneCrit0,
  .doneCrit1,
  .doneCrit2,
  .doneCrit3 {
    stroke: ${t.critBorderColor};
    fill: ${t.doneTaskBkgColor};
    stroke-width: 2;
    cursor: pointer;
    shape-rendering: crispEdges;
  }

  .milestone {
    transform: rotate(45deg) scale(0.8,0.8);
  }

  .milestoneText {
    font-style: italic;
  }
  .doneCritText0,
  .doneCritText1,
  .doneCritText2,
  .doneCritText3 {
    fill: ${t.taskTextDarkColor} !important;
  }

  .vert {
    stroke: ${t.vertLineColor};
  }

  .vertText {
    font-size: 15px;
    text-anchor: middle;
    fill: ${t.vertLineColor} !important;
  }

  .activeCritText0,
  .activeCritText1,
  .activeCritText2,
  .activeCritText3 {
    fill: ${t.taskTextDarkColor} !important;
  }

  .titleText {
    text-anchor: middle;
    font-size: 18px;
    fill: ${t.titleColor||t.textColor};
    font-family: ${t.fontFamily};
  }
`,"getStyles"),ti=Jr,Ci={parser:fr,db:Xr,renderer:Kr,styles:ti};export{Ci as diagram};
