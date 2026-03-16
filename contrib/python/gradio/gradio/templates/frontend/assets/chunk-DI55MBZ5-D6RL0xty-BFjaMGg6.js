import{g as te}from"./chunk-55IACEB6-DFDtoedX-Cbmn-Slb.js";import{s as ee}from"./chunk-QN33PNHL-ChAeawbz-BCeqv_G1.js";import{_ as d,l as m,c as P,r as se,u as ie,a as re,b as ae,g as ne,s as le,p as oe,q as ce,T as he,k as W,y as ue}from"./mermaid.core-vMMZVCDT-DDcWQHpZ.js";var ft,kt=(function(){var t=d(function(V,n,h,r){for(h=h||{},r=V.length;r--;h[V[r]]=n);return h},"o"),e=[1,2],a=[1,3],s=[1,4],c=[2,4],l=[1,9],S=[1,11],_=[1,16],f=[1,17],T=[1,18],v=[1,19],g=[1,33],k=[1,20],Y=[1,21],L=[1,22],N=[1,23],C=[1,24],u=[1,26],D=[1,27],w=[1,28],O=[1,29],$=[1,30],G=[1,31],F=[1,32],rt=[1,35],at=[1,36],nt=[1,37],lt=[1,38],z=[1,34],p=[1,4,5,16,17,19,21,22,24,25,26,27,28,29,33,35,37,38,41,45,48,51,52,53,54,57],ot=[1,4,5,14,15,16,17,19,21,22,24,25,26,27,28,29,33,35,37,38,39,40,41,45,48,51,52,53,54,57],xt=[4,5,16,17,19,21,22,24,25,26,27,28,29,33,35,37,38,41,45,48,51,52,53,54,57],gt={trace:d(function(){},"trace"),yy:{},symbols_:{error:2,start:3,SPACE:4,NL:5,SD:6,document:7,line:8,statement:9,classDefStatement:10,styleStatement:11,cssClassStatement:12,idStatement:13,DESCR:14,"-->":15,HIDE_EMPTY:16,scale:17,WIDTH:18,COMPOSIT_STATE:19,STRUCT_START:20,STRUCT_STOP:21,STATE_DESCR:22,AS:23,ID:24,FORK:25,JOIN:26,CHOICE:27,CONCURRENT:28,note:29,notePosition:30,NOTE_TEXT:31,direction:32,acc_title:33,acc_title_value:34,acc_descr:35,acc_descr_value:36,acc_descr_multiline_value:37,CLICK:38,STRING:39,HREF:40,classDef:41,CLASSDEF_ID:42,CLASSDEF_STYLEOPTS:43,DEFAULT:44,style:45,STYLE_IDS:46,STYLEDEF_STYLEOPTS:47,class:48,CLASSENTITY_IDS:49,STYLECLASS:50,direction_tb:51,direction_bt:52,direction_rl:53,direction_lr:54,eol:55,";":56,EDGE_STATE:57,STYLE_SEPARATOR:58,left_of:59,right_of:60,$accept:0,$end:1},terminals_:{2:"error",4:"SPACE",5:"NL",6:"SD",14:"DESCR",15:"-->",16:"HIDE_EMPTY",17:"scale",18:"WIDTH",19:"COMPOSIT_STATE",20:"STRUCT_START",21:"STRUCT_STOP",22:"STATE_DESCR",23:"AS",24:"ID",25:"FORK",26:"JOIN",27:"CHOICE",28:"CONCURRENT",29:"note",31:"NOTE_TEXT",33:"acc_title",34:"acc_title_value",35:"acc_descr",36:"acc_descr_value",37:"acc_descr_multiline_value",38:"CLICK",39:"STRING",40:"HREF",41:"classDef",42:"CLASSDEF_ID",43:"CLASSDEF_STYLEOPTS",44:"DEFAULT",45:"style",46:"STYLE_IDS",47:"STYLEDEF_STYLEOPTS",48:"class",49:"CLASSENTITY_IDS",50:"STYLECLASS",51:"direction_tb",52:"direction_bt",53:"direction_rl",54:"direction_lr",56:";",57:"EDGE_STATE",58:"STYLE_SEPARATOR",59:"left_of",60:"right_of"},productions_:[0,[3,2],[3,2],[3,2],[7,0],[7,2],[8,2],[8,1],[8,1],[9,1],[9,1],[9,1],[9,1],[9,2],[9,3],[9,4],[9,1],[9,2],[9,1],[9,4],[9,3],[9,6],[9,1],[9,1],[9,1],[9,1],[9,4],[9,4],[9,1],[9,2],[9,2],[9,1],[9,5],[9,5],[10,3],[10,3],[11,3],[12,3],[32,1],[32,1],[32,1],[32,1],[55,1],[55,1],[13,1],[13,1],[13,3],[13,3],[30,1],[30,1]],performAction:d(function(n,h,r,y,E,i,Q){var o=i.length-1;switch(E){case 3:return y.setRootDoc(i[o]),i[o];case 4:this.$=[];break;case 5:i[o]!="nl"&&(i[o-1].push(i[o]),this.$=i[o-1]);break;case 6:case 7:this.$=i[o];break;case 8:this.$="nl";break;case 12:this.$=i[o];break;case 13:const Z=i[o-1];Z.description=y.trimColon(i[o]),this.$=Z;break;case 14:this.$={stmt:"relation",state1:i[o-2],state2:i[o]};break;case 15:const _t=y.trimColon(i[o]);this.$={stmt:"relation",state1:i[o-3],state2:i[o-1],description:_t};break;case 19:this.$={stmt:"state",id:i[o-3],type:"default",description:"",doc:i[o-1]};break;case 20:var U=i[o],K=i[o-2].trim();if(i[o].match(":")){var ht=i[o].split(":");U=ht[0],K=[K,ht[1]]}this.$={stmt:"state",id:U,type:"default",description:K};break;case 21:this.$={stmt:"state",id:i[o-3],type:"default",description:i[o-5],doc:i[o-1]};break;case 22:this.$={stmt:"state",id:i[o],type:"fork"};break;case 23:this.$={stmt:"state",id:i[o],type:"join"};break;case 24:this.$={stmt:"state",id:i[o],type:"choice"};break;case 25:this.$={stmt:"state",id:y.getDividerId(),type:"divider"};break;case 26:this.$={stmt:"state",id:i[o-1].trim(),note:{position:i[o-2].trim(),text:i[o].trim()}};break;case 29:this.$=i[o].trim(),y.setAccTitle(this.$);break;case 30:case 31:this.$=i[o].trim(),y.setAccDescription(this.$);break;case 32:this.$={stmt:"click",id:i[o-3],url:i[o-2],tooltip:i[o-1]};break;case 33:this.$={stmt:"click",id:i[o-3],url:i[o-1],tooltip:""};break;case 34:case 35:this.$={stmt:"classDef",id:i[o-1].trim(),classes:i[o].trim()};break;case 36:this.$={stmt:"style",id:i[o-1].trim(),styleClass:i[o].trim()};break;case 37:this.$={stmt:"applyClass",id:i[o-1].trim(),styleClass:i[o].trim()};break;case 38:y.setDirection("TB"),this.$={stmt:"dir",value:"TB"};break;case 39:y.setDirection("BT"),this.$={stmt:"dir",value:"BT"};break;case 40:y.setDirection("RL"),this.$={stmt:"dir",value:"RL"};break;case 41:y.setDirection("LR"),this.$={stmt:"dir",value:"LR"};break;case 44:case 45:this.$={stmt:"state",id:i[o].trim(),type:"default",description:""};break;case 46:this.$={stmt:"state",id:i[o-2].trim(),classes:[i[o].trim()],type:"default",description:""};break;case 47:this.$={stmt:"state",id:i[o-2].trim(),classes:[i[o].trim()],type:"default",description:""};break}},"anonymous"),table:[{3:1,4:e,5:a,6:s},{1:[3]},{3:5,4:e,5:a,6:s},{3:6,4:e,5:a,6:s},t([1,4,5,16,17,19,22,24,25,26,27,28,29,33,35,37,38,41,45,48,51,52,53,54,57],c,{7:7}),{1:[2,1]},{1:[2,2]},{1:[2,3],4:l,5:S,8:8,9:10,10:12,11:13,12:14,13:15,16:_,17:f,19:T,22:v,24:g,25:k,26:Y,27:L,28:N,29:C,32:25,33:u,35:D,37:w,38:O,41:$,45:G,48:F,51:rt,52:at,53:nt,54:lt,57:z},t(p,[2,5]),{9:39,10:12,11:13,12:14,13:15,16:_,17:f,19:T,22:v,24:g,25:k,26:Y,27:L,28:N,29:C,32:25,33:u,35:D,37:w,38:O,41:$,45:G,48:F,51:rt,52:at,53:nt,54:lt,57:z},t(p,[2,7]),t(p,[2,8]),t(p,[2,9]),t(p,[2,10]),t(p,[2,11]),t(p,[2,12],{14:[1,40],15:[1,41]}),t(p,[2,16]),{18:[1,42]},t(p,[2,18],{20:[1,43]}),{23:[1,44]},t(p,[2,22]),t(p,[2,23]),t(p,[2,24]),t(p,[2,25]),{30:45,31:[1,46],59:[1,47],60:[1,48]},t(p,[2,28]),{34:[1,49]},{36:[1,50]},t(p,[2,31]),{13:51,24:g,57:z},{42:[1,52],44:[1,53]},{46:[1,54]},{49:[1,55]},t(ot,[2,44],{58:[1,56]}),t(ot,[2,45],{58:[1,57]}),t(p,[2,38]),t(p,[2,39]),t(p,[2,40]),t(p,[2,41]),t(p,[2,6]),t(p,[2,13]),{13:58,24:g,57:z},t(p,[2,17]),t(xt,c,{7:59}),{24:[1,60]},{24:[1,61]},{23:[1,62]},{24:[2,48]},{24:[2,49]},t(p,[2,29]),t(p,[2,30]),{39:[1,63],40:[1,64]},{43:[1,65]},{43:[1,66]},{47:[1,67]},{50:[1,68]},{24:[1,69]},{24:[1,70]},t(p,[2,14],{14:[1,71]}),{4:l,5:S,8:8,9:10,10:12,11:13,12:14,13:15,16:_,17:f,19:T,21:[1,72],22:v,24:g,25:k,26:Y,27:L,28:N,29:C,32:25,33:u,35:D,37:w,38:O,41:$,45:G,48:F,51:rt,52:at,53:nt,54:lt,57:z},t(p,[2,20],{20:[1,73]}),{31:[1,74]},{24:[1,75]},{39:[1,76]},{39:[1,77]},t(p,[2,34]),t(p,[2,35]),t(p,[2,36]),t(p,[2,37]),t(ot,[2,46]),t(ot,[2,47]),t(p,[2,15]),t(p,[2,19]),t(xt,c,{7:78}),t(p,[2,26]),t(p,[2,27]),{5:[1,79]},{5:[1,80]},{4:l,5:S,8:8,9:10,10:12,11:13,12:14,13:15,16:_,17:f,19:T,21:[1,81],22:v,24:g,25:k,26:Y,27:L,28:N,29:C,32:25,33:u,35:D,37:w,38:O,41:$,45:G,48:F,51:rt,52:at,53:nt,54:lt,57:z},t(p,[2,32]),t(p,[2,33]),t(p,[2,21])],defaultActions:{5:[2,1],6:[2,2],47:[2,48],48:[2,49]},parseError:d(function(n,h){if(h.recoverable)this.trace(n);else{var r=new Error(n);throw r.hash=h,r}},"parseError"),parse:d(function(n){var h=this,r=[0],y=[],E=[null],i=[],Q=this.table,o="",U=0,K=0,ht=2,Z=1,_t=i.slice.call(arguments,1),b=Object.create(this.lexer),j={yy:{}};for(var Tt in this.yy)Object.prototype.hasOwnProperty.call(this.yy,Tt)&&(j.yy[Tt]=this.yy[Tt]);b.setInput(n,j.yy),j.yy.lexer=b,j.yy.parser=this,typeof b.yylloc>"u"&&(b.yylloc={});var Et=b.yylloc;i.push(Et);var Qt=b.options&&b.options.ranges;typeof j.yy.parseError=="function"?this.parseError=j.yy.parseError:this.parseError=Object.getPrototypeOf(this).parseError;function Zt(I){r.length=r.length-2*I,E.length=E.length-I,i.length=i.length-I}d(Zt,"popStack");function Lt(){var I;return I=y.pop()||b.lex()||Z,typeof I!="number"&&(I instanceof Array&&(y=I,I=y.pop()),I=h.symbols_[I]||I),I}d(Lt,"lex");for(var x,H,R,mt,X={},ut,B,It,dt;;){if(H=r[r.length-1],this.defaultActions[H]?R=this.defaultActions[H]:((x===null||typeof x>"u")&&(x=Lt()),R=Q[H]&&Q[H][x]),typeof R>"u"||!R.length||!R[0]){var vt="";dt=[];for(ut in Q[H])this.terminals_[ut]&&ut>ht&&dt.push("'"+this.terminals_[ut]+"'");b.showPosition?vt="Parse error on line "+(U+1)+`:
`+b.showPosition()+`
Expecting `+dt.join(", ")+", got '"+(this.terminals_[x]||x)+"'":vt="Parse error on line "+(U+1)+": Unexpected "+(x==Z?"end of input":"'"+(this.terminals_[x]||x)+"'"),this.parseError(vt,{text:b.match,token:this.terminals_[x]||x,line:b.yylineno,loc:Et,expected:dt})}if(R[0]instanceof Array&&R.length>1)throw new Error("Parse Error: multiple actions possible at state: "+H+", token: "+x);switch(R[0]){case 1:r.push(x),E.push(b.yytext),i.push(b.yylloc),r.push(R[1]),x=null,K=b.yyleng,o=b.yytext,U=b.yylineno,Et=b.yylloc;break;case 2:if(B=this.productions_[R[1]][1],X.$=E[E.length-B],X._$={first_line:i[i.length-(B||1)].first_line,last_line:i[i.length-1].last_line,first_column:i[i.length-(B||1)].first_column,last_column:i[i.length-1].last_column},Qt&&(X._$.range=[i[i.length-(B||1)].range[0],i[i.length-1].range[1]]),mt=this.performAction.apply(X,[o,K,U,j.yy,R[1],E,i].concat(_t)),typeof mt<"u")return mt;B&&(r=r.slice(0,-1*B*2),E=E.slice(0,-1*B),i=i.slice(0,-1*B)),r.push(this.productions_[R[1]][0]),E.push(X.$),i.push(X._$),It=Q[r[r.length-2]][r[r.length-1]],r.push(It);break;case 3:return!0}}return!0},"parse")},qt=(function(){var V={EOF:1,parseError:d(function(h,r){if(this.yy.parser)this.yy.parser.parseError(h,r);else throw new Error(h)},"parseError"),setInput:d(function(n,h){return this.yy=h||this.yy||{},this._input=n,this._more=this._backtrack=this.done=!1,this.yylineno=this.yyleng=0,this.yytext=this.matched=this.match="",this.conditionStack=["INITIAL"],this.yylloc={first_line:1,first_column:0,last_line:1,last_column:0},this.options.ranges&&(this.yylloc.range=[0,0]),this.offset=0,this},"setInput"),input:d(function(){var n=this._input[0];this.yytext+=n,this.yyleng++,this.offset++,this.match+=n,this.matched+=n;var h=n.match(/(?:\r\n?|\n).*/g);return h?(this.yylineno++,this.yylloc.last_line++):this.yylloc.last_column++,this.options.ranges&&this.yylloc.range[1]++,this._input=this._input.slice(1),n},"input"),unput:d(function(n){var h=n.length,r=n.split(/(?:\r\n?|\n)/g);this._input=n+this._input,this.yytext=this.yytext.substr(0,this.yytext.length-h),this.offset-=h;var y=this.match.split(/(?:\r\n?|\n)/g);this.match=this.match.substr(0,this.match.length-1),this.matched=this.matched.substr(0,this.matched.length-1),r.length-1&&(this.yylineno-=r.length-1);var E=this.yylloc.range;return this.yylloc={first_line:this.yylloc.first_line,last_line:this.yylineno+1,first_column:this.yylloc.first_column,last_column:r?(r.length===y.length?this.yylloc.first_column:0)+y[y.length-r.length].length-r[0].length:this.yylloc.first_column-h},this.options.ranges&&(this.yylloc.range=[E[0],E[0]+this.yyleng-h]),this.yyleng=this.yytext.length,this},"unput"),more:d(function(){return this._more=!0,this},"more"),reject:d(function(){if(this.options.backtrack_lexer)this._backtrack=!0;else return this.parseError("Lexical error on line "+(this.yylineno+1)+`. You can only invoke reject() in the lexer when the lexer is of the backtracking persuasion (options.backtrack_lexer = true).
`+this.showPosition(),{text:"",token:null,line:this.yylineno});return this},"reject"),less:d(function(n){this.unput(this.match.slice(n))},"less"),pastInput:d(function(){var n=this.matched.substr(0,this.matched.length-this.match.length);return(n.length>20?"...":"")+n.substr(-20).replace(/\n/g,"")},"pastInput"),upcomingInput:d(function(){var n=this.match;return n.length<20&&(n+=this._input.substr(0,20-n.length)),(n.substr(0,20)+(n.length>20?"...":"")).replace(/\n/g,"")},"upcomingInput"),showPosition:d(function(){var n=this.pastInput(),h=new Array(n.length+1).join("-");return n+this.upcomingInput()+`
`+h+"^"},"showPosition"),test_match:d(function(n,h){var r,y,E;if(this.options.backtrack_lexer&&(E={yylineno:this.yylineno,yylloc:{first_line:this.yylloc.first_line,last_line:this.last_line,first_column:this.yylloc.first_column,last_column:this.yylloc.last_column},yytext:this.yytext,match:this.match,matches:this.matches,matched:this.matched,yyleng:this.yyleng,offset:this.offset,_more:this._more,_input:this._input,yy:this.yy,conditionStack:this.conditionStack.slice(0),done:this.done},this.options.ranges&&(E.yylloc.range=this.yylloc.range.slice(0))),y=n[0].match(/(?:\r\n?|\n).*/g),y&&(this.yylineno+=y.length),this.yylloc={first_line:this.yylloc.last_line,last_line:this.yylineno+1,first_column:this.yylloc.last_column,last_column:y?y[y.length-1].length-y[y.length-1].match(/\r?\n?/)[0].length:this.yylloc.last_column+n[0].length},this.yytext+=n[0],this.match+=n[0],this.matches=n,this.yyleng=this.yytext.length,this.options.ranges&&(this.yylloc.range=[this.offset,this.offset+=this.yyleng]),this._more=!1,this._backtrack=!1,this._input=this._input.slice(n[0].length),this.matched+=n[0],r=this.performAction.call(this,this.yy,this,h,this.conditionStack[this.conditionStack.length-1]),this.done&&this._input&&(this.done=!1),r)return r;if(this._backtrack){for(var i in E)this[i]=E[i];return!1}return!1},"test_match"),next:d(function(){if(this.done)return this.EOF;this._input||(this.done=!0);var n,h,r,y;this._more||(this.yytext="",this.match="");for(var E=this._currentRules(),i=0;i<E.length;i++)if(r=this._input.match(this.rules[E[i]]),r&&(!h||r[0].length>h[0].length)){if(h=r,y=i,this.options.backtrack_lexer){if(n=this.test_match(r,E[i]),n!==!1)return n;if(this._backtrack){h=!1;continue}else return!1}else if(!this.options.flex)break}return h?(n=this.test_match(h,E[y]),n!==!1?n:!1):this._input===""?this.EOF:this.parseError("Lexical error on line "+(this.yylineno+1)+`. Unrecognized text.
`+this.showPosition(),{text:"",token:null,line:this.yylineno})},"next"),lex:d(function(){var h=this.next();return h||this.lex()},"lex"),begin:d(function(h){this.conditionStack.push(h)},"begin"),popState:d(function(){var h=this.conditionStack.length-1;return h>0?this.conditionStack.pop():this.conditionStack[0]},"popState"),_currentRules:d(function(){return this.conditionStack.length&&this.conditionStack[this.conditionStack.length-1]?this.conditions[this.conditionStack[this.conditionStack.length-1]].rules:this.conditions.INITIAL.rules},"_currentRules"),topState:d(function(h){return h=this.conditionStack.length-1-Math.abs(h||0),h>=0?this.conditionStack[h]:"INITIAL"},"topState"),pushState:d(function(h){this.begin(h)},"pushState"),stateStackSize:d(function(){return this.conditionStack.length},"stateStackSize"),options:{"case-insensitive":!0},performAction:d(function(h,r,y,E){switch(y){case 0:return 38;case 1:return 40;case 2:return 39;case 3:return 44;case 4:return 51;case 5:return 52;case 6:return 53;case 7:return 54;case 8:break;case 9:break;case 10:return 5;case 11:break;case 12:break;case 13:break;case 14:break;case 15:return this.pushState("SCALE"),17;case 16:return 18;case 17:this.popState();break;case 18:return this.begin("acc_title"),33;case 19:return this.popState(),"acc_title_value";case 20:return this.begin("acc_descr"),35;case 21:return this.popState(),"acc_descr_value";case 22:this.begin("acc_descr_multiline");break;case 23:this.popState();break;case 24:return"acc_descr_multiline_value";case 25:return this.pushState("CLASSDEF"),41;case 26:return this.popState(),this.pushState("CLASSDEFID"),"DEFAULT_CLASSDEF_ID";case 27:return this.popState(),this.pushState("CLASSDEFID"),42;case 28:return this.popState(),43;case 29:return this.pushState("CLASS"),48;case 30:return this.popState(),this.pushState("CLASS_STYLE"),49;case 31:return this.popState(),50;case 32:return this.pushState("STYLE"),45;case 33:return this.popState(),this.pushState("STYLEDEF_STYLES"),46;case 34:return this.popState(),47;case 35:return this.pushState("SCALE"),17;case 36:return 18;case 37:this.popState();break;case 38:this.pushState("STATE");break;case 39:return this.popState(),r.yytext=r.yytext.slice(0,-8).trim(),25;case 40:return this.popState(),r.yytext=r.yytext.slice(0,-8).trim(),26;case 41:return this.popState(),r.yytext=r.yytext.slice(0,-10).trim(),27;case 42:return this.popState(),r.yytext=r.yytext.slice(0,-8).trim(),25;case 43:return this.popState(),r.yytext=r.yytext.slice(0,-8).trim(),26;case 44:return this.popState(),r.yytext=r.yytext.slice(0,-10).trim(),27;case 45:return 51;case 46:return 52;case 47:return 53;case 48:return 54;case 49:this.pushState("STATE_STRING");break;case 50:return this.pushState("STATE_ID"),"AS";case 51:return this.popState(),"ID";case 52:this.popState();break;case 53:return"STATE_DESCR";case 54:return 19;case 55:this.popState();break;case 56:return this.popState(),this.pushState("struct"),20;case 57:break;case 58:return this.popState(),21;case 59:break;case 60:return this.begin("NOTE"),29;case 61:return this.popState(),this.pushState("NOTE_ID"),59;case 62:return this.popState(),this.pushState("NOTE_ID"),60;case 63:this.popState(),this.pushState("FLOATING_NOTE");break;case 64:return this.popState(),this.pushState("FLOATING_NOTE_ID"),"AS";case 65:break;case 66:return"NOTE_TEXT";case 67:return this.popState(),"ID";case 68:return this.popState(),this.pushState("NOTE_TEXT"),24;case 69:return this.popState(),r.yytext=r.yytext.substr(2).trim(),31;case 70:return this.popState(),r.yytext=r.yytext.slice(0,-8).trim(),31;case 71:return 6;case 72:return 6;case 73:return 16;case 74:return 57;case 75:return 24;case 76:return r.yytext=r.yytext.trim(),14;case 77:return 15;case 78:return 28;case 79:return 58;case 80:return 5;case 81:return"INVALID"}},"anonymous"),rules:[/^(?:click\b)/i,/^(?:href\b)/i,/^(?:"[^"]*")/i,/^(?:default\b)/i,/^(?:.*direction\s+TB[^\n]*)/i,/^(?:.*direction\s+BT[^\n]*)/i,/^(?:.*direction\s+RL[^\n]*)/i,/^(?:.*direction\s+LR[^\n]*)/i,/^(?:%%(?!\{)[^\n]*)/i,/^(?:[^\}]%%[^\n]*)/i,/^(?:[\n]+)/i,/^(?:[\s]+)/i,/^(?:((?!\n)\s)+)/i,/^(?:#[^\n]*)/i,/^(?:%[^\n]*)/i,/^(?:scale\s+)/i,/^(?:\d+)/i,/^(?:\s+width\b)/i,/^(?:accTitle\s*:\s*)/i,/^(?:(?!\n||)*[^\n]*)/i,/^(?:accDescr\s*:\s*)/i,/^(?:(?!\n||)*[^\n]*)/i,/^(?:accDescr\s*\{\s*)/i,/^(?:[\}])/i,/^(?:[^\}]*)/i,/^(?:classDef\s+)/i,/^(?:DEFAULT\s+)/i,/^(?:\w+\s+)/i,/^(?:[^\n]*)/i,/^(?:class\s+)/i,/^(?:(\w+)+((,\s*\w+)*))/i,/^(?:[^\n]*)/i,/^(?:style\s+)/i,/^(?:[\w,]+\s+)/i,/^(?:[^\n]*)/i,/^(?:scale\s+)/i,/^(?:\d+)/i,/^(?:\s+width\b)/i,/^(?:state\s+)/i,/^(?:.*<<fork>>)/i,/^(?:.*<<join>>)/i,/^(?:.*<<choice>>)/i,/^(?:.*\[\[fork\]\])/i,/^(?:.*\[\[join\]\])/i,/^(?:.*\[\[choice\]\])/i,/^(?:.*direction\s+TB[^\n]*)/i,/^(?:.*direction\s+BT[^\n]*)/i,/^(?:.*direction\s+RL[^\n]*)/i,/^(?:.*direction\s+LR[^\n]*)/i,/^(?:["])/i,/^(?:\s*as\s+)/i,/^(?:[^\n\{]*)/i,/^(?:["])/i,/^(?:[^"]*)/i,/^(?:[^\n\s\{]+)/i,/^(?:\n)/i,/^(?:\{)/i,/^(?:%%(?!\{)[^\n]*)/i,/^(?:\})/i,/^(?:[\n])/i,/^(?:note\s+)/i,/^(?:left of\b)/i,/^(?:right of\b)/i,/^(?:")/i,/^(?:\s*as\s*)/i,/^(?:["])/i,/^(?:[^"]*)/i,/^(?:[^\n]*)/i,/^(?:\s*[^:\n\s\-]+)/i,/^(?:\s*:[^:\n;]+)/i,/^(?:[\s\S]*?end note\b)/i,/^(?:stateDiagram\s+)/i,/^(?:stateDiagram-v2\s+)/i,/^(?:hide empty description\b)/i,/^(?:\[\*\])/i,/^(?:[^:\n\s\-\{]+)/i,/^(?:\s*:[^:\n;]+)/i,/^(?:-->)/i,/^(?:--)/i,/^(?::::)/i,/^(?:$)/i,/^(?:.)/i],conditions:{LINE:{rules:[12,13],inclusive:!1},struct:{rules:[12,13,25,29,32,38,45,46,47,48,57,58,59,60,74,75,76,77,78],inclusive:!1},FLOATING_NOTE_ID:{rules:[67],inclusive:!1},FLOATING_NOTE:{rules:[64,65,66],inclusive:!1},NOTE_TEXT:{rules:[69,70],inclusive:!1},NOTE_ID:{rules:[68],inclusive:!1},NOTE:{rules:[61,62,63],inclusive:!1},STYLEDEF_STYLEOPTS:{rules:[],inclusive:!1},STYLEDEF_STYLES:{rules:[34],inclusive:!1},STYLE_IDS:{rules:[],inclusive:!1},STYLE:{rules:[33],inclusive:!1},CLASS_STYLE:{rules:[31],inclusive:!1},CLASS:{rules:[30],inclusive:!1},CLASSDEFID:{rules:[28],inclusive:!1},CLASSDEF:{rules:[26,27],inclusive:!1},acc_descr_multiline:{rules:[23,24],inclusive:!1},acc_descr:{rules:[21],inclusive:!1},acc_title:{rules:[19],inclusive:!1},SCALE:{rules:[16,17,36,37],inclusive:!1},ALIAS:{rules:[],inclusive:!1},STATE_ID:{rules:[51],inclusive:!1},STATE_STRING:{rules:[52,53],inclusive:!1},FORK_STATE:{rules:[],inclusive:!1},STATE:{rules:[12,13,39,40,41,42,43,44,49,50,54,55,56],inclusive:!1},ID:{rules:[12,13],inclusive:!1},INITIAL:{rules:[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,18,20,22,25,29,32,35,38,56,60,71,72,73,74,75,76,77,79,80,81],inclusive:!0}}};return V})();gt.lexer=qt;function ct(){this.yy={}}return d(ct,"Parser"),ct.prototype=gt,gt.Parser=ct,new ct})();kt.parser=kt;var Fe=kt,de="TB",Gt="TB",Ot="dir",q="state",J="root",Ct="relation",fe="classDef",pe="style",Se="applyClass",st="default",Bt="divider",Ft="fill:none",Vt="fill: #333",Mt="c",Ut="text",jt="normal",bt="rect",Dt="rectWithTitle",ye="stateStart",ge="stateEnd",Rt="divider",Nt="roundedWithTitle",_e="note",Te="noteGroup",it="statediagram",Ee="state",me=`${it}-${Ee}`,Ht="transition",ve="note",be="note-edge",De=`${Ht} ${be}`,ke=`${it}-${ve}`,Ce="cluster",Ae=`${it}-${Ce}`,xe="cluster-alt",Le=`${it}-${xe}`,Wt="parent",zt="note",Ie="state",At="----",Oe=`${At}${zt}`,wt=`${At}${Wt}`,Kt=d((t,e=Gt)=>{if(!t.doc)return e;let a=e;for(const s of t.doc)s.stmt==="dir"&&(a=s.value);return a},"getDir"),Re=d(function(t,e){return e.db.getClasses()},"getClasses"),Ne=d(async function(t,e,a,s){m.info("REF0:"),m.info("Drawing state diagram (v2)",e);const{securityLevel:c,state:l,layout:S}=P();s.db.extract(s.db.getRootDocV2());const _=s.db.getData(),f=te(e,c);_.type=s.type,_.layoutAlgorithm=S,_.nodeSpacing=l?.nodeSpacing||50,_.rankSpacing=l?.rankSpacing||50,_.markers=["barb"],_.diagramId=e,await se(_,f);const T=8;try{(typeof s.db.getLinks=="function"?s.db.getLinks():new Map).forEach((g,k)=>{var Y;const L=typeof k=="string"?k:typeof k?.id=="string"?k.id:"";if(!L){m.warn("⚠️ Invalid or missing stateId from key:",JSON.stringify(k));return}const N=(Y=f.node())==null?void 0:Y.querySelectorAll("g");let C;if(N?.forEach(O=>{var $;(($=O.textContent)==null?void 0:$.trim())===L&&(C=O)}),!C){m.warn("⚠️ Could not find node matching text:",L);return}const u=C.parentNode;if(!u){m.warn("⚠️ Node has no parent, cannot wrap:",L);return}const D=document.createElementNS("http://www.w3.org/2000/svg","a"),w=g.url.replace(/^"+|"+$/g,"");if(D.setAttributeNS("http://www.w3.org/1999/xlink","xlink:href",w),D.setAttribute("target","_blank"),g.tooltip){const O=g.tooltip.replace(/^"+|"+$/g,"");D.setAttribute("title",O)}u.replaceChild(D,C),D.appendChild(C),m.info("🔗 Wrapped node in <a> tag for:",L,g.url)})}catch(v){m.error("❌ Error injecting clickable links:",v)}ie.insertTitle(f,"statediagramTitleText",l?.titleTopMargin??25,s.db.getDiagramTitle()),ee(f,T,it,l?.useMaxWidth??!0)},"draw"),Ve={getClasses:Re,draw:Ne,getDir:Kt},St=new Map,M=0;function yt(t="",e=0,a="",s=At){const c=a!==null&&a.length>0?`${s}${a}`:"";return`${Ie}-${t}${c}-${e}`}d(yt,"stateDomId");var we=d((t,e,a,s,c,l,S,_)=>{m.trace("items",e),e.forEach(f=>{switch(f.stmt){case q:et(t,f,a,s,c,l,S,_);break;case st:et(t,f,a,s,c,l,S,_);break;case Ct:{et(t,f.state1,a,s,c,l,S,_),et(t,f.state2,a,s,c,l,S,_);const T={id:"edge"+M,start:f.state1.id,end:f.state2.id,arrowhead:"normal",arrowTypeEnd:"arrow_barb",style:Ft,labelStyle:"",label:W.sanitizeText(f.description??"",P()),arrowheadStyle:Vt,labelpos:Mt,labelType:Ut,thickness:jt,classes:Ht,look:S};c.push(T),M++}break}})},"setupDoc"),$t=d((t,e=Gt)=>{let a=e;if(t.doc)for(const s of t.doc)s.stmt==="dir"&&(a=s.value);return a},"getDir");function tt(t,e,a){if(!e.id||e.id==="</join></fork>"||e.id==="</choice>")return;e.cssClasses&&(Array.isArray(e.cssCompiledStyles)||(e.cssCompiledStyles=[]),e.cssClasses.split(" ").forEach(c=>{const l=a.get(c);l&&(e.cssCompiledStyles=[...e.cssCompiledStyles??[],...l.styles])}));const s=t.find(c=>c.id===e.id);s?Object.assign(s,e):t.push(e)}d(tt,"insertOrUpdateNode");function Xt(t){var e;return((e=t?.classes)==null?void 0:e.join(" "))??""}d(Xt,"getClassesFromDbInfo");function Jt(t){return t?.styles??[]}d(Jt,"getStylesFromDbInfo");var et=d((t,e,a,s,c,l,S,_)=>{var f,T,v;const g=e.id,k=a.get(g),Y=Xt(k),L=Jt(k),N=P();if(m.info("dataFetcher parsedItem",e,k,L),g!=="root"){let C=bt;e.start===!0?C=ye:e.start===!1&&(C=ge),e.type!==st&&(C=e.type),St.get(g)||St.set(g,{id:g,shape:C,description:W.sanitizeText(g,N),cssClasses:`${Y} ${me}`,cssStyles:L});const u=St.get(g);e.description&&(Array.isArray(u.description)?(u.shape=Dt,u.description.push(e.description)):(f=u.description)!=null&&f.length&&u.description.length>0?(u.shape=Dt,u.description===g?u.description=[e.description]:u.description=[u.description,e.description]):(u.shape=bt,u.description=e.description),u.description=W.sanitizeTextOrArray(u.description,N)),((T=u.description)==null?void 0:T.length)===1&&u.shape===Dt&&(u.type==="group"?u.shape=Nt:u.shape=bt),!u.type&&e.doc&&(m.info("Setting cluster for XCX",g,$t(e)),u.type="group",u.isGroup=!0,u.dir=$t(e),u.shape=e.type===Bt?Rt:Nt,u.cssClasses=`${u.cssClasses} ${Ae} ${l?Le:""}`);const D={labelStyle:"",shape:u.shape,label:u.description,cssClasses:u.cssClasses,cssCompiledStyles:[],cssStyles:u.cssStyles,id:g,dir:u.dir,domId:yt(g,M),type:u.type,isGroup:u.type==="group",padding:8,rx:10,ry:10,look:S};if(D.shape===Rt&&(D.label=""),t&&t.id!=="root"&&(m.trace("Setting node ",g," to be child of its parent ",t.id),D.parentId=t.id),D.centerLabel=!0,e.note){const w={labelStyle:"",shape:_e,label:e.note.text,cssClasses:ke,cssStyles:[],cssCompiledStyles:[],id:g+Oe+"-"+M,domId:yt(g,M,zt),type:u.type,isGroup:u.type==="group",padding:(v=N.flowchart)==null?void 0:v.padding,look:S,position:e.note.position},O=g+wt,$={labelStyle:"",shape:Te,label:e.note.text,cssClasses:u.cssClasses,cssStyles:[],id:g+wt,domId:yt(g,M,Wt),type:"group",isGroup:!0,padding:16,look:S,position:e.note.position};M++,$.id=O,w.parentId=O,tt(s,$,_),tt(s,w,_),tt(s,D,_);let G=g,F=w.id;e.note.position==="left of"&&(G=w.id,F=g),c.push({id:G+"-"+F,start:G,end:F,arrowhead:"none",arrowTypeEnd:"",style:Ft,labelStyle:"",classes:De,arrowheadStyle:Vt,labelpos:Mt,labelType:Ut,thickness:jt,look:S})}else tt(s,D,_)}e.doc&&(m.trace("Adding nodes children "),we(e,e.doc,a,s,c,!l,S,_))},"dataFetcher"),$e=d(()=>{St.clear(),M=0},"reset"),A={START_NODE:"[*]",START_TYPE:"start",END_NODE:"[*]",END_TYPE:"end",COLOR_KEYWORD:"color",FILL_KEYWORD:"fill",BG_FILL:"bgFill",STYLECLASS_SEP:","},Pt=d(()=>new Map,"newClassesList"),Yt=d(()=>({relations:[],states:new Map,documents:{}}),"newDoc"),pt=d(t=>JSON.parse(JSON.stringify(t)),"clone"),Me=(ft=class{constructor(t){this.version=t,this.nodes=[],this.edges=[],this.rootDoc=[],this.classes=Pt(),this.documents={root:Yt()},this.currentDocument=this.documents.root,this.startEndCount=0,this.dividerCnt=0,this.links=new Map,this.getAccTitle=re,this.setAccTitle=ae,this.getAccDescription=ne,this.setAccDescription=le,this.setDiagramTitle=oe,this.getDiagramTitle=ce,this.clear(),this.setRootDoc=this.setRootDoc.bind(this),this.getDividerId=this.getDividerId.bind(this),this.setDirection=this.setDirection.bind(this),this.trimColon=this.trimColon.bind(this)}extract(t){this.clear(!0);for(const s of Array.isArray(t)?t:t.doc)switch(s.stmt){case q:this.addState(s.id.trim(),s.type,s.doc,s.description,s.note);break;case Ct:this.addRelation(s.state1,s.state2,s.description);break;case fe:this.addStyleClass(s.id.trim(),s.classes);break;case pe:this.handleStyleDef(s);break;case Se:this.setCssClass(s.id.trim(),s.styleClass);break;case"click":this.addLink(s.id,s.url,s.tooltip);break}const e=this.getStates(),a=P();$e(),et(void 0,this.getRootDocV2(),e,this.nodes,this.edges,!0,a.look,this.classes);for(const s of this.nodes)if(Array.isArray(s.label)){if(s.description=s.label.slice(1),s.isGroup&&s.description.length>0)throw new Error(`Group nodes can only have label. Remove the additional description for node [${s.id}]`);s.label=s.label[0]}}handleStyleDef(t){const e=t.id.trim().split(","),a=t.styleClass.split(",");for(const s of e){let c=this.getState(s);if(!c){const l=s.trim();this.addState(l),c=this.getState(l)}c&&(c.styles=a.map(l=>{var S;return(S=l.replace(/;/g,""))==null?void 0:S.trim()}))}}setRootDoc(t){m.info("Setting root doc",t),this.rootDoc=t,this.version===1?this.extract(t):this.extract(this.getRootDocV2())}docTranslator(t,e,a){if(e.stmt===Ct){this.docTranslator(t,e.state1,!0),this.docTranslator(t,e.state2,!1);return}if(e.stmt===q&&(e.id===A.START_NODE?(e.id=t.id+(a?"_start":"_end"),e.start=a):e.id=e.id.trim()),e.stmt!==J&&e.stmt!==q||!e.doc)return;const s=[];let c=[];for(const l of e.doc)if(l.type===Bt){const S=pt(l);S.doc=pt(c),s.push(S),c=[]}else c.push(l);if(s.length>0&&c.length>0){const l={stmt:q,id:he(),type:"divider",doc:pt(c)};s.push(pt(l)),e.doc=s}e.doc.forEach(l=>this.docTranslator(e,l,!0))}getRootDocV2(){return this.docTranslator({id:J,stmt:J},{id:J,stmt:J,doc:this.rootDoc},!0),{id:J,doc:this.rootDoc}}addState(t,e=st,a=void 0,s=void 0,c=void 0,l=void 0,S=void 0,_=void 0){const f=t?.trim();if(!this.currentDocument.states.has(f))m.info("Adding state ",f,s),this.currentDocument.states.set(f,{stmt:q,id:f,descriptions:[],type:e,doc:a,note:c,classes:[],styles:[],textStyles:[]});else{const T=this.currentDocument.states.get(f);if(!T)throw new Error(`State not found: ${f}`);T.doc||(T.doc=a),T.type||(T.type=e)}if(s&&(m.info("Setting state description",f,s),(Array.isArray(s)?s:[s]).forEach(v=>this.addDescription(f,v.trim()))),c){const T=this.currentDocument.states.get(f);if(!T)throw new Error(`State not found: ${f}`);T.note=c,T.note.text=W.sanitizeText(T.note.text,P())}l&&(m.info("Setting state classes",f,l),(Array.isArray(l)?l:[l]).forEach(v=>this.setCssClass(f,v.trim()))),S&&(m.info("Setting state styles",f,S),(Array.isArray(S)?S:[S]).forEach(v=>this.setStyle(f,v.trim()))),_&&(m.info("Setting state styles",f,S),(Array.isArray(_)?_:[_]).forEach(v=>this.setTextStyle(f,v.trim())))}clear(t){this.nodes=[],this.edges=[],this.documents={root:Yt()},this.currentDocument=this.documents.root,this.startEndCount=0,this.classes=Pt(),t||(this.links=new Map,ue())}getState(t){return this.currentDocument.states.get(t)}getStates(){return this.currentDocument.states}logDocuments(){m.info("Documents = ",this.documents)}getRelations(){return this.currentDocument.relations}addLink(t,e,a){this.links.set(t,{url:e,tooltip:a}),m.warn("Adding link",t,e,a)}getLinks(){return this.links}startIdIfNeeded(t=""){return t===A.START_NODE?(this.startEndCount++,`${A.START_TYPE}${this.startEndCount}`):t}startTypeIfNeeded(t="",e=st){return t===A.START_NODE?A.START_TYPE:e}endIdIfNeeded(t=""){return t===A.END_NODE?(this.startEndCount++,`${A.END_TYPE}${this.startEndCount}`):t}endTypeIfNeeded(t="",e=st){return t===A.END_NODE?A.END_TYPE:e}addRelationObjs(t,e,a=""){const s=this.startIdIfNeeded(t.id.trim()),c=this.startTypeIfNeeded(t.id.trim(),t.type),l=this.startIdIfNeeded(e.id.trim()),S=this.startTypeIfNeeded(e.id.trim(),e.type);this.addState(s,c,t.doc,t.description,t.note,t.classes,t.styles,t.textStyles),this.addState(l,S,e.doc,e.description,e.note,e.classes,e.styles,e.textStyles),this.currentDocument.relations.push({id1:s,id2:l,relationTitle:W.sanitizeText(a,P())})}addRelation(t,e,a){if(typeof t=="object"&&typeof e=="object")this.addRelationObjs(t,e,a);else if(typeof t=="string"&&typeof e=="string"){const s=this.startIdIfNeeded(t.trim()),c=this.startTypeIfNeeded(t),l=this.endIdIfNeeded(e.trim()),S=this.endTypeIfNeeded(e);this.addState(s,c),this.addState(l,S),this.currentDocument.relations.push({id1:s,id2:l,relationTitle:a?W.sanitizeText(a,P()):void 0})}}addDescription(t,e){var a;const s=this.currentDocument.states.get(t),c=e.startsWith(":")?e.replace(":","").trim():e;(a=s?.descriptions)==null||a.push(W.sanitizeText(c,P()))}cleanupLabel(t){return t.startsWith(":")?t.slice(2).trim():t.trim()}getDividerId(){return this.dividerCnt++,`divider-id-${this.dividerCnt}`}addStyleClass(t,e=""){this.classes.has(t)||this.classes.set(t,{id:t,styles:[],textStyles:[]});const a=this.classes.get(t);e&&a&&e.split(A.STYLECLASS_SEP).forEach(s=>{const c=s.replace(/([^;]*);/,"$1").trim();if(RegExp(A.COLOR_KEYWORD).exec(s)){const S=c.replace(A.FILL_KEYWORD,A.BG_FILL).replace(A.COLOR_KEYWORD,A.FILL_KEYWORD);a.textStyles.push(S)}a.styles.push(c)})}getClasses(){return this.classes}setCssClass(t,e){t.split(",").forEach(a=>{var s;let c=this.getState(a);if(!c){const l=a.trim();this.addState(l),c=this.getState(l)}(s=c?.classes)==null||s.push(e)})}setStyle(t,e){var a,s;(s=(a=this.getState(t))==null?void 0:a.styles)==null||s.push(e)}setTextStyle(t,e){var a,s;(s=(a=this.getState(t))==null?void 0:a.textStyles)==null||s.push(e)}getDirectionStatement(){return this.rootDoc.find(t=>t.stmt===Ot)}getDirection(){var t;return((t=this.getDirectionStatement())==null?void 0:t.value)??de}setDirection(t){const e=this.getDirectionStatement();e?e.value=t:this.rootDoc.unshift({stmt:Ot,value:t})}trimColon(t){return t.startsWith(":")?t.slice(1).trim():t.trim()}getData(){const t=P();return{nodes:this.nodes,edges:this.edges,other:{},config:t,direction:Kt(this.getRootDocV2())}}getConfig(){return P().state}},d(ft,"StateDB"),ft.relationType={AGGREGATION:0,EXTENSION:1,COMPOSITION:2,DEPENDENCY:3},ft),Pe=d(t=>`
defs #statediagram-barbEnd {
    fill: ${t.transitionColor};
    stroke: ${t.transitionColor};
  }
g.stateGroup text {
  fill: ${t.nodeBorder};
  stroke: none;
  font-size: 10px;
}
g.stateGroup text {
  fill: ${t.textColor};
  stroke: none;
  font-size: 10px;

}
g.stateGroup .state-title {
  font-weight: bolder;
  fill: ${t.stateLabelColor};
}

g.stateGroup rect {
  fill: ${t.mainBkg};
  stroke: ${t.nodeBorder};
}

g.stateGroup line {
  stroke: ${t.lineColor};
  stroke-width: 1;
}

.transition {
  stroke: ${t.transitionColor};
  stroke-width: 1;
  fill: none;
}

.stateGroup .composit {
  fill: ${t.background};
  border-bottom: 1px
}

.stateGroup .alt-composit {
  fill: #e0e0e0;
  border-bottom: 1px
}

.state-note {
  stroke: ${t.noteBorderColor};
  fill: ${t.noteBkgColor};

  text {
    fill: ${t.noteTextColor};
    stroke: none;
    font-size: 10px;
  }
}

.stateLabel .box {
  stroke: none;
  stroke-width: 0;
  fill: ${t.mainBkg};
  opacity: 0.5;
}

.edgeLabel .label rect {
  fill: ${t.labelBackgroundColor};
  opacity: 0.5;
}
.edgeLabel {
  background-color: ${t.edgeLabelBackground};
  p {
    background-color: ${t.edgeLabelBackground};
  }
  rect {
    opacity: 0.5;
    background-color: ${t.edgeLabelBackground};
    fill: ${t.edgeLabelBackground};
  }
  text-align: center;
}
.edgeLabel .label text {
  fill: ${t.transitionLabelColor||t.tertiaryTextColor};
}
.label div .edgeLabel {
  color: ${t.transitionLabelColor||t.tertiaryTextColor};
}

.stateLabel text {
  fill: ${t.stateLabelColor};
  font-size: 10px;
  font-weight: bold;
}

.node circle.state-start {
  fill: ${t.specialStateColor};
  stroke: ${t.specialStateColor};
}

.node .fork-join {
  fill: ${t.specialStateColor};
  stroke: ${t.specialStateColor};
}

.node circle.state-end {
  fill: ${t.innerEndBackground};
  stroke: ${t.background};
  stroke-width: 1.5
}
.end-state-inner {
  fill: ${t.compositeBackground||t.background};
  // stroke: ${t.background};
  stroke-width: 1.5
}

.node rect {
  fill: ${t.stateBkg||t.mainBkg};
  stroke: ${t.stateBorder||t.nodeBorder};
  stroke-width: 1px;
}
.node polygon {
  fill: ${t.mainBkg};
  stroke: ${t.stateBorder||t.nodeBorder};;
  stroke-width: 1px;
}
#statediagram-barbEnd {
  fill: ${t.lineColor};
}

.statediagram-cluster rect {
  fill: ${t.compositeTitleBackground};
  stroke: ${t.stateBorder||t.nodeBorder};
  stroke-width: 1px;
}

.cluster-label, .nodeLabel {
  color: ${t.stateLabelColor};
  // line-height: 1;
}

.statediagram-cluster rect.outer {
  rx: 5px;
  ry: 5px;
}
.statediagram-state .divider {
  stroke: ${t.stateBorder||t.nodeBorder};
}

.statediagram-state .title-state {
  rx: 5px;
  ry: 5px;
}
.statediagram-cluster.statediagram-cluster .inner {
  fill: ${t.compositeBackground||t.background};
}
.statediagram-cluster.statediagram-cluster-alt .inner {
  fill: ${t.altBackground?t.altBackground:"#efefef"};
}

.statediagram-cluster .inner {
  rx:0;
  ry:0;
}

.statediagram-state rect.basic {
  rx: 5px;
  ry: 5px;
}
.statediagram-state rect.divider {
  stroke-dasharray: 10,10;
  fill: ${t.altBackground?t.altBackground:"#efefef"};
}

.note-edge {
  stroke-dasharray: 5;
}

.statediagram-note rect {
  fill: ${t.noteBkgColor};
  stroke: ${t.noteBorderColor};
  stroke-width: 1px;
  rx: 0;
  ry: 0;
}
.statediagram-note rect {
  fill: ${t.noteBkgColor};
  stroke: ${t.noteBorderColor};
  stroke-width: 1px;
  rx: 0;
  ry: 0;
}

.statediagram-note text {
  fill: ${t.noteTextColor};
}

.statediagram-note .nodeLabel {
  color: ${t.noteTextColor};
}
.statediagram .edgeLabel {
  color: red; // ${t.noteTextColor};
}

#dependencyStart, #dependencyEnd {
  fill: ${t.lineColor};
  stroke: ${t.lineColor};
  stroke-width: 1;
}

.statediagramTitleText {
  text-anchor: middle;
  font-size: 18px;
  fill: ${t.textColor};
}
`,"getStyles"),Ue=Pe;export{Me as S,Fe as a,Ve as b,Ue as s};
