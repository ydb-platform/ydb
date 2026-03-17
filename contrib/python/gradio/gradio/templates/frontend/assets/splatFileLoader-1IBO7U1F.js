import{aq as be,bb as pt,h as Pt,bY as Nt,S as N,a9 as Wt,x as Vt,y as Zt,z as jt,K as Xt,X as Gt,Y as $t,b1 as _t,a2 as qt,a4 as Kt,a6 as Jt,e as mt,ac as Qt,J as q,R as Lt,bZ as Yt,b_ as es,m as Ie,V as T,l as Ne,M as Ce,b$ as ts,c as J,L as de,c0 as He,c1 as ke,c2 as ss,aO as $,c3 as rs,ah as oe,b9 as L,ao as os,C as Se,ai as he,bA as re,B as ns,c4 as Re,d as is,ax as as}from"./index-BZt0m9TU.js";import{_ as ye}from"./index-xGeN4i2A.js";import{ShaderMaterial as xt}from"./shaderMaterial-XltpvdFi.js";import"./clipPlaneFragment-Bia1Py68.js";import"./logDepthDeclaration-DBCLquxJ.js";import"./fogFragment-CEm9xpff.js";import"./sceneUboDeclaration-DZIv4upk.js";import"./meshUboDeclaration-BD5u1Aes.js";import"./clipPlaneVertex-CHAAXtfr.js";import"./logDepthVertex-C6DbuQZI.js";import"./helperFunctions-B2gYs5dd.js";import"./clipPlaneFragment-CbUd-PEp.js";import"./logDepthDeclaration-o4HPWZv_.js";import"./fogFragment-qir_mjFT.js";import"./sceneUboDeclaration-DDBZ5qvn.js";import"./meshUboDeclaration-_4M1at8l.js";import"./helperFunctions-DnKtpQT_.js";import"./clipPlaneVertex-DsjlXMYl.js";import"./logDepthVertex-CABTy0g-.js";import{R as we}from"./rawTexture-BbVql4Zp.js";import"./thinInstanceMesh-Bh9KP5eX.js";import{A as cs}from"./assetContainer-DSQHYjvp.js";import{Ray as ls}from"./ray-DKs2EnxQ.js";import{S as hs}from"./standardMaterial-BrI4Rke5.js";import"./svelte/svelte_svelte.js";import"./svelte/svelte_animate.js";import"./svelte/svelte_attachments.js";import"./svelte/svelte_easing.js";import"./svelte/svelte_internal_client.js";import"./svelte/svelte_internal_flags_async.js";import"./svelte/svelte_internal_flags_legacy.js";import"./svelte/svelte_internal_flags_tracing.js";import"./svelte/svelte_internal_server.js";import"./svelte/svelte_legacy.js";import"./svelte/svelte_motion.js";import"./svelte/svelte_reactivity.js";import"./svelte/svelte_reactivity_window.js";import"./svelte/svelte_server.js";import"./svelte/svelte_store.js";import"./svelte/svelte_transition.js";import"./svelte/svelte_events.js";class fs{constructor(){this.mm=new Map}get(e,t){const s=this.mm.get(e);if(s!==void 0)return s.get(t)}set(e,t,s){let n=this.mm.get(e);n===void 0&&this.mm.set(e,n=new Map),n.set(t,s)}}class us{get standalone(){return this._options?.standalone??!1}get baseMaterial(){return this._baseMaterial}get doNotInjectCode(){return this._options?.doNotInjectCode??!1}constructor(e,t,s){this._baseMaterial=e,this._scene=t??be.LastCreatedScene,this._options=s,this._subMeshToEffect=new Map,this._subMeshToDepthWrapper=new fs,this._meshes=new Map,this._onEffectCreatedObserver=this._baseMaterial.onEffectCreatedObservable.add(n=>{const o=n.subMesh?.getMesh();o&&!this._meshes.has(o)&&this._meshes.set(o,o.onDisposeObservable.add(i=>{const l=this._subMeshToEffect.keys();for(let h=l.next();h.done!==!0;h=l.next()){const a=h.value;a?.getMesh()===i&&(this._subMeshToEffect.delete(a),this._deleteDepthWrapperEffect(a))}})),this._subMeshToEffect.get(n.subMesh)?.[0]!==n.effect&&(this._subMeshToEffect.set(n.subMesh,[n.effect,this._scene.getEngine().currentRenderPassId]),this._deleteDepthWrapperEffect(n.subMesh))})}_deleteDepthWrapperEffect(e){const t=this._subMeshToDepthWrapper.mm.get(e);t&&(t.forEach(s=>{s.mainDrawWrapper.effect?.dispose()}),this._subMeshToDepthWrapper.mm.delete(e))}getEffect(e,t,s){const n=this._subMeshToDepthWrapper.mm.get(e)?.get(t);if(!n)return null;let o=n.drawWrapper[s];return o||(o=n.drawWrapper[s]=new pt(this._scene.getEngine()),o.setEffect(n.mainDrawWrapper.effect,n.mainDrawWrapper.defines)),o}isReadyForSubMesh(e,t,s,n,o){return this.standalone&&!this._baseMaterial.isReadyForSubMesh(e.getMesh(),e,n)?!1:this._makeEffect(e,t,s,o)?.isReady()??!1}dispose(){this._baseMaterial.onEffectCreatedObservable.remove(this._onEffectCreatedObserver),this._onEffectCreatedObserver=null;const e=this._meshes.entries();for(let t=e.next();t.done!==!0;t=e.next()){const[s,n]=t.value;s.onDisposeObservable.remove(n)}}_makeEffect(e,t,s,n){const o=this._scene.getEngine(),i=this._subMeshToEffect.get(e);if(!i)return null;const[l,h]=i;if(!l.isReady())return null;let a=this._subMeshToDepthWrapper.get(e,s);if(!a){const m=new pt(o);m.defines=e._getDrawWrapper(h)?.defines??null,a={drawWrapper:[],mainDrawWrapper:m,depthDefines:"",token:Pt()},a.drawWrapper[n]=m,this._subMeshToDepthWrapper.set(e,s,a)}const _=t.join(`
`);if(a.mainDrawWrapper.effect&&_===a.depthDefines)return a.mainDrawWrapper.effect;a.depthDefines=_;const u=l.getUniformNames().slice();let f=l.vertexSourceCodeBeforeMigration,p=l.fragmentSourceCodeBeforeMigration;if(!f&&!p)return null;if(!this.doNotInjectCode){const m=this._options&&this._options.remappedVariables?`#include<shadowMapVertexNormalBias>(${this._options.remappedVariables.join(",")})`:"#include<shadowMapVertexNormalBias>",C=this._options&&this._options.remappedVariables?`#include<shadowMapVertexMetric>(${this._options.remappedVariables.join(",")})`:"#include<shadowMapVertexMetric>",E=this._options&&this._options.remappedVariables?`#include<shadowMapFragmentSoftTransparentShadow>(${this._options.remappedVariables.join(",")})`:"#include<shadowMapFragmentSoftTransparentShadow>",c="#include<shadowMapFragment>",g="#include<shadowMapVertexExtraDeclaration>";l.shaderLanguage===0?f=f.replace(/void\s+?main/g,`
${g}
void main`):f=f.replace(/@vertex/g,`
${g}
@vertex`),f=f.replace(/#define SHADOWDEPTH_NORMALBIAS|#define CUSTOM_VERTEX_UPDATE_WORLDPOS/g,m),f.indexOf("#define SHADOWDEPTH_METRIC")!==-1?f=f.replace(/#define SHADOWDEPTH_METRIC/g,C):f=f.replace(/}\s*$/g,C+`
}`),f=f.replace(/#define SHADER_NAME.*?\n|out vec4 glFragColor;\n/g,"");const S=p.indexOf("#define SHADOWDEPTH_SOFTTRANSPARENTSHADOW")>=0||p.indexOf("#define CUSTOM_FRAGMENT_BEFORE_FOG")>=0,w=p.indexOf("#define SHADOWDEPTH_FRAGMENT")!==-1;let x="";S?p=p.replace(/#define SHADOWDEPTH_SOFTTRANSPARENTSHADOW|#define CUSTOM_FRAGMENT_BEFORE_FOG/g,E):x=E+`
`,p=p.replace(/void\s+?main/g,Nt.IncludesShadersStore.shadowMapFragmentExtraDeclaration+`
void main`),w?p=p.replace(/#define SHADOWDEPTH_FRAGMENT/g,c):x+=c+`
`,x&&(p=p.replace(/}\s*$/g,x+"}")),u.push("biasAndScaleSM","depthValuesSM","lightDataSM","softTransparentShadowSM")}a.mainDrawWrapper.effect=o.createEffect({vertexSource:f,fragmentSource:p,vertexToken:a.token,fragmentToken:a.token},{attributes:l.getAttributesNames(),uniformsNames:u,uniformBuffersNames:l.getUniformBuffersNames(),samplers:l.getSamplers(),defines:_+`
`+l.defines.replace("#define SHADOWS","").replace(/#define SHADOW\d/g,""),indexParameters:l.getIndexParameters(),shaderLanguage:l.shaderLanguage},o);for(let m=0;m<a.drawWrapper.length;++m)m!==n&&a.drawWrapper[m]?.setEffect(a.mainDrawWrapper.effect,a.mainDrawWrapper.defines);return a.mainDrawWrapper.effect}}const vt="gaussianSplattingFragmentDeclaration",ds=`vec4 gaussianColor(vec4 inColor)
{float A=-dot(vPosition,vPosition);if (A<-4.0) discard;float B=exp(A)*inColor.a;
#include<logDepthFragment>
vec3 color=inColor.rgb;
#ifdef FOG
#include<fogFragment>
#endif
return vec4(color,B);}
`;N.IncludesShadersStore[vt]||(N.IncludesShadersStore[vt]=ds);const Fe="gaussianSplattingPixelShader",Ot=`#include<clipPlaneFragmentDeclaration>
#include<logDepthDeclaration>
#include<fogFragmentDeclaration>
varying vec4 vColor;varying vec2 vPosition;
#include<gaussianSplattingFragmentDeclaration>
void main () { 
#include<clipPlaneFragment>
gl_FragColor=gaussianColor(vColor);}
`;N.ShadersStore[Fe]||(N.ShadersStore[Fe]=Ot);const ps={name:Fe,shader:Ot},_s=Object.freeze(Object.defineProperty({__proto__:null,gaussianSplattingPixelShader:ps},Symbol.toStringTag,{value:"Module"})),gt="gaussianSplattingVertexDeclaration",ms="attribute vec3 position;attribute vec4 splatIndex0;attribute vec4 splatIndex1;attribute vec4 splatIndex2;attribute vec4 splatIndex3;uniform mat4 view;uniform mat4 projection;uniform mat4 world;uniform vec4 vEyePosition;";N.IncludesShadersStore[gt]||(N.IncludesShadersStore[gt]=ms);const St="gaussianSplattingUboDeclaration",xs=`#include<sceneUboDeclaration>
#include<meshUboDeclaration>
attribute vec3 position;attribute vec4 splatIndex0;attribute vec4 splatIndex1;attribute vec4 splatIndex2;attribute vec4 splatIndex3;
`;N.IncludesShadersStore[St]||(N.IncludesShadersStore[St]=xs);const yt="gaussianSplatting",vs=`#if !defined(WEBGL2) && !defined(WEBGPU) && !defined(NATIVE)
mat3 transpose(mat3 matrix) {return mat3(matrix[0][0],matrix[1][0],matrix[2][0],
matrix[0][1],matrix[1][1],matrix[2][1],
matrix[0][2],matrix[1][2],matrix[2][2]);}
#endif
vec2 getDataUV(float index,vec2 textureSize) {float y=floor(index/textureSize.x);float x=index-y*textureSize.x;return vec2((x+0.5)/textureSize.x,(y+0.5)/textureSize.y);}
#if SH_DEGREE>0
ivec2 getDataUVint(float index,vec2 textureSize) {float y=floor(index/textureSize.x);float x=index-y*textureSize.x;return ivec2(uint(x+0.5),uint(y+0.5));}
#endif
struct Splat {vec4 center;vec4 color;vec4 covA;vec4 covB;
#if SH_DEGREE>0
uvec4 sh0; 
#endif
#if SH_DEGREE>1
uvec4 sh1;
#endif
#if SH_DEGREE>2
uvec4 sh2;
#endif
};float getSplatIndex(int localIndex)
{float splatIndex;switch (localIndex)
{case 0: splatIndex=splatIndex0.x; break;case 1: splatIndex=splatIndex0.y; break;case 2: splatIndex=splatIndex0.z; break;case 3: splatIndex=splatIndex0.w; break;case 4: splatIndex=splatIndex1.x; break;case 5: splatIndex=splatIndex1.y; break;case 6: splatIndex=splatIndex1.z; break;case 7: splatIndex=splatIndex1.w; break;case 8: splatIndex=splatIndex2.x; break;case 9: splatIndex=splatIndex2.y; break;case 10: splatIndex=splatIndex2.z; break;case 11: splatIndex=splatIndex2.w; break;case 12: splatIndex=splatIndex3.x; break;case 13: splatIndex=splatIndex3.y; break;case 14: splatIndex=splatIndex3.z; break;case 15: splatIndex=splatIndex3.w; break;}
return splatIndex;}
Splat readSplat(float splatIndex)
{Splat splat;vec2 splatUV=getDataUV(splatIndex,dataTextureSize);splat.center=texture2D(centersTexture,splatUV);splat.color=texture2D(colorsTexture,splatUV);splat.covA=texture2D(covariancesATexture,splatUV)*splat.center.w;splat.covB=texture2D(covariancesBTexture,splatUV)*splat.center.w;
#if SH_DEGREE>0
ivec2 splatUVint=getDataUVint(splatIndex,dataTextureSize);splat.sh0=texelFetch(shTexture0,splatUVint,0);
#endif
#if SH_DEGREE>1
splat.sh1=texelFetch(shTexture1,splatUVint,0);
#endif
#if SH_DEGREE>2
splat.sh2=texelFetch(shTexture2,splatUVint,0);
#endif
return splat;}
#if defined(WEBGL2) || defined(WEBGPU) || defined(NATIVE)
vec3 computeColorFromSHDegree(vec3 dir,const vec3 sh[16])
{const float SH_C0=0.28209479;const float SH_C1=0.48860251;float SH_C2[5];SH_C2[0]=1.092548430;SH_C2[1]=-1.09254843;SH_C2[2]=0.315391565;SH_C2[3]=-1.09254843;SH_C2[4]=0.546274215;float SH_C3[7];SH_C3[0]=-0.59004358;SH_C3[1]=2.890611442;SH_C3[2]=-0.45704579;SH_C3[3]=0.373176332;SH_C3[4]=-0.45704579;SH_C3[5]=1.445305721;SH_C3[6]=-0.59004358;vec3 result=/*SH_C0**/sh[0];
#if SH_DEGREE>0
float x=dir.x;float y=dir.y;float z=dir.z;result+=- SH_C1*y*sh[1]+SH_C1*z*sh[2]-SH_C1*x*sh[3];
#if SH_DEGREE>1
float xx=x*x,yy=y*y,zz=z*z;float xy=x*y,yz=y*z,xz=x*z;result+=
SH_C2[0]*xy*sh[4] +
SH_C2[1]*yz*sh[5] +
SH_C2[2]*(2.0*zz-xx-yy)*sh[6] +
SH_C2[3]*xz*sh[7] +
SH_C2[4]*(xx-yy)*sh[8];
#if SH_DEGREE>2
result+=
SH_C3[0]*y*(3.0*xx-yy)*sh[9] +
SH_C3[1]*xy*z*sh[10] +
SH_C3[2]*y*(4.0*zz-xx-yy)*sh[11] +
SH_C3[3]*z*(2.0*zz-3.0*xx-3.0*yy)*sh[12] +
SH_C3[4]*x*(4.0*zz-xx-yy)*sh[13] +
SH_C3[5]*z*(xx-yy)*sh[14] +
SH_C3[6]*x*(xx-3.0*yy)*sh[15];
#endif
#endif
#endif
return result;}
vec4 decompose(uint value)
{vec4 components=vec4(
float((value ) & 255u),
float((value>>uint( 8)) & 255u),
float((value>>uint(16)) & 255u),
float((value>>uint(24)) & 255u));return components*vec4(2./255.)-vec4(1.);}
vec3 computeSH(Splat splat,vec3 dir)
{vec3 sh[16];sh[0]=vec3(0.,0.,0.);
#if SH_DEGREE>0
vec4 sh00=decompose(splat.sh0.x);vec4 sh01=decompose(splat.sh0.y);vec4 sh02=decompose(splat.sh0.z);sh[1]=vec3(sh00.x,sh00.y,sh00.z);sh[2]=vec3(sh00.w,sh01.x,sh01.y);sh[3]=vec3(sh01.z,sh01.w,sh02.x);
#endif
#if SH_DEGREE>1
vec4 sh03=decompose(splat.sh0.w);vec4 sh04=decompose(splat.sh1.x);vec4 sh05=decompose(splat.sh1.y);sh[4]=vec3(sh02.y,sh02.z,sh02.w);sh[5]=vec3(sh03.x,sh03.y,sh03.z);sh[6]=vec3(sh03.w,sh04.x,sh04.y);sh[7]=vec3(sh04.z,sh04.w,sh05.x);sh[8]=vec3(sh05.y,sh05.z,sh05.w);
#endif
#if SH_DEGREE>2
vec4 sh06=decompose(splat.sh1.z);vec4 sh07=decompose(splat.sh1.w);vec4 sh08=decompose(splat.sh2.x);vec4 sh09=decompose(splat.sh2.y);vec4 sh10=decompose(splat.sh2.z);vec4 sh11=decompose(splat.sh2.w);sh[9]=vec3(sh06.x,sh06.y,sh06.z);sh[10]=vec3(sh06.w,sh07.x,sh07.y);sh[11]=vec3(sh07.z,sh07.w,sh08.x);sh[12]=vec3(sh08.y,sh08.z,sh08.w);sh[13]=vec3(sh09.x,sh09.y,sh09.z);sh[14]=vec3(sh09.w,sh10.x,sh10.y);sh[15]=vec3(sh10.z,sh10.w,sh11.x); 
#endif
return computeColorFromSHDegree(dir,sh);}
#else
vec3 computeSH(Splat splat,vec3 dir)
{return vec3(0.,0.,0.);}
#endif
vec4 gaussianSplatting(vec2 meshPos,vec3 worldPos,vec2 scale,vec3 covA,vec3 covB,mat4 worldMatrix,mat4 viewMatrix,mat4 projectionMatrix)
{mat4 modelView=viewMatrix*worldMatrix;vec4 camspace=viewMatrix*vec4(worldPos,1.);vec4 pos2d=projectionMatrix*camspace;float bounds=1.2*pos2d.w;if (pos2d.z<-pos2d.w || pos2d.x<-bounds || pos2d.x>bounds
|| pos2d.y<-bounds || pos2d.y>bounds) {return vec4(0.0,0.0,2.0,1.0);}
mat3 Vrk=mat3(
covA.x,covA.y,covA.z,
covA.y,covB.x,covB.y,
covA.z,covB.y,covB.z
);bool isOrtho=abs(projectionMatrix[3][3]-1.0)<0.001;mat3 J;if (isOrtho) {J=mat3(
focal.x,0.,0.,
0.,focal.y,0.,
0.,0.,0.
);} else {J=mat3(
focal.x/camspace.z,0.,-(focal.x*camspace.x)/(camspace.z*camspace.z),
0.,focal.y/camspace.z,-(focal.y*camspace.y)/(camspace.z*camspace.z),
0.,0.,0.
);}
mat3 T=transpose(mat3(modelView))*J;mat3 cov2d=transpose(T)*Vrk*T;
#if COMPENSATION
float c00=cov2d[0][0];float c11=cov2d[1][1];float c01=cov2d[0][1];float detOrig=c00*c11-c01*c01;
#endif
cov2d[0][0]+=kernelSize;cov2d[1][1]+=kernelSize;
#if COMPENSATION
vec3 c2d=vec3(cov2d[0][0],c01,cov2d[1][1]);float detBlur=c2d.x*c2d.z-c2d.y*c2d.y;float compensation=sqrt(max(0.,detOrig/detBlur));vColor.w*=compensation;
#endif
float mid=(cov2d[0][0]+cov2d[1][1])/2.0;float radius=length(vec2((cov2d[0][0]-cov2d[1][1])/2.0,cov2d[0][1]));float epsilon=0.0001;float lambda1=mid+radius+epsilon,lambda2=mid-radius+epsilon;if (lambda2<0.0)
{return vec4(0.0,0.0,2.0,1.0);}
vec2 diagonalVector=normalize(vec2(cov2d[0][1],lambda1-cov2d[0][0]));vec2 majorAxis=min(sqrt(2.0*lambda1),1024.0)*diagonalVector;vec2 minorAxis=min(sqrt(2.0*lambda2),1024.0)*vec2(diagonalVector.y,-diagonalVector.x);vec2 vCenter=vec2(pos2d);float scaleFactor=isOrtho ? 1.0 : pos2d.w;return vec4(
vCenter 
+ ((meshPos.x*majorAxis
+ meshPos.y*minorAxis)*invViewport*scaleFactor)*scale,pos2d.zw);}`;N.IncludesShadersStore[yt]||(N.IncludesShadersStore[yt]=vs);const Be="gaussianSplattingVertexShader",Ft=`#include<__decl__gaussianSplattingVertex>
#ifdef LOGARITHMICDEPTH
#extension GL_EXT_frag_depth : enable
#endif
#include<clipPlaneVertexDeclaration>
#include<fogVertexDeclaration>
#include<logDepthDeclaration>
#include<helperFunctions>
uniform vec2 invViewport;uniform vec2 dataTextureSize;uniform vec2 focal;uniform float kernelSize;uniform vec3 eyePosition;uniform float alpha;uniform sampler2D covariancesATexture;uniform sampler2D covariancesBTexture;uniform sampler2D centersTexture;uniform sampler2D colorsTexture;
#if SH_DEGREE>0
uniform highp usampler2D shTexture0;
#endif
#if SH_DEGREE>1
uniform highp usampler2D shTexture1;
#endif
#if SH_DEGREE>2
uniform highp usampler2D shTexture2;
#endif
varying vec4 vColor;varying vec2 vPosition;
#include<gaussianSplatting>
void main () {float splatIndex=getSplatIndex(int(position.z+0.5));Splat splat=readSplat(splatIndex);vec3 covA=splat.covA.xyz;vec3 covB=vec3(splat.covA.w,splat.covB.xy);vec4 worldPos=world*vec4(splat.center.xyz,1.0);vColor=splat.color;vPosition=position.xy;
#if SH_DEGREE>0
mat3 worldRot=mat3(world);mat3 normWorldRot=inverseMat3(worldRot);vec3 eyeToSplatLocalSpace=normalize(normWorldRot*(worldPos.xyz-eyePosition));vColor.xyz=splat.color.xyz+computeSH(splat,eyeToSplatLocalSpace);
#endif
vColor.w*=alpha;gl_Position=gaussianSplatting(position.xy,worldPos.xyz,vec2(1.,1.),covA,covB,world,view,projection);
#include<clipPlaneVertex>
#include<fogVertex>
#include<logDepthVertex>
}
`;N.ShadersStore[Be]||(N.ShadersStore[Be]=Ft);const gs={name:Be,shader:Ft},Ss=Object.freeze(Object.defineProperty({__proto__:null,gaussianSplattingVertexShader:gs},Symbol.toStringTag,{value:"Module"})),wt="gaussianSplattingFragmentDeclaration",ys=`fn gaussianColor(inColor: vec4f,inPosition: vec2f)->vec4f
{var A : f32=-dot(inPosition,inPosition);if (A>-4.0)
{var B: f32=exp(A)*inColor.a;
#include<logDepthFragment>
var color: vec3f=inColor.rgb;
#ifdef FOG
#include<fogFragment>
#endif
return vec4f(color,B);} else {return vec4f(0.0);}}
`;N.IncludesShadersStoreWGSL[wt]||(N.IncludesShadersStoreWGSL[wt]=ys);const Ue="gaussianSplattingPixelShader",Bt=`#include<clipPlaneFragmentDeclaration>
#include<logDepthDeclaration>
#include<fogFragmentDeclaration>
varying vColor: vec4f;varying vPosition: vec2f;
#include<gaussianSplattingFragmentDeclaration>
@fragment
fn main(input: FragmentInputs)->FragmentOutputs {
#include<clipPlaneFragment>
fragmentOutputs.color=gaussianColor(input.vColor,input.vPosition);}
`;N.ShadersStoreWGSL[Ue]||(N.ShadersStoreWGSL[Ue]=Bt);const ws={name:Ue,shader:Bt},Cs=Object.freeze(Object.defineProperty({__proto__:null,gaussianSplattingPixelShaderWGSL:ws},Symbol.toStringTag,{value:"Module"})),Ct="gaussianSplatting",bs=`fn getDataUV(index: f32,dataTextureSize: vec2f)->vec2<f32> {let y: f32=floor(index/dataTextureSize.x);let x: f32=index-y*dataTextureSize.x;return vec2f((x+0.5),(y+0.5));}
struct Splat {center: vec4f,
color: vec4f,
covA: vec4f,
covB: vec4f,
#if SH_DEGREE>0
sh0: vec4<u32>,
#endif
#if SH_DEGREE>1
sh1: vec4<u32>,
#endif
#if SH_DEGREE>2
sh2: vec4<u32>,
#endif
};fn getSplatIndex(localIndex: i32,splatIndex0: vec4f,splatIndex1: vec4f,splatIndex2: vec4f,splatIndex3: vec4f)->f32 {var splatIndex: f32;switch (localIndex)
{case 0:
{splatIndex=splatIndex0.x;break;}
case 1:
{splatIndex=splatIndex0.y;break;}
case 2:
{splatIndex=splatIndex0.z;break;}
case 3:
{splatIndex=splatIndex0.w;break;}
case 4:
{splatIndex=splatIndex1.x;break;}
case 5:
{splatIndex=splatIndex1.y;break;}
case 6:
{splatIndex=splatIndex1.z;break;}
case 7:
{splatIndex=splatIndex1.w;break;}
case 8:
{splatIndex=splatIndex2.x;break;}
case 9:
{splatIndex=splatIndex2.y;break;}
case 10:
{splatIndex=splatIndex2.z;break;}
case 11:
{splatIndex=splatIndex2.w;break;}
case 12:
{splatIndex=splatIndex3.x;break;}
case 13:
{splatIndex=splatIndex3.y;break;}
case 14:
{splatIndex=splatIndex3.z;break;}
default:
{splatIndex=splatIndex3.w;break;}}
return splatIndex;}
fn readSplat(splatIndex: f32,dataTextureSize: vec2f)->Splat {var splat: Splat;let splatUV=getDataUV(splatIndex,dataTextureSize);let splatUVi32=vec2<i32>(i32(splatUV.x),i32(splatUV.y));splat.center=textureLoad(centersTexture,splatUVi32,0);splat.color=textureLoad(colorsTexture,splatUVi32,0);splat.covA=textureLoad(covariancesATexture,splatUVi32,0)*splat.center.w;splat.covB=textureLoad(covariancesBTexture,splatUVi32,0)*splat.center.w;
#if SH_DEGREE>0
splat.sh0=textureLoad(shTexture0,splatUVi32,0);
#endif
#if SH_DEGREE>1
splat.sh1=textureLoad(shTexture1,splatUVi32,0);
#endif
#if SH_DEGREE>2
splat.sh2=textureLoad(shTexture2,splatUVi32,0);
#endif
return splat;}
fn computeColorFromSHDegree(dir: vec3f,sh: array<vec3<f32>,16>)->vec3f
{let SH_C0: f32=0.28209479;let SH_C1: f32=0.48860251;var SH_C2: array<f32,5>=array<f32,5>(
1.092548430,
-1.09254843,
0.315391565,
-1.09254843,
0.546274215
);var SH_C3: array<f32,7>=array<f32,7>(
-0.59004358,
2.890611442,
-0.45704579,
0.373176332,
-0.45704579,
1.445305721,
-0.59004358
);var result: vec3f=/*SH_C0**/sh[0];
#if SH_DEGREE>0
let x: f32=dir.x;let y: f32=dir.y;let z: f32=dir.z;result+=-SH_C1*y*sh[1]+SH_C1*z*sh[2]-SH_C1*x*sh[3];
#if SH_DEGREE>1
let xx: f32=x*x;let yy: f32=y*y;let zz: f32=z*z;let xy: f32=x*y;let yz: f32=y*z;let xz: f32=x*z;result+=
SH_C2[0]*xy*sh[4] +
SH_C2[1]*yz*sh[5] +
SH_C2[2]*(2.0f*zz-xx-yy)*sh[6] +
SH_C2[3]*xz*sh[7] +
SH_C2[4]*(xx-yy)*sh[8];
#if SH_DEGREE>2
result+=
SH_C3[0]*y*(3.0f*xx-yy)*sh[9] +
SH_C3[1]*xy*z*sh[10] +
SH_C3[2]*y*(4.0f*zz-xx-yy)*sh[11] +
SH_C3[3]*z*(2.0f*zz-3.0f*xx-3.0f*yy)*sh[12] +
SH_C3[4]*x*(4.0f*zz-xx-yy)*sh[13] +
SH_C3[5]*z*(xx-yy)*sh[14] +
SH_C3[6]*x*(xx-3.0f*yy)*sh[15];
#endif
#endif
#endif
return result;}
fn decompose(value: u32)->vec4f
{let components : vec4f=vec4f(
f32((value ) & 255u),
f32((value>>u32( 8)) & 255u),
f32((value>>u32(16)) & 255u),
f32((value>>u32(24)) & 255u));return components*vec4f(2./255.)-vec4f(1.);}
fn computeSH(splat: Splat,dir: vec3f)->vec3f
{var sh: array<vec3<f32>,16>;sh[0]=vec3f(0.,0.,0.);
#if SH_DEGREE>0
let sh00: vec4f=decompose(splat.sh0.x);let sh01: vec4f=decompose(splat.sh0.y);let sh02: vec4f=decompose(splat.sh0.z);sh[1]=vec3f(sh00.x,sh00.y,sh00.z);sh[2]=vec3f(sh00.w,sh01.x,sh01.y);sh[3]=vec3f(sh01.z,sh01.w,sh02.x);
#endif
#if SH_DEGREE>1
let sh03: vec4f=decompose(splat.sh0.w);let sh04: vec4f=decompose(splat.sh1.x);let sh05: vec4f=decompose(splat.sh1.y);sh[4]=vec3f(sh02.y,sh02.z,sh02.w);sh[5]=vec3f(sh03.x,sh03.y,sh03.z);sh[6]=vec3f(sh03.w,sh04.x,sh04.y);sh[7]=vec3f(sh04.z,sh04.w,sh05.x);sh[8]=vec3f(sh05.y,sh05.z,sh05.w);
#endif
#if SH_DEGREE>2
let sh06: vec4f=decompose(splat.sh1.z);let sh07: vec4f=decompose(splat.sh1.w);let sh08: vec4f=decompose(splat.sh2.x);let sh09: vec4f=decompose(splat.sh2.y);let sh10: vec4f=decompose(splat.sh2.z);let sh11: vec4f=decompose(splat.sh2.w);sh[9]=vec3f(sh06.x,sh06.y,sh06.z);sh[10]=vec3f(sh06.w,sh07.x,sh07.y);sh[11]=vec3f(sh07.z,sh07.w,sh08.x);sh[12]=vec3f(sh08.y,sh08.z,sh08.w);sh[13]=vec3f(sh09.x,sh09.y,sh09.z);sh[14]=vec3f(sh09.w,sh10.x,sh10.y);sh[15]=vec3f(sh10.z,sh10.w,sh11.x); 
#endif
return computeColorFromSHDegree(dir,sh);}
fn gaussianSplatting(
meshPos: vec2<f32>,
worldPos: vec3<f32>,
scale: vec2<f32>,
covA: vec3<f32>,
covB: vec3<f32>,
worldMatrix: mat4x4<f32>,
viewMatrix: mat4x4<f32>,
projectionMatrix: mat4x4<f32>,
focal: vec2f,
invViewport: vec2f,
kernelSize: f32
)->vec4f {let modelView=viewMatrix*worldMatrix;let camspace=viewMatrix*vec4f(worldPos,1.0);let pos2d=projectionMatrix*camspace;let bounds=1.2*pos2d.w;if (pos2d.z<0. || pos2d.x<-bounds || pos2d.x>bounds || pos2d.y<-bounds || pos2d.y>bounds) {return vec4f(0.0,0.0,2.0,1.0);}
let Vrk=mat3x3<f32>(
covA.x,covA.y,covA.z,
covA.y,covB.x,covB.y,
covA.z,covB.y,covB.z
);let isOrtho=abs(projectionMatrix[3][3]-1.0)<0.001;var J: mat3x3<f32>;if (isOrtho) {J=mat3x3<f32>(
focal.x,0.0,0.0,
0.0,focal.y,0.0,
0.0,0.0,0.0
);} else {J=mat3x3<f32>(
focal.x/camspace.z,0.0,-(focal.x*camspace.x)/(camspace.z*camspace.z),
0.0,focal.y/camspace.z,-(focal.y*camspace.y)/(camspace.z*camspace.z),
0.0,0.0,0.0
);}
let T=transpose(mat3x3<f32>(
modelView[0].xyz,
modelView[1].xyz,
modelView[2].xyz))*J;var cov2d=transpose(T)*Vrk*T;
#if COMPENSATION
let c00: f32=cov2d[0][0];let c11: f32=cov2d[1][1];let c01: f32=cov2d[0][1];let detOrig: f32=c00*c11-c01*c01;
#endif
cov2d[0][0]+=kernelSize;cov2d[1][1]+=kernelSize;
#if COMPENSATION
let c2d: vec3f=vec3f(cov2d[0][0],c01,cov2d[1][1]);let detBlur: f32=c2d.x*c2d.z-c2d.y*c2d.y;let compensation: f32=sqrt(max(0.,detOrig/detBlur));vertexOutputs.vColor.w*=compensation;
#endif
let mid=(cov2d[0][0]+cov2d[1][1])/2.0;let radius=length(vec2<f32>((cov2d[0][0]-cov2d[1][1])/2.0,cov2d[0][1]));let lambda1=mid+radius;let lambda2=mid-radius;if (lambda2<0.0) {return vec4f(0.0,0.0,2.0,1.0);}
let diagonalVector=normalize(vec2<f32>(cov2d[0][1],lambda1-cov2d[0][0]));let majorAxis=min(sqrt(2.0*lambda1),1024.0)*diagonalVector;let minorAxis=min(sqrt(2.0*lambda2),1024.0)*vec2<f32>(diagonalVector.y,-diagonalVector.x);let vCenter=vec2<f32>(pos2d.x,pos2d.y);let scaleFactor=select(pos2d.w,1.0,isOrtho);return vec4f(
vCenter+((meshPos.x*majorAxis+meshPos.y*minorAxis)*invViewport*scaleFactor)*scale,
pos2d.z,
pos2d.w
);}`;N.IncludesShadersStoreWGSL[Ct]||(N.IncludesShadersStoreWGSL[Ct]=bs);const Pe="gaussianSplattingVertexShader",Ut=`#include<sceneUboDeclaration>
#include<meshUboDeclaration>
#include<helperFunctions>
#include<clipPlaneVertexDeclaration>
#include<fogVertexDeclaration>
#include<logDepthDeclaration>
attribute splatIndex0: vec4f;attribute splatIndex1: vec4f;attribute splatIndex2: vec4f;attribute splatIndex3: vec4f;attribute position: vec3f;uniform invViewport: vec2f;uniform dataTextureSize: vec2f;uniform focal: vec2f;uniform kernelSize: f32;uniform eyePosition: vec3f;uniform alpha: f32;var covariancesATexture: texture_2d<f32>;var covariancesBTexture: texture_2d<f32>;var centersTexture: texture_2d<f32>;var colorsTexture: texture_2d<f32>;
#if SH_DEGREE>0
var shTexture0: texture_2d<u32>;
#endif
#if SH_DEGREE>1
var shTexture1: texture_2d<u32>;
#endif
#if SH_DEGREE>2
var shTexture2: texture_2d<u32>;
#endif
varying vColor: vec4f;varying vPosition: vec2f;
#include<gaussianSplatting>
@vertex
fn main(input : VertexInputs)->FragmentInputs {let splatIndex: f32=getSplatIndex(i32(input.position.z+0.5),input.splatIndex0,input.splatIndex1,input.splatIndex2,input.splatIndex3);var splat: Splat=readSplat(splatIndex,uniforms.dataTextureSize);var covA: vec3f=splat.covA.xyz;var covB: vec3f=vec3f(splat.covA.w,splat.covB.xy);let worldPos: vec4f=mesh.world*vec4f(splat.center.xyz,1.0);vertexOutputs.vPosition=input.position.xy;
#if SH_DEGREE>0
let worldRot: mat3x3f= mat3x3f(mesh.world[0].xyz,mesh.world[1].xyz,mesh.world[2].xyz);let normWorldRot: mat3x3f=inverseMat3(worldRot);var eyeToSplatLocalSpace: vec3f=normalize(normWorldRot*(worldPos.xyz-uniforms.eyePosition.xyz));vertexOutputs.vColor=vec4f(splat.color.xyz+computeSH(splat,eyeToSplatLocalSpace),splat.color.w*uniforms.alpha);
#else
vertexOutputs.vColor=vec4f(splat.color.xyz,splat.color.w*uniforms.alpha);
#endif
vertexOutputs.position=gaussianSplatting(input.position.xy,worldPos.xyz,vec2f(1.0,1.0),covA,covB,mesh.world,scene.view,scene.projection,uniforms.focal,uniforms.invViewport,uniforms.kernelSize);
#include<clipPlaneVertex>
#include<fogVertex>
#include<logDepthVertex>
}
`;N.ShadersStoreWGSL[Pe]||(N.ShadersStoreWGSL[Pe]=Ut);const Is={name:Pe,shader:Ut},Es=Object.freeze(Object.defineProperty({__proto__:null,gaussianSplattingVertexShaderWGSL:Is},Symbol.toStringTag,{value:"Module"})),bt="gaussianSplattingDepthPixelShader",Ts=`precision highp float;varying vec2 vPosition;varying vec4 vColor;
#ifdef DEPTH_RENDER
varying float vDepthMetric;
#endif
void main(void) {float A=-dot(vPosition,vPosition);
#if defined(SM_SOFTTRANSPARENTSHADOW) && SM_SOFTTRANSPARENTSHADOW==1
float alpha=exp(A)*vColor.a;if (A<-4.) discard;
#else
if (A<-vColor.a) discard;
#endif
#ifdef DEPTH_RENDER
gl_FragColor=vec4(vDepthMetric,0.0,0.0,1.0);
#endif
}`;N.ShadersStore[bt]||(N.ShadersStore[bt]=Ts);const It="gaussianSplattingDepthVertexShader",As=`#include<__decl__gaussianSplattingVertex>
uniform vec2 invViewport;uniform vec2 dataTextureSize;uniform vec2 focal;uniform float kernelSize;uniform float alpha;uniform sampler2D covariancesATexture;uniform sampler2D covariancesBTexture;uniform sampler2D centersTexture;uniform sampler2D colorsTexture;varying vec2 vPosition;varying vec4 vColor;
#include<gaussianSplatting>
#ifdef DEPTH_RENDER
uniform vec2 depthValues;varying float vDepthMetric;
#endif
void main(void) {float splatIndex=getSplatIndex(int(position.z+0.5));Splat splat=readSplat(splatIndex);vec3 covA=splat.covA.xyz;vec3 covB=vec3(splat.covA.w,splat.covB.xy);vec4 worldPosGS=world*vec4(splat.center.xyz,1.0);vPosition=position.xy;vColor=splat.color;vColor.w*=alpha;gl_Position=gaussianSplatting(position.xy,worldPosGS.xyz,vec2(1.,1.),covA,covB,world,view,projection);
#ifdef DEPTH_RENDER
#ifdef USE_REVERSE_DEPTHBUFFER
vDepthMetric=((-gl_Position.z+depthValues.x)/(depthValues.y));
#else
vDepthMetric=((gl_Position.z+depthValues.x)/(depthValues.y));
#endif
#endif
}`;N.ShadersStore[It]||(N.ShadersStore[It]=As);const Et="gaussianSplattingDepthPixelShader",Ds=`#include<gaussianSplattingFragmentDeclaration>
varying vPosition: vec2f;varying vColor: vec4f;
#ifdef DEPTH_RENDER
varying vDepthMetric: f32;
#endif
fn checkDiscard(inPosition: vec2f,inColor: vec4f)->vec4f {var A : f32=-dot(inPosition,inPosition);var alpha : f32=exp(A)*inColor.a;
#if defined(SM_SOFTTRANSPARENTSHADOW) && SM_SOFTTRANSPARENTSHADOW==1
if (A<-4.) {discard;}
#else
if (A<-inColor.a) {discard;}
#endif
#ifdef DEPTH_RENDER
return vec4f(fragmentInputs.vDepthMetric,0.0,0.0,1.0);
#else
return vec4f(inColor.rgb,alpha);
#endif
}
#define CUSTOM_FRAGMENT_DEFINITIONS
@fragment
fn main(input: FragmentInputs)->FragmentOutputs {fragmentOutputs.color=checkDiscard(fragmentInputs.vPosition,fragmentInputs.vColor);
#if defined(SM_SOFTTRANSPARENTSHADOW) && SM_SOFTTRANSPARENTSHADOW==1
var alpha : f32=fragmentOutputs.color.a;
#endif
}
`;N.ShadersStoreWGSL[Et]||(N.ShadersStoreWGSL[Et]=Ds);const Tt="gaussianSplattingDepthVertexShader",Ms=`#include<sceneUboDeclaration>
#include<meshUboDeclaration>
attribute splatIndex0: vec4f;attribute splatIndex1: vec4f;attribute splatIndex2: vec4f;attribute splatIndex3: vec4f;attribute position: vec3f;uniform invViewport: vec2f;uniform dataTextureSize: vec2f;uniform focal: vec2f;uniform kernelSize: f32;uniform alpha: f32;var covariancesATexture: texture_2d<f32>;var covariancesBTexture: texture_2d<f32>;var centersTexture: texture_2d<f32>;var colorsTexture: texture_2d<f32>;varying vPosition: vec2f;varying vColor: vec4f;
#ifdef DEPTH_RENDER
uniform depthValues: vec2f;varying vDepthMetric: f32;
#endif
#include<gaussianSplatting>
@vertex
fn main(input : VertexInputs)->FragmentInputs {let splatIndex: f32=getSplatIndex(i32(input.position.z+0.5),input.splatIndex0,input.splatIndex1,input.splatIndex2,input.splatIndex3);var splat: Splat=readSplat(splatIndex,uniforms.dataTextureSize);var covA: vec3f=splat.covA.xyz;var covB: vec3f=vec3f(splat.covA.w,splat.covB.xy);let worldPos: vec4f=mesh.world*vec4f(splat.center.xyz,1.0);vertexOutputs.vPosition=input.position.xy;vertexOutputs.vColor=splat.color;vertexOutputs.vColor.w*=uniforms.alpha;vertexOutputs.position=gaussianSplatting(input.position.xy,worldPos.xyz,vec2f(1.0,1.0),covA,covB,mesh.world,scene.view,scene.projection,uniforms.focal,uniforms.invViewport,uniforms.kernelSize);
#ifdef DEPTH_RENDER
#ifdef USE_REVERSE_DEPTHBUFFER
vertexOutputs.vDepthMetric=((-vertexOutputs.position.z+uniforms.depthValues.x)/(uniforms.depthValues.y));
#else
vertexOutputs.vDepthMetric=((vertexOutputs.position.z+uniforms.depthValues.x)/(uniforms.depthValues.y));
#endif
#endif
}`;N.ShadersStoreWGSL[Tt]||(N.ShadersStoreWGSL[Tt]=Ms);class zs extends Qt{constructor(){super(),this.FOG=!1,this.THIN_INSTANCES=!0,this.LOGARITHMICDEPTH=!1,this.CLIPPLANE=!1,this.CLIPPLANE2=!1,this.CLIPPLANE3=!1,this.CLIPPLANE4=!1,this.CLIPPLANE5=!1,this.CLIPPLANE6=!1,this.SH_DEGREE=0,this.COMPENSATION=!1,this.rebuild()}}class M extends Wt{constructor(e,t){super(e,t),this.kernelSize=M.KernelSize,this._compensation=M.Compensation,this._isDirty=!1,this._sourceMesh=null,this.backFaceCulling=!1,this.shadowDepthWrapper=M._MakeGaussianSplattingShadowDepthWrapper(t,this.shaderLanguage)}set compensation(e){this._isDirty=this._isDirty!=e,this._compensation=e}get compensation(){return this._compensation}get hasRenderTargetTextures(){return!1}needAlphaTesting(){return!1}needAlphaBlending(){return!0}isReadyForSubMesh(e,t){const n=t._drawWrapper;let o=t.materialDefines;if(o&&this._isDirty&&o.markAsUnprocessed(),n.effect&&this.isFrozen&&n._wasPreviouslyReady&&n._wasPreviouslyUsingInstances===!0)return!0;t.materialDefines||(o=t.materialDefines=new zs);const i=this.getScene();if(this._isReadyForSubMesh(t))return!0;if(!this._sourceMesh)return!1;const l=i.getEngine(),h=this._sourceMesh;Vt(e,i,this._useLogarithmicDepth,this.pointsCloud,this.fogEnabled,!1,o,void 0,void 0,void 0,this._isVertexOutputInvariant),Zt(i,l,this,o,!0,null,!0),jt(e,o,!1,!1),(l.version>1||l.isWebGPU)&&(o.SH_DEGREE=h.shDegree);const a=h.material;if(o.COMPENSATION=a&&a.compensation?a.compensation:M.Compensation,o.isDirty){o.markAsProcessed(),i.resetCachedMaterial(),Xt(M._Attribs,o),Gt({uniformsNames:M._Uniforms,uniformBuffersNames:M._UniformBuffers,samplers:M._Samplers,defines:o}),$t(M._Uniforms);const _=o.toString(),u=i.getEngine().createEffect("gaussianSplatting",{attributes:M._Attribs,uniformsNames:M._Uniforms,uniformBuffersNames:M._UniformBuffers,samplers:M._Samplers,defines:_,onCompiled:this.onCompiled,onError:this.onError,indexParameters:{},shaderLanguage:this._shaderLanguage,extraInitializationsAsync:async()=>{this._shaderLanguage===1?await Promise.all([ye(()=>Promise.resolve().then(()=>Cs),void 0,import.meta.url),ye(()=>Promise.resolve().then(()=>Es),void 0,import.meta.url)]):await Promise.all([ye(()=>Promise.resolve().then(()=>_s),void 0,import.meta.url),ye(()=>Promise.resolve().then(()=>Ss),void 0,import.meta.url)])}},l);t.setEffect(u,o,this._materialContext)}return!t.effect||!t.effect.isReady()?!1:(o._renderId=i.getRenderId(),n._wasPreviouslyReady=!0,n._wasPreviouslyUsingInstances=!0,this._isDirty=!1,!0)}setSourceMesh(e){this._sourceMesh=e}static BindEffect(e,t,s){const n=s.getEngine(),o=s.activeCamera,i=n.getRenderWidth()*o.viewport.width,l=n.getRenderHeight()*o.viewport.height,h=e.material;if(!h._sourceMesh)return;const a=h._sourceMesh,_=o?.rigParent?.rigCameras.length||1;t.setFloat2("invViewport",1/(i/_),1/l);let u=1e3;if(o){const f=o.getProjectionMatrix().m[5];o.fovMode==_t.FOVMODE_VERTICAL_FIXED?u=l*f/2:u=i*f/2}if(t.setFloat2("focal",u,u),t.setFloat("kernelSize",h&&h.kernelSize?h.kernelSize:M.KernelSize),t.setFloat("alpha",h.alpha),s.bindEyePosition(t,"eyePosition",!0),a.covariancesATexture){const f=a.covariancesATexture.getSize();if(t.setFloat2("dataTextureSize",f.width,f.height),t.setTexture("covariancesATexture",a.covariancesATexture),t.setTexture("covariancesBTexture",a.covariancesBTexture),t.setTexture("centersTexture",a.centersTexture),t.setTexture("colorsTexture",a.colorsTexture),a.shTextures)for(let p=0;p<a.shTextures?.length;p++)t.setTexture(`shTexture${p}`,a.shTextures[p])}}bindForSubMesh(e,t,s){const n=this.getScene(),o=s.materialDefines;if(!o)return;const i=s.effect;if(!i)return;this._activeEffect=i,t.getMeshUniformBuffer().bindToEffect(i,"Mesh"),t.transferToEffect(e),this._mustRebind(n,i,s,t.visibility)?(this.bindView(i),this.bindViewProjection(i),M.BindEffect(t,this._activeEffect,n),qt(i,this,n)):n.getEngine()._features.needToAlwaysBindUniformBuffers&&(this._needToBindSceneUbo=!0),Kt(n,t,i),this.useLogarithmicDepth&&Jt(o,i,n),this._afterBind(t,this._activeEffect,s)}static _BindEffectUniforms(e,t,s,n){const o=n.getEngine(),i=s.getEffect();e.getMeshUniformBuffer().bindToEffect(i,"Mesh"),s.bindView(i),s.bindViewProjection(i);const l=o.getRenderWidth(),h=o.getRenderHeight();i.setFloat2("invViewport",1/l,1/h);const _=n.getProjectionMatrix().m[5],u=l*_/2;i.setFloat2("focal",u,u),i.setFloat("kernelSize",t&&t.kernelSize?t.kernelSize:M.KernelSize),i.setFloat("alpha",t.alpha);let f,p;const m=n.activeCamera;if(!m)return;if(m.mode===_t.ORTHOGRAPHIC_CAMERA?(f=!o.useReverseDepthBuffer&&o.isNDCHalfZRange?0:1,p=o.useReverseDepthBuffer&&o.isNDCHalfZRange?0:1):(f=o.useReverseDepthBuffer&&o.isNDCHalfZRange?m.minZ:o.isNDCHalfZRange?0:m.minZ,p=o.useReverseDepthBuffer&&o.isNDCHalfZRange?0:m.maxZ),i.setFloat2("depthValues",f,f+p),e.covariancesATexture){const E=e.covariancesATexture.getSize();i.setFloat2("dataTextureSize",E.width,E.height),i.setTexture("covariancesATexture",e.covariancesATexture),i.setTexture("covariancesBTexture",e.covariancesBTexture),i.setTexture("centersTexture",e.centersTexture),i.setTexture("colorsTexture",e.colorsTexture)}}makeDepthRenderingMaterial(e,t){const s=new xt("gaussianSplattingDepthRender",e,{vertex:"gaussianSplattingDepth",fragment:"gaussianSplattingDepth"},{attributes:M._Attribs,uniforms:M._Uniforms,samplers:M._Samplers,uniformBuffers:M._UniformBuffers,shaderLanguage:t,defines:["#define DEPTH_RENDER"]});return s.onBindObservable.add(n=>{const o=n.material,i=n;M._BindEffectUniforms(i,o,s,e)}),s}static _MakeGaussianSplattingShadowDepthWrapper(e,t){const s=new xt("gaussianSplattingDepth",e,{vertex:"gaussianSplattingDepth",fragment:"gaussianSplattingDepth"},{attributes:M._Attribs,uniforms:M._Uniforms,samplers:M._Samplers,uniformBuffers:M._UniformBuffers,shaderLanguage:t}),n=new us(s,e,{standalone:!0});return s.onBindObservable.add(o=>{const i=o.material,l=o;M._BindEffectUniforms(l,i,s,e)}),n}clone(e){return mt.Clone(()=>new M(e,this.getScene()),this)}serialize(){const e=super.serialize();return e.customType="BABYLON.GaussianSplattingMaterial",e}getClassName(){return"GaussianSplattingMaterial"}static Parse(e,t,s){return mt.Parse(()=>new M(e.name,t),e,t,s)}}M.KernelSize=.3;M.Compensation=!1;M._Attribs=[q.PositionKind,"splatIndex0","splatIndex1","splatIndex2","splatIndex3"];M._Samplers=["covariancesATexture","covariancesBTexture","centersTexture","colorsTexture","shTexture0","shTexture1","shTexture2"];M._UniformBuffers=["Scene","Mesh"];M._Uniforms=["world","view","projection","vFogInfos","vFogColor","logarithmicDepthConstant","invViewport","dataTextureSize","focal","eyePosition","kernelSize","alpha","depthValues"];Lt("BABYLON.GaussianSplattingMaterial",M);const Hs=es,X={...Yt,TwoPi:Math.PI*2,Sign:Math.sign,Log2:Math.log2,HCF:Hs},Q=(r,e)=>{const t=(1<<e)-1;return(r&t)/t},At=(r,e)=>{e.x=Q(r>>>21,11),e.y=Q(r>>>11,10),e.z=Q(r,11)},ks=(r,e)=>{e[0]=Q(r>>>24,8)*255,e[1]=Q(r>>>16,8)*255,e[2]=Q(r>>>8,8)*255,e[3]=Q(r,8)*255},Rs=(r,e)=>{const t=1/(Math.sqrt(2)*.5),s=(Q(r>>>20,10)-.5)*t,n=(Q(r>>>10,10)-.5)*t,o=(Q(r,10)-.5)*t,i=Math.sqrt(1-(s*s+n*n+o*o));switch(r>>>30){case 0:e.set(i,s,n,o);break;case 1:e.set(s,i,n,o);break;case 2:e.set(s,n,i,o);break;case 3:e.set(s,n,o,i);break}};var Dt;(function(r){r[r.FLOAT=0]="FLOAT",r[r.INT=1]="INT",r[r.UINT=2]="UINT",r[r.DOUBLE=3]="DOUBLE",r[r.UCHAR=4]="UCHAR",r[r.UNDEFINED=5]="UNDEFINED"})(Dt||(Dt={}));var Mt;(function(r){r[r.MIN_X=0]="MIN_X",r[r.MIN_Y=1]="MIN_Y",r[r.MIN_Z=2]="MIN_Z",r[r.MAX_X=3]="MAX_X",r[r.MAX_Y=4]="MAX_Y",r[r.MAX_Z=5]="MAX_Z",r[r.MIN_SCALE_X=6]="MIN_SCALE_X",r[r.MIN_SCALE_Y=7]="MIN_SCALE_Y",r[r.MIN_SCALE_Z=8]="MIN_SCALE_Z",r[r.MAX_SCALE_X=9]="MAX_SCALE_X",r[r.MAX_SCALE_Y=10]="MAX_SCALE_Y",r[r.MAX_SCALE_Z=11]="MAX_SCALE_Z",r[r.PACKED_POSITION=12]="PACKED_POSITION",r[r.PACKED_ROTATION=13]="PACKED_ROTATION",r[r.PACKED_SCALE=14]="PACKED_SCALE",r[r.PACKED_COLOR=15]="PACKED_COLOR",r[r.X=16]="X",r[r.Y=17]="Y",r[r.Z=18]="Z",r[r.SCALE_0=19]="SCALE_0",r[r.SCALE_1=20]="SCALE_1",r[r.SCALE_2=21]="SCALE_2",r[r.DIFFUSE_RED=22]="DIFFUSE_RED",r[r.DIFFUSE_GREEN=23]="DIFFUSE_GREEN",r[r.DIFFUSE_BLUE=24]="DIFFUSE_BLUE",r[r.OPACITY=25]="OPACITY",r[r.F_DC_0=26]="F_DC_0",r[r.F_DC_1=27]="F_DC_1",r[r.F_DC_2=28]="F_DC_2",r[r.F_DC_3=29]="F_DC_3",r[r.ROT_0=30]="ROT_0",r[r.ROT_1=31]="ROT_1",r[r.ROT_2=32]="ROT_2",r[r.ROT_3=33]="ROT_3",r[r.MIN_COLOR_R=34]="MIN_COLOR_R",r[r.MIN_COLOR_G=35]="MIN_COLOR_G",r[r.MIN_COLOR_B=36]="MIN_COLOR_B",r[r.MAX_COLOR_R=37]="MAX_COLOR_R",r[r.MAX_COLOR_G=38]="MAX_COLOR_G",r[r.MAX_COLOR_B=39]="MAX_COLOR_B",r[r.SH_0=40]="SH_0",r[r.SH_1=41]="SH_1",r[r.SH_2=42]="SH_2",r[r.SH_3=43]="SH_3",r[r.SH_4=44]="SH_4",r[r.SH_5=45]="SH_5",r[r.SH_6=46]="SH_6",r[r.SH_7=47]="SH_7",r[r.SH_8=48]="SH_8",r[r.SH_9=49]="SH_9",r[r.SH_10=50]="SH_10",r[r.SH_11=51]="SH_11",r[r.SH_12=52]="SH_12",r[r.SH_13=53]="SH_13",r[r.SH_14=54]="SH_14",r[r.SH_15=55]="SH_15",r[r.SH_16=56]="SH_16",r[r.SH_17=57]="SH_17",r[r.SH_18=58]="SH_18",r[r.SH_19=59]="SH_19",r[r.SH_20=60]="SH_20",r[r.SH_21=61]="SH_21",r[r.SH_22=62]="SH_22",r[r.SH_23=63]="SH_23",r[r.SH_24=64]="SH_24",r[r.SH_25=65]="SH_25",r[r.SH_26=66]="SH_26",r[r.SH_27=67]="SH_27",r[r.SH_28=68]="SH_28",r[r.SH_29=69]="SH_29",r[r.SH_30=70]="SH_30",r[r.SH_31=71]="SH_31",r[r.SH_32=72]="SH_32",r[r.SH_33=73]="SH_33",r[r.SH_34=74]="SH_34",r[r.SH_35=75]="SH_35",r[r.SH_36=76]="SH_36",r[r.SH_37=77]="SH_37",r[r.SH_38=78]="SH_38",r[r.SH_39=79]="SH_39",r[r.SH_40=80]="SH_40",r[r.SH_41=81]="SH_41",r[r.SH_42=82]="SH_42",r[r.SH_43=83]="SH_43",r[r.SH_44=84]="SH_44",r[r.UNDEFINED=85]="UNDEFINED"})(Mt||(Mt={}));class D extends Ie{get disableDepthSort(){return this._disableDepthSort}set disableDepthSort(e){!this._disableDepthSort&&e?(this._worker?.terminate(),this._worker=null,this._disableDepthSort=!0):this._disableDepthSort&&!e&&(this._disableDepthSort=!1,this._sortIsDirty=!0,this._instanciateWorker())}get viewDirectionFactor(){return T.OneReadOnly}get shDegree(){return this._shDegree}get splatCount(){return this._splatIndex?.length}get splatsData(){return this._splatsData}get covariancesATexture(){return this._covariancesATexture}get covariancesBTexture(){return this._covariancesBTexture}get centersTexture(){return this._centersTexture}get colorsTexture(){return this._colorsTexture}get shTextures(){return this._shTextures}get kernelSize(){return this._material instanceof M?this._material.kernelSize:0}get compensation(){return this._material instanceof M?this._material.compensation:!1}set material(e){this._material=e,this._material.backFaceCulling=!1,this._material.cullBackFaces=!1,e.resetDrawCache()}get material(){return this._material}static _MakeSplatGeometryForMesh(e){const t=new Ne,s=[-2,-2,0,2,-2,0,2,2,0,-2,2,0],n=[0,1,2,0,2,3],o=[],i=[];for(let l=0;l<D._BatchSize;l++){for(let h=0;h<12;h++)h==2||h==5||h==8||h==11?o.push(l):o.push(s[h]);i.push(n.map(h=>h+l*4))}t.positions=o,t.indices=i.flat(),t.applyToMesh(e)}constructor(e,t=null,s=null,n=!1){super(e,s),this._vertexCount=0,this._worker=null,this._modelViewMatrix=Ce.Identity(),this._canPostToWorker=!0,this._readyToDisplay=!1,this._covariancesATexture=null,this._covariancesBTexture=null,this._centersTexture=null,this._colorsTexture=null,this._splatPositions=null,this._splatIndex=null,this._shTextures=null,this._splatsData=null,this._keepInRam=!1,this._delayedTextureUpdate=null,this._useRGBACovariants=!1,this._material=null,this._tmpCovariances=[0,0,0,0,0,0],this._sortIsDirty=!1,this._shDegree=0,this._cameraViewInfos=new Map,this._disableDepthSort=!1,this._loadingPromise=null,this.subMeshes=[],new ts(0,0,4*D._BatchSize,0,6*D._BatchSize,this),this.setEnabled(!1),this._useRGBACovariants=!this.getEngine().isWebGPU&&this.getEngine().version===1,this._keepInRam=n,t&&(this._loadingPromise=this.loadFileAsync(t));const o=new M(this.name+"_material",this._scene);o.setSourceMesh(this),this._material=o,this._scene.onCameraRemovedObservable.add(i=>{const l=i.uniqueId;this._cameraViewInfos.has(l)&&(this._cameraViewInfos.get(l)?.mesh.dispose(),this._cameraViewInfos.delete(l))})}getLoadingPromise(){return this._loadingPromise}getClassName(){return"GaussianSplattingMesh"}getTotalVertices(){return this._vertexCount}isReady(e=!1){return super.isReady(e,!0)?this._readyToDisplay?!0:(this._postToWorker(!0),!1):!1}_getCameraDirection(e){const t=e.getViewMatrix();this.getWorldMatrix().multiplyToRef(t,this._modelViewMatrix);const s=J.Vector3[1];return s.set(this._modelViewMatrix.m[2],this._modelViewMatrix.m[6],this._modelViewMatrix.m[10]),s.normalize(),s}_postToWorker(e=!1){const s=this._scene.getFrameId();let n=!1;this._cameraViewInfos.forEach(h=>{h.frameIdLastUpdate!==s&&(n=!0)});const o=this._scene.activeCameras?.length?this._scene.activeCameras:[this._scene.activeCamera],i=[];o.forEach(h=>{if(!h)return;const a=h.uniqueId,_=this._cameraViewInfos.get(a);if(_)i.push(_);else{const u=new Ie(this.name+"_cameraMesh_"+a,this._scene);u.reservedDataStore={hidden:!0},u.setEnabled(!1),u.material=this.material,D._MakeSplatGeometryForMesh(u);const f={camera:h,cameraDirection:new T(0,0,0),mesh:u,frameIdLastUpdate:s,splatIndexBufferSet:!1};i.push(f),this._cameraViewInfos.set(a,f)}}),i.sort((h,a)=>h.frameIdLastUpdate-a.frameIdLastUpdate);const l=this._worker||_native&&_native.sortSplats||this._disableDepthSort;(e||n)&&l&&(this._scene.activeCameras?.length||this._scene.activeCamera)&&this._canPostToWorker?i.forEach(h=>{const a=h.camera,_=this._getCameraDirection(a),u=h.cameraDirection,f=T.Dot(_,u);(e||Math.abs(f-1)>=.01)&&this._canPostToWorker&&(h.cameraDirection.copyFrom(_),h.frameIdLastUpdate=s,this._canPostToWorker=!1,this._worker?this._worker.postMessage({view:this._modelViewMatrix.m,depthMix:this._depthMix,useRightHandedSystem:this._scene.useRightHandedSystem,cameraId:a.uniqueId},[this._depthMix.buffer]):_native&&_native.sortSplats&&(_native.sortSplats(this._modelViewMatrix,this._splatPositions,this._splatIndex,this._scene.useRightHandedSystem),h.splatIndexBufferSet?h.mesh.thinInstanceBufferUpdated("splatIndex"):(h.mesh.thinInstanceSetBuffer("splatIndex",this._splatIndex,16,!1),h.splatIndexBufferSet=!0),this._canPostToWorker=!0,this._readyToDisplay=!0))}):this._disableDepthSort&&(i.forEach(h=>{h.splatIndexBufferSet||(h.mesh.thinInstanceSetBuffer("splatIndex",this._splatIndex,16,!1),h.splatIndexBufferSet=!0)}),this._canPostToWorker=!0,this._readyToDisplay=!0)}render(e,t,s){this._postToWorker(),!this._geometry&&this._cameraViewInfos.size&&(this._geometry=this._cameraViewInfos.values().next().value.mesh.geometry);const n=this._scene.activeCamera.uniqueId,o=this._cameraViewInfos.get(n);if(!o||!o.splatIndexBufferSet)return this;const i=o.mesh;return i.getWorldMatrix().copyFrom(this.getWorldMatrix()),i.render(e,t,s)}static _TypeNameToEnum(e){switch(e){case"float":return 0;case"int":return 1;case"uint":return 2;case"double":return 3;case"uchar":return 4}return 5}static _ValueNameToEnum(e){switch(e){case"min_x":return 0;case"min_y":return 1;case"min_z":return 2;case"max_x":return 3;case"max_y":return 4;case"max_z":return 5;case"min_scale_x":return 6;case"min_scale_y":return 7;case"min_scale_z":return 8;case"max_scale_x":return 9;case"max_scale_y":return 10;case"max_scale_z":return 11;case"packed_position":return 12;case"packed_rotation":return 13;case"packed_scale":return 14;case"packed_color":return 15;case"x":return 16;case"y":return 17;case"z":return 18;case"scale_0":return 19;case"scale_1":return 20;case"scale_2":return 21;case"diffuse_red":case"red":return 22;case"diffuse_green":case"green":return 23;case"diffuse_blue":case"blue":return 24;case"f_dc_0":return 26;case"f_dc_1":return 27;case"f_dc_2":return 28;case"f_dc_3":return 29;case"opacity":return 25;case"rot_0":return 30;case"rot_1":return 31;case"rot_2":return 32;case"rot_3":return 33;case"min_r":return 34;case"min_g":return 35;case"min_b":return 36;case"max_r":return 37;case"max_g":return 38;case"max_b":return 39;case"f_rest_0":return 40;case"f_rest_1":return 41;case"f_rest_2":return 42;case"f_rest_3":return 43;case"f_rest_4":return 44;case"f_rest_5":return 45;case"f_rest_6":return 46;case"f_rest_7":return 47;case"f_rest_8":return 48;case"f_rest_9":return 49;case"f_rest_10":return 50;case"f_rest_11":return 51;case"f_rest_12":return 52;case"f_rest_13":return 53;case"f_rest_14":return 54;case"f_rest_15":return 55;case"f_rest_16":return 56;case"f_rest_17":return 57;case"f_rest_18":return 58;case"f_rest_19":return 59;case"f_rest_20":return 60;case"f_rest_21":return 61;case"f_rest_22":return 62;case"f_rest_23":return 63;case"f_rest_24":return 64;case"f_rest_25":return 65;case"f_rest_26":return 66;case"f_rest_27":return 67;case"f_rest_28":return 68;case"f_rest_29":return 69;case"f_rest_30":return 70;case"f_rest_31":return 71;case"f_rest_32":return 72;case"f_rest_33":return 73;case"f_rest_34":return 74;case"f_rest_35":return 75;case"f_rest_36":return 76;case"f_rest_37":return 77;case"f_rest_38":return 78;case"f_rest_39":return 79;case"f_rest_40":return 80;case"f_rest_41":return 81;case"f_rest_42":return 82;case"f_rest_43":return 83;case"f_rest_44":return 84}return 85}static ParseHeader(e){const t=new Uint8Array(e),s=new TextDecoder().decode(t.slice(0,1024*10)),n=`end_header
`,o=s.indexOf(n);if(o<0||!s)return null;const i=parseInt(/element vertex (\d+)\n/.exec(s)[1]),l=/element chunk (\d+)\n/.exec(s);let h=0;l&&(h=parseInt(l[1]));let a=0,_=0;const u={double:8,int:4,uint:4,float:4,short:2,ushort:2,uchar:1,list:0};let f;(function(v){v[v.Vertex=0]="Vertex",v[v.Chunk=1]="Chunk",v[v.SH=2]="SH",v[v.Unused=3]="Unused"})(f||(f={}));let p=1;const m=[],C=[],E=s.slice(0,o).split(`
`);let c=0;for(const v of E)if(v.startsWith("property ")){const[,d,y]=v.split(" "),A=D._ValueNameToEnum(y);A!=85&&(A>=84?c=3:A>=64?c=Math.max(c,2):A>=48&&(c=Math.max(c,1)));const H=D._TypeNameToEnum(d);p==1?(C.push({value:A,type:H,offset:_}),_+=u[d]):p==0?(m.push({value:A,type:H,offset:a}),a+=u[d]):p==2&&m.push({value:A,type:H,offset:a}),u[d]||de.Warn(`Unsupported property type: ${d}.`)}else if(v.startsWith("element ")){const[,d]=v.split(" ");d=="chunk"?p=1:d=="vertex"?p=0:d=="sh"?p=2:p=3}const g=new DataView(e,o+n.length),S=new ArrayBuffer(D._RowOutputLength*i);let w=null,x=0;return c&&(x=((c+1)*(c+1)-1)*3,w=new ArrayBuffer(x*i)),{vertexCount:i,chunkCount:h,rowVertexLength:a,rowChunkLength:_,vertexProperties:m,chunkProperties:C,dataView:g,buffer:S,shDegree:c,shCoefficientCount:x,shBuffer:w}}static _GetCompressedChunks(e,t){if(!e.chunkCount)return null;const s=e.dataView,n=new Array(e.chunkCount);for(let o=0;o<e.chunkCount;o++){const i={min:new T,max:new T,minScale:new T,maxScale:new T,minColor:new T(0,0,0),maxColor:new T(1,1,1)};n[o]=i;for(let l=0;l<e.chunkProperties.length;l++){const h=e.chunkProperties[l];let a;if(h.type===0)a=s.getFloat32(h.offset+t.value,!0);else continue;switch(h.value){case 0:i.min.x=a;break;case 1:i.min.y=a;break;case 2:i.min.z=a;break;case 3:i.max.x=a;break;case 4:i.max.y=a;break;case 5:i.max.z=a;break;case 6:i.minScale.x=a;break;case 7:i.minScale.y=a;break;case 8:i.minScale.z=a;break;case 9:i.maxScale.x=a;break;case 10:i.maxScale.y=a;break;case 11:i.maxScale.z=a;break;case 34:i.minColor.x=a;break;case 35:i.minColor.y=a;break;case 36:i.minColor.z=a;break;case 37:i.maxColor.x=a;break;case 38:i.maxColor.y=a;break;case 39:i.maxColor.z=a;break}}t.value+=e.rowChunkLength}return n}static _GetSplat(e,t,s,n){const o=J.Quaternion[0],i=J.Vector3[0],l=D._RowOutputLength,h=e.buffer,a=e.dataView,_=new Float32Array(h,t*l,3),u=new Float32Array(h,t*l+12,3),f=new Uint8ClampedArray(h,t*l+24,4),p=new Uint8ClampedArray(h,t*l+28,4);let m=null;e.shBuffer&&(m=new Uint8ClampedArray(e.shBuffer,t*e.shCoefficientCount,e.shCoefficientCount));const C=t>>8;let E=255,c=0,g=0,S=0;const w=[];for(let x=0;x<e.vertexProperties.length;x++){const v=e.vertexProperties[x];let d;switch(v.type){case 0:d=a.getFloat32(n.value+v.offset,!0);break;case 1:d=a.getInt32(n.value+v.offset,!0);break;case 2:d=a.getUint32(n.value+v.offset,!0);break;case 3:d=a.getFloat64(n.value+v.offset,!0);break;case 4:d=a.getUint8(n.value+v.offset);break;default:continue}switch(v.value){case 12:{const y=s[C];At(d,i),_[0]=X.Lerp(y.min.x,y.max.x,i.x),_[1]=X.Lerp(y.min.y,y.max.y,i.y),_[2]=X.Lerp(y.min.z,y.max.z,i.z)}break;case 13:Rs(d,o),E=o.x,c=o.y,g=o.z,S=o.w;break;case 14:{const y=s[C];At(d,i),u[0]=Math.exp(X.Lerp(y.minScale.x,y.maxScale.x,i.x)),u[1]=Math.exp(X.Lerp(y.minScale.y,y.maxScale.y,i.y)),u[2]=Math.exp(X.Lerp(y.minScale.z,y.maxScale.z,i.z))}break;case 15:{const y=s[C];ks(d,f),f[0]=X.Lerp(y.minColor.x,y.maxColor.x,f[0]/255)*255,f[1]=X.Lerp(y.minColor.y,y.maxColor.y,f[1]/255)*255,f[2]=X.Lerp(y.minColor.z,y.maxColor.z,f[2]/255)*255}break;case 16:_[0]=d;break;case 17:_[1]=d;break;case 18:_[2]=d;break;case 19:u[0]=Math.exp(d);break;case 20:u[1]=Math.exp(d);break;case 21:u[2]=Math.exp(d);break;case 22:f[0]=d;break;case 23:f[1]=d;break;case 24:f[2]=d;break;case 26:f[0]=(.5+D._SH_C0*d)*255;break;case 27:f[1]=(.5+D._SH_C0*d)*255;break;case 28:f[2]=(.5+D._SH_C0*d)*255;break;case 29:f[3]=(.5+D._SH_C0*d)*255;break;case 25:f[3]=1/(1+Math.exp(-d))*255;break;case 30:E=d;break;case 31:c=d;break;case 32:g=d;break;case 33:S=d;break}if(m&&v.value>=40&&v.value<=84){const y=v.value-40;if(v.type==4&&e.chunkCount){const A=a.getUint8(e.rowChunkLength*e.chunkCount+e.vertexCount*e.rowVertexLength+t*e.shCoefficientCount+y);w[y]=(A*(8/255)-4)*127.5+127.5}else{const A=X.Clamp(d*127.5+127.5,0,255);w[y]=A}}}if(m){const x=e.shDegree==1?3:e.shDegree==2?8:15;for(let v=0;v<x;v++)m[v*3+0]=w[v],m[v*3+1]=w[v+x],m[v*3+2]=w[v+x*2]}o.set(c,g,S,E),o.normalize(),p[0]=o.w*127.5+127.5,p[1]=o.x*127.5+127.5,p[2]=o.y*127.5+127.5,p[3]=o.z*127.5+127.5,n.value+=e.rowVertexLength}static*ConvertPLYWithSHToSplat(e,t=!1){const s=D.ParseHeader(e);if(!s)return{buffer:e};const n={value:0},o=D._GetCompressedChunks(s,n);for(let l=0;l<s.vertexCount;l++)D._GetSplat(s,l,o,n),l%D._PlyConversionBatchSize===0&&t&&(yield);let i=null;if(s.shDegree&&s.shBuffer){const l=Math.ceil(s.shCoefficientCount/16);let h=0;const a=new Uint8Array(s.shBuffer);i=[];const _=s.vertexCount,u=be.LastCreatedEngine;if(u){const f=u.getCaps().maxTextureSize,p=Math.ceil(_/f);for(let m=0;m<l;m++){const C=new Uint8Array(p*f*4*4);i.push(C)}for(let m=0;m<_;m++)for(let C=0;C<s.shCoefficientCount;C++){const E=a[h++],c=Math.floor(C/16),g=i[c],S=C%16,w=m*16;g[S+w]=E}}}return{buffer:s.buffer,sh:i}}static*ConvertPLYToSplat(e,t=!1){const s=D.ParseHeader(e);if(!s)return e;const n={value:0},o=D._GetCompressedChunks(s,n);for(let i=0;i<s.vertexCount;i++)D._GetSplat(s,i,o,n),i%D._PlyConversionBatchSize===0&&t&&(yield);return s.buffer}static async ConvertPLYToSplatAsync(e){return await He(D.ConvertPLYToSplat(e,!0),ke())}static async ConvertPLYWithSHToSplatAsync(e){return await He(D.ConvertPLYWithSHToSplat(e,!0),ke())}async loadDataAsync(e){return await this.updateDataAsync(e)}async loadFileAsync(e,t){await ss(e,t||be.LastCreatedScene,{pluginOptions:{splat:{gaussianSplattingMesh:this}}})}dispose(e){if(this._covariancesATexture?.dispose(),this._covariancesBTexture?.dispose(),this._centersTexture?.dispose(),this._colorsTexture?.dispose(),this._shTextures)for(const t of this._shTextures)t.dispose();this._covariancesATexture=null,this._covariancesBTexture=null,this._centersTexture=null,this._colorsTexture=null,this._shTextures=null,this._worker?.terminate(),this._worker=null,this._cameraViewInfos.forEach(t=>{t.mesh.dispose()}),super.dispose(e,!0)}_copyTextures(e){if(this._covariancesATexture=e.covariancesATexture?.clone(),this._covariancesBTexture=e.covariancesBTexture?.clone(),this._centersTexture=e.centersTexture?.clone(),this._colorsTexture=e.colorsTexture?.clone(),e._shTextures){this._shTextures=[];for(const t of this._shTextures)this._shTextures?.push(t.clone())}}clone(e=""){const t=new D(e,void 0,this.getScene());t._copySource(this),t.makeGeometryUnique(),t._vertexCount=this._vertexCount,t._copyTextures(this),t._modelViewMatrix=Ce.Identity(),t._splatPositions=this._splatPositions,t._readyToDisplay=!1,t._disableDepthSort=this._disableDepthSort,t._instanciateWorker();const s=this.getBoundingInfo();return t.getBoundingInfo().reConstruct(s.minimum,s.maximum,this.getWorldMatrix()),t.forcedInstanceCount=this.forcedInstanceCount,t.setEnabled(!0),t}_makeEmptySplat(e,t,s,n){const o=this._useRGBACovariants?4:2;this._splatPositions[4*e+0]=0,this._splatPositions[4*e+1]=0,this._splatPositions[4*e+2]=0,t[e*4+0]=$(0),t[e*4+1]=$(0),t[e*4+2]=$(0),t[e*4+3]=$(0),s[e*o+0]=$(0),s[e*o+1]=$(0),n[e*4+3]=0}_makeSplat(e,t,s,n,o,i,l,h,a){const _=J.Matrix[0],u=J.Matrix[1],f=J.Quaternion[0],p=this._useRGBACovariants?4:2,m=t[8*e+0],C=t[8*e+1]*(a.flipY?-1:1),E=t[8*e+2];this._splatPositions[4*e+0]=m,this._splatPositions[4*e+1]=C,this._splatPositions[4*e+2]=E,l.minimizeInPlaceFromFloats(m,C,E),h.maximizeInPlaceFromFloats(m,C,E),f.set((s[32*e+28+1]-127.5)/127.5,(s[32*e+28+2]-127.5)/127.5,(s[32*e+28+3]-127.5)/127.5,-(s[32*e+28+0]-127.5)/127.5),f.normalize(),f.toRotationMatrix(_),Ce.ScalingToRef(t[8*e+3+0]*2,t[8*e+3+1]*2,t[8*e+3+2]*2,u);const c=_.multiplyToRef(u,J.Matrix[0]).m,g=this._tmpCovariances;g[0]=c[0]*c[0]+c[1]*c[1]+c[2]*c[2],g[1]=c[0]*c[4]+c[1]*c[5]+c[2]*c[6],g[2]=c[0]*c[8]+c[1]*c[9]+c[2]*c[10],g[3]=c[4]*c[4]+c[5]*c[5]+c[6]*c[6],g[4]=c[4]*c[8]+c[5]*c[9]+c[6]*c[10],g[5]=c[8]*c[8]+c[9]*c[9]+c[10]*c[10];let S=-1e4;for(let x=0;x<6;x++)S=Math.max(S,Math.abs(g[x]));this._splatPositions[4*e+3]=S;const w=S;n[e*4+0]=$(g[0]/w),n[e*4+1]=$(g[1]/w),n[e*4+2]=$(g[2]/w),n[e*4+3]=$(g[3]/w),o[e*p+0]=$(g[4]/w),o[e*p+1]=$(g[5]/w),i[e*4+0]=s[32*e+24+0],i[e*4+1]=s[32*e+24+1],i[e*4+2]=s[32*e+24+2],i[e*4+3]=s[32*e+24+3]}_updateTextures(e,t,s,n){const o=this._getTextureSize(this._vertexCount),i=(_,u,f,p)=>new we(_,u,f,p,this._scene,!1,!1,2,1),l=(_,u,f,p)=>new we(_,u,f,p,this._scene,!1,!1,2,0),h=(_,u,f,p)=>new we(_,u,f,p,this._scene,!1,!1,1,7),a=(_,u,f,p)=>new we(_,u,f,p,this._scene,!1,!1,2,2);if(this._covariancesATexture){this._delayedTextureUpdate={covA:e,covB:t,colors:s,centers:this._splatPositions,sh:n};const _=Float32Array.from(this._splatPositions),u=this._vertexCount;this._worker&&this._worker.postMessage({positions:_,vertexCount:u},[_.buffer]),this._postToWorker(!0)}else{if(this._covariancesATexture=a(e,o.x,o.y,5),this._covariancesBTexture=a(t,o.x,o.y,this._useRGBACovariants?5:7),this._centersTexture=i(this._splatPositions,o.x,o.y,5),this._colorsTexture=l(s,o.x,o.y,5),n){this._shTextures=[];for(const _ of n){const u=new Uint32Array(_.buffer),f=h(u,o.x,o.y,11);f.wrapU=0,f.wrapV=0,this._shTextures.push(f)}}this._instanciateWorker()}}*_updateData(e,t,s,n={flipY:!1}){this._covariancesATexture||(this._readyToDisplay=!1);const o=new Uint8Array(e),i=new Float32Array(o.buffer);this._keepInRam&&(this._splatsData=e);const l=o.length/D._RowOutputLength;l!=this._vertexCount&&this._updateSplatIndexBuffer(l),this._vertexCount=l,this._shDegree=s?s.length:0;const h=this._getTextureSize(l),a=h.x*h.y,_=D.ProgressiveUpdateAmount??h.y,u=h.x*_;this._splatPositions=new Float32Array(4*a);const f=new Uint16Array(a*4),p=new Uint16Array((this._useRGBACovariants?4:2)*a),m=new Uint8Array(a*4),C=new T(Number.MAX_VALUE,Number.MAX_VALUE,Number.MAX_VALUE),E=new T(-Number.MAX_VALUE,-Number.MAX_VALUE,-Number.MAX_VALUE);if(D.ProgressiveUpdateAmount){this._updateTextures(f,p,m,s),this.setEnabled(!0);const c=Math.ceil(h.y/_);for(let w=0;w<c;w++){const x=w*_,v=x*h.x;for(let d=0;d<u;d++)this._makeSplat(v+d,i,o,f,p,m,C,E,n);this._updateSubTextures(this._splatPositions,f,p,m,x,Math.min(_,h.y-x)),this.getBoundingInfo().reConstruct(C,E,this.getWorldMatrix()),t&&(yield)}const g=Float32Array.from(this._splatPositions),S=this._vertexCount;this._worker&&this._worker.postMessage({positions:g,vertexCount:S},[g.buffer]),this._sortIsDirty=!0}else{const c=l+15&-16;for(let g=0;g<l;g++)this._makeSplat(g,i,o,f,p,m,C,E,n),t&&g%D._SplatBatchSize===0&&(yield);for(let g=l;g<c;g++)this._makeEmptySplat(g,f,p,m);this._updateTextures(f,p,m,s),this.getBoundingInfo().reConstruct(C,E,this.getWorldMatrix()),this.setEnabled(!0),this._sortIsDirty=!0}this._postToWorker(!0)}async updateDataAsync(e,t){return await He(this._updateData(e,!0,t),ke())}updateData(e,t,s={flipY:!0}){rs(this._updateData(e,!1,t,s))}refreshBoundingInfo(){return this.thinInstanceRefreshBoundingInfo(!1),this}_updateSplatIndexBuffer(e){const t=e+15&-16;if(!this._splatIndex||e>this._splatIndex.length){this._splatIndex=new Float32Array(t);for(let s=0;s<t;s++)this._splatIndex[s]=s;this._cameraViewInfos.forEach(s=>{s.mesh.thinInstanceSetBuffer("splatIndex",this._splatIndex,16,!1)})}this.forcedInstanceCount=t>>4}_updateSubTextures(e,t,s,n,o,i,l){const h=(c,g,S,w,x)=>{this.getEngine().updateTextureData(c.getInternalTexture(),g,0,w,S,x,0,0,!1)},a=this._getTextureSize(this._vertexCount),_=this._useRGBACovariants?4:2,u=o*a.x,f=i*a.x,p=new Uint16Array(t.buffer,u*4*Uint16Array.BYTES_PER_ELEMENT,f*4),m=new Uint16Array(s.buffer,u*_*Uint16Array.BYTES_PER_ELEMENT,f*_),C=new Uint8Array(n.buffer,u*4,f*4),E=new Float32Array(e.buffer,u*4*Float32Array.BYTES_PER_ELEMENT,f*4);if(h(this._covariancesATexture,p,a.x,o,i),h(this._covariancesBTexture,m,a.x,o,i),h(this._centersTexture,E,a.x,o,i),h(this._colorsTexture,C,a.x,o,i),l)for(let c=0;c<l.length;c++){const S=new Uint32Array(l[c].buffer,u*4*4,f*4);h(this._shTextures[c],S,a.x,o,i)}}_instanciateWorker(){if(!this._vertexCount||this._disableDepthSort||(this._updateSplatIndexBuffer(this._vertexCount),_native))return;this._worker?.terminate(),this._worker=new Worker(URL.createObjectURL(new Blob(["(",D._CreateWorker.toString(),")(self)"],{type:"application/javascript"})));const e=this._vertexCount+15&-16;this._depthMix=new BigInt64Array(e);const t=Float32Array.from(this._splatPositions);this._worker.postMessage({positions:t},[t.buffer]),this._worker.onmessage=s=>{this._depthMix=s.data.depthMix;const n=s.data.cameraId,o=new Uint32Array(s.data.depthMix.buffer);if(this._splatIndex)for(let l=0;l<e;l++)this._splatIndex[l]=o[2*l];if(this._delayedTextureUpdate){const l=this._getTextureSize(e);this._updateSubTextures(this._delayedTextureUpdate.centers,this._delayedTextureUpdate.covA,this._delayedTextureUpdate.covB,this._delayedTextureUpdate.colors,0,l.y,this._delayedTextureUpdate.sh),this._delayedTextureUpdate=null}const i=this._cameraViewInfos.get(n);i&&(i.splatIndexBufferSet?i.mesh.thinInstanceBufferUpdated("splatIndex"):(i.mesh.thinInstanceSetBuffer("splatIndex",this._splatIndex,16,!1),i.splatIndexBufferSet=!0)),this._canPostToWorker=!0,this._readyToDisplay=!0,this._sortIsDirty&&(this._postToWorker(!0),this._sortIsDirty=!1)}}_getTextureSize(e){const t=this._scene.getEngine(),s=t.getCaps().maxTextureSize;let n=1;if(t.version===1&&!t.isWebGPU)for(;s*n<e;)n*=2;else n=Math.ceil(e/s);return n>s&&(de.Error("GaussianSplatting texture size: ("+s+", "+n+"), maxTextureSize: "+s),n=s),new oe(s,n)}}D._RowOutputLength=32;D._SH_C0=.28209479177387814;D._SplatBatchSize=327680;D._PlyConversionBatchSize=32768;D._BatchSize=16;D.ProgressiveUpdateAmount=0;D._CreateWorker=function(r){let e,t,s,n;r.onmessage=o=>{if(o.data.positions)e=o.data.positions;else{const i=o.data.cameraId,l=o.data.view,h=e.length/4+15&-16;if(!e||!l)throw new Error("positions or view is not defined!");t=o.data.depthMix,s=new Uint32Array(t.buffer),n=new Float32Array(t.buffer);for(let _=0;_<h;_++)s[2*_]=_;let a=-1;o.data.useRightHandedSystem&&(a=1);for(let _=0;_<h;_++)n[2*_+1]=1e4+(l[2]*e[4*_+0]+l[6]*e[4*_+1]+l[10]*e[4*_+2])*a;t.sort(),r.postMessage({depthMix:t,cameraId:i},[t.buffer])}}};class Os{constructor(e,t,s,n,o){this.idx=0,this.color=new L(1,1,1,1),this.position=T.Zero(),this.rotation=T.Zero(),this.uv=new oe(0,0),this.velocity=T.Zero(),this.pivot=T.Zero(),this.translateFromPivot=!1,this._pos=0,this._ind=0,this.groupId=0,this.idxInGroup=0,this._stillInvisible=!1,this._rotationMatrix=[1,0,0,0,1,0,0,0,1],this.parentId=null,this._globalPosition=T.Zero(),this.idx=e,this._group=t,this.groupId=s,this.idxInGroup=n,this._pcs=o}get size(){return this.size}set size(e){this.size=e}get quaternion(){return this.rotationQuaternion}set quaternion(e){this.rotationQuaternion=e}intersectsMesh(e,t){if(!e.hasBoundingInfo)return!1;if(!this._pcs.mesh)throw new Error("Point Cloud System doesnt contain the Mesh");if(t)return e.getBoundingInfo().boundingSphere.intersectsPoint(this.position.add(this._pcs.mesh.position));const s=e.getBoundingInfo().boundingBox,n=s.maximumWorld.x,o=s.minimumWorld.x,i=s.maximumWorld.y,l=s.minimumWorld.y,h=s.maximumWorld.z,a=s.minimumWorld.z,_=this.position.x+this._pcs.mesh.position.x,u=this.position.y+this._pcs.mesh.position.y,f=this.position.z+this._pcs.mesh.position.z;return o<=_&&_<=n&&l<=u&&u<=i&&a<=f&&f<=h}getRotationMatrix(e){let t;if(this.rotationQuaternion)t=this.rotationQuaternion;else{t=J.Quaternion[0];const s=this.rotation;os.RotationYawPitchRollToRef(s.y,s.x,s.z,t)}t.toRotationMatrix(e)}}class Oe{get groupID(){return this.groupId}set groupID(e){this.groupId=e}constructor(e,t){this.groupId=e,this._positionFunction=t}}var zt;(function(r){r[r.Color=2]="Color",r[r.UV=1]="UV",r[r.Random=0]="Random",r[r.Stated=3]="Stated"})(zt||(zt={}));class Fs{get positions(){return this._positions32}get colors(){return this._colors32}get uvs(){return this._uvs32}constructor(e,t,s,n){this.particles=new Array,this.nbParticles=0,this.counter=0,this.vars={},this._promises=[],this._positions=new Array,this._indices=new Array,this._normals=new Array,this._colors=new Array,this._uvs=new Array,this._updatable=!0,this._isVisibilityBoxLocked=!1,this._alwaysVisible=!1,this._groups=new Array,this._groupCounter=0,this._computeParticleColor=!0,this._computeParticleTexture=!0,this._computeParticleRotation=!0,this._computeBoundingBox=!1,this._isReady=!1,this.name=e,this._size=t,this._scene=s||be.LastCreatedScene,n&&n.updatable!==void 0?this._updatable=n.updatable:this._updatable=!0}async buildMeshAsync(e){return await Promise.all(this._promises),this._isReady=!0,await this._buildMeshAsync(e)}async _buildMeshAsync(e){this.nbParticles===0&&this.addPoints(1),this._positions32=new Float32Array(this._positions),this._uvs32=new Float32Array(this._uvs),this._colors32=new Float32Array(this._colors);const t=new Ne;t.set(this._positions32,q.PositionKind),this._uvs32.length>0&&t.set(this._uvs32,q.UVKind);let s=0;this._colors32.length>0&&(s=1,t.set(this._colors32,q.ColorKind));const n=new Ie(this.name,this._scene);t.applyToMesh(n,this._updatable),this.mesh=n,this._positions=null,this._uvs=null,this._colors=null,this._updatable||(this.particles.length=0);let o=e;return o||(o=new hs("point cloud material",this._scene),o.emissiveColor=new Se(s,s,s),o.disableLighting=!0,o.pointsCloud=!0,o.pointSize=this._size),n.material=o,n}_addParticle(e,t,s,n){const o=new Os(e,t,s,n,this);return this.particles.push(o),o}_randomUnitVector(e){e.position=new T(Math.random(),Math.random(),Math.random()),e.color=new L(1,1,1,1)}_getColorIndicesForCoord(e,t,s,n){const o=e._groupImageData,i=s*(n*4)+t*4,l=[i,i+1,i+2,i+3],h=l[0],a=l[1],_=l[2],u=l[3],f=o[h],p=o[a],m=o[_],C=o[u];return new L(f/255,p/255,m/255,C)}_setPointsColorOrUV(e,t,s,n,o,i,l,h){h=h??0,s&&e.updateFacetData();const _=2*e.getBoundingInfo().boundingSphere.radius;let u=e.getVerticesData(q.PositionKind);const f=e.getIndices(),p=e.getVerticesData(q.UVKind+(h?h+1:"")),m=e.getVerticesData(q.ColorKind),C=T.Zero();e.computeWorldMatrix();const E=e.getWorldMatrix();if(!E.isIdentity()){u=u.slice(0);for(let j=0;j<u.length/3;j++)T.TransformCoordinatesFromFloatsToRef(u[3*j],u[3*j+1],u[3*j+2],E,C),u[3*j]=C.x,u[3*j+1]=C.y,u[3*j+2]=C.z}let c=0,g=0,S=0,w=0,x=0,v=0,d=0,y=0,A=0,H=0,B=0,b=0,U=0;const W=T.Zero(),z=T.Zero(),R=T.Zero(),V=T.Zero(),k=T.Zero();let Z=0,O=0,I=0,K=0,Y=0,ne=0;const te=oe.Zero(),F=oe.Zero(),We=oe.Zero(),Ve=oe.Zero(),Ze=oe.Zero();let je=0,Xe=0,Ge=0,$e=0,qe=0,Ke=0,Je=0,Qe=0,Le=0,Ye=0,et=0,tt=0;const fe=he.Zero(),Ee=he.Zero(),st=he.Zero(),rt=he.Zero(),ot=he.Zero();let se=0,pe=0;l=l||0;let ue,_e,P=new he(0,0,0,0),Te=T.Zero(),Ae=T.Zero(),nt=T.Zero(),ie=0,it=T.Zero(),at=0,ct=0;const me=new ls(T.Zero(),new T(1,0,0));let De,xe=T.Zero();for(let j=0;j<f.length/3;j++){g=f[3*j],S=f[3*j+1],w=f[3*j+2],x=u[3*g],v=u[3*g+1],d=u[3*g+2],y=u[3*S],A=u[3*S+1],H=u[3*S+2],B=u[3*w],b=u[3*w+1],U=u[3*w+2],W.set(x,v,d),z.set(y,A,H),R.set(B,b,U),z.subtractToRef(W,V),R.subtractToRef(z,k),p&&(Z=p[2*g],O=p[2*g+1],I=p[2*S],K=p[2*S+1],Y=p[2*w],ne=p[2*w+1],te.set(Z,O),F.set(I,K),We.set(Y,ne),F.subtractToRef(te,Ve),We.subtractToRef(F,Ze)),m&&n&&(je=m[4*g],Xe=m[4*g+1],Ge=m[4*g+2],$e=m[4*g+3],qe=m[4*S],Ke=m[4*S+1],Je=m[4*S+2],Qe=m[4*S+3],Le=m[4*w],Ye=m[4*w+1],et=m[4*w+2],tt=m[4*w+3],fe.set(je,Xe,Ge,$e),Ee.set(qe,Ke,Je,Qe),st.set(Le,Ye,et,tt),Ee.subtractToRef(fe,rt),st.subtractToRef(Ee,ot));let Me,lt,ht,ft,ut,ae,ce,ve;const dt=new Se(0,0,0),ge=new Se(0,0,0);let le,G;for(let ze=0;ze<t._groupDensity[j];ze++)c=this.particles.length,this._addParticle(c,t,this._groupCounter,j+ze),G=this.particles[c],se=Math.sqrt(re(0,1)),pe=re(0,1),ue=W.add(V.scale(se)).add(k.scale(se*pe)),s&&(Te=e.getFacetNormal(j).normalize().scale(-1),Ae=V.clone().normalize(),nt=T.Cross(Te,Ae),ie=re(0,2*Math.PI),it=Ae.scale(Math.cos(ie)).add(nt.scale(Math.sin(ie))),ie=re(.1,Math.PI/2),xe=it.scale(Math.cos(ie)).add(Te.scale(Math.sin(ie))),me.origin=ue.add(xe.scale(1e-5)),me.direction=xe,me.length=_,De=me.intersectsMesh(e),De.hit&&(ct=De.pickedPoint.subtract(ue).length(),at=re(0,1)*ct,ue.addInPlace(xe.scale(at)))),G.position=ue.clone(),this._positions.push(G.position.x,G.position.y,G.position.z),n!==void 0?p&&(_e=te.add(Ve.scale(se)).add(Ze.scale(se*pe)),n?o&&t._groupImageData!==null?(Me=t._groupImgWidth,lt=t._groupImgHeight,le=this._getColorIndicesForCoord(t,Math.round(_e.x*Me),Math.round(_e.y*lt),Me),G.color=le,this._colors.push(le.r,le.g,le.b,le.a)):m?(P=fe.add(rt.scale(se)).add(ot.scale(se*pe)),G.color=new L(P.x,P.y,P.z,P.w),this._colors.push(P.x,P.y,P.z,P.w)):(P=fe.set(Math.random(),Math.random(),Math.random(),1),G.color=new L(P.x,P.y,P.z,P.w),this._colors.push(P.x,P.y,P.z,P.w)):(G.uv=_e.clone(),this._uvs.push(G.uv.x,G.uv.y))):(i?(dt.set(i.r,i.g,i.b),ht=re(-l,l),ft=re(-l,l),ve=dt.toHSV(),ut=ve.r,ae=ve.g+ht,ce=ve.b+ft,ae<0&&(ae=0),ae>1&&(ae=1),ce<0&&(ce=0),ce>1&&(ce=1),Se.HSVtoRGBToRef(ut,ae,ce,ge),P.set(ge.r,ge.g,ge.b,1)):P=fe.set(Math.random(),Math.random(),Math.random(),1),G.color=new L(P.x,P.y,P.z,P.w),this._colors.push(P.x,P.y,P.z,P.w))}}_colorFromTexture(e,t,s){if(e.material===null){de.Warn(e.name+"has no material."),t._groupImageData=null,this._setPointsColorOrUV(e,t,s,!0,!1);return}const o=e.material.getActiveTextures();if(o.length===0){de.Warn(e.name+"has no usable texture."),t._groupImageData=null,this._setPointsColorOrUV(e,t,s,!0,!1);return}const i=e.clone();i.setEnabled(!1),this._promises.push(new Promise(l=>{ns.WhenAllReady(o,()=>{let h=t._textureNb;h<0&&(h=0),h>o.length-1&&(h=o.length-1);const a=()=>{t._groupImgWidth=o[h].getSize().width,t._groupImgHeight=o[h].getSize().height,this._setPointsColorOrUV(i,t,s,!0,!0,void 0,void 0,o[h].coordinatesIndex),i.dispose(),l()};t._groupImageData=null;const _=o[h].readPixels();_?_.then(u=>{t._groupImageData=u,a()}):a()})}))}_calculateDensity(e,t,s){let n,o,i,l,h,a,_,u,f,p,m,C;const E=T.Zero(),c=T.Zero(),g=T.Zero(),S=T.Zero(),w=T.Zero(),x=T.Zero();let v;const d=[];let y=0;const A=s.length/3;for(let b=0;b<A;b++)n=s[3*b],o=s[3*b+1],i=s[3*b+2],l=t[3*n],h=t[3*n+1],a=t[3*n+2],_=t[3*o],u=t[3*o+1],f=t[3*o+2],p=t[3*i],m=t[3*i+1],C=t[3*i+2],E.set(l,h,a),c.set(_,u,f),g.set(p,m,C),c.subtractToRef(E,S),g.subtractToRef(c,w),T.CrossToRef(S,w,x),v=.5*x.length(),y+=v,d[b]=y;const H=new Array(A);let B=e;for(let b=A-1;b>0;b--){const U=d[b];if(U===0)H[b]=0;else{const z=(U-d[b-1])/U*B,R=Math.floor(z),V=z-R,k=+(Math.random()<V),Z=R+k;H[b]=Z,B-=Z}}return H[0]=B,H}addPoints(e,t=this._randomUnitVector){const s=new Oe(this._groupCounter,t);let n,o=this.nbParticles;for(let i=0;i<e;i++)n=this._addParticle(o,s,this._groupCounter,i),s&&s._positionFunction&&s._positionFunction(n,o,i),this._positions.push(n.position.x,n.position.y,n.position.z),n.color&&this._colors.push(n.color.r,n.color.g,n.color.b,n.color.a),n.uv&&this._uvs.push(n.uv.x,n.uv.y),o++;return this.nbParticles+=e,this._groupCounter++,this._groupCounter}addSurfacePoints(e,t,s,n,o){let i=s||0;(isNaN(i)||i<0||i>3)&&(i=0);const l=e.getVerticesData(q.PositionKind),h=e.getIndices();this._groups.push(this._groupCounter);const a=new Oe(this._groupCounter,null);switch(a._groupDensity=this._calculateDensity(t,l,h),i===2?a._textureNb=n||0:n=n||new L(1,1,1,1),i){case 2:this._colorFromTexture(e,a,!1);break;case 1:this._setPointsColorOrUV(e,a,!1,!1,!1);break;case 0:this._setPointsColorOrUV(e,a,!1);break;case 3:this._setPointsColorOrUV(e,a,!1,void 0,void 0,n,o);break}return this.nbParticles+=t,this._groupCounter++,this._groupCounter-1}addVolumePoints(e,t,s,n,o){let i=s||0;(isNaN(i)||i<0||i>3)&&(i=0);const l=e.getVerticesData(q.PositionKind),h=e.getIndices();this._groups.push(this._groupCounter);const a=new Oe(this._groupCounter,null);switch(a._groupDensity=this._calculateDensity(t,l,h),i===2?a._textureNb=n||0:n=n||new L(1,1,1,1),i){case 2:this._colorFromTexture(e,a,!0);break;case 1:this._setPointsColorOrUV(e,a,!0,!1,!1);break;case 0:this._setPointsColorOrUV(e,a,!0);break;case 3:this._setPointsColorOrUV(e,a,!0,void 0,void 0,n,o);break}return this.nbParticles+=t,this._groupCounter++,this._groupCounter-1}setParticles(e=0,t=this.nbParticles-1,s=!0){if(!this._updatable||!this._isReady)return this;this.beforeUpdateParticles(e,t,s);const n=J.Matrix[0],o=this.mesh,i=this._colors32,l=this._positions32,h=this._uvs32,a=J.Vector3,_=a[5].copyFromFloats(1,0,0),u=a[6].copyFromFloats(0,1,0),f=a[7].copyFromFloats(0,0,1),p=a[8].setAll(Number.MAX_VALUE),m=a[9].setAll(-Number.MAX_VALUE);Ce.IdentityToRef(n);let C=0;if(this.mesh?.isFacetDataEnabled&&(this._computeBoundingBox=!0),t=t>=this.nbParticles?this.nbParticles-1:t,this._computeBoundingBox&&(e!=0||t!=this.nbParticles-1)){const S=this.mesh?.getBoundingInfo();S&&(p.copyFrom(S.minimum),m.copyFrom(S.maximum))}C=0;let E=0,c=0,g=0;for(let S=e;S<=t;S++){const w=this.particles[S];C=w.idx,E=3*C,c=4*C,g=2*C,this.updateParticle(w);const x=w._rotationMatrix,v=w.position,d=w._globalPosition;if(this._computeParticleRotation&&w.getRotationMatrix(n),w.parentId!==null){const O=this.particles[w.parentId],I=O._rotationMatrix,K=O._globalPosition,Y=v.x*I[1]+v.y*I[4]+v.z*I[7],ne=v.x*I[0]+v.y*I[3]+v.z*I[6],te=v.x*I[2]+v.y*I[5]+v.z*I[8];if(d.x=K.x+ne,d.y=K.y+Y,d.z=K.z+te,this._computeParticleRotation){const F=n.m;x[0]=F[0]*I[0]+F[1]*I[3]+F[2]*I[6],x[1]=F[0]*I[1]+F[1]*I[4]+F[2]*I[7],x[2]=F[0]*I[2]+F[1]*I[5]+F[2]*I[8],x[3]=F[4]*I[0]+F[5]*I[3]+F[6]*I[6],x[4]=F[4]*I[1]+F[5]*I[4]+F[6]*I[7],x[5]=F[4]*I[2]+F[5]*I[5]+F[6]*I[8],x[6]=F[8]*I[0]+F[9]*I[3]+F[10]*I[6],x[7]=F[8]*I[1]+F[9]*I[4]+F[10]*I[7],x[8]=F[8]*I[2]+F[9]*I[5]+F[10]*I[8]}}else if(d.x=0,d.y=0,d.z=0,this._computeParticleRotation){const O=n.m;x[0]=O[0],x[1]=O[1],x[2]=O[2],x[3]=O[4],x[4]=O[5],x[5]=O[6],x[6]=O[8],x[7]=O[9],x[8]=O[10]}const A=a[11];w.translateFromPivot?A.setAll(0):A.copyFrom(w.pivot);const H=a[0];H.copyFrom(w.position);const B=H.x-w.pivot.x,b=H.y-w.pivot.y,U=H.z-w.pivot.z;let W=B*x[0]+b*x[3]+U*x[6],z=B*x[1]+b*x[4]+U*x[7],R=B*x[2]+b*x[5]+U*x[8];W+=A.x,z+=A.y,R+=A.z;const V=l[E]=d.x+_.x*W+u.x*z+f.x*R,k=l[E+1]=d.y+_.y*W+u.y*z+f.y*R,Z=l[E+2]=d.z+_.z*W+u.z*z+f.z*R;if(this._computeBoundingBox&&(p.minimizeInPlaceFromFloats(V,k,Z),m.maximizeInPlaceFromFloats(V,k,Z)),this._computeParticleColor&&w.color){const O=w.color,I=this._colors32;I[c]=O.r,I[c+1]=O.g,I[c+2]=O.b,I[c+3]=O.a}if(this._computeParticleTexture&&w.uv){const O=w.uv,I=this._uvs32;I[g]=O.x,I[g+1]=O.y}}return o&&(s&&(this._computeParticleColor&&o.updateVerticesData(q.ColorKind,i,!1,!1),this._computeParticleTexture&&o.updateVerticesData(q.UVKind,h,!1,!1),o.updateVerticesData(q.PositionKind,l,!1,!1)),this._computeBoundingBox&&(o.hasBoundingInfo?o.getBoundingInfo().reConstruct(p,m,o._worldMatrix):o.buildBoundingInfo(p,m,o._worldMatrix))),this.afterUpdateParticles(e,t,s),this}dispose(){this.mesh?.dispose(),this.vars=null,this._positions=null,this._indices=null,this._normals=null,this._uvs=null,this._colors=null,this._indices32=null,this._positions32=null,this._uvs32=null,this._colors32=null}refreshVisibleSize(){return this._isVisibilityBoxLocked||this.mesh?.refreshBoundingInfo(),this}setVisibilityBox(e){if(!this.mesh)return;const t=e/2;this.mesh.buildBoundingInfo(new T(-t,-t,-t),new T(t,t,t))}get isAlwaysVisible(){return this._alwaysVisible}set isAlwaysVisible(e){this.mesh&&(this._alwaysVisible=e,this.mesh.alwaysSelectAsActiveMesh=e)}set computeParticleRotation(e){this._computeParticleRotation=e}set computeParticleColor(e){this._computeParticleColor=e}set computeParticleTexture(e){this._computeParticleTexture=e}get computeParticleColor(){return this._computeParticleColor}get computeParticleTexture(){return this._computeParticleTexture}set computeBoundingBox(e){this._computeBoundingBox=e}get computeBoundingBox(){return this._computeBoundingBox}initParticles(){}recycleParticle(e){return e}updateParticle(e){return e}beforeUpdateParticles(e,t,s){}afterUpdateParticles(e,t,s){}}function Bs(r,e,t){const s=new Uint8Array(r),n=new Uint32Array(r.slice(0,12)),o=n[2],i=s[12],l=s[13],h=s[14],a=s[15],_=n[1];if(a||n[0]!=1347635022||_!=2&&_!=3)return new Promise(d=>{d({mode:3,data:f,hasVertexColors:!1})});const u=32,f=new ArrayBuffer(u*o),p=1/(1<<l),m=new Int32Array(1),C=new Uint8Array(m.buffer),E=function(d,y){return C[0]=d[y+0],C[1]=d[y+1],C[2]=d[y+2],C[3]=d[y+2]&128?255:0,m[0]*p};let c=16;const g=new Float32Array(f),S=new Float32Array(f),w=new Uint8ClampedArray(f),x=new Uint8ClampedArray(f);for(let d=0;d<o;d++)g[d*8+0]=E(s,c+0),g[d*8+1]=E(s,c+3),g[d*8+2]=E(s,c+6),c+=9;const v=.282;for(let d=0;d<o;d++){for(let y=0;y<3;y++){const H=(s[c+o+d*3+y]-127.5)/(.15*255);w[d*32+24+y]=X.Clamp((.5+v*H)*255,0,255)}w[d*32+24+3]=s[c+d]}c+=o*4;for(let d=0;d<o;d++)S[d*8+3+0]=Math.exp(s[c+0]/16-10),S[d*8+3+1]=Math.exp(s[c+1]/16-10),S[d*8+3+2]=Math.exp(s[c+2]/16-10),c+=3;if(_>=3){const d=Math.SQRT1_2;for(let y=0;y<o;y++){const A=[s[c+0],s[c+1],s[c+2],s[c+3]],H=A[0]+(A[1]<<8)+(A[2]<<16)+(A[3]<<24),B=511,b=[],U=H>>>30;let W=H,z=0;for(let k=3;k>=0;--k)if(k!==U){const Z=W&B,O=W>>>9&1;W=W>>>10,b[k]=d*(Z/B),O===1&&(b[k]=-b[k]),z+=b[k]*b[k]}const R=1-z;b[U]=Math.sqrt(Math.max(R,0));const V=[3,0,1,2];for(let k=0;k<4;k++)x[y*32+28+k]=Math.round(127.5+b[V[k]]*127.5);c+=4}}else for(let d=0;d<o;d++){const y=s[c+0],A=s[c+1],H=s[c+2],B=y/127.5-1,b=A/127.5-1,U=H/127.5-1;x[d*32+28+1]=y,x[d*32+28+2]=A,x[d*32+28+3]=H;const W=1-(B*B+b*b+U*U);x[d*32+28+0]=127.5+Math.sqrt(W<0?0:W)*127.5,c+=3}if(i){const y=((i+1)*(i+1)-1)*3,A=Math.ceil(y/16);let H=c;const B=[],U=e.getEngine().getCaps().maxTextureSize,W=Math.ceil(o/U);for(let z=0;z<A;z++){const R=new Uint8Array(W*U*4*4);B.push(R)}for(let z=0;z<o;z++)for(let R=0;R<y;R++){const V=s[H++],k=Math.floor(R/16),Z=B[k],O=R%16,I=z*16;Z[O+I]=V}return new Promise(z=>{z({mode:0,data:f,hasVertexColors:!1,sh:B,trainedWithAntialiasing:!!h})})}return new Promise(d=>{d({mode:0,data:f,hasVertexColors:!1,trainedWithAntialiasing:!!h})})}const Ht=.28209479177387814;async function kt(r,e,t){return await new Promise((n,o)=>{const i=t.createCanvasImage();if(!i)throw new Error("Failed to create ImageBitmap");i.onload=()=>{try{const h=t.createCanvas(i.width,i.height);if(!h)throw new Error("Failed to create canvas");const a=h.getContext("2d");if(!a)throw new Error("Failed to get 2D context");a.drawImage(i,0,0);const _=a.getImageData(0,0,h.width,h.height);n({bits:new Uint8Array(_.data.buffer),width:_.width})}catch(h){o(`Error loading image ${i.src} with exception: ${h}`)}},i.onerror=h=>{o(`Error loading image ${i.src} with exception: ${h}`)},i.crossOrigin="anonymous";let l;if(typeof r=="string"){if(!e)throw new Error("filename is required when using a URL");i.src=r+e}else{const h=new Blob([r],{type:"image/webp"});l=URL.createObjectURL(h),i.src=l}})}async function Us(r,e,t){const s=r.count?r.count:r.means.shape[0],n=32,o=new ArrayBuffer(n*s),i=new Float32Array(o),l=new Float32Array(o),h=new Uint8ClampedArray(o),a=new Uint8ClampedArray(o),_=c=>Math.sign(c)*(Math.exp(Math.abs(c))-1),u=e[0].bits,f=e[1].bits;if(!Array.isArray(r.means.mins)||!Array.isArray(r.means.maxs))throw new Error("Missing arrays in SOG data.");for(let c=0;c<s;c++){const g=c*4;for(let S=0;S<3;S++){const w=r.means.mins[S],x=r.means.maxs[S],v=f[g+S],d=u[g+S],y=v<<8|d,A=X.Lerp(w,x,y/65535);i[c*8+S]=_(A)}}const p=e[2].bits;if(r.version===2){if(!r.scales.codebook)throw new Error("Missing codebook in SOG version 2 scales data.");for(let c=0;c<s;c++){const g=c*4;for(let S=0;S<3;S++){const w=r.scales.codebook[p[g+S]],x=Math.exp(w);l[c*8+3+S]=x}}}else{if(!Array.isArray(r.scales.mins)||!Array.isArray(r.scales.maxs))throw new Error("Missing arrays in SOG scales data.");for(let c=0;c<s;c++){const g=c*4;for(let S=0;S<3;S++){const w=p[g+S],x=X.Lerp(r.scales.mins[S],r.scales.maxs[S],w/255),v=Math.exp(x);l[c*8+3+S]=v}}}const m=e[4].bits;if(r.version===2){if(!r.sh0.codebook)throw new Error("Missing codebook in SOG version 2 sh0 data.");for(let c=0;c<s;c++){const g=c*4;for(let S=0;S<3;S++){const w=.5+r.sh0.codebook[m[g+S]]*Ht;h[c*32+24+S]=Math.max(0,Math.min(255,Math.round(255*w)))}h[c*32+24+3]=m[g+3]}}else{if(!Array.isArray(r.sh0.mins)||!Array.isArray(r.sh0.maxs))throw new Error("Missing arrays in SOG sh0 data.");for(let c=0;c<s;c++){const g=c*4;for(let S=0;S<4;S++){const w=r.sh0.mins[S],x=r.sh0.maxs[S],v=m[g+S],d=X.Lerp(w,x,v/255);let y;S<3?y=.5+d*Ht:y=1/(1+Math.exp(-d)),h[c*32+24+S]=Math.max(0,Math.min(255,Math.round(255*y)))}}}const C=c=>(c/255-.5)*2/Math.SQRT2,E=e[3].bits;for(let c=0;c<s;c++){const g=E[c*4+0],S=E[c*4+1],w=E[c*4+2],x=E[c*4+3],v=C(g),d=C(S),y=C(w),A=x-252,H=v*v+d*d+y*y,B=Math.sqrt(Math.max(0,1-H));let b;switch(A){case 0:b=[B,v,d,y];break;case 1:b=[v,B,d,y];break;case 2:b=[v,d,B,y];break;case 3:b=[v,d,y,B];break;default:throw new Error("Invalid quaternion mode")}a[c*32+28+0]=b[0]*127.5+127.5,a[c*32+28+1]=b[1]*127.5+127.5,a[c*32+28+2]=b[2]*127.5+127.5,a[c*32+28+3]=b[3]*127.5+127.5}if(r.shN){const c=[0,3,8,15],g=r.shN.bands?c[r.shN.bands]:r.shN.shape[1]/3,S=e[5].bits,w=e[6].bits,x=e[5].width,v=g*3,d=Math.ceil(v/16),y=[],H=t.getEngine().getCaps().maxTextureSize,B=Math.ceil(s/H);for(let b=0;b<d;b++){const U=new Uint8Array(B*H*4*4);y.push(U)}if(r.version===2){if(!r.shN.codebook)throw new Error("Missing codebook in SOG version 2 shN data.");for(let b=0;b<s;b++){const U=w[b*4+0]+(w[b*4+1]<<8),W=U%64*g,z=Math.floor(U/64);for(let R=0;R<g;R++)for(let V=0;V<3;V++){const k=R*3+V,Z=Math.floor(k/16),O=y[Z],I=k%16,K=b*16,Y=r.shN.codebook[S[(W+R)*4+V+z*x*4]]*127.5+127.5;O[I+K]=Math.max(0,Math.min(255,Y))}}}else for(let b=0;b<s;b++){const U=w[b*4+0]+(w[b*4+1]<<8),W=U%64*g,z=Math.floor(U/64),R=r.shN.mins,V=r.shN.maxs;for(let k=0;k<3;k++)for(let Z=0;Z<g/3;Z++){const O=Z*3+k,I=Math.floor(O/16),K=y[I],Y=O%16,ne=b*16,te=X.Lerp(R,V,S[(W+Z)*4+k+z*x*4]/255)*127.5+127.5;K[Y+ne]=Math.max(0,Math.min(255,te))}}return await new Promise(b=>{b({mode:0,data:o,hasVertexColors:!1,sh:y})})}return await new Promise(c=>{c({mode:0,data:o,hasVertexColors:!1})})}async function Rt(r,e,t){let s,n;if(r instanceof Map){n=r;const l=n.get("meta.json");if(!l)throw new Error("meta.json not found in files Map");s=JSON.parse(new TextDecoder().decode(l))}else s=r;const o=[...s.means.files,...s.scales.files,...s.quats.files,...s.sh0.files];s.shN&&o.push(...s.shN.files);const i=await Promise.all(o.map(async l=>{if(n&&n.has(l)){const h=n.get(l);return await kt(h,l,t.getEngine())}else return await kt(e,l,t.getEngine())}));return await Us(s,i,t)}class ee{constructor(e=ee._DefaultLoadingOptions){this.name=Re.name,this._assetContainer=null,this.extensions=Re.extensions,this._loadingOptions=e}createPlugin(e){return new ee(e[Re.name])}async importMeshAsync(e,t,s,n,o,i){return await this._parseAsync(e,t,s,n).then(l=>({meshes:l,particleSystems:[],skeletons:[],animationGroups:[],transformNodes:[],geometries:[],lights:[],spriteManagers:[]}))}static _BuildPointCloud(e,t){if(!t.byteLength)return!1;const s=new Uint8Array(t),n=new Float32Array(t),o=32,i=s.length/o,l=function(h,a){const _=n[8*a+0],u=n[8*a+1],f=n[8*a+2];h.position=new T(_,u,f);const p=s[o*a+24+0]/255,m=s[o*a+24+1]/255,C=s[o*a+24+2]/255;h.color=new L(p,m,C,1)};return e.addPoints(i,l),!0}static _BuildMesh(e,t){const s=new Ie("PLYMesh",e),n=new Uint8Array(t.data),o=new Float32Array(t.data),i=32,l=n.length/i,h=[],a=new Ne;for(let _=0;_<l;_++){const u=o[8*_+0],f=o[8*_+1],p=o[8*_+2];h.push(u,f,p)}if(t.hasVertexColors){const _=new Float32Array(l*4);for(let u=0;u<l;u++){const f=n[i*u+24+0]/255,p=n[i*u+24+1]/255,m=n[i*u+24+2]/255;_[u*4+0]=f,_[u*4+1]=p,_[u*4+2]=m,_[u*4+3]=1}a.colors=_}return a.positions=h,a.indices=t.faces,a.applyToMesh(s),s}async _unzipWithFFlateAsync(e){let t=this._loadingOptions.fflate;t||(typeof window.fflate>"u"&&await is.LoadScriptAsync(this._loadingOptions.deflateURL??"https://unpkg.com/fflate/umd/index.js"),t=window.fflate);const{unzipSync:s}=t,n=s(e),o=new Map;for(const[i,l]of Object.entries(n))o.set(i,l);return o}_parseAsync(e,t,s,n){const o=[],i=u=>{t._blockEntityCollection=!!this._assetContainer;const f=this._loadingOptions.gaussianSplattingMesh??new D("GaussianSplatting",null,t,this._loadingOptions.keepInRam);f._parentContainer=this._assetContainer,o.push(f),f.updateData(u.data,u.sh,{flipY:!1}),f.scaling.y*=-1,f.computeWorldMatrix(!0),t._blockEntityCollection=!1};if(typeof s=="string"){const u=JSON.parse(s);if(u&&u.means&&u.scales&&u.quats&&u.sh0)return new Promise(f=>{Rt(u,n,t).then(p=>{i(p),f(o)}).catch(()=>{throw new Error("Failed to parse SOG data.")})})}const l=s instanceof ArrayBuffer?new Uint8Array(s):s;if(l[0]===80&&l[1]===75)return new Promise(u=>{this._unzipWithFFlateAsync(l).then(f=>{Rt(f,n,t).then(p=>{i(p),u(o)}).catch(()=>{throw new Error("Failed to parse SOG zip data.")})})});const h=new ReadableStream({start(u){u.enqueue(new Uint8Array(s)),u.close()}}),a=new DecompressionStream("gzip"),_=h.pipeThrough(a);return new Promise(u=>{new Response(_).arrayBuffer().then(f=>{Bs(f,t,this._loadingOptions).then(p=>{t._blockEntityCollection=!!this._assetContainer;const m=this._loadingOptions.gaussianSplattingMesh??new D("GaussianSplatting",null,t,this._loadingOptions.keepInRam);if(p.trainedWithAntialiasing){const C=m.material;C.kernelSize=.1,C.compensation=!0}m._parentContainer=this._assetContainer,o.push(m),m.updateData(p.data,p.sh,{flipY:!1}),this._loadingOptions.flipY||(m.scaling.y*=-1,m.computeWorldMatrix(!0)),t._blockEntityCollection=!1,this.applyAutoCameraLimits(p,t),u(o)})}).catch(()=>{ee._ConvertPLYToSplat(s).then(async f=>{switch(t._blockEntityCollection=!!this._assetContainer,f.mode){case 0:{const p=this._loadingOptions.gaussianSplattingMesh??new D("GaussianSplatting",null,t,this._loadingOptions.keepInRam);switch(p._parentContainer=this._assetContainer,o.push(p),p.updateData(f.data,f.sh,{flipY:!1}),p.scaling.y*=-1,f.chirality==="RightHanded"&&(p.scaling.y*=-1),f.upAxis){case"X":p.rotation=new T(0,0,Math.PI/2);break;case"Y":p.rotation=new T(0,0,Math.PI);break;case"Z":p.rotation=new T(-Math.PI/2,Math.PI,0);break}p.computeWorldMatrix(!0)}break;case 1:{const p=new Fs("PointCloud",1,t);ee._BuildPointCloud(p,f.data)?await p.buildMeshAsync().then(m=>{o.push(m)}):p.dispose()}break;case 2:if(f.faces)o.push(ee._BuildMesh(t,f));else throw new Error("PLY mesh doesn't contain face informations.");break;default:throw new Error("Unsupported Splat mode")}t._blockEntityCollection=!1,this.applyAutoCameraLimits(f,t),u(o)})})})}applyAutoCameraLimits(e,t){if(!this._loadingOptions.disableAutoCameraLimits&&(e.safeOrbitCameraRadiusMin!==void 0||e.safeOrbitCameraElevationMinMax!==void 0)&&t.activeCamera?.getClassName()==="ArcRotateCamera"){const s=t.activeCamera;e.safeOrbitCameraElevationMinMax&&(s.lowerBetaLimit=Math.PI*.5-e.safeOrbitCameraElevationMinMax[1],s.upperBetaLimit=Math.PI*.5-e.safeOrbitCameraElevationMinMax[0]),e.safeOrbitCameraRadiusMin&&(s.lowerRadiusLimit=e.safeOrbitCameraRadiusMin)}}loadAssetContainerAsync(e,t,s){const n=new cs(e);return this._assetContainer=n,this.importMeshAsync(null,e,t,s).then(o=>{for(const i of o.meshes)n.meshes.push(i);return this._assetContainer=null,n}).catch(o=>{throw this._assetContainer=null,o})}loadAsync(e,t,s){return this.importMeshAsync(null,e,t,s).then(()=>{})}static _ConvertPLYToSplat(e){const t=new Uint8Array(e),s=new TextDecoder().decode(t.slice(0,1024*10)),n=`end_header
`,o=s.indexOf(n);if(o<0||!s)return new Promise(x=>{x({mode:0,data:e,rawSplat:!0})});const i=parseInt(/element vertex (\d+)\n/.exec(s)[1]),l=/element face (\d+)\n/.exec(s);let h=0;l&&(h=parseInt(l[1]));const a=/element chunk (\d+)\n/.exec(s);let _=0;a&&(_=parseInt(a[1]));let u=0,f=0;const p={double:8,int:4,uint:4,float:4,short:2,ushort:2,uchar:1,list:0},m={Vertex:0,Chunk:1,SH:2,Float_Tuple:3,Float:4,Uchar:5};let C=m.Chunk;const E=[],c=s.slice(0,o).split(`
`),g={};for(const x of c)if(x.startsWith("property ")){const[,v,d]=x.split(" ");if(C==m.Chunk)f+=p[v];else if(C==m.Vertex)E.push({name:d,type:v,offset:u}),u+=p[v];else if(C==m.SH)E.push({name:d,type:v,offset:u});else if(C==m.Float_Tuple){const y=new DataView(e,f,p.float*2);g.safeOrbitCameraElevationMinMax=[y.getFloat32(0,!0),y.getFloat32(4,!0)]}else if(C==m.Float){const y=new DataView(e,f,p.float);g.safeOrbitCameraRadiusMin=y.getFloat32(0,!0)}else if(C==m.Uchar){const y=new DataView(e,f,p.uchar);d=="up_axis"?g.upAxis=y.getUint8(0)==0?"X":y.getUint8(0)==1?"Y":"Z":d=="chirality"&&(g.chirality=y.getUint8(0)==0?"LeftHanded":"RightHanded")}p[v]||de.Warn(`Unsupported property type: ${v}.`)}else if(x.startsWith("element ")){const[,v]=x.split(" ");v=="chunk"?C=m.Chunk:v=="vertex"?C=m.Vertex:v=="sh"?C=m.SH:v=="safe_orbit_camera_elevation_min_max_radians"?C=m.Float_Tuple:v=="safe_orbit_camera_radius_min"?C=m.Float:(v=="up_axis"||v=="chirality")&&(C=m.Uchar)}const S=u,w=f;return D.ConvertPLYWithSHToSplatAsync(e).then(async x=>{const v=new DataView(e,o+n.length);let d=w*_+S*i;const y=[];if(h)for(let z=0;z<h;z++){const R=v.getUint8(d);if(R==3){d+=1;for(let V=0;V<R;V++){const k=v.getUint32(d+(2-V)*4,!0);y.push(k)}d+=12}}if(_)return await new Promise(z=>{z({mode:0,data:x.buffer,sh:x.sh,faces:y,hasVertexColors:!1,compressed:!0,rawSplat:!1})});let A=0,H=0;const B=["x","y","z","scale_0","scale_1","scale_2","opacity","rot_0","rot_1","rot_2","rot_3"],b=["red","green","blue","f_dc_0","f_dc_1","f_dc_2"];for(let z=0;z<E.length;z++){const R=E[z];B.includes(R.name)&&A++,b.includes(R.name)&&H++}const U=A==B.length&&H==3,W=h?2:U?0:1;return await new Promise(z=>{z({...g,mode:W,data:x.buffer,sh:x.sh,faces:y,hasVertexColors:!!H,compressed:!1,rawSplat:!1})})})}}ee._DefaultLoadingOptions={keepInRam:!1,flipY:!1};as(new ee);export{ee as SPLATFileLoader};
