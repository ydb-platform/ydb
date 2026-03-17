const __vite__mapDeps=(i,m=__vite__mapDeps,d=(m.f||(m.f=["./geometry.vertex-CogSg9xe.js","./index-BZt0m9TU.js","./index-xGeN4i2A.js","./index-BsFpDktE.css","./bonesDeclaration-RhEfGZic.js","./instancesVertex-DeEBuJpq.js","./morphTargetsVertexDeclaration-DSOJO1BG.js","./instancesDeclaration-CapwP630.js","./sceneUboDeclaration-DDBZ5qvn.js","./clipPlaneVertex-DsjlXMYl.js","./morphTargetsVertex-Byo7PSGg.js","./bonesVertex-BQQGmbsQ.js","./bakedVertexAnimation-D1vjYnqT.js","./bumpVertex-_6uSNf7Q.js","./geometry.fragment-CQxQfgny.js","./clipPlaneFragment-CbUd-PEp.js","./bumpFragment-DXaJPBAJ.js","./samplerFragmentDeclaration-D5Wv7G0t.js","./helperFunctions-DnKtpQT_.js"])))=>i.map(i=>d[i]);
import{_ as O}from"./index-xGeN4i2A.js";import{S as E,b9 as D,J as p,a7 as F,b3 as z,b2 as G,b4 as j,bz as $,T as W,M as K,b5 as q,a2 as J,a5 as Q,o as N,Y as Z,be as ee}from"./index-BZt0m9TU.js";import"./clipPlaneFragment-Bia1Py68.js";import"./bumpFragment-5SJSvhvv.js";import"./helperFunctions-B2gYs5dd.js";import"./bakedVertexAnimation-BuWxqXit.js";import"./morphTargetsVertex-BsrYdCoc.js";import"./instancesDeclaration-DMHYiRGq.js";import"./sceneUboDeclaration-DZIv4upk.js";import"./clipPlaneVertex-CHAAXtfr.js";import"./bumpVertex-DW7Nc4K_.js";const B="mrtFragmentDeclaration",te=`#if defined(WEBGL2) || defined(WEBGPU) || defined(NATIVE)
layout(location=0) out vec4 glFragData[{X}];
#endif
`;E.IncludesShadersStore[B]||(E.IncludesShadersStore[B]=te);const A="geometryPixelShader",w=`#extension GL_EXT_draw_buffers : require
#if defined(BUMP) || !defined(NORMAL)
#extension GL_OES_standard_derivatives : enable
#endif
precision highp float;
#ifdef BUMP
varying mat4 vWorldView;varying vec3 vNormalW;
#else
varying vec3 vNormalV;
#endif
varying vec4 vViewPos;
#if defined(POSITION) || defined(BUMP)
varying vec3 vPositionW;
#endif
#if defined(VELOCITY) || defined(VELOCITY_LINEAR)
varying vec4 vCurrentPosition;varying vec4 vPreviousPosition;
#endif
#ifdef NEED_UV
varying vec2 vUV;
#endif
#ifdef BUMP
uniform vec3 vBumpInfos;uniform vec2 vTangentSpaceParams;
#endif
#if defined(REFLECTIVITY)
#if defined(ORMTEXTURE) || defined(SPECULARGLOSSINESSTEXTURE) || defined(REFLECTIVITYTEXTURE)
uniform sampler2D reflectivitySampler;varying vec2 vReflectivityUV;
#else
#ifdef METALLIC_TEXTURE
uniform sampler2D metallicSampler;varying vec2 vMetallicUV;
#endif
#ifdef ROUGHNESS_TEXTURE
uniform sampler2D roughnessSampler;varying vec2 vRoughnessUV;
#endif
#endif
#ifdef ALBEDOTEXTURE
varying vec2 vAlbedoUV;uniform sampler2D albedoSampler;
#endif
#ifdef REFLECTIVITYCOLOR
uniform vec3 reflectivityColor;
#endif
#ifdef ALBEDOCOLOR
uniform vec3 albedoColor;
#endif
#ifdef METALLIC
uniform float metallic;
#endif
#if defined(ROUGHNESS) || defined(GLOSSINESS)
uniform float glossiness;
#endif
#endif
#if defined(ALPHATEST) && defined(NEED_UV)
uniform sampler2D diffuseSampler;
#endif
#include<clipPlaneFragmentDeclaration>
#include<mrtFragmentDeclaration>[SCENE_MRT_COUNT]
#include<bumpFragmentMainFunctions>
#include<bumpFragmentFunctions>
#include<helperFunctions>
void main() {
#include<clipPlaneFragment>
#ifdef ALPHATEST
if (texture2D(diffuseSampler,vUV).a<0.4)
discard;
#endif
vec3 normalOutput;
#ifdef BUMP
vec3 normalW=normalize(vNormalW);
#include<bumpFragment>
#ifdef NORMAL_WORLDSPACE
normalOutput=normalW;
#else
normalOutput=normalize(vec3(vWorldView*vec4(normalW,0.0)));
#endif
#elif defined(HAS_NORMAL_ATTRIBUTE)
normalOutput=normalize(vNormalV);
#elif defined(POSITION)
normalOutput=normalize(-cross(dFdx(vPositionW),dFdy(vPositionW)));
#endif
#ifdef ENCODE_NORMAL
normalOutput=normalOutput*0.5+0.5;
#endif
#ifdef DEPTH
gl_FragData[DEPTH_INDEX]=vec4(vViewPos.z/vViewPos.w,0.0,0.0,1.0);
#endif
#ifdef NORMAL
gl_FragData[NORMAL_INDEX]=vec4(normalOutput,1.0);
#endif
#ifdef SCREENSPACE_DEPTH
gl_FragData[SCREENSPACE_DEPTH_INDEX]=vec4(gl_FragCoord.z,0.0,0.0,1.0);
#endif
#ifdef POSITION
gl_FragData[POSITION_INDEX]=vec4(vPositionW,1.0);
#endif
#ifdef VELOCITY
vec2 a=(vCurrentPosition.xy/vCurrentPosition.w)*0.5+0.5;vec2 b=(vPreviousPosition.xy/vPreviousPosition.w)*0.5+0.5;vec2 velocity=abs(a-b);velocity=vec2(pow(velocity.x,1.0/3.0),pow(velocity.y,1.0/3.0))*sign(a-b)*0.5+0.5;gl_FragData[VELOCITY_INDEX]=vec4(velocity,0.0,1.0);
#endif
#ifdef VELOCITY_LINEAR
vec2 velocity=vec2(0.5)*((vPreviousPosition.xy/vPreviousPosition.w) -
(vCurrentPosition.xy/vCurrentPosition.w));gl_FragData[VELOCITY_LINEAR_INDEX]=vec4(velocity,0.0,1.0);
#endif
#ifdef REFLECTIVITY
vec4 reflectivity=vec4(0.0,0.0,0.0,1.0);
#ifdef METALLICWORKFLOW
float metal=1.0;float roughness=1.0;
#ifdef ORMTEXTURE
metal*=texture2D(reflectivitySampler,vReflectivityUV).b;roughness*=texture2D(reflectivitySampler,vReflectivityUV).g;
#else
#ifdef METALLIC_TEXTURE
metal*=texture2D(metallicSampler,vMetallicUV).r;
#endif
#ifdef ROUGHNESS_TEXTURE
roughness*=texture2D(roughnessSampler,vRoughnessUV).r;
#endif
#endif
#ifdef METALLIC
metal*=metallic;
#endif
#ifdef ROUGHNESS
roughness*=(1.0-glossiness); 
#endif
reflectivity.a-=roughness;vec3 color=vec3(1.0);
#ifdef ALBEDOTEXTURE
color=texture2D(albedoSampler,vAlbedoUV).rgb;
#ifdef GAMMAALBEDO
color=toLinearSpace(color);
#endif
#endif
#ifdef ALBEDOCOLOR
color*=albedoColor.xyz;
#endif
reflectivity.rgb=mix(vec3(0.04),color,metal);
#else
#if defined(SPECULARGLOSSINESSTEXTURE) || defined(REFLECTIVITYTEXTURE)
reflectivity=texture2D(reflectivitySampler,vReflectivityUV);
#ifdef GAMMAREFLECTIVITYTEXTURE
reflectivity.rgb=toLinearSpace(reflectivity.rgb);
#endif
#else 
#ifdef REFLECTIVITYCOLOR
reflectivity.rgb=toLinearSpace(reflectivityColor.xyz);reflectivity.a=1.0;
#endif
#endif
#ifdef GLOSSINESSS
reflectivity.a*=glossiness; 
#endif
#endif
gl_FragData[REFLECTIVITY_INDEX]=reflectivity;
#endif
}
`;E.ShadersStore[A]||(E.ShadersStore[A]=w);const ie={name:A,shader:w},se=Object.freeze(Object.defineProperty({__proto__:null,geometryPixelShader:ie},Symbol.toStringTag,{value:"Module"})),Y="geometryVertexDeclaration",re="uniform mat4 viewProjection;uniform mat4 view;";E.IncludesShadersStore[Y]||(E.IncludesShadersStore[Y]=re);const X="geometryUboDeclaration",ne=`#include<sceneUboDeclaration>
`;E.IncludesShadersStore[X]||(E.IncludesShadersStore[X]=ne);const y="geometryVertexShader",H=`precision highp float;
#include<bonesDeclaration>
#include<bakedVertexAnimationDeclaration>
#include<morphTargetsVertexGlobalDeclaration>
#include<morphTargetsVertexDeclaration>[0..maxSimultaneousMorphTargets]
#include<instancesDeclaration>
#include<__decl__geometryVertex>
#include<clipPlaneVertexDeclaration>
attribute vec3 position;
#ifdef HAS_NORMAL_ATTRIBUTE
attribute vec3 normal;
#endif
#ifdef NEED_UV
varying vec2 vUV;
#ifdef ALPHATEST
uniform mat4 diffuseMatrix;
#endif
#ifdef BUMP
uniform mat4 bumpMatrix;varying vec2 vBumpUV;
#endif
#ifdef REFLECTIVITY
uniform mat4 reflectivityMatrix;uniform mat4 albedoMatrix;varying vec2 vReflectivityUV;varying vec2 vAlbedoUV;
#endif
#ifdef METALLIC_TEXTURE
varying vec2 vMetallicUV;uniform mat4 metallicMatrix;
#endif
#ifdef ROUGHNESS_TEXTURE
varying vec2 vRoughnessUV;uniform mat4 roughnessMatrix;
#endif
#ifdef UV1
attribute vec2 uv;
#endif
#ifdef UV2
attribute vec2 uv2;
#endif
#endif
#ifdef BUMP
varying mat4 vWorldView;
#endif
#ifdef BUMP
varying vec3 vNormalW;
#else
varying vec3 vNormalV;
#endif
varying vec4 vViewPos;
#if defined(POSITION) || defined(BUMP)
varying vec3 vPositionW;
#endif
#if defined(VELOCITY) || defined(VELOCITY_LINEAR)
uniform mat4 previousViewProjection;varying vec4 vCurrentPosition;varying vec4 vPreviousPosition;
#endif
#define CUSTOM_VERTEX_DEFINITIONS
void main(void)
{vec3 positionUpdated=position;
#ifdef HAS_NORMAL_ATTRIBUTE
vec3 normalUpdated=normal;
#else
vec3 normalUpdated=vec3(0.0,0.0,0.0);
#endif
#ifdef UV1
vec2 uvUpdated=uv;
#endif
#ifdef UV2
vec2 uv2Updated=uv2;
#endif
#include<morphTargetsVertexGlobal>
#include<morphTargetsVertex>[0..maxSimultaneousMorphTargets]
#include<instancesVertex>
#if (defined(VELOCITY) || defined(VELOCITY_LINEAR)) && !defined(BONES_VELOCITY_ENABLED)
vCurrentPosition=viewProjection*finalWorld*vec4(positionUpdated,1.0);vPreviousPosition=previousViewProjection*finalPreviousWorld*vec4(positionUpdated,1.0);
#endif
#include<bonesVertex>
#include<bakedVertexAnimation>
vec4 worldPos=vec4(finalWorld*vec4(positionUpdated,1.0));
#ifdef BUMP
vWorldView=view*finalWorld;mat3 normalWorld=mat3(finalWorld);vNormalW=normalize(normalWorld*normalUpdated);
#else
#ifdef NORMAL_WORLDSPACE
vNormalV=normalize(vec3(finalWorld*vec4(normalUpdated,0.0)));
#else
vNormalV=normalize(vec3((view*finalWorld)*vec4(normalUpdated,0.0)));
#endif
#endif
vViewPos=view*worldPos;
#if (defined(VELOCITY) || defined(VELOCITY_LINEAR)) && defined(BONES_VELOCITY_ENABLED)
vCurrentPosition=viewProjection*finalWorld*vec4(positionUpdated,1.0);
#if NUM_BONE_INFLUENCERS>0
mat4 previousInfluence;previousInfluence=mPreviousBones[int(matricesIndices[0])]*matricesWeights[0];
#if NUM_BONE_INFLUENCERS>1
previousInfluence+=mPreviousBones[int(matricesIndices[1])]*matricesWeights[1];
#endif
#if NUM_BONE_INFLUENCERS>2
previousInfluence+=mPreviousBones[int(matricesIndices[2])]*matricesWeights[2];
#endif
#if NUM_BONE_INFLUENCERS>3
previousInfluence+=mPreviousBones[int(matricesIndices[3])]*matricesWeights[3];
#endif
#if NUM_BONE_INFLUENCERS>4
previousInfluence+=mPreviousBones[int(matricesIndicesExtra[0])]*matricesWeightsExtra[0];
#endif
#if NUM_BONE_INFLUENCERS>5
previousInfluence+=mPreviousBones[int(matricesIndicesExtra[1])]*matricesWeightsExtra[1];
#endif
#if NUM_BONE_INFLUENCERS>6
previousInfluence+=mPreviousBones[int(matricesIndicesExtra[2])]*matricesWeightsExtra[2];
#endif
#if NUM_BONE_INFLUENCERS>7
previousInfluence+=mPreviousBones[int(matricesIndicesExtra[3])]*matricesWeightsExtra[3];
#endif
vPreviousPosition=previousViewProjection*finalPreviousWorld*previousInfluence*vec4(positionUpdated,1.0);
#else
vPreviousPosition=previousViewProjection*finalPreviousWorld*vec4(positionUpdated,1.0);
#endif
#endif
#if defined(POSITION) || defined(BUMP)
vPositionW=worldPos.xyz/worldPos.w;
#endif
gl_Position=viewProjection*finalWorld*vec4(positionUpdated,1.0);
#include<clipPlaneVertex>
#ifdef NEED_UV
#ifdef UV1
#if defined(ALPHATEST) && defined(ALPHATEST_UV1)
vUV=vec2(diffuseMatrix*vec4(uvUpdated,1.0,0.0));
#else
vUV=uvUpdated;
#endif
#ifdef BUMP_UV1
vBumpUV=vec2(bumpMatrix*vec4(uvUpdated,1.0,0.0));
#endif
#ifdef REFLECTIVITY_UV1
vReflectivityUV=vec2(reflectivityMatrix*vec4(uvUpdated,1.0,0.0));
#else
#ifdef METALLIC_UV1
vMetallicUV=vec2(metallicMatrix*vec4(uvUpdated,1.0,0.0));
#endif
#ifdef ROUGHNESS_UV1
vRoughnessUV=vec2(roughnessMatrix*vec4(uvUpdated,1.0,0.0));
#endif
#endif
#ifdef ALBEDO_UV1
vAlbedoUV=vec2(albedoMatrix*vec4(uvUpdated,1.0,0.0));
#endif
#endif
#ifdef UV2
#if defined(ALPHATEST) && defined(ALPHATEST_UV2)
vUV=vec2(diffuseMatrix*vec4(uv2Updated,1.0,0.0));
#else
vUV=uv2Updated;
#endif
#ifdef BUMP_UV2
vBumpUV=vec2(bumpMatrix*vec4(uv2Updated,1.0,0.0));
#endif
#ifdef REFLECTIVITY_UV2
vReflectivityUV=vec2(reflectivityMatrix*vec4(uv2Updated,1.0,0.0));
#else
#ifdef METALLIC_UV2
vMetallicUV=vec2(metallicMatrix*vec4(uv2Updated,1.0,0.0));
#endif
#ifdef ROUGHNESS_UV2
vRoughnessUV=vec2(roughnessMatrix*vec4(uv2Updated,1.0,0.0));
#endif
#endif
#ifdef ALBEDO_UV2
vAlbedoUV=vec2(albedoMatrix*vec4(uv2Updated,1.0,0.0));
#endif
#endif
#endif
#include<bumpVertex>
}
`;E.ShadersStore[y]||(E.ShadersStore[y]=H);const ae={name:y,shader:H},le=Object.freeze(Object.defineProperty({__proto__:null,geometryVertexShader:ae},Symbol.toStringTag,{value:"Module"})),k=["world","mBones","viewProjection","diffuseMatrix","view","previousWorld","previousViewProjection","mPreviousBones","bumpMatrix","reflectivityMatrix","albedoMatrix","reflectivityColor","albedoColor","metallic","glossiness","vTangentSpaceParams","vBumpInfos","morphTargetInfluences","morphTargetCount","morphTargetTextureInfo","morphTargetTextureIndices","boneTextureWidth"];Z(k);class l{get normalsAreUnsigned(){return this._normalsAreUnsigned}_linkPrePassRenderer(t){this._linkedWithPrePass=!0,this._prePassRenderer=t,this._multiRenderTarget&&(this._multiRenderTarget.onClearObservable.clear(),this._multiRenderTarget.onClearObservable.add(()=>{}))}_unlinkPrePassRenderer(){this._linkedWithPrePass=!1,this._createRenderTargets()}_resetLayout(){this._enableDepth=!0,this._enableNormal=!0,this._enablePosition=!1,this._enableReflectivity=!1,this._enableVelocity=!1,this._enableVelocityLinear=!1,this._enableScreenspaceDepth=!1,this._attachmentsFromPrePass=[]}_forceTextureType(t,u){t===l.POSITION_TEXTURE_TYPE?(this._positionIndex=u,this._enablePosition=!0):t===l.VELOCITY_TEXTURE_TYPE?(this._velocityIndex=u,this._enableVelocity=!0):t===l.VELOCITY_LINEAR_TEXTURE_TYPE?(this._velocityLinearIndex=u,this._enableVelocityLinear=!0):t===l.REFLECTIVITY_TEXTURE_TYPE?(this._reflectivityIndex=u,this._enableReflectivity=!0):t===l.DEPTH_TEXTURE_TYPE?(this._depthIndex=u,this._enableDepth=!0):t===l.NORMAL_TEXTURE_TYPE?(this._normalIndex=u,this._enableNormal=!0):t===l.SCREENSPACE_DEPTH_TEXTURE_TYPE&&(this._screenspaceDepthIndex=u,this._enableScreenspaceDepth=!0)}_setAttachments(t){this._attachmentsFromPrePass=t}_linkInternalTexture(t){this._multiRenderTarget.setInternalTexture(t,0,!1)}get renderList(){return this._multiRenderTarget.renderList}set renderList(t){this._multiRenderTarget.renderList=t}get isSupported(){return this._multiRenderTarget.isSupported}getTextureIndex(t){switch(t){case l.POSITION_TEXTURE_TYPE:return this._positionIndex;case l.VELOCITY_TEXTURE_TYPE:return this._velocityIndex;case l.VELOCITY_LINEAR_TEXTURE_TYPE:return this._velocityLinearIndex;case l.REFLECTIVITY_TEXTURE_TYPE:return this._reflectivityIndex;case l.DEPTH_TEXTURE_TYPE:return this._depthIndex;case l.NORMAL_TEXTURE_TYPE:return this._normalIndex;case l.SCREENSPACE_DEPTH_TEXTURE_TYPE:return this._screenspaceDepthIndex;default:return-1}}get enableDepth(){return this._enableDepth}set enableDepth(t){this._enableDepth=t,this._linkedWithPrePass||(this.dispose(),this._createRenderTargets())}get enableNormal(){return this._enableNormal}set enableNormal(t){this._enableNormal=t,this._linkedWithPrePass||(this.dispose(),this._createRenderTargets())}get enablePosition(){return this._enablePosition}set enablePosition(t){this._enablePosition=t,this._linkedWithPrePass||(this.dispose(),this._createRenderTargets())}get enableVelocity(){return this._enableVelocity}set enableVelocity(t){this._enableVelocity=t,t||(this._previousTransformationMatrices={}),this._linkedWithPrePass||(this.dispose(),this._createRenderTargets()),this._scene.needsPreviousWorldMatrices=t}get enableVelocityLinear(){return this._enableVelocityLinear}set enableVelocityLinear(t){this._enableVelocityLinear=t,this._linkedWithPrePass||(this.dispose(),this._createRenderTargets())}get enableReflectivity(){return this._enableReflectivity}set enableReflectivity(t){this._enableReflectivity=t,this._linkedWithPrePass||(this.dispose(),this._createRenderTargets())}get enableScreenspaceDepth(){return this._enableScreenspaceDepth}set enableScreenspaceDepth(t){this._enableScreenspaceDepth=t,this._linkedWithPrePass||(this.dispose(),this._createRenderTargets())}get scene(){return this._scene}get ratio(){return typeof this._ratioOrDimensions=="object"?1:this._ratioOrDimensions}get shaderLanguage(){return this._shaderLanguage}constructor(t,u=1,i=15,e){this._previousTransformationMatrices={},this._previousBonesTransformationMatrices={},this.excludedSkinnedMeshesFromVelocity=[],this.renderTransparentMeshes=!0,this.generateNormalsInWorldSpace=!1,this._normalsAreUnsigned=!1,this._resizeObserver=null,this._enableDepth=!0,this._enableNormal=!0,this._enablePosition=!1,this._enableVelocity=!1,this._enableVelocityLinear=!1,this._enableReflectivity=!1,this._enableScreenspaceDepth=!1,this._clearColor=new D(0,0,0,0),this._clearDepthColor=new D(0,0,0,1),this._positionIndex=-1,this._velocityIndex=-1,this._velocityLinearIndex=-1,this._reflectivityIndex=-1,this._depthIndex=-1,this._normalIndex=-1,this._screenspaceDepthIndex=-1,this._linkedWithPrePass=!1,this.useSpecificClearForDepthTexture=!1,this._shaderLanguage=0,this._shadersLoaded=!1,this._scene=t,this._ratioOrDimensions=u,this._useUbo=t.getEngine().supportsUniformBuffers,this._depthFormat=i,this._textureTypesAndFormats=e||{},this._initShaderSourceAsync(),l._SceneComponentInitialization(this._scene),this._createRenderTargets()}async _initShaderSourceAsync(){this._scene.getEngine().isWebGPU&&!l.ForceGLSL?(this._shaderLanguage=1,await Promise.all([O(()=>import("./geometry.vertex-CogSg9xe.js"),__vite__mapDeps([0,1,2,3,4,5,6,7,8,9,10,11,12,13]),import.meta.url),O(()=>import("./geometry.fragment-CQxQfgny.js"),__vite__mapDeps([14,1,2,3,15,16,17,18]),import.meta.url)])):await Promise.all([O(()=>Promise.resolve().then(()=>le),void 0,import.meta.url),O(()=>Promise.resolve().then(()=>se),void 0,import.meta.url)]),this._shadersLoaded=!0}isReady(t,u){if(!this._shadersLoaded)return!1;const i=t.getMaterial();if(i&&i.disableDepthWrite)return!1;const e=[],h=[p.PositionKind],c=t.getMesh();c.isVerticesDataPresent(p.NormalKind)&&(e.push("#define HAS_NORMAL_ATTRIBUTE"),h.push(p.NormalKind));let g=!1,R=!1;const S=!1;if(i){let n=!1;if(i.needAlphaTestingForMesh(c)&&i.getAlphaTestTexture()&&(e.push("#define ALPHATEST"),e.push(`#define ALPHATEST_UV${i.getAlphaTestTexture().coordinatesIndex+1}`),n=!0),(i.bumpTexture||i.normalTexture||i.geometryNormalTexture)&&F.BumpTextureEnabled){const a=i.bumpTexture||i.normalTexture||i.geometryNormalTexture;e.push("#define BUMP"),e.push(`#define BUMP_UV${a.coordinatesIndex+1}`),n=!0}if(this._enableReflectivity){let a=!1;if(i.getClassName()==="PBRMetallicRoughnessMaterial")i.metallicRoughnessTexture&&(e.push("#define ORMTEXTURE"),e.push(`#define REFLECTIVITY_UV${i.metallicRoughnessTexture.coordinatesIndex+1}`),e.push("#define METALLICWORKFLOW"),n=!0,a=!0),i.metallic!=null&&(e.push("#define METALLIC"),e.push("#define METALLICWORKFLOW"),a=!0),i.roughness!=null&&(e.push("#define ROUGHNESS"),e.push("#define METALLICWORKFLOW"),a=!0),a&&(i.baseTexture&&(e.push("#define ALBEDOTEXTURE"),e.push(`#define ALBEDO_UV${i.baseTexture.coordinatesIndex+1}`),i.baseTexture.gammaSpace&&e.push("#define GAMMAALBEDO"),n=!0),i.baseColor&&e.push("#define ALBEDOCOLOR"));else if(i.getClassName()==="PBRSpecularGlossinessMaterial")i.specularGlossinessTexture?(e.push("#define SPECULARGLOSSINESSTEXTURE"),e.push(`#define REFLECTIVITY_UV${i.specularGlossinessTexture.coordinatesIndex+1}`),n=!0,i.specularGlossinessTexture.gammaSpace&&e.push("#define GAMMAREFLECTIVITYTEXTURE")):i.specularColor&&e.push("#define REFLECTIVITYCOLOR"),i.glossiness!=null&&e.push("#define GLOSSINESS");else if(i.getClassName()==="PBRMaterial")i.metallicTexture&&(e.push("#define ORMTEXTURE"),e.push(`#define REFLECTIVITY_UV${i.metallicTexture.coordinatesIndex+1}`),e.push("#define METALLICWORKFLOW"),n=!0,a=!0),i.metallic!=null&&(e.push("#define METALLIC"),e.push("#define METALLICWORKFLOW"),a=!0),i.roughness!=null&&(e.push("#define ROUGHNESS"),e.push("#define METALLICWORKFLOW"),a=!0),a?(i.albedoTexture&&(e.push("#define ALBEDOTEXTURE"),e.push(`#define ALBEDO_UV${i.albedoTexture.coordinatesIndex+1}`),i.albedoTexture.gammaSpace&&e.push("#define GAMMAALBEDO"),n=!0),i.albedoColor&&e.push("#define ALBEDOCOLOR")):(i.reflectivityTexture?(e.push("#define SPECULARGLOSSINESSTEXTURE"),e.push(`#define REFLECTIVITY_UV${i.reflectivityTexture.coordinatesIndex+1}`),i.reflectivityTexture.gammaSpace&&e.push("#define GAMMAREFLECTIVITYTEXTURE"),n=!0):i.reflectivityColor&&e.push("#define REFLECTIVITYCOLOR"),i.microSurface!=null&&e.push("#define GLOSSINESS"));else if(i.getClassName()==="StandardMaterial")i.specularTexture&&(e.push("#define REFLECTIVITYTEXTURE"),e.push(`#define REFLECTIVITY_UV${i.specularTexture.coordinatesIndex+1}`),i.specularTexture.gammaSpace&&e.push("#define GAMMAREFLECTIVITYTEXTURE"),n=!0),i.specularColor&&e.push("#define REFLECTIVITYCOLOR");else if(i.getClassName()==="OpenPBRMaterial"){const d=i;e.push("#define METALLICWORKFLOW"),a=!0,e.push("#define METALLIC"),e.push("#define ROUGHNESS"),d._useRoughnessFromMetallicTextureGreen&&d.baseMetalnessTexture?(e.push("#define ORMTEXTURE"),e.push(`#define REFLECTIVITY_UV${d.baseMetalnessTexture.coordinatesIndex+1}`),n=!0):d.baseMetalnessTexture?(e.push("#define METALLIC_TEXTURE"),e.push(`#define METALLIC_UV${d.baseMetalnessTexture.coordinatesIndex+1}`),n=!0):d.specularRoughnessTexture&&(e.push("#define ROUGHNESS_TEXTURE"),e.push(`#define ROUGHNESS_UV${d.specularRoughnessTexture.coordinatesIndex+1}`),n=!0),d.baseColorTexture&&(e.push("#define ALBEDOTEXTURE"),e.push(`#define ALBEDO_UV${d.baseColorTexture.coordinatesIndex+1}`),d.baseColorTexture.gammaSpace&&e.push("#define GAMMAALBEDO"),n=!0),d.baseColor&&e.push("#define ALBEDOCOLOR")}}n&&(e.push("#define NEED_UV"),c.isVerticesDataPresent(p.UVKind)&&(h.push(p.UVKind),e.push("#define UV1"),g=!0),c.isVerticesDataPresent(p.UV2Kind)&&(h.push(p.UV2Kind),e.push("#define UV2"),R=!0))}this._enableDepth&&(e.push("#define DEPTH"),e.push("#define DEPTH_INDEX "+this._depthIndex)),this._enableNormal&&(e.push("#define NORMAL"),e.push("#define NORMAL_INDEX "+this._normalIndex)),this._enablePosition&&(e.push("#define POSITION"),e.push("#define POSITION_INDEX "+this._positionIndex)),this._enableVelocity&&(e.push("#define VELOCITY"),e.push("#define VELOCITY_INDEX "+this._velocityIndex),this.excludedSkinnedMeshesFromVelocity.indexOf(c)===-1&&e.push("#define BONES_VELOCITY_ENABLED")),this._enableVelocityLinear&&(e.push("#define VELOCITY_LINEAR"),e.push("#define VELOCITY_LINEAR_INDEX "+this._velocityLinearIndex),this.excludedSkinnedMeshesFromVelocity.indexOf(c)===-1&&e.push("#define BONES_VELOCITY_ENABLED")),this._enableReflectivity&&(e.push("#define REFLECTIVITY"),e.push("#define REFLECTIVITY_INDEX "+this._reflectivityIndex)),this._enableScreenspaceDepth&&this._screenspaceDepthIndex!==-1&&(e.push("#define SCREENSPACE_DEPTH_INDEX "+this._screenspaceDepthIndex),e.push("#define SCREENSPACE_DEPTH")),this.generateNormalsInWorldSpace&&e.push("#define NORMAL_WORLDSPACE"),this._normalsAreUnsigned&&e.push("#define ENCODE_NORMAL"),c.useBones&&c.computeBonesUsingShaders&&c.skeleton?(h.push(p.MatricesIndicesKind),h.push(p.MatricesWeightsKind),c.numBoneInfluencers>4&&(h.push(p.MatricesIndicesExtraKind),h.push(p.MatricesWeightsExtraKind)),e.push("#define NUM_BONE_INFLUENCERS "+c.numBoneInfluencers),e.push("#define BONETEXTURE "+c.skeleton.isUsingTextureForMatrices),e.push("#define BonesPerMesh "+(c.skeleton.bones.length+1))):(e.push("#define NUM_BONE_INFLUENCERS 0"),e.push("#define BONETEXTURE false"),e.push("#define BonesPerMesh 0"));const U=c.morphTargetManager?z(c.morphTargetManager,e,h,c,!0,!0,!1,g,R,S):0;u&&(e.push("#define INSTANCES"),G(h,this._enableVelocity||this._enableVelocityLinear),t.getRenderingMesh().hasThinInstances&&e.push("#define THIN_INSTANCES")),this._linkedWithPrePass?e.push("#define SCENE_MRT_COUNT "+this._attachmentsFromPrePass.length):e.push("#define SCENE_MRT_COUNT "+this._multiRenderTarget.textures.length),j(i,this._scene,e);const L=this._scene.getEngine(),M=t._getDrawWrapper(void 0,!0),C=M.defines,_=e.join(`
`);return C!==_&&M.setEffect(L.createEffect("geometry",{attributes:h,uniformsNames:k,samplers:["diffuseSampler","bumpSampler","reflectivitySampler","albedoSampler","morphTargets","boneSampler"],defines:_,onCompiled:null,fallbacks:null,onError:null,uniformBuffersNames:["Scene"],indexParameters:{buffersCount:this._multiRenderTarget.textures.length-1,maxSimultaneousMorphTargets:U},shaderLanguage:this.shaderLanguage},L),_),M.effect.isReady()}getGBuffer(){return this._multiRenderTarget}get samples(){return this._multiRenderTarget.samples}set samples(t){this._multiRenderTarget.samples=t}dispose(){this._resizeObserver&&(this._scene.getEngine().onResizeObservable.remove(this._resizeObserver),this._resizeObserver=null),this.getGBuffer().dispose()}_assignRenderTargetIndices(){const t=[],u=[];let i=0;return this._enableDepth&&(this._depthIndex=i,i++,t.push("gBuffer_Depth"),u.push(this._textureTypesAndFormats[l.DEPTH_TEXTURE_TYPE])),this._enableNormal&&(this._normalIndex=i,i++,t.push("gBuffer_Normal"),u.push(this._textureTypesAndFormats[l.NORMAL_TEXTURE_TYPE])),this._enablePosition&&(this._positionIndex=i,i++,t.push("gBuffer_Position"),u.push(this._textureTypesAndFormats[l.POSITION_TEXTURE_TYPE])),this._enableVelocity&&(this._velocityIndex=i,i++,t.push("gBuffer_Velocity"),u.push(this._textureTypesAndFormats[l.VELOCITY_TEXTURE_TYPE])),this._enableVelocityLinear&&(this._velocityLinearIndex=i,i++,t.push("gBuffer_VelocityLinear"),u.push(this._textureTypesAndFormats[l.VELOCITY_LINEAR_TEXTURE_TYPE])),this._enableReflectivity&&(this._reflectivityIndex=i,i++,t.push("gBuffer_Reflectivity"),u.push(this._textureTypesAndFormats[l.REFLECTIVITY_TEXTURE_TYPE])),this._enableScreenspaceDepth&&(this._screenspaceDepthIndex=i,i++,t.push("gBuffer_ScreenspaceDepth"),u.push(this._textureTypesAndFormats[l.SCREENSPACE_DEPTH_TEXTURE_TYPE])),[i,t,u]}_createRenderTargets(){const t=this._scene.getEngine(),[u,i,e]=this._assignRenderTargetIndices();let h=0;t._caps.textureFloat&&t._caps.textureFloatLinearFiltering?h=1:t._caps.textureHalfFloat&&t._caps.textureHalfFloatLinearFiltering&&(h=2);const c=this._ratioOrDimensions.width!==void 0?this._ratioOrDimensions:{width:t.getRenderWidth()*this._ratioOrDimensions,height:t.getRenderHeight()*this._ratioOrDimensions},v=[],g=[];for(const n of e)n?(v.push(n.textureType),g.push(n.textureFormat)):(v.push(h),g.push(5));if(this._normalsAreUnsigned=v[l.NORMAL_TEXTURE_TYPE]===11||v[l.NORMAL_TEXTURE_TYPE]===13,this._multiRenderTarget=new $("gBuffer",c,u,this._scene,{generateMipMaps:!1,generateDepthTexture:!0,types:v,formats:g,depthTextureFormat:this._depthFormat},i.concat("gBuffer_DepthBuffer")),!this.isSupported)return;this._multiRenderTarget.wrapU=W.CLAMP_ADDRESSMODE,this._multiRenderTarget.wrapV=W.CLAMP_ADDRESSMODE,this._multiRenderTarget.refreshRate=1,this._multiRenderTarget.renderParticles=!1,this._multiRenderTarget.renderList=null;const R=[!0],S=[!1],U=[!0];for(let n=1;n<u;++n)R.push(!0),U.push(!1),S.push(!0);const L=t.buildTextureLayout(R),M=t.buildTextureLayout(S),C=t.buildTextureLayout(U);this._multiRenderTarget.onClearObservable.add(n=>{n.bindAttachments(this.useSpecificClearForDepthTexture?M:L),n.clear(this._clearColor,!0,!0,!0),this.useSpecificClearForDepthTexture&&(n.bindAttachments(C),n.clear(this._clearDepthColor,!0,!0,!0)),n.bindAttachments(L)}),this._resizeObserver=t.onResizeObservable.add(()=>{if(this._multiRenderTarget){const n=this._ratioOrDimensions.width!==void 0?this._ratioOrDimensions:{width:t.getRenderWidth()*this._ratioOrDimensions,height:t.getRenderHeight()*this._ratioOrDimensions};this._multiRenderTarget.resize(n)}});const _=n=>{const a=n.getRenderingMesh(),d=n.getEffectiveMesh(),T=this._scene,f=T.getEngine(),s=n.getMaterial();if(!s)return;if(d._internalAbstractMeshDataInfo._isActiveIntermediate=!1,(this._enableVelocity||this._enableVelocityLinear)&&!this._previousTransformationMatrices[d.uniqueId]&&(this._previousTransformationMatrices[d.uniqueId]={world:K.Identity(),viewProjection:T.getTransformMatrix()},a.skeleton)){const m=a.skeleton.getTransformMatrices(a);this._previousBonesTransformationMatrices[a.uniqueId]=this._copyBonesTransformationMatrices(m,new Float32Array(m.length))}const x=a._getInstancesRenderList(n._id,!!n.getReplacementMesh());if(x.mustReturn)return;const I=f.getCaps().instancedArrays&&(x.visibleInstances[n._id]!==null||a.hasThinInstances),b=d.getWorldMatrix();if(this.isReady(n,I)){const m=n._getDrawWrapper();if(!m)return;const r=m.effect;f.enableEffect(m),I||a._bind(n,r,s.fillMode),this._useUbo?(q(r,this._scene.getSceneUniformBuffer()),this._scene.finalizeSceneUbo()):(r.setMatrix("viewProjection",T.getTransformMatrix()),r.setMatrix("view",T.getViewMatrix()));let P;if(!a._instanceDataStorage.isFrozen&&(s.backFaceCulling||s.sideOrientation!==null)){const o=d._getWorldMatrixDeterminant();P=s._getEffectiveOrientation(a),o<0&&(P=P===N.ClockWiseSideOrientation?N.CounterClockWiseSideOrientation:N.ClockWiseSideOrientation)}else P=a._effectiveSideOrientation;if(s._preBind(m,P),s.needAlphaTestingForMesh(d)){const o=s.getAlphaTestTexture();o&&(r.setTexture("diffuseSampler",o),r.setMatrix("diffuseMatrix",o.getTextureMatrix()))}if((s.bumpTexture||s.normalTexture||s.geometryNormalTexture)&&T.getEngine().getCaps().standardDerivatives&&F.BumpTextureEnabled){const o=s.bumpTexture||s.normalTexture||s.geometryNormalTexture;r.setFloat3("vBumpInfos",o.coordinatesIndex,1/o.level,s.parallaxScaleBias),r.setMatrix("bumpMatrix",o.getTextureMatrix()),r.setTexture("bumpSampler",o),r.setFloat2("vTangentSpaceParams",s.invertNormalMapX?-1:1,s.invertNormalMapY?-1:1)}if(this._enableReflectivity){if(s.getClassName()==="PBRMetallicRoughnessMaterial")s.metallicRoughnessTexture!==null&&(r.setTexture("reflectivitySampler",s.metallicRoughnessTexture),r.setMatrix("reflectivityMatrix",s.metallicRoughnessTexture.getTextureMatrix())),s.metallic!==null&&r.setFloat("metallic",s.metallic),s.roughness!==null&&r.setFloat("glossiness",1-s.roughness),s.baseTexture!==null&&(r.setTexture("albedoSampler",s.baseTexture),r.setMatrix("albedoMatrix",s.baseTexture.getTextureMatrix())),s.baseColor!==null&&r.setColor3("albedoColor",s.baseColor);else if(s.getClassName()==="PBRSpecularGlossinessMaterial")s.specularGlossinessTexture!==null?(r.setTexture("reflectivitySampler",s.specularGlossinessTexture),r.setMatrix("reflectivityMatrix",s.specularGlossinessTexture.getTextureMatrix())):s.specularColor!==null&&r.setColor3("reflectivityColor",s.specularColor),s.glossiness!==null&&r.setFloat("glossiness",s.glossiness);else if(s.getClassName()==="PBRMaterial")s.metallicTexture!==null&&(r.setTexture("reflectivitySampler",s.metallicTexture),r.setMatrix("reflectivityMatrix",s.metallicTexture.getTextureMatrix())),s.metallic!==null&&r.setFloat("metallic",s.metallic),s.roughness!==null&&r.setFloat("glossiness",1-s.roughness),s.roughness!==null||s.metallic!==null||s.metallicTexture!==null?(s.albedoTexture!==null&&(r.setTexture("albedoSampler",s.albedoTexture),r.setMatrix("albedoMatrix",s.albedoTexture.getTextureMatrix())),s.albedoColor!==null&&r.setColor3("albedoColor",s.albedoColor)):(s.reflectivityTexture!==null?(r.setTexture("reflectivitySampler",s.reflectivityTexture),r.setMatrix("reflectivityMatrix",s.reflectivityTexture.getTextureMatrix())):s.reflectivityColor!==null&&r.setColor3("reflectivityColor",s.reflectivityColor),s.microSurface!==null&&r.setFloat("glossiness",s.microSurface));else if(s.getClassName()==="StandardMaterial")s.specularTexture!==null&&(r.setTexture("reflectivitySampler",s.specularTexture),r.setMatrix("reflectivityMatrix",s.specularTexture.getTextureMatrix())),s.specularColor!==null&&r.setColor3("reflectivityColor",s.specularColor);else if(s.getClassName()==="OpenPBRMaterial"){const o=s;o._useRoughnessFromMetallicTextureGreen&&o.baseMetalnessTexture?(r.setTexture("reflectivitySampler",o.baseMetalnessTexture),r.setMatrix("reflectivityMatrix",o.baseMetalnessTexture.getTextureMatrix())):o.baseMetalnessTexture?(r.setTexture("metallicSampler",o.baseMetalnessTexture),r.setMatrix("metallicMatrix",o.baseMetalnessTexture.getTextureMatrix())):o.specularRoughnessTexture&&(r.setTexture("roughnessSampler",o.specularRoughnessTexture),r.setMatrix("roughnessMatrix",o.specularRoughnessTexture.getTextureMatrix())),r.setFloat("metallic",o.baseMetalness),r.setFloat("glossiness",1-o.specularRoughness),o.baseColorTexture!==null&&(r.setTexture("albedoSampler",o.baseColorTexture),r.setMatrix("albedoMatrix",o.baseColorTexture.getTextureMatrix())),o.baseColor!==null&&r.setColor3("albedoColor",o.baseColor)}}if(J(r,s,this._scene),a.useBones&&a.computeBonesUsingShaders&&a.skeleton){const o=a.skeleton;if(o.isUsingTextureForMatrices&&r.getUniformIndex("boneTextureWidth")>-1){const V=o.getTransformMatrixTexture(a);r.setTexture("boneSampler",V),r.setFloat("boneTextureWidth",4*(o.bones.length+1))}else r.setMatrices("mBones",a.skeleton.getTransformMatrices(a));(this._enableVelocity||this._enableVelocityLinear)&&r.setMatrices("mPreviousBones",this._previousBonesTransformationMatrices[a.uniqueId])}Q(a,r),a.morphTargetManager&&a.morphTargetManager.isUsingTextureForTargets&&a.morphTargetManager._bind(r),(this._enableVelocity||this._enableVelocityLinear)&&(r.setMatrix("previousWorld",this._previousTransformationMatrices[d.uniqueId].world),r.setMatrix("previousViewProjection",this._previousTransformationMatrices[d.uniqueId].viewProjection)),I&&a.hasThinInstances&&r.setMatrix("world",b),a._processRendering(d,n,r,s.fillMode,x,I,(o,V)=>{o||r.setMatrix("world",V)})}(this._enableVelocity||this._enableVelocityLinear)&&(this._previousTransformationMatrices[d.uniqueId].world=b.clone(),this._previousTransformationMatrices[d.uniqueId].viewProjection=this._scene.getTransformMatrix().clone(),a.skeleton&&this._copyBonesTransformationMatrices(a.skeleton.getTransformMatrices(a),this._previousBonesTransformationMatrices[d.uniqueId]))};this._multiRenderTarget.customIsReadyFunction=(n,a,d)=>{if((d||a===0)&&n.subMeshes)for(let T=0;T<n.subMeshes.length;++T){const f=n.subMeshes[T],s=f.getMaterial(),x=f.getRenderingMesh();if(!s)continue;const I=x._getInstancesRenderList(f._id,!!f.getReplacementMesh()),b=t.getCaps().instancedArrays&&(I.visibleInstances[f._id]!==null||x.hasThinInstances);if(!this.isReady(f,b))return!1}return!0},this._multiRenderTarget.customRenderFunction=(n,a,d,T)=>{let f;if(this._linkedWithPrePass){if(!this._prePassRenderer.enabled)return;this._scene.getEngine().bindAttachments(this._attachmentsFromPrePass)}if(T.length){for(t.setColorWrite(!1),f=0;f<T.length;f++)_(T.data[f]);t.setColorWrite(!0)}for(f=0;f<n.length;f++)_(n.data[f]);for(t.setDepthWrite(!1),f=0;f<a.length;f++)_(a.data[f]);if(this.renderTransparentMeshes)for(f=0;f<d.length;f++)_(d.data[f]);t.setDepthWrite(!0)}}_copyBonesTransformationMatrices(t,u){for(let i=0;i<t.length;i++)u[i]=t[i];return u}}l.ForceGLSL=!1;l.DEPTH_TEXTURE_TYPE=0;l.NORMAL_TEXTURE_TYPE=1;l.POSITION_TEXTURE_TYPE=2;l.VELOCITY_TEXTURE_TYPE=3;l.REFLECTIVITY_TEXTURE_TYPE=4;l.SCREENSPACE_DEPTH_TEXTURE_TYPE=5;l.VELOCITY_LINEAR_TEXTURE_TYPE=6;l._SceneComponentInitialization=oe=>{throw ee("GeometryBufferRendererSceneComponent")};export{l as G};
