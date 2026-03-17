import{S as e}from"./index-BZt0m9TU.js";import"./oitFragment-B3ICXEfv.js";import"./openpbrUboDeclaration-DSnUVdJI.js";import"./pbrDebug-CwuJJp5k.js";import"./shadowsFragmentFunctions-94P2zo7r.js";import"./samplerFragmentDeclaration-D5Wv7G0t.js";import"./imageProcessingFunctions-DtjvTVCQ.js";import"./clipPlaneFragment-CbUd-PEp.js";import"./logDepthDeclaration-o4HPWZv_.js";import"./fogFragment-qir_mjFT.js";import"./helperFunctions-DnKtpQT_.js";import"./hdrFilteringFunctions-JeqIkoOq.js";import"./harmonicsFunctions-BfjpwoEj.js";import"./pbrBRDFFunctions-CzAwJrxS.js";import"./index-xGeN4i2A.js";import"./svelte/svelte_svelte.js";import"./svelte/svelte_animate.js";import"./svelte/svelte_attachments.js";import"./svelte/svelte_easing.js";import"./svelte/svelte_internal_client.js";import"./svelte/svelte_internal_flags_async.js";import"./svelte/svelte_internal_flags_legacy.js";import"./svelte/svelte_internal_flags_tracing.js";import"./svelte/svelte_internal_server.js";import"./svelte/svelte_legacy.js";import"./svelte/svelte_motion.js";import"./svelte/svelte_reactivity.js";import"./svelte/svelte_reactivity_window.js";import"./svelte/svelte_server.js";import"./svelte/svelte_store.js";import"./svelte/svelte_transition.js";import"./svelte/svelte_events.js";import"./sceneUboDeclaration-DDBZ5qvn.js";import"./meshUboDeclaration-_4M1at8l.js";import"./mainUVVaryingDeclaration-Dpi69S6s.js";const a="openpbrFragmentSamplersDeclaration",S=`#include<samplerFragmentDeclaration>(_DEFINENAME_,BASE_COLOR,_VARYINGNAME_,BaseColor,_SAMPLERNAME_,baseColor)
#include<samplerFragmentDeclaration>(_DEFINENAME_,BASE_WEIGHT,_VARYINGNAME_,BaseWeight,_SAMPLERNAME_,baseWeight)
#include<samplerFragmentDeclaration>(_DEFINENAME_,BASE_DIFFUSE_ROUGHNESS,_VARYINGNAME_,BaseDiffuseRoughness,_SAMPLERNAME_,baseDiffuseRoughness)
#include<samplerFragmentDeclaration>(_DEFINENAME_,BASE_METALNESS,_VARYINGNAME_,BaseMetalness,_SAMPLERNAME_,baseMetalness)
#include<samplerFragmentDeclaration>(_DEFINENAME_,SPECULAR_WEIGHT,_VARYINGNAME_,SpecularWeight,_SAMPLERNAME_,specularWeight)
#include<samplerFragmentDeclaration>(_DEFINENAME_,SPECULAR_COLOR,_VARYINGNAME_,SpecularColor,_SAMPLERNAME_,specularColor)
#include<samplerFragmentDeclaration>(_DEFINENAME_,SPECULAR_ROUGHNESS,_VARYINGNAME_,SpecularRoughness,_SAMPLERNAME_,specularRoughness)
#include<samplerFragmentDeclaration>(_DEFINENAME_,SPECULAR_ROUGHNESS_ANISOTROPY,_VARYINGNAME_,SpecularRoughnessAnisotropy,_SAMPLERNAME_,specularRoughnessAnisotropy)
#include<samplerFragmentDeclaration>(_DEFINENAME_,COAT_WEIGHT,_VARYINGNAME_,CoatWeight,_SAMPLERNAME_,coatWeight)
#include<samplerFragmentDeclaration>(_DEFINENAME_,COAT_COLOR,_VARYINGNAME_,CoatColor,_SAMPLERNAME_,coatColor)
#include<samplerFragmentDeclaration>(_DEFINENAME_,COAT_ROUGHNESS,_VARYINGNAME_,CoatRoughness,_SAMPLERNAME_,coatRoughness)
#include<samplerFragmentDeclaration>(_DEFINENAME_,COAT_ROUGHNESS_ANISOTROPY,_VARYINGNAME_,CoatRoughnessAnisotropy,_SAMPLERNAME_,coatRoughnessAnisotropy)
#include<samplerFragmentDeclaration>(_DEFINENAME_,COAT_DARKENING,_VARYINGNAME_,CoatDarkening,_SAMPLERNAME_,coatDarkening)
#include<samplerFragmentDeclaration>(_DEFINENAME_,FUZZ_WEIGHT,_VARYINGNAME_,FuzzWeight,_SAMPLERNAME_,fuzzWeight)
#include<samplerFragmentDeclaration>(_DEFINENAME_,FUZZ_COLOR,_VARYINGNAME_,FuzzColor,_SAMPLERNAME_,fuzzColor)
#include<samplerFragmentDeclaration>(_DEFINENAME_,FUZZ_ROUGHNESS,_VARYINGNAME_,FuzzRoughness,_SAMPLERNAME_,fuzzRoughness)
#include<samplerFragmentDeclaration>(_DEFINENAME_,GEOMETRY_OPACITY,_VARYINGNAME_,GeometryOpacity,_SAMPLERNAME_,geometryOpacity)
#include<samplerFragmentDeclaration>(_DEFINENAME_,GEOMETRY_TANGENT,_VARYINGNAME_,GeometryTangent,_SAMPLERNAME_,geometryTangent)
#include<samplerFragmentDeclaration>(_DEFINENAME_,GEOMETRY_COAT_TANGENT,_VARYINGNAME_,GeometryCoatTangent,_SAMPLERNAME_,geometryCoatTangent)
#include<samplerFragmentDeclaration>(_DEFINENAME_,EMISSION_COLOR,_VARYINGNAME_,EmissionColor,_SAMPLERNAME_,emissionColor)
#include<samplerFragmentDeclaration>(_DEFINENAME_,THIN_FILM_WEIGHT,_VARYINGNAME_,ThinFilmWeight,_SAMPLERNAME_,thinFilmWeight)
#include<samplerFragmentDeclaration>(_DEFINENAME_,THIN_FILM_THICKNESS,_VARYINGNAME_,ThinFilmThickness,_SAMPLERNAME_,thinFilmThickness)
#include<samplerFragmentDeclaration>(_DEFINENAME_,AMBIENT_OCCLUSION,_VARYINGNAME_,AmbientOcclusion,_SAMPLERNAME_,ambientOcclusion)
#include<samplerFragmentDeclaration>(_DEFINENAME_,DECAL,_VARYINGNAME_,Decal,_SAMPLERNAME_,decal)
#ifdef REFLECTION
#ifdef REFLECTIONMAP_3D
var reflectionSamplerSampler: sampler;var reflectionSampler: texture_cube<f32>;
#ifdef LODBASEDMICROSFURACE
#else
var reflectionLowSamplerSampler: sampler;var reflectionLowSampler: texture_cube<f32>;var reflectionHighSamplerSampler: sampler;var reflectionHighSampler: texture_cube<f32>;
#endif
#ifdef USEIRRADIANCEMAP
var irradianceSamplerSampler: sampler;var irradianceSampler: texture_cube<f32>;
#endif
#else
var reflectionSamplerSampler: sampler;var reflectionSampler: texture_2d<f32>;
#ifdef LODBASEDMICROSFURACE
#else
var reflectionLowSamplerSampler: sampler;var reflectionLowSampler: texture_2d<f32>;var reflectionHighSamplerSampler: sampler;var reflectionHighSampler: texture_2d<f32>;
#endif
#ifdef USEIRRADIANCEMAP
var irradianceSamplerSampler: sampler;var irradianceSampler: texture_2d<f32>;
#endif
#endif
#ifdef REFLECTIONMAP_SKYBOX
varying vPositionUVW: vec3f;
#else
#if defined(REFLECTIONMAP_EQUIRECTANGULAR_FIXED) || defined(REFLECTIONMAP_MIRROREDEQUIRECTANGULAR_FIXED)
varying vDirectionW: vec3f;
#endif
#endif
#endif
#ifdef ENVIRONMENTBRDF
var environmentBrdfSamplerSampler: sampler;var environmentBrdfSampler: texture_2d<f32>;
#endif
#ifdef FUZZENVIRONMENTBRDF
var environmentFuzzBrdfSamplerSampler: sampler;var environmentFuzzBrdfSampler: texture_2d<f32>;
#endif
#if defined(ANISOTROPIC) || defined(FUZZ)
var blueNoiseSamplerSampler: sampler;var blueNoiseSampler: texture_2d<f32>;
#endif
#ifdef IBL_CDF_FILTERING
var icdfSamplerSampler: sampler;var icdfSampler: texture_2d<f32>;
#endif
`;e.IncludesShadersStoreWGSL[a]||(e.IncludesShadersStoreWGSL[a]=S);const n="openpbrNormalMapFragmentMainFunctions",h=`#if defined(GEOMETRY_NORMAL) || defined(CLEARCOAT_BUMP) || defined(ANISOTROPIC) || defined(FUZZ) || defined(DETAIL)
#if defined(TANGENT) && defined(NORMAL) 
varying vTBN0: vec3f;varying vTBN1: vec3f;varying vTBN2: vec3f;
#endif
#ifdef OBJECTSPACE_NORMALMAP
uniform normalMatrix: mat4x4f;fn toNormalMatrix(m: mat4x4f)->mat4x4f
{var a00=m[0][0];var a01=m[0][1];var a02=m[0][2];var a03=m[0][3];var a10=m[1][0];var a11=m[1][1];var a12=m[1][2];var a13=m[1][3];var a20=m[2][0]; 
var a21=m[2][1];var a22=m[2][2];var a23=m[2][3];var a30=m[3][0]; 
var a31=m[3][1];var a32=m[3][2];var a33=m[3][3];var b00=a00*a11-a01*a10;var b01=a00*a12-a02*a10;var b02=a00*a13-a03*a10;var b03=a01*a12-a02*a11;var b04=a01*a13-a03*a11;var b05=a02*a13-a03*a12;var b06=a20*a31-a21*a30;var b07=a20*a32-a22*a30;var b08=a20*a33-a23*a30;var b09=a21*a32-a22*a31;var b10=a21*a33-a23*a31;var b11=a22*a33-a23*a32;var det=b00*b11-b01*b10+b02*b09+b03*b08-b04*b07+b05*b06;var mi=mat4x4<f32>(
(a11*b11-a12*b10+a13*b09)/det,
(a02*b10-a01*b11-a03*b09)/det,
(a31*b05-a32*b04+a33*b03)/det,
(a22*b04-a21*b05-a23*b03)/det,
(a12*b08-a10*b11-a13*b07)/det,
(a00*b11-a02*b08+a03*b07)/det,
(a32*b02-a30*b05-a33*b01)/det,
(a20*b05-a22*b02+a23*b01)/det,
(a10*b10-a11*b08+a13*b06)/det,
(a01*b08-a00*b10-a03*b06)/det,
(a30*b04-a31*b02+a33*b00)/det,
(a21*b02-a20*b04-a23*b00)/det,
(a11*b07-a10*b09-a12*b06)/det,
(a00*b09-a01*b07+a02*b06)/det,
(a31*b01-a30*b03-a32*b00)/det,
(a20*b03-a21*b01+a22*b00)/det);return mat4x4<f32>(mi[0][0],mi[1][0],mi[2][0],mi[3][0],
mi[0][1],mi[1][1],mi[2][1],mi[3][1],
mi[0][2],mi[1][2],mi[2][2],mi[3][2],
mi[0][3],mi[1][3],mi[2][3],mi[3][3]);}
#endif
fn perturbNormalBase(cotangentFrame: mat3x3f,normal: vec3f,scale: f32)->vec3f
{var output=normal;
#ifdef NORMALXYSCALE
output=normalize(output* vec3f(scale,scale,1.0));
#endif
return normalize(cotangentFrame*output);}
fn perturbNormal(cotangentFrame: mat3x3f,textureSample: vec3f,scale: f32)->vec3f
{return perturbNormalBase(cotangentFrame,textureSample*2.0-1.0,scale);}
fn cotangent_frame(normal: vec3f,p: vec3f,uv: vec2f,tangentSpaceParams: vec2f)->mat3x3f
{var dp1: vec3f=dpdx(p);var dp2: vec3f=dpdy(p);var duv1: vec2f=dpdx(uv);var duv2: vec2f=dpdy(uv);var dp2perp: vec3f=cross(dp2,normal);var dp1perp: vec3f=cross(normal,dp1);var tangent: vec3f=dp2perp*duv1.x+dp1perp*duv2.x;var bitangent: vec3f=dp2perp*duv1.y+dp1perp*duv2.y;tangent*=tangentSpaceParams.x;bitangent*=tangentSpaceParams.y;var det: f32=max(dot(tangent,tangent),dot(bitangent,bitangent));var invmax: f32=select(inverseSqrt(det),0.0,det==0.0);return mat3x3f(tangent*invmax,bitangent*invmax,normal);}
#endif
`;e.IncludesShadersStoreWGSL[n]||(e.IncludesShadersStoreWGSL[n]=h);const i="openpbrNormalMapFragmentFunctions",R=`#if defined(GEOMETRY_NORMAL)
#include<samplerFragmentDeclaration>(_DEFINENAME_,GEOMETRY_NORMAL,_VARYINGNAME_,GeometryNormal,_SAMPLERNAME_,geometryNormal)
#endif
#if defined(GEOMETRY_COAT_NORMAL)
#include<samplerFragmentDeclaration>(_DEFINENAME_,GEOMETRY_COAT_NORMAL,_VARYINGNAME_,GeometryCoatNormal,_SAMPLERNAME_,geometryCoatNormal)
#endif
#if defined(DETAIL)
#include<samplerFragmentDeclaration>(_DEFINENAME_,DETAIL,_VARYINGNAME_,Detail,_SAMPLERNAME_,detail)
#endif
#if defined(GEOMETRY_NORMAL) && defined(PARALLAX)
const minSamples: f32=4.;const maxSamples: f32=15.;const iMaxSamples: i32=15;fn parallaxOcclusion(vViewDirCoT: vec3f,vNormalCoT: vec3f,texCoord: vec2f,parallaxScale: f32)->vec2f {var parallaxLimit: f32=length(vViewDirCoT.xy)/vViewDirCoT.z;parallaxLimit*=parallaxScale;var vOffsetDir: vec2f=normalize(vViewDirCoT.xy);var vMaxOffset: vec2f=vOffsetDir*parallaxLimit;var numSamples: f32=maxSamples+(dot(vViewDirCoT,vNormalCoT)*(minSamples-maxSamples));var stepSize: f32=1.0/numSamples;var currRayHeight: f32=1.0;var vCurrOffset: vec2f= vec2f(0,0);var vLastOffset: vec2f= vec2f(0,0);var lastSampledHeight: f32=1.0;var currSampledHeight: f32=1.0;var keepWorking: bool=true;for (var i: i32=0; i<iMaxSamples; i++)
{currSampledHeight=textureSample(geometryNormalSampler,geometryNormalSamplerSampler,texCoord+vCurrOffset).w;if (!keepWorking)
{}
else if (currSampledHeight>currRayHeight)
{var delta1: f32=currSampledHeight-currRayHeight;var delta2: f32=(currRayHeight+stepSize)-lastSampledHeight;var ratio: f32=delta1/(delta1+delta2);vCurrOffset=(ratio)* vLastOffset+(1.0-ratio)*vCurrOffset;keepWorking=false;}
else
{currRayHeight-=stepSize;vLastOffset=vCurrOffset;
#ifdef PARALLAX_RHS
vCurrOffset-=stepSize*vMaxOffset;
#else
vCurrOffset+=stepSize*vMaxOffset;
#endif
lastSampledHeight=currSampledHeight;}}
return vCurrOffset;}
fn parallaxOffset(viewDir: vec3f,heightScale: f32)->vec2f
{var height: f32=textureSample(geometryNormalSampler,geometryNormalSamplerSampler,fragmentInputs.vGeometryNormalUV).w;var texCoordOffset: vec2f=heightScale*viewDir.xy*height;
#ifdef PARALLAX_RHS
return texCoordOffset;
#else
return -texCoordOffset;
#endif
}
#endif
`;e.IncludesShadersStoreWGSL[i]||(e.IncludesShadersStoreWGSL[i]=R);const o="openpbrDielectricReflectance",N=`struct ReflectanceParams
{F0: f32,
F90: f32,
coloredF0: vec3f,
coloredF90: vec3f,};
#define pbr_inline
fn dielectricReflectance(
insideIOR: f32,outsideIOR: f32,specularColor: vec3f,specularWeight: f32
)->ReflectanceParams
{var outParams: ReflectanceParams;let dielectricF0=pow((insideIOR-outsideIOR)/(insideIOR+outsideIOR),2.0);
#if DIELECTRIC_SPECULAR_MODEL==DIELECTRIC_SPECULAR_MODEL_GLTF
let maxF0=max(specularColor.r,max(specularColor.g,specularColor.b));outParams.F0=dielectricF0*maxF0*specularWeight;
#else
outParams.F0=dielectricF0*specularWeight;
#endif
let f90Scale=clamp(2.0f*abs(insideIOR-outsideIOR),0.0f,1.0f);outParams.F90=f90Scale*specularWeight;outParams.coloredF0=vec3f(dielectricF0*specularWeight)*specularColor.rgb;
#if (DIELECTRIC_SPECULAR_MODEL==DIELECTRIC_SPECULAR_MODEL_OPENPBR)
let dielectricColorF90: vec3f=specularColor.rgb*vec3f(f90Scale)*specularWeight;
#else
let dielectricColorF90: vec3f=vec3f(f90Scale)*specularWeight;
#endif
outParams.coloredF90=dielectricColorF90;return outParams;}
`;e.IncludesShadersStoreWGSL[o]||(e.IncludesShadersStoreWGSL[o]=N);const t="openpbrConductorReflectance",A=`#define pbr_inline
fn conductorReflectance(baseColor: vec3f,specularColor: vec3f,specularWeight: f32)->ReflectanceParams
{var outParams: ReflectanceParams;
#if (CONDUCTOR_SPECULAR_MODEL==CONDUCTOR_SPECULAR_MODEL_OPENPBR)
outParams.coloredF0=baseColor*specularWeight;outParams.coloredF90=specularColor*specularWeight;
#else
outParams.coloredF0=baseColor;outParams.coloredF90=vec3f(1.0f);
#endif
outParams.F0=1.0f;outParams.F90=1.0f;return outParams;}`;e.IncludesShadersStoreWGSL[t]||(e.IncludesShadersStoreWGSL[t]=A);const f="openpbrBlockAmbientOcclusion",L=`struct ambientOcclusionOutParams
{ambientOcclusionColor: vec3f,
#if DEBUGMODE>0 && defined(AMBIENT_OCCLUSION)
ambientOcclusionColorMap: vec3f
#endif
};
#define pbr_inline
fn ambientOcclusionBlock(
#ifdef AMBIENT_OCCLUSION
ambientOcclusionColorMap_: vec3f,
ambientInfos: vec2f
#endif
)->ambientOcclusionOutParams
{ 
var outParams: ambientOcclusionOutParams;var ambientOcclusionColor: vec3f= vec3f(1.,1.,1.);
#ifdef AMBIENT_OCCLUSION
var ambientOcclusionColorMap: vec3f=ambientOcclusionColorMap_*ambientInfos.y;
#ifdef AMBIENTINGRAYSCALE
ambientOcclusionColorMap= vec3f(ambientOcclusionColorMap.r,ambientOcclusionColorMap.r,ambientOcclusionColorMap.r);
#endif
#if DEBUGMODE>0
outParams.ambientOcclusionColorMap=ambientOcclusionColorMap;
#endif
#endif
outParams.ambientOcclusionColor=ambientOcclusionColor;return outParams;}
`;e.IncludesShadersStoreWGSL[f]||(e.IncludesShadersStoreWGSL[f]=L);const l="openpbrGeometryInfo",T=`struct geometryInfoOutParams
{NdotV: f32,
NdotVUnclamped: f32,
environmentBrdf: vec3f,
horizonOcclusion: f32};struct geometryInfoAnisoOutParams
{NdotV: f32,
NdotVUnclamped: f32,
environmentBrdf: vec3f,
horizonOcclusion: f32,
anisotropy: f32,
anisotropicTangent: vec3f,
anisotropicBitangent: vec3f,
TBN: mat3x3<f32>};fn geometryInfo(
normalW: vec3f,viewDirectionW: vec3f,roughness: f32,geometricNormalW: vec3f
)->geometryInfoOutParams
{var outParams: geometryInfoOutParams;outParams.NdotVUnclamped=dot(normalW,viewDirectionW);outParams.NdotV=absEps(outParams.NdotVUnclamped);
#if defined(ENVIRONMENTBRDF)
outParams.environmentBrdf=getBRDFLookup(outParams.NdotV,roughness);
#else
outParams.environmentBrdf=vec3f(0.0);
#endif
outParams.horizonOcclusion=1.0f;
#if defined(ENVIRONMENTBRDF) && !defined(REFLECTIONMAP_SKYBOX)
#ifdef HORIZONOCCLUSION
#if defined(GEOMETRY_NORMAL) || defined(GEOMETRY_COAT_NORMAL)
#ifdef REFLECTIONMAP_3D
outParams.horizonOcclusion=environmentHorizonOcclusion(-viewDirectionW,normalW,geometricNormalW);
#endif
#endif
#endif
#endif
return outParams;}
fn geometryInfoAniso(
normalW: vec3f,viewDirectionW: vec3f,roughness: f32,geometricNormalW: vec3f
,vAnisotropy: vec3f,TBN: mat3x3<f32>
)->geometryInfoAnisoOutParams
{let geoInfo: geometryInfoOutParams=geometryInfo(normalW,viewDirectionW,roughness,geometricNormalW);var outParams: geometryInfoAnisoOutParams;outParams.NdotV=geoInfo.NdotV;outParams.NdotVUnclamped=geoInfo.NdotVUnclamped;outParams.environmentBrdf=geoInfo.environmentBrdf;outParams.horizonOcclusion=geoInfo.horizonOcclusion;outParams.anisotropy=vAnisotropy.b;let anisotropyDirection: vec3f=vec3f(vAnisotropy.xy,0.);let anisoTBN: mat3x3<f32>=mat3x3<f32>(normalize(TBN[0]),normalize(TBN[1]),normalize(TBN[2]));outParams.anisotropicTangent=normalize(anisoTBN*anisotropyDirection);outParams.anisotropicBitangent=normalize(cross(anisoTBN[2],outParams.anisotropicTangent));outParams.TBN=TBN;return outParams;}
`;e.IncludesShadersStoreWGSL[l]||(e.IncludesShadersStoreWGSL[l]=T);const s="openpbrIblFunctions",F=`#ifdef REFLECTION
fn sampleIrradiance(
surfaceNormal: vec3f
#if defined(NORMAL) && defined(USESPHERICALINVERTEX)
,vEnvironmentIrradianceSH: vec3f
#endif
#if (defined(USESPHERICALFROMREFLECTIONMAP) && (!defined(NORMAL) || !defined(USESPHERICALINVERTEX))) || (defined(USEIRRADIANCEMAP) && defined(REFLECTIONMAP_3D))
,iblMatrix: mat4x4f
#endif
#ifdef USEIRRADIANCEMAP
#ifdef REFLECTIONMAP_3D
,irradianceSampler: texture_cube<f32>
,irradianceSamplerSampler: sampler
#else
,irradianceSampler: texture_2d<f32>
,irradianceSamplerSampler: sampler
#endif
#ifdef USE_IRRADIANCE_DOMINANT_DIRECTION
,reflectionDominantDirection: vec3f
#endif
#endif
#ifdef REALTIME_FILTERING
,reflectionFilteringInfo: vec2f
#ifdef IBL_CDF_FILTERING
,icdfSampler: texture_2d<f32>
,icdfSamplerSampler: sampler
#endif
#endif
,reflectionInfos: vec2f
,viewDirectionW: vec3f
,diffuseRoughness: f32
,surfaceAlbedo: vec3f
)->vec3f {var environmentIrradiance=vec3f(0.,0.,0.);
#if (defined(USESPHERICALFROMREFLECTIONMAP) && (!defined(NORMAL) || !defined(USESPHERICALINVERTEX))) || (defined(USEIRRADIANCEMAP) && defined(REFLECTIONMAP_3D))
var irradianceVector=(iblMatrix*vec4f(surfaceNormal,0.0f)).xyz;let irradianceView=(iblMatrix*vec4f(viewDirectionW,0.0f)).xyz;
#if !defined(USE_IRRADIANCE_DOMINANT_DIRECTION) && !defined(REALTIME_FILTERING)
#if BASE_DIFFUSE_MODEL != BRDF_DIFFUSE_MODEL_LAMBERT && BASE_DIFFUSE_MODEL != BRDF_DIFFUSE_MODEL_LEGACY
{let NdotV=max(dot(surfaceNormal,viewDirectionW),0.0f);irradianceVector=mix(irradianceVector,irradianceView,(0.5f*(1.0f-NdotV))*diffuseRoughness);}
#endif
#endif
#ifdef REFLECTIONMAP_OPPOSITEZ
irradianceVector.z*=-1.0f;
#endif
#ifdef INVERTCUBICMAP
irradianceVector.y*=-1.0f;
#endif
#endif
#ifdef USESPHERICALFROMREFLECTIONMAP
#if defined(NORMAL) && defined(USESPHERICALINVERTEX)
environmentIrradiance=vEnvironmentIrradianceSH;
#else
#if defined(REALTIME_FILTERING)
environmentIrradiance=irradiance(reflectionSampler,reflectionSamplerSampler,irradianceVector,reflectionFilteringInfo,diffuseRoughness,surfaceAlbedo,irradianceView
#ifdef IBL_CDF_FILTERING
,icdfSampler
,icdfSamplerSampler
#endif
);
#else
environmentIrradiance=computeEnvironmentIrradiance(irradianceVector);
#endif
#endif
#elif defined(USEIRRADIANCEMAP)
#ifdef REFLECTIONMAP_3D
let environmentIrradianceFromTexture: vec4f=textureSample(irradianceSampler,irradianceSamplerSampler,irradianceVector);
#else
let environmentIrradianceFromTexture: vec4f=textureSample(irradianceSampler,irradianceSamplerSampler,reflectionCoords);
#endif
environmentIrradiance=environmentIrradianceFromTexture.rgb;
#ifdef RGBDREFLECTION
environmentIrradiance.rgb=fromRGBD(environmentIrradianceFromTexture);
#endif
#ifdef GAMMAREFLECTION
environmentIrradiance.rgb=toLinearSpace(environmentIrradiance.rgb);
#endif
#ifdef USE_IRRADIANCE_DOMINANT_DIRECTION
let Ls: vec3f=normalize(reflectionDominantDirection);let NoL: f32=dot(irradianceVector,Ls);let NoV: f32=dot(irradianceVector,irradianceView);var diffuseRoughnessTerm=vec3f(1.0f);
#if BASE_DIFFUSE_MODEL==BRDF_DIFFUSE_MODEL_EON
let LoV: f32=dot (Ls,irradianceView);let mag: f32=length(reflectionDominantDirection)*2.0f;let clampedAlbedo: vec3f=clamp(surfaceAlbedo,vec3f(0.1f),vec3f(1.0f));diffuseRoughnessTerm=diffuseBRDF_EON(clampedAlbedo,diffuseRoughness,NoL,NoV,LoV)*PI;diffuseRoughnessTerm=diffuseRoughnessTerm/clampedAlbedo;diffuseRoughnessTerm=mix(vec3f(1.0f),diffuseRoughnessTerm,sqrt(clamp(mag*NoV,0.0f,1.0f)));
#elif BASE_DIFFUSE_MODEL==BRDF_DIFFUSE_MODEL_BURLEY
let H: vec3f=(irradianceView+Ls)*0.5f;let VoH: f32=dot(irradianceView,H);diffuseRoughnessTerm=vec3f(diffuseBRDF_Burley(NoL,NoV,VoH,diffuseRoughness)*PI);
#endif
environmentIrradiance=environmentIrradiance.rgb*diffuseRoughnessTerm;
#endif
#endif
environmentIrradiance*=reflectionInfos.x;return environmentIrradiance;}
#ifdef REFLECTIONMAP_3D
fn createReflectionCoords(vPositionW: vec3f,normalW: vec3f)->vec3f
#else
fn createReflectionCoords(vPositionW: vec3f,normalW: vec3f)->vec2f
#endif
{var reflectionVector: vec3f=computeReflectionCoords(vec4f(vPositionW,1.0f),normalW);
#ifdef REFLECTIONMAP_OPPOSITEZ
reflectionVector.z*=-1.0;
#endif
#ifdef REFLECTIONMAP_3D
var reflectionCoords: vec3f=reflectionVector;
#else
var reflectionCoords: vec2f=reflectionVector.xy;
#ifdef REFLECTIONMAP_PROJECTION
reflectionCoords/=reflectionVector.z;
#endif
reflectionCoords.y=1.0f-reflectionCoords.y;
#endif
return reflectionCoords;}
fn sampleRadiance(
alphaG: f32
,reflectionMicrosurfaceInfos: vec3f
,reflectionInfos: vec2f
,geoInfo: geometryInfoOutParams
#ifdef REFLECTIONMAP_3D
,reflectionSampler: texture_cube<f32>
,reflectionSamplerSampler: sampler
,reflectionCoords: vec3f
#else
,reflectionSampler: texture_2d<f32>
,reflectionSamplerSampler: sampler
,reflectionCoords: vec2f
#endif
#ifdef REALTIME_FILTERING
,reflectionFilteringInfo: vec2f
#endif
)->vec3f {var environmentRadiance: vec4f=vec4f(0.f,0.f,0.f,0.f);
#if defined(LODINREFLECTIONALPHA) && !defined(REFLECTIONMAP_SKYBOX)
var reflectionLOD: f32=getLodFromAlphaG(reflectionMicrosurfaceInfos.x,alphaG,geoInfo.NdotVUnclamped);
#elif defined(LINEARSPECULARREFLECTION)
var reflectionLOD: f32=getLinearLodFromRoughness(reflectionMicrosurfaceInfos.x,roughness);
#else
var reflectionLOD: f32=getLodFromAlphaG(reflectionMicrosurfaceInfos.x,alphaG);
#endif
reflectionLOD=reflectionLOD*reflectionMicrosurfaceInfos.y+reflectionMicrosurfaceInfos.z;
#ifdef REALTIME_FILTERING
environmentRadiance=vec4f(radiance(alphaG,reflectionSampler,reflectionSamplerSampler,reflectionCoords,reflectionFilteringInfo),1.0f);
#else
environmentRadiance=textureSampleLevel(reflectionSampler,reflectionSamplerSampler,reflectionCoords,reflectionLOD);
#endif
#ifdef RGBDREFLECTION
environmentRadiance.rgb=fromRGBD(environmentRadiance);
#endif
#ifdef GAMMAREFLECTION
environmentRadiance.rgb=toLinearSpace(environmentRadiance.rgb);
#endif
return environmentRadiance.rgb;}
#if defined(ANISOTROPIC)
fn sampleRadianceAnisotropic(
alphaG: f32
,reflectionMicrosurfaceInfos: vec3f
,reflectionInfos: vec2f
,geoInfo: geometryInfoAnisoOutParams
,normalW: vec3f
,viewDirectionW: vec3f
,positionW: vec3f
,noise: vec3f
#ifdef REFLECTIONMAP_3D
,reflectionSampler: texture_cube<f32>
,reflectionSamplerSampler: sampler
#else
,reflectionSampler: texture_2d<f32>
,reflectionSamplerSampler: sampler
#endif
#ifdef REALTIME_FILTERING
,reflectionFilteringInfo: vec2f
#endif
)->vec3f {var environmentRadiance: vec4f=vec4f(0.f,0.f,0.f,0.f);let alphaT=alphaG*sqrt(2.0f/(1.0f+(1.0f-geoInfo.anisotropy)*(1.0f-geoInfo.anisotropy)));let alphaB=(1.0f-geoInfo.anisotropy)*alphaT;let modifiedAlphaG=alphaB;
#if defined(LODINREFLECTIONALPHA) && !defined(REFLECTIONMAP_SKYBOX)
var reflectionLOD: f32=getLodFromAlphaG(reflectionMicrosurfaceInfos.x,modifiedAlphaG,geoInfo.NdotVUnclamped);
#elif defined(LINEARSPECULARREFLECTION)
var reflectionLOD: f32=getLinearLodFromRoughness(reflectionMicrosurfaceInfos.x,roughness);
#else
var reflectionLOD: f32=getLodFromAlphaG(reflectionMicrosurfaceInfos.x,modifiedAlphaG);
#endif
reflectionLOD=reflectionLOD*reflectionMicrosurfaceInfos.y+reflectionMicrosurfaceInfos.z;
#ifdef REALTIME_FILTERING
var view=(uniforms.reflectionMatrix*vec4f(viewDirectionW,0.0f)).xyz;var tangent=(uniforms.reflectionMatrix*vec4f(geoInfo.anisotropicTangent,0.0f)).xyz;var bitangent=(uniforms.reflectionMatrix*vec4f(geoInfo.anisotropicBitangent,0.0f)).xyz;var normal=(uniforms.reflectionMatrix*vec4f(normalW,0.0f)).xyz;
#ifdef REFLECTIONMAP_OPPOSITEZ
view.z*=-1.0f;tangent.z*=-1.0f;bitangent.z*=-1.0f;normal.z*=-1.0f;
#endif
environmentRadiance =
vec4f(radianceAnisotropic(alphaT,alphaB,reflectionSampler,reflectionSamplerSampler,
view,tangent,
bitangent,normal,
reflectionFilteringInfo,noise.xy),
1.0f);
#else
const samples: i32=16;var radianceSample=vec4f(0.0);var accumulatedRadiance=vec3f(0.0);var reflectionCoords=vec3f(0.0);var sample_weight=0.0f;var total_weight=0.0f;let step=1.0f/f32(max(samples-1,1));for (var i: i32=0; i<samples; i++) {var t: f32=mix(-1.0,1.0,f32(i)*step);t+=step*2.0*noise.x;sample_weight=max(1.0-abs(t),0.001);sample_weight*=sample_weight;t*=min(4.0*alphaT*geoInfo.anisotropy,1.0);var bentNormal: vec3f;if (t<0.0) {let blend: f32=t+1.0;bentNormal=normalize(mix(-geoInfo.anisotropicTangent,normalW,blend));} else if (t>0.0) {let blend: f32=t;bentNormal=normalize(mix(normalW,geoInfo.anisotropicTangent,blend));} else {bentNormal=normalW;}
reflectionCoords=createReflectionCoords(positionW,bentNormal);radianceSample=textureSampleLevel(reflectionSampler,reflectionSamplerSampler,reflectionCoords,reflectionLOD);
#ifdef RGBDREFLECTION
accumulatedRadiance+=vec3f(sample_weight)*fromRGBD(radianceSample);
#elif defined(GAMMAREFLECTION)
accumulatedRadiance+=vec3f(sample_weight)*toLinearSpace(radianceSample.rgb);
#else
accumulatedRadiance+=vec3f(sample_weight)*radianceSample.rgb;
#endif
total_weight+=sample_weight;}
environmentRadiance=vec4f(accumulatedRadiance/vec3f(total_weight),1.0f);
#endif
environmentRadiance=vec4f(environmentRadiance.rgb*reflectionInfos.xxx,environmentRadiance.a);return environmentRadiance.rgb;}
#endif
fn conductorIblFresnel(reflectance: ReflectanceParams,NdotV: f32,roughness: f32,environmentBrdf: vec3f)->vec3f
{
#if (CONDUCTOR_SPECULAR_MODEL==CONDUCTOR_SPECULAR_MODEL_OPENPBR)
let albedoF0: vec3f=mix(reflectance.coloredF0,pow(reflectance.coloredF0,vec3f(1.4f)),roughness);return getF82Specular(NdotV,albedoF0,reflectance.coloredF90,roughness);
#else
return getReflectanceFromBRDFLookup(reflectance.coloredF0,reflectance.coloredF90,environmentBrdf);
#endif
}
#endif
`;e.IncludesShadersStoreWGSL[s]||(e.IncludesShadersStoreWGSL[s]=F);const c="openpbrNormalMapFragment",O=`var uvOffset: vec2f= vec2f(0.0,0.0);
#if defined(GEOMETRY_NORMAL) || defined(GEOMETRY_COAT_NORMAL) || defined(PARALLAX) || defined(DETAIL)
#ifdef NORMALXYSCALE
var normalScale: f32=1.0;
#elif defined(GEOMETRY_NORMAL)
var normalScale: f32=uniforms.vGeometryNormalInfos.y;
#else
var normalScale: f32=1.0;
#endif
#if defined(TANGENT) && defined(NORMAL)
var TBN: mat3x3f=mat3x3<f32>(input.vTBN0,input.vTBN1,input.vTBN2); 
#elif defined(GEOMETRY_NORMAL)
var TBNUV: vec2f=select(-fragmentInputs.vGeometryNormalUV,fragmentInputs.vGeometryNormalUV,fragmentInputs.frontFacing);var TBN: mat3x3f=cotangent_frame(normalW*normalScale,input.vPositionW,TBNUV,uniforms.vTangentSpaceParams);
#else
var TBNUV: vec2f=select(-fragmentInputs.vDetailUV,fragmentInputs.vDetailUV,fragmentInputs.frontFacing);var TBN: mat3x3f=cotangent_frame(normalW*normalScale,input.vPositionW,TBNUV, vec2f(1.,1.));
#endif
#elif defined(ANISOTROPIC) || defined(FUZZ)
#if defined(TANGENT) && defined(NORMAL)
var TBN: mat3x3f=mat3x3<f32>(input.vTBN0,input.vTBN1,input.vTBN2);
#else
var TBNUV: vec2f=select( -fragmentInputs.vMainUV1,fragmentInputs.vMainUV1,fragmentInputs.frontFacing);var TBN: mat3x3f=cotangent_frame(normalW,input.vPositionW,TBNUV, vec2f(1.,1.));
#endif
#endif
#ifdef PARALLAX
var invTBN: mat3x3f=transposeMat3(TBN);
#ifdef PARALLAXOCCLUSION
#else
#endif
#endif
#ifdef DETAIL
var detailColor: vec4f=textureSample(detailSampler,detailSamplerSampler,fragmentInputs.vDetailUV+uvOffset);var detailNormalRG: vec2f=detailColor.wy*2.0-1.0;var detailNormalB: f32=sqrt(1.-saturate(dot(detailNormalRG,detailNormalRG)));var detailNormal: vec3f= vec3f(detailNormalRG,detailNormalB);
#endif
#ifdef GEOMETRY_COAT_NORMAL
coatNormalW=perturbNormal(TBN,textureSample(geometryCoatNormalSampler,geometryCoatNormalSamplerSampler,fragmentInputs.vGeometryCoatNormalUV+uvOffset).xyz,uniforms.vGeometryCoatNormalInfos.y);
#endif
#ifdef GEOMETRY_NORMAL
#ifdef OBJECTSPACE_NORMALMAP
#define CUSTOM_FRAGMENT_BUMP_FRAGMENT
normalW=normalize(textureSample(geometryNormalSampler,geometryNormalSamplerSampler,fragmentInputs.vGeometryNormalUV).xyz *2.0-1.0);normalW=normalize(mat3x3f(uniforms.normalMatrix[0].xyz,uniforms.normalMatrix[1].xyz,uniforms.normalMatrix[2].xyz)*normalW);
#elif !defined(DETAIL)
normalW=perturbNormal(TBN,textureSample(geometryNormalSampler,geometryNormalSamplerSampler,fragmentInputs.vGeometryNormalUV+uvOffset).xyz,uniforms.vGeometryNormalInfos.y);
#else
var sampledNormal: vec3f=textureSample(geometryNormalSampler,geometryNormalSamplerSampler,fragmentInputs.vGeometryNormalUV+uvOffset).xyz*2.0-1.0;
#if DETAIL_NORMALBLENDMETHOD==0 
detailNormal=vec3f(detailNormal.xy*uniforms.vDetailInfos.z,detailNormal.z);var blendedNormal: vec3f=normalize( vec3f(sampledNormal.xy+detailNormal.xy,sampledNormal.z*detailNormal.z));
#elif DETAIL_NORMALBLENDMETHOD==1 
detailNormal=vec3f(detailNormal.xy*uniforms.vDetailInfos.z,detailNormal.z);sampledNormal+= vec3f(0.0,0.0,1.0);detailNormal*= vec3f(-1.0,-1.0,1.0);var blendedNormal: vec3f=sampledNormal*dot(sampledNormal,detailNormal)/sampledNormal.z-detailNormal;
#endif
normalW=perturbNormalBase(TBN,blendedNormal,uniforms.vGeometryNormalInfos.y);
#endif
#elif defined(DETAIL)
detailNormal=vec3f(detailNormal.xy*uniforms.vDetailInfos.z,detailNormal.z);normalW=perturbNormalBase(TBN,detailNormal,uniforms.vDetailInfos.z);
#endif
`;e.IncludesShadersStoreWGSL[c]||(e.IncludesShadersStoreWGSL[c]=O);const m="openpbrBlockNormalFinal",b=`#if defined(FORCENORMALFORWARD) && defined(NORMAL)
var faceNormal: vec3f=normalize(cross(dpdx(fragmentInputs.vPositionW),dpdy(fragmentInputs.vPositionW)))*scene.vEyePosition.w;
#if defined(TWOSIDEDLIGHTING)
faceNormal=select(-faceNormal,faceNormal,fragmentInputs.frontFacing);
#endif
normalW*=sign(dot(normalW,faceNormal));coatNormalW*=sign(dot(coatNormalW,faceNormal));
#endif
#if defined(TWOSIDEDLIGHTING) && defined(NORMAL)
#if defined(MIRRORED)
normalW=select(normalW,-normalW,fragmentInputs.frontFacing);coatNormalW=select(coatNormalW,-coatNormalW,fragmentInputs.frontFacing);
#else
normalW=select(-normalW,normalW,fragmentInputs.frontFacing);coatNormalW=select(-coatNormalW,coatNormalW,fragmentInputs.frontFacing);
#endif
#endif
`;e.IncludesShadersStoreWGSL[m]||(e.IncludesShadersStoreWGSL[m]=b);const d="openpbrBaseLayerData",C=`var base_color=vec3f(0.8);var base_metalness: f32=0.0;var base_diffuse_roughness: f32=0.0;var specular_weight: f32=1.0;var specular_roughness: f32=0.3;var specular_color: vec3f=vec3f(1.0);var specular_roughness_anisotropy: f32=0.0;var specular_ior: f32=1.5;var alpha: f32=1.0;var geometry_tangent: vec2f=vec2f(1.0,0.0);
#ifdef BASE_WEIGHT
let baseWeightFromTexture: vec4f=textureSample(baseWeightSampler,baseWeightSamplerSampler,fragmentInputs.vBaseWeightUV+uvOffset);
#endif
#ifdef BASE_COLOR
let baseColorFromTexture: vec4f=textureSample(baseColorSampler,baseColorSamplerSampler,fragmentInputs.vBaseColorUV+uvOffset);
#endif
#ifdef BASE_METALNESS
let metallicFromTexture: vec4f=textureSample(baseMetalnessSampler,baseMetalnessSamplerSampler,fragmentInputs.vBaseMetalnessUV+uvOffset);
#endif
#ifdef BASE_DIFFUSE_ROUGHNESS
let baseDiffuseRoughnessFromTexture: f32=textureSample(baseDiffuseRoughnessSampler,baseDiffuseRoughnessSamplerSampler,fragmentInputs.vBaseDiffuseRoughnessUV+uvOffset).r;
#endif
#ifdef GEOMETRY_TANGENT
let geometryTangentFromTexture: vec3f=textureSample(geometryTangentSampler,geometryTangentSamplerSampler,fragmentInputs.vGeometryTangentUV+uvOffset).rgb;
#endif
#ifdef SPECULAR_ROUGHNESS_ANISOTROPY
let anisotropyFromTexture: f32=textureSample(specularRoughnessAnisotropySampler,specularRoughnessAnisotropySamplerSampler,fragmentInputs.vSpecularRoughnessAnisotropyUV+uvOffset).r*uniforms.vSpecularRoughnessAnisotropyInfos.y;
#endif
#ifdef GEOMETRY_OPACITY
let opacityFromTexture: vec4f=textureSample(geometryOpacitySampler,geometryOpacitySamplerSampler,fragmentInputs.vGeometryOpacityUV+uvOffset);
#endif
#ifdef DECAL
let decalFromTexture: vec4f=textureSample(decalSampler,decalSamplerSampler,fragmentInputs.vDecalUV+uvOffset);
#endif
#ifdef SPECULAR_COLOR
let specularColorFromTexture: vec4f=textureSample(specularColorSampler,specularColorSamplerSampler,fragmentInputs.vSpecularColorUV+uvOffset);
#endif
#if defined(SPECULAR_WEIGHT)
#ifdef SPECULAR_WEIGHT_IN_ALPHA
let specularWeightFromTexture: f32=textureSample(specularWeightSampler,specularWeightSamplerSampler,fragmentInputs.vSpecularWeightUV+uvOffset).a;
#else
let specularWeightFromTexture: f32=textureSample(specularWeightSampler,specularWeightSamplerSampler,fragmentInputs.vSpecularWeightUV+uvOffset).r;
#endif
#endif
#if defined(ANISOTROPIC) || defined(FUZZ)
let noise=textureSample(blueNoiseSampler,blueNoiseSamplerSampler,fragmentInputs.position.xy/256.0).xyz;
#endif
#if defined(ROUGHNESSSTOREINMETALMAPGREEN) && defined(BASE_METALNESS)
let roughnessFromTexture: f32=metallicFromTexture.g;
#elif defined(SPECULAR_ROUGHNESS)
let roughnessFromTexture: f32=textureSample(specularRoughnessSampler,specularRoughnessSamplerSampler,fragmentInputs.vSpecularRoughnessUV+uvOffset).r;
#endif
base_color=uniforms.vBaseColor.rgb;
#if defined(VERTEXCOLOR) || defined(INSTANCESCOLOR) && defined(INSTANCES)
base_color*=fragmentInputs.vColor.rgb;
#endif
#if defined(VERTEXALPHA) || defined(INSTANCESCOLOR) && defined(INSTANCES)
alpha*=fragmentInputs.vColor.a;
#endif
base_color*=vec3(uniforms.vBaseWeight);alpha=uniforms.vBaseColor.a;base_metalness=uniforms.vReflectanceInfo.x;base_diffuse_roughness=uniforms.vBaseDiffuseRoughness;specular_roughness=uniforms.vReflectanceInfo.y;specular_color=uniforms.vSpecularColor.rgb;specular_weight=uniforms.vReflectanceInfo.a;specular_ior=uniforms.vReflectanceInfo.z;specular_roughness_anisotropy=uniforms.vSpecularAnisotropy.b;geometry_tangent=uniforms.vSpecularAnisotropy.rg;
#ifdef BASE_COLOR
#ifdef BASE_COLOR_GAMMA
base_color*=toLinearSpace(baseColorFromTexture.rgb);
#else
base_color*=baseColorFromTexture.rgb;
#endif
base_color*=uniforms.vBaseColorInfos.y;
#endif
#ifdef BASE_WEIGHT
base_color*=baseWeightFromTexture.r;
#endif
#if defined(BASE_COLOR) && defined(ALPHA_FROM_BASE_COLOR_TEXTURE)
alpha*=baseColorFromTexture.a;
#elif defined(GEOMETRY_OPACITY)
alpha*=opacityFromTexture.a;alpha*=uniforms.vGeometryOpacityInfos.y;
#endif
#ifdef ALPHATEST
#if DEBUGMODE != 88
if (alpha<ALPHATESTVALUE)
discard;
#endif
#ifndef ALPHABLEND
alpha=1.0;
#endif
#endif
#ifdef BASE_METALNESS
#ifdef METALLNESSSTOREINMETALMAPBLUE
base_metalness*=metallicFromTexture.b;
#else
base_metalness*=metallicFromTexture.r;
#endif
#endif
#ifdef BASE_DIFFUSE_ROUGHNESS
base_diffuse_roughness*=baseDiffuseRoughnessFromTexture*uniforms.vBaseDiffuseRoughnessInfos.y;
#endif
#ifdef SPECULAR_COLOR
#ifdef SPECULAR_COLOR_GAMMA
specular_color*=toLinearSpace(specularColorFromTexture.rgb);
#else
specular_color*=specularColorFromTexture.rgb;
#endif
#endif
#ifdef SPECULAR_WEIGHT_FROM_SPECULAR_COLOR_TEXTURE
specular_weight*=specularColorFromTexture.a;
#elif defined(SPECULAR_WEIGHT)
specular_weight*=specularWeightFromTexture;
#endif
#if defined(SPECULAR_ROUGHNESS) || (defined(ROUGHNESSSTOREINMETALMAPGREEN) && defined(BASE_METALNESS))
specular_roughness*=roughnessFromTexture;
#endif
#ifdef GEOMETRY_TANGENT
{let tangentFromTexture: vec2f=normalize(geometryTangentFromTexture.xy*vec2f(2.0f)-vec2f(1.0f));let tangent_angle_texture: f32=atan2(tangentFromTexture.y,tangentFromTexture.x);let tangent_angle_uniform: f32=atan2(geometry_tangent.y,geometry_tangent.x);let tangent_angle: f32=tangent_angle_texture+tangent_angle_uniform;geometry_tangent=vec2f(cos(tangent_angle),sin(tangent_angle));}
#endif
#if defined(GEOMETRY_TANGENT) && defined(SPECULAR_ROUGHNESS_ANISOTROPY_FROM_TANGENT_TEXTURE)
specular_roughness_anisotropy*=geometryTangentFromTexture.b;
#elif defined(SPECULAR_ROUGHNESS_ANISOTROPY)
specular_roughness_anisotropy*=anisotropyFromTexture;
#endif
#ifdef DETAIL
let detailRoughness: f32=mix(0.5f,detailColor.b,vDetailInfos.w);let loLerp: f32=mix(0.f,specular_roughness,detailRoughness*2.f);let hiLerp: f32=mix(specular_roughness,1.f,(detailRoughness-0.5f)*2.f);specular_roughness=mix(loLerp,hiLerp,step(detailRoughness,0.5f));
#endif
#ifdef USE_GLTF_STYLE_ANISOTROPY
let baseAlpha: f32=specular_roughness*specular_roughness;let roughnessT: f32=mix(baseAlpha,1.0f,specular_roughness_anisotropy*specular_roughness_anisotropy);let roughnessB: f32=baseAlpha;specular_roughness_anisotropy=1.0f-roughnessB/max(roughnessT,0.00001f);specular_roughness=sqrt(roughnessT/sqrt(2.0f/(1.0f+(1.0f-specular_roughness_anisotropy)*(1.0f-specular_roughness_anisotropy))));
#endif
`;e.IncludesShadersStoreWGSL[d]||(e.IncludesShadersStoreWGSL[d]=C);const u="openpbrCoatLayerData",D=`var coat_weight: f32=0.0f;var coat_color: vec3f=vec3f(1.0f);var coat_roughness: f32=0.0f;var coat_roughness_anisotropy: f32=0.0f;var coat_ior: f32=1.6f;var coat_darkening: f32=1.0f;var geometry_coat_tangent: vec2f=vec2f(1.0f,0.0f);
#ifdef COAT_WEIGHT
var coatWeightFromTexture: vec4f=textureSample(coatWeightSampler,coatWeightSamplerSampler,fragmentInputs.vCoatWeightUV+uvOffset);
#endif
#ifdef COAT_COLOR
var coatColorFromTexture: vec4f=textureSample(coatColorSampler,coatColorSamplerSampler,fragmentInputs.vCoatColorUV+uvOffset);
#endif
#ifdef COAT_ROUGHNESS
var coatRoughnessFromTexture: vec4f=textureSample(coatRoughnessSampler,coatRoughnessSamplerSampler,fragmentInputs.vCoatRoughnessUV+uvOffset);
#endif
#ifdef COAT_ROUGHNESS_ANISOTROPY
var coatRoughnessAnisotropyFromTexture: f32=textureSample(coatRoughnessAnisotropySampler,coatRoughnessAnisotropySamplerSampler,fragmentInputs.vCoatRoughnessAnisotropyUV+uvOffset).r;
#endif
#ifdef COAT_DARKENING
var coatDarkeningFromTexture: vec4f=textureSample(coatDarkeningSampler,coatDarkeningSamplerSampler,fragmentInputs.vCoatDarkeningUV+uvOffset);
#endif
#ifdef GEOMETRY_COAT_TANGENT
var geometryCoatTangentFromTexture: vec3f=textureSample(geometryCoatTangentSampler,geometryCoatTangentSamplerSampler,fragmentInputs.vGeometryCoatTangentUV+uvOffset).rgb;
#endif
coat_color=uniforms.vCoatColor.rgb;coat_weight=uniforms.vCoatWeight;coat_roughness=uniforms.vCoatRoughness;coat_roughness_anisotropy=uniforms.vCoatRoughnessAnisotropy;coat_ior=uniforms.vCoatIor;coat_darkening=uniforms.vCoatDarkening;geometry_coat_tangent=uniforms.vGeometryCoatTangent.rg;
#ifdef COAT_WEIGHT
coat_weight*=coatWeightFromTexture.r;
#endif
#ifdef COAT_COLOR
#ifdef COAT_COLOR_GAMMA
coat_color*=toLinearSpace(coatColorFromTexture.rgb);
#else
coat_color*=coatColorFromTexture.rgb;
#endif
coat_color*=uniforms.vCoatColorInfos.y;
#endif
#ifdef COAT_ROUGHNESS
#ifdef COAT_ROUGHNESS_FROM_GREEN_CHANNEL
coat_roughness*=coatRoughnessFromTexture.g;
#else
coat_roughness*=coatRoughnessFromTexture.r;
#endif
#endif
#if defined(GEOMETRY_COAT_TANGENT) && defined(COAT_ROUGHNESS_ANISOTROPY_FROM_TANGENT_TEXTURE)
coat_roughness_anisotropy*=geometryCoatTangentFromTexture.b;
#elif defined(COAT_ROUGHNESS_ANISOTROPY)
coat_roughness_anisotropy*=coatRoughnessAnisotropyFromTexture;
#endif
#ifdef COAT_DARKENING
coat_darkening*=coatDarkeningFromTexture.r;
#endif
#ifdef GEOMETRY_COAT_TANGENT
{let tangentFromTexture: vec2f=normalize(geometryCoatTangentFromTexture.xy*vec2f(2.0f)-vec2f(1.0f));let tangent_angle_texture: f32=atan2(tangentFromTexture.y,tangentFromTexture.x);let tangent_angle_uniform: f32=atan2(geometry_coat_tangent.y,geometry_coat_tangent.x);let tangent_angle: f32=tangent_angle_texture+tangent_angle_uniform;geometry_coat_tangent=vec2f(cos(tangent_angle),sin(tangent_angle));}
#endif
#ifdef USE_GLTF_STYLE_ANISOTROPY
let coatAlpha: f32=coat_roughness*coat_roughness;let coatRoughnessT: f32=mix(coatAlpha,1.0f,coat_roughness_anisotropy*coat_roughness_anisotropy);let coatRoughnessB: f32=coatAlpha;coat_roughness_anisotropy=1.0f-coatRoughnessB/max(coatRoughnessT,0.00001f);coat_roughness=sqrt(coatRoughnessT/sqrt(2.0f/(1.0f+(1.0f-coat_roughness_anisotropy)*(1.0f-coat_roughness_anisotropy))));
#endif
`;e.IncludesShadersStoreWGSL[u]||(e.IncludesShadersStoreWGSL[u]=D);const p="openpbrThinFilmLayerData",M=`#ifdef THIN_FILM
var thin_film_weight: f32=uniforms.vThinFilmWeight;var thin_film_thickness: f32=uniforms.vThinFilmThickness.r*1000.0f; 
var thin_film_ior: f32=uniforms.vThinFilmIor;
#ifdef THIN_FILM_WEIGHT
var thinFilmWeightFromTexture: f32=textureSample(thinFilmWeightSampler,thinFilmWeightSamplerSampler,fragmentInputs.vThinFilmWeightUV+uvOffset).r*uniforms.vThinFilmWeightInfos.y;
#endif
#ifdef THIN_FILM_THICKNESS
var thinFilmThicknessFromTexture: f32=textureSample(thinFilmThicknessSampler,thinFilmThicknessSamplerSampler,fragmentInputs.vThinFilmThicknessUV+uvOffset).g*uniforms.vThinFilmThicknessInfos.y;
#endif
#ifdef THIN_FILM_WEIGHT
thin_film_weight*=thinFilmWeightFromTexture;
#endif
#ifdef THIN_FILM_THICKNESS
thin_film_thickness*=thinFilmThicknessFromTexture;
#endif
#endif
`;e.IncludesShadersStoreWGSL[p]||(e.IncludesShadersStoreWGSL[p]=M);const _="openpbrFuzzLayerData",G=`var fuzz_weight: f32=0.0f;var fuzz_color: vec3f=vec3f(1.0);var fuzz_roughness: f32=0.0f;
#ifdef FUZZ
#ifdef FUZZ_WEIGHT
let fuzzWeightFromTexture: vec4=textureSample(fuzzWeightSampler,fuzzWeightSamplerSampler,fragmentInputs.vFuzzWeightUV+uvOffset);
#endif
#ifdef FUZZ_COLOR
var fuzzColorFromTexture: vec4=textureSample(fuzzColorSampler,fuzzColorSamplerSampler,fragmentInputs.vFuzzColorUV+uvOffset);
#endif
#ifdef FUZZ_ROUGHNESS
let fuzzRoughnessFromTexture: vec4=textureSample(fuzzRoughnessSampler,fuzzRoughnessSamplerSampler,fragmentInputs.vFuzzRoughnessUV+uvOffset);
#endif
fuzz_color=uniforms.vFuzzColor.rgb;fuzz_weight=uniforms.vFuzzWeight;fuzz_roughness=uniforms.vFuzzRoughness;
#ifdef FUZZ_WEIGHT
fuzz_weight*=fuzzWeightFromTexture.r;
#endif
#ifdef FUZZ_COLOR
#ifdef FUZZ_COLOR_GAMMA
fuzz_color*=toLinearSpace(fuzzColorFromTexture.rgb);
#else
fuzz_color*=fuzzColorFromTexture.rgb;
#endif
fuzz_color*=uniforms.vFuzzColorInfos.y;
#endif
#if defined(FUZZ_ROUGHNESS) && defined(FUZZ_ROUGHNESS_FROM_TEXTURE_ALPHA)
fuzz_roughness*=fuzzRoughnessFromTexture.a;
#elif defined(FUZZ_ROUGHNESS)
fuzz_roughness*=fuzzRoughnessFromTexture.r;
#endif
#endif
`;e.IncludesShadersStoreWGSL[_]||(e.IncludesShadersStoreWGSL[_]=G);const g="openpbrEnvironmentLighting",x=`#ifdef REFLECTION
#ifdef FUZZ
let environmentFuzzBrdf: vec3f=getFuzzBRDFLookup(fuzzGeoInfo.NdotV,sqrt(fuzz_roughness));
#endif
var baseDiffuseEnvironmentLight: vec3f=sampleIrradiance(
normalW
#if defined(NORMAL) && defined(USESPHERICALINVERTEX)
,vEnvironmentIrradiance 
#endif
#if (defined(USESPHERICALFROMREFLECTIONMAP) && (!defined(NORMAL) || !defined(USESPHERICALINVERTEX))) || (defined(USEIRRADIANCEMAP) && defined(REFLECTIONMAP_3D))
,uniforms.reflectionMatrix
#endif
#ifdef USEIRRADIANCEMAP
,irradianceSampler
,irradianceSamplerSampler
#ifdef USE_IRRADIANCE_DOMINANT_DIRECTION
,uniforms.vReflectionDominantDirection
#endif
#endif
#ifdef REALTIME_FILTERING
,uniforms.vReflectionFilteringInfo
#ifdef IBL_CDF_FILTERING
,icdfSampler
,icdfSamplerSampler
#endif
#endif
,uniforms.vReflectionInfos
,viewDirectionW
,base_diffuse_roughness
,base_color
);
#ifdef REFLECTIONMAP_3D
var reflectionCoords: vec3f=vec3f(0.f,0.f,0.f);
#else
var reflectionCoords: vec2f=vec2f(0.f,0.f);
#endif
let specularAlphaG: f32=specular_roughness*specular_roughness;
#ifdef ANISOTROPIC_BASE
var baseSpecularEnvironmentLight: vec3f=sampleRadianceAnisotropic(specularAlphaG,uniforms.vReflectionMicrosurfaceInfos.rgb,uniforms.vReflectionInfos
,baseGeoInfo
,normalW
,viewDirectionW
,fragmentInputs.vPositionW
,noise
,reflectionSampler
,reflectionSamplerSampler
#ifdef REALTIME_FILTERING
,uniforms.vReflectionFilteringInfo
#endif
);
#else
reflectionCoords=createReflectionCoords(fragmentInputs.vPositionW,normalW);var baseSpecularEnvironmentLight: vec3f=sampleRadiance(specularAlphaG,uniforms.vReflectionMicrosurfaceInfos.rgb,uniforms.vReflectionInfos
,baseGeoInfo
,reflectionSampler
,reflectionSamplerSampler
,reflectionCoords
#ifdef REALTIME_FILTERING
,uniforms.vReflectionFilteringInfo
#endif
);
#endif
#ifdef ANISOTROPIC_BASE
baseSpecularEnvironmentLight=mix(baseSpecularEnvironmentLight.rgb,baseDiffuseEnvironmentLight,specularAlphaG*specularAlphaG *max(1.0f-baseGeoInfo.anisotropy,0.3f));
#else
baseSpecularEnvironmentLight=mix(baseSpecularEnvironmentLight.rgb,baseDiffuseEnvironmentLight,specularAlphaG);
#endif
var coatEnvironmentLight: vec3f=vec3f(0.f,0.f,0.f);if (coat_weight>0.0) {
#ifdef REFLECTIONMAP_3D
var reflectionCoords: vec3f=vec3f(0.f,0.f,0.f);
#else
var reflectionCoords: vec2f=vec2f(0.f,0.f);
#endif
reflectionCoords=createReflectionCoords(fragmentInputs.vPositionW,coatNormalW);var coatAlphaG: f32=coat_roughness*coat_roughness;
#ifdef ANISOTROPIC_COAT
coatEnvironmentLight=sampleRadianceAnisotropic(coatAlphaG,uniforms.vReflectionMicrosurfaceInfos.rgb,uniforms.vReflectionInfos
,coatGeoInfo
,coatNormalW
,viewDirectionW
,fragmentInputs.vPositionW
,noise
,reflectionSampler
,reflectionSamplerSampler
#ifdef REALTIME_FILTERING
,uniforms.vReflectionFilteringInfo
#endif
);
#else
coatEnvironmentLight=sampleRadiance(coatAlphaG,uniforms.vReflectionMicrosurfaceInfos.rgb,uniforms.vReflectionInfos
,coatGeoInfo
,reflectionSampler
,reflectionSamplerSampler
,reflectionCoords
#ifdef REALTIME_FILTERING
,uniforms.vReflectionFilteringInfo
#endif
);
#endif
}
#ifdef FUZZ
let modifiedFuzzRoughness: f32=clamp(fuzz_roughness*(1.0f-0.5f*environmentFuzzBrdf.y),0.0f,1.0f);var fuzzEnvironmentLight=vec3f(0.0f,0.0f,0.0f);var totalWeight=0.0f;let fuzzIblFresnel: f32=sqrt(environmentFuzzBrdf.z);for (var i: i32=0; i<i32(FUZZ_IBL_SAMPLES); i++) {var angle: f32=(f32(i)+noise.x)*(3.141592f*2.0f/f32(FUZZ_IBL_SAMPLES));var fiberCylinderNormal: vec3f=normalize(cos(angle)*fuzzTangent+sin(angle)*fuzzBitangent);let fiberBend=min(environmentFuzzBrdf.x*environmentFuzzBrdf.x*modifiedFuzzRoughness,1.0f);fiberCylinderNormal=normalize(mix(fiberCylinderNormal,fuzzNormalW,fiberBend));let sampleWeight=max(dot(viewDirectionW,fiberCylinderNormal),0.0f);var fuzzReflectionCoords=createReflectionCoords(fragmentInputs.vPositionW,fiberCylinderNormal);let radianceSample: vec3f=sampleRadiance(modifiedFuzzRoughness,uniforms.vReflectionMicrosurfaceInfos.rgb,uniforms.vReflectionInfos
,fuzzGeoInfo
,reflectionSampler
,reflectionSamplerSampler
,fuzzReflectionCoords
#ifdef REALTIME_FILTERING
,uniforms.vReflectionFilteringInfo
#endif
);fuzzEnvironmentLight+=sampleWeight*mix(radianceSample,baseDiffuseEnvironmentLight,fiberBend);totalWeight+=sampleWeight;}
fuzzEnvironmentLight/=totalWeight;
#endif
let dielectricIblFresnel: f32=getReflectanceFromBRDFWithEnvLookup(vec3f(baseDielectricReflectance.F0),vec3f(baseDielectricReflectance.F90),baseGeoInfo.environmentBrdf).r;var dielectricIblColoredFresnel: vec3f=dielectricIblFresnel*specular_color;
#ifdef THIN_FILM
let thinFilmIorScale: f32=clamp(2.0f*abs(thin_film_ior-1.0f),0.0f,1.0f);var thin_film_dielectric: vec3f=evalIridescence(thin_film_outside_ior,thin_film_ior,baseGeoInfo.NdotV,thin_film_thickness,baseDielectricReflectance.coloredF0);let thin_film_desaturation_scale=(thin_film_ior-1.0)*sqrt(thin_film_thickness*0.001f*baseGeoInfo.NdotV);thin_film_dielectric=mix(thin_film_dielectric,vec3(dot(thin_film_dielectric,vec3f(0.3333f))),thin_film_desaturation_scale);dielectricIblColoredFresnel=mix(dielectricIblColoredFresnel,thin_film_dielectric*specular_color,thin_film_weight*thinFilmIorScale);
#endif
var conductorIblFresnel: vec3f=conductorIblFresnel(baseConductorReflectance,baseGeoInfo.NdotV,specular_roughness,baseGeoInfo.environmentBrdf);
#ifdef THIN_FILM
var thinFilmConductorFresnel: vec3f=specular_weight*evalIridescence(thin_film_outside_ior,thin_film_ior,baseGeoInfo.NdotV,thin_film_thickness,baseConductorReflectance.coloredF0);thinFilmConductorFresnel=mix(thinFilmConductorFresnel,vec3(dot(thinFilmConductorFresnel,vec3f(0.3333f))),thin_film_desaturation_scale);conductorIblFresnel=mix(conductorIblFresnel,thinFilmConductorFresnel,thin_film_weight*thinFilmIorScale);
#endif
var coatIblFresnel: f32=0.0;if (coat_weight>0.0) {coatIblFresnel=getReflectanceFromBRDFWithEnvLookup(vec3f(coatReflectance.F0),vec3f(coatReflectance.F90),coatGeoInfo.environmentBrdf).r;}
var slab_diffuse_ibl: vec3f=vec3f(0.,0.,0.);var slab_glossy_ibl: vec3f=vec3f(0.,0.,0.);var slab_metal_ibl: vec3f=vec3f(0.,0.,0.);var slab_coat_ibl: vec3f=vec3f(0.,0.,0.);slab_diffuse_ibl=baseDiffuseEnvironmentLight*uniforms.vLightingIntensity.z;slab_diffuse_ibl*=aoOut.ambientOcclusionColor;slab_glossy_ibl=baseSpecularEnvironmentLight*uniforms.vLightingIntensity.z;slab_metal_ibl=baseSpecularEnvironmentLight*conductorIblFresnel*uniforms.vLightingIntensity.z;var coatAbsorption=vec3f(1.0);if (coat_weight>0.0) {slab_coat_ibl=coatEnvironmentLight*uniforms.vLightingIntensity.z;let hemisphere_avg_fresnel: f32=coatReflectance.F0+0.5f*(1.0f-coatReflectance.F0);var averageReflectance: f32=(coatIblFresnel+hemisphere_avg_fresnel)*0.5f;let roughnessFactor=1.0f-coat_roughness*0.5f;averageReflectance*=roughnessFactor;var darkened_transmission: f32=(1.0f-averageReflectance)*(1.0f-averageReflectance);darkened_transmission=mix(1.0,darkened_transmission,coat_darkening);var sin2: f32=1.0f-coatGeoInfo.NdotV*coatGeoInfo.NdotV;sin2=sin2/(coat_ior*coat_ior);let cos_t: f32=sqrt(1.0f-sin2);let coatPathLength=1.0f/cos_t;let colored_transmission: vec3f=pow(coat_color,vec3f(coatPathLength));coatAbsorption=mix(vec3f(1.0f),colored_transmission*vec3f(darkened_transmission),coat_weight);}
#ifdef FUZZ
let slab_fuzz_ibl=fuzzEnvironmentLight*uniforms.vLightingIntensity.z;
#endif
var slab_subsurface_ibl: vec3f=vec3f(0.,0.,0.);var slab_translucent_base_ibl: vec3f=vec3f(0.,0.,0.);slab_diffuse_ibl*=base_color.rgb;
#define CUSTOM_FRAGMENT_BEFORE_IBLLAYERCOMPOSITION
let material_opaque_base_ibl: vec3f=mix(slab_diffuse_ibl,slab_subsurface_ibl,subsurface_weight);let material_dielectric_base_ibl: vec3f=mix(material_opaque_base_ibl,slab_translucent_base_ibl,transmission_weight);let material_dielectric_gloss_ibl: vec3f=material_dielectric_base_ibl*(1.0-dielectricIblFresnel)+slab_glossy_ibl*dielectricIblColoredFresnel;let material_base_substrate_ibl: vec3f=mix(material_dielectric_gloss_ibl,slab_metal_ibl,base_metalness);let material_coated_base_ibl: vec3f=layer(material_base_substrate_ibl,slab_coat_ibl,coatIblFresnel,coatAbsorption,vec3f(1.0f));
#ifdef FUZZ
material_surface_ibl=layer(material_coated_base_ibl,slab_fuzz_ibl,fuzzIblFresnel*fuzz_weight,vec3(1.0),fuzz_color);
#else
material_surface_ibl=material_coated_base_ibl;
#endif
#endif
`;e.IncludesShadersStoreWGSL[g]||(e.IncludesShadersStoreWGSL[g]=x);const v="openpbrDirectLightingInit",P=`#ifdef LIGHT{X}
var preInfo{X}: preLightingInfo;var preInfoCoat{X}: preLightingInfo;let lightColor{X}: vec4f=light{X}.vLightDiffuse;var shadow{X}: f32=1.0f;
#if defined(SHADOWONLY) || defined(LIGHTMAP) && defined(LIGHTMAPEXCLUDED{X}) && defined(LIGHTMAPNOSPECULAR{X})
#else
#define CUSTOM_LIGHT{X}_COLOR 
#ifdef SPOTLIGHT{X}
preInfo{X}=computePointAndSpotPreLightingInfo(light{X}.vLightData,viewDirectionW,normalW,vPositionW);preInfoCoat{X}=computePointAndSpotPreLightingInfo(light{X}.vLightData,viewDirectionW,coatNormalW,vPositionW);
#elif defined(POINTLIGHT{X})
preInfo{X}=computePointAndSpotPreLightingInfo(light{X}.vLightData,viewDirectionW,normalW,vPositionW);preInfoCoat{X}=computePointAndSpotPreLightingInfo(light{X}.vLightData,viewDirectionW,coatNormalW,vPositionW);
#elif defined(HEMILIGHT{X})
preInfo{X}=computeHemisphericPreLightingInfo(light{X}.vLightData,viewDirectionW,normalW);preInfoCoat{X}=computeHemisphericPreLightingInfo(light{X}.vLightData,viewDirectionW,coatNormalW);
#elif defined(DIRLIGHT{X})
preInfo{X}=computeDirectionalPreLightingInfo(light{X}.vLightData,viewDirectionW,normalW);preInfoCoat{X}=computeDirectionalPreLightingInfo(light{X}.vLightData,viewDirectionW,coatNormalW);
#elif defined(AREALIGHT{X}) && defined(AREALIGHTUSED) && defined(AREALIGHTSUPPORTED)
preInfo{X}=computeAreaPreLightingInfo(areaLightsLTC1Sampler,areaLightsLTC2Sampler,viewDirectionW,normalW,vPositionW,light{X}.vLightData,light{X}.vLightWidth.xyz,light{X}.vLightHeight.xyz,specular_roughness);preInfoCoat{X}=computeAreaPreLightingInfo(areaLightsLTC1Sampler,areaLightsLTC2Sampler,viewDirectionW,coatNormalW,vPositionW,light{X}.vLightData,light{X}.vLightWidth.xyz,light{X}.vLightHeight.xyz,coat_roughness);
#endif
preInfo{X}.NdotV=baseGeoInfo.NdotV;preInfoCoat{X}.NdotV=coatGeoInfo.NdotV;
#ifdef SPOTLIGHT{X}
#ifdef LIGHT_FALLOFF_GLTF{X}
preInfo{X}.attenuation=computeDistanceLightFalloff_GLTF(preInfo{X}.lightDistanceSquared,light{X}.vLightFalloff.y);
#ifdef IESLIGHTTEXTURE{X}
preInfo{X}.attenuation*=computeDirectionalLightFalloff_IES(light{X}.vLightDirection.xyz,preInfo{X}.L,iesLightTexture{X});
#else
preInfo{X}.attenuation*=computeDirectionalLightFalloff_GLTF(light{X}.vLightDirection.xyz,preInfo{X}.L,light{X}.vLightFalloff.z,light{X}.vLightFalloff.w);
#endif
#elif defined(LIGHT_FALLOFF_PHYSICAL{X})
preInfo{X}.attenuation=computeDistanceLightFalloff_Physical(preInfo{X}.lightDistanceSquared);
#ifdef IESLIGHTTEXTURE{X}
preInfo{X}.attenuation*=computeDirectionalLightFalloff_IES(light{X}.vLightDirection.xyz,preInfo{X}.L,iesLightTexture{X});
#else
preInfo{X}.attenuation*=computeDirectionalLightFalloff_Physical(light{X}.vLightDirection.xyz,preInfo{X}.L,light{X}.vLightDirection.w);
#endif
#elif defined(LIGHT_FALLOFF_STANDARD{X})
preInfo{X}.attenuation=computeDistanceLightFalloff_Standard(preInfo{X}.lightOffset,light{X}.vLightFalloff.x);
#ifdef IESLIGHTTEXTURE{X}
preInfo{X}.attenuation*=computeDirectionalLightFalloff_IES(light{X}.vLightDirection.xyz,preInfo{X}.L,iesLightTexture{X});
#else
preInfo{X}.attenuation*=computeDirectionalLightFalloff_Standard(light{X}.vLightDirection.xyz,preInfo{X}.L,light{X}.vLightDirection.w,light{X}.vLightData.w);
#endif
#else
preInfo{X}.attenuation=computeDistanceLightFalloff(preInfo{X}.lightOffset,preInfo{X}.lightDistanceSquared,light{X}.vLightFalloff.x,light{X}.vLightFalloff.y);
#ifdef IESLIGHTTEXTURE{X}
preInfo{X}.attenuation*=computeDirectionalLightFalloff_IES(light{X}.vLightDirection.xyz,preInfo{X}.L,iesLightTexture{X});
#else
preInfo{X}.attenuation*=computeDirectionalLightFalloff(light{X}.vLightDirection.xyz,preInfo{X}.L,light{X}.vLightDirection.w,light{X}.vLightData.w,light{X}.vLightFalloff.z,light{X}.vLightFalloff.w);
#endif
#endif
#elif defined(POINTLIGHT{X})
#ifdef LIGHT_FALLOFF_GLTF{X}
preInfo{X}.attenuation=computeDistanceLightFalloff_GLTF(preInfo{X}.lightDistanceSquared,light{X}.vLightFalloff.y);
#elif defined(LIGHT_FALLOFF_PHYSICAL{X})
preInfo{X}.attenuation=computeDistanceLightFalloff_Physical(preInfo{X}.lightDistanceSquared);
#elif defined(LIGHT_FALLOFF_STANDARD{X})
preInfo{X}.attenuation=computeDistanceLightFalloff_Standard(preInfo{X}.lightOffset,light{X}.vLightFalloff.x);
#else
preInfo{X}.attenuation=computeDistanceLightFalloff(preInfo{X}.lightOffset,preInfo{X}.lightDistanceSquared,light{X}.vLightFalloff.x,light{X}.vLightFalloff.y);
#endif
#else
preInfo{X}.attenuation=1.0f;
#endif
preInfoCoat{X}.attenuation=preInfo{X}.attenuation;
#if defined(HEMILIGHT{X})
preInfo{X}.roughness=specular_roughness;preInfoCoat{X}.roughness=coat_roughness;
#elif defined(AREALIGHT{X}) && defined(AREALIGHTUSED) && defined(AREALIGHTSUPPORTED)
preInfo{X}.roughness=specular_roughness;preInfoCoat{X}.roughness=coat_roughness;
#else
preInfo{X}.roughness=adjustRoughnessFromLightProperties(specular_roughness,light{X}.vLightSpecular.a,preInfo{X}.lightDistance);preInfoCoat{X}.roughness=adjustRoughnessFromLightProperties(coat_roughness,light{X}.vLightSpecular.a,preInfoCoat{X}.lightDistance);
#endif
preInfo{X}.diffuseRoughness=base_diffuse_roughness;preInfo{X}.surfaceAlbedo=base_color.rgb;
#endif
#endif
`;e.IncludesShadersStoreWGSL[v]||(e.IncludesShadersStoreWGSL[v]=P);const I="openpbrDirectLighting",z=`#ifdef LIGHT{X}
{var slab_diffuse: vec3f=vec3f(0.f,0.f,0.f);var slab_subsurface: vec3f=vec3f(0.f,0.f,0.f);var slab_translucent: vec3f=vec3f(0.f,0.f,0.f);var slab_glossy: vec3f=vec3f(0.f,0.f,0.f);var specularFresnel: f32=0.0f;var specularColoredFresnel: vec3f=vec3f(0.f,0.f,0.f);var slab_metal: vec3f=vec3f(0.f,0.f,0.f);var slab_coat: vec3f=vec3f(0.f,0.f,0.f);var coatFresnel: f32=0.0f;var slab_fuzz: vec3f=vec3f(0.f,0.f,0.f);var fuzzFresnel: f32=0.0f;
#ifdef HEMILIGHT{X}
slab_diffuse=computeHemisphericDiffuseLighting(preInfo{X},lightColor{X}.rgb,light{X}.vLightGround);
#elif defined(AREALIGHT{X}) && defined(AREALIGHTUSED) && defined(AREALIGHTSUPPORTED)
slab_diffuse=computeAreaDiffuseLighting(preInfo{X},lightColor{X}.rgb);
#else
slab_diffuse=computeDiffuseLighting(preInfo{X},lightColor{X}.rgb);
#endif
#ifdef PROJECTEDLIGHTTEXTURE{X}
slab_diffuse*=computeProjectionTextureDiffuseLighting(projectionLightTexture{X},textureProjectionMatrix{X},vPositionW);
#endif
numLights+=1.0f;
#ifdef FUZZ
let fuzzNdotH: f32=max(dot(fuzzNormalW,preInfo{X}.H),0.0f);let fuzzBrdf: vec3f=getFuzzBRDFLookup(fuzzNdotH,sqrt(fuzz_roughness));
#endif
#ifdef THIN_FILM
let thin_film_desaturation_scale: f32=(thin_film_ior-1.0f)*sqrt(thin_film_thickness*0.001f);
#endif
#if defined(AREALIGHT{X}) && defined(AREALIGHTUSED) && defined(AREALIGHTSUPPORTED)
slab_glossy=computeAreaSpecularLighting(preInfo{X},light{X}.vLightSpecular.rgb,baseConductorReflectance.F0,baseConductorReflectance.F90);
#else
{
#ifdef ANISOTROPIC_BASE
slab_glossy=computeAnisotropicSpecularLighting(preInfo{X},viewDirectionW,normalW,
baseGeoInfo.anisotropicTangent,baseGeoInfo.anisotropicBitangent,baseGeoInfo.anisotropy,
0.0f,lightColor{X}.rgb);
#else
slab_glossy=computeSpecularLighting(preInfo{X},normalW,vec3(1.0),vec3(1.0),specular_roughness,lightColor{X}.rgb);
#endif
let NdotH: f32=dot(normalW,preInfo{X}.H);specularFresnel=fresnelSchlickGGX(NdotH,baseDielectricReflectance.F0,baseDielectricReflectance.F90);specularColoredFresnel=specularFresnel*specular_color;
#ifdef THIN_FILM
let thinFilmIorScale: f32=clamp(2.0f*abs(thin_film_ior-1.0f),0.0f,1.0f);var thinFilmDielectricFresnel: vec3f=evalIridescence(thin_film_outside_ior,thin_film_ior,preInfo{X}.VdotH,thin_film_thickness,baseDielectricReflectance.coloredF0);thinFilmDielectricFresnel=mix(thinFilmDielectricFresnel,vec3f(dot(thinFilmDielectricFresnel,vec3f(0.3333f))),thin_film_desaturation_scale);specularColoredFresnel=mix(specularColoredFresnel,thinFilmDielectricFresnel*specular_color,thin_film_weight*thinFilmIorScale);
#endif
}
#endif
#if defined(AREALIGHT{X}) && defined(AREALIGHTUSED) && defined(AREALIGHTSUPPORTED)
slab_metal=computeAreaSpecularLighting(preInfo{X},light{X}.vLightSpecular.rgb,baseConductorReflectance.F0,baseConductorReflectance.F90);
#else
{
#if (CONDUCTOR_SPECULAR_MODEL==CONDUCTOR_SPECULAR_MODEL_OPENPBR)
var coloredFresnel: vec3f=getF82Specular(preInfo{X}.VdotH,baseConductorReflectance.coloredF0,baseConductorReflectance.coloredF90,specular_roughness);
#else
var coloredFresnel: vec3f=fresnelSchlickGGX(preInfo{X}.VdotH,baseConductorReflectance.coloredF0,baseConductorReflectance.coloredF90);
#endif
#ifdef THIN_FILM
let thinFilmIorScale: f32=clamp(2.0f*abs(thin_film_ior-1.0f),0.0f,1.0f);var thinFilmConductorFresnel=evalIridescence(thin_film_outside_ior,thin_film_ior,preInfo{X}.VdotH,thin_film_thickness,baseConductorReflectance.coloredF0);thinFilmConductorFresnel=mix(thinFilmConductorFresnel,vec3f(dot(thinFilmConductorFresnel,vec3f(0.3333f))),thin_film_desaturation_scale);coloredFresnel=mix(coloredFresnel,specular_weight*thinFilmIorScale*thinFilmConductorFresnel,thin_film_weight);
#endif
#ifdef ANISOTROPIC_BASE
slab_metal=computeAnisotropicSpecularLighting(preInfo{X},viewDirectionW,normalW,baseGeoInfo.anisotropicTangent,baseGeoInfo.anisotropicBitangent,baseGeoInfo.anisotropy,0.0,lightColor{X}.rgb);
#else
slab_metal=computeSpecularLighting(preInfo{X},normalW,vec3f(baseConductorReflectance.coloredF0),coloredFresnel,specular_roughness,lightColor{X}.rgb);
#endif
}
#endif
#if defined(AREALIGHT{X}) && defined(AREALIGHTUSED) && defined(AREALIGHTSUPPORTED)
slab_coat=computeAreaSpecularLighting(preInfoCoat{X},light{X}.vLightSpecular.rgb,coatReflectance.F0,coatReflectance.F90);
#else
{
#ifdef ANISOTROPIC_COAT
slab_coat=computeAnisotropicSpecularLighting(preInfoCoat{X},viewDirectionW,coatNormalW,
coatGeoInfo.anisotropicTangent,coatGeoInfo.anisotropicBitangent,coatGeoInfo.anisotropy,0.0,
lightColor{X}.rgb);
#else
slab_coat=computeSpecularLighting(preInfoCoat{X},coatNormalW,vec3f(coatReflectance.F0),vec3f(1.0f),coat_roughness,lightColor{X}.rgb);
#endif
let NdotH: f32=dot(coatNormalW,preInfoCoat{X}.H);coatFresnel=fresnelSchlickGGX(NdotH,coatReflectance.F0,coatReflectance.F90);}
#endif
var coatAbsorption=vec3f(1.0f);if (coat_weight>0.0) {let cosTheta_view: f32=max(preInfoCoat{X}.NdotV,0.001f);let cosTheta_light: f32=max(preInfoCoat{X}.NdotL,0.001f);let fresnel_view: f32=coatReflectance.F0+(1.0f-coatReflectance.F0)*pow(1.0f-cosTheta_view,5.0);let fresnel_light: f32=coatReflectance.F0+(1.0f-coatReflectance.F0)*pow(1.0f-cosTheta_light,5.0);let averageReflectance: f32=(fresnel_view+fresnel_light)*0.5;var darkened_transmission: f32=(1.0f-averageReflectance)/(1.0f+averageReflectance);darkened_transmission=mix(1.0f,darkened_transmission,coat_darkening);var sin2: f32=1.0f-coatGeoInfo.NdotV*coatGeoInfo.NdotV;sin2=sin2/(coat_ior*coat_ior);let cos_t: f32=sqrt(1.0f-sin2);let coatPathLength=1.0f/cos_t;let colored_transmission: vec3f=pow(coat_color,vec3f(coatPathLength));coatAbsorption=mix(vec3f(1.0f),colored_transmission*vec3f(darkened_transmission),coat_weight);}
#ifdef FUZZ
fuzzFresnel=fuzzBrdf.z;let fuzzNormalW=mix(normalW,coatNormalW,coat_weight);let fuzzNdotV: f32=max(dot(fuzzNormalW,viewDirectionW.xyz),0.0f);let fuzzNdotL: f32=max(dot(fuzzNormalW,preInfo{X}.L),0.0);slab_fuzz=lightColor{X}.rgb*preInfo{X}.attenuation*evalFuzz(preInfo{X}.L,fuzzNdotL,fuzzNdotV,fuzzTangent,fuzzBitangent,fuzzBrdf);
#else
let fuzz_color=vec3f(0.0);
#endif
slab_diffuse*=base_color.rgb;let material_opaque_base: vec3f=mix(slab_diffuse,slab_subsurface,subsurface_weight);let material_dielectric_base: vec3f=mix(material_opaque_base,slab_translucent,transmission_weight);let material_dielectric_gloss: vec3f=material_dielectric_base*(1.0f-specularFresnel)+slab_glossy*specularColoredFresnel;let material_base_substrate: vec3f=mix(material_dielectric_gloss,slab_metal,base_metalness);let material_coated_base: vec3f=layer(material_base_substrate,slab_coat,coatFresnel,coatAbsorption,vec3f(1.0f));material_surface_direct+=layer(material_coated_base,slab_fuzz,fuzzFresnel*fuzz_weight,vec3f(1.0f),fuzz_color);}
#endif
`;e.IncludesShadersStoreWGSL[I]||(e.IncludesShadersStoreWGSL[I]=z);const r="openpbrPixelShader",E=`#define OPENPBR_FRAGMENT_SHADER
#define CUSTOM_FRAGMENT_BEGIN
#include<prePassDeclaration>[SCENE_MRT_COUNT]
#include<oitDeclaration>
#ifndef FROMLINEARSPACE
#define FROMLINEARSPACE
#endif
#include<openpbrUboDeclaration>
#include<pbrFragmentExtraDeclaration>
#include<lightUboDeclaration>[0..maxSimultaneousLights]
#include<openpbrFragmentSamplersDeclaration>
#include<imageProcessingDeclaration>
#include<clipPlaneFragmentDeclaration>
#include<logDepthDeclaration>
#include<fogFragmentDeclaration>
#include<helperFunctions>
#include<subSurfaceScatteringFunctions>
#include<importanceSampling>
#include<pbrHelperFunctions>
#include<imageProcessingFunctions>
#include<shadowsFragmentFunctions>
#include<harmonicsFunctions>
#include<pbrDirectLightingSetupFunctions>
#include<pbrDirectLightingFalloffFunctions>
#include<pbrBRDFFunctions>
#include<hdrFilteringFunctions>
#include<pbrDirectLightingFunctions>
#include<pbrIBLFunctions>
#include<openpbrNormalMapFragmentMainFunctions>
#include<openpbrNormalMapFragmentFunctions>
#ifdef REFLECTION
#include<reflectionFunction>
#endif
#define CUSTOM_FRAGMENT_DEFINITIONS
#include<openpbrDielectricReflectance>
#include<openpbrConductorReflectance>
#include<openpbrBlockAmbientOcclusion>
#include<openpbrGeometryInfo>
#include<openpbrIblFunctions>
fn layer(slab_bottom: vec3f,slab_top: vec3f,lerp_factor: f32,bottom_multiplier: vec3f,top_multiplier: vec3f)->vec3f {return mix(slab_bottom*bottom_multiplier,slab_top*top_multiplier,lerp_factor);}
@fragment
fn main(input: FragmentInputs)->FragmentOutputs {
#define CUSTOM_FRAGMENT_MAIN_BEGIN
#include<clipPlaneFragment>
#include<pbrBlockNormalGeometric>
var coatNormalW: vec3f=normalW;
#include<openpbrNormalMapFragment>
#include<openpbrBlockNormalFinal>
#include<openpbrBaseLayerData>
#include<openpbrCoatLayerData>
#include<openpbrThinFilmLayerData>
#include<openpbrFuzzLayerData>
var subsurface_weight: f32=0.0f;var transmission_weight: f32=0.0f;
#define CUSTOM_FRAGMENT_UPDATE_ALPHA
#include<depthPrePass>
#define CUSTOM_FRAGMENT_BEFORE_LIGHTS
var aoOut: ambientOcclusionOutParams;
#ifdef AMBIENT_OCCLUSION
var ambientOcclusionFromTexture: vec3f=textureSample(ambientOcclusionSampler,ambientOcclusionSamplerSampler,fragmentInputs.vAmbientOcclusionUV+uvOffset).rgb;
#endif
aoOut=ambientOcclusionBlock(
#ifdef AMBIENT_OCCLUSION
ambientOcclusionFromTexture,
uniforms.vAmbientOcclusionInfos
#endif
);
#ifdef ANISOTROPIC_COAT
let coatGeoInfo: geometryInfoAnisoOutParams=geometryInfoAniso(
coatNormalW,viewDirectionW.xyz,coat_roughness,geometricNormalW
,vec3f(geometry_coat_tangent.x,geometry_coat_tangent.y,coat_roughness_anisotropy),TBN
);
#else
let coatGeoInfo: geometryInfoOutParams=geometryInfo(
coatNormalW,viewDirectionW.xyz,coat_roughness,geometricNormalW
);
#endif
specular_roughness=mix(specular_roughness,pow(min(1.0f,pow(specular_roughness,4.0f)+2.0f*pow(coat_roughness,4.0f)),0.25f),coat_weight);
#ifdef ANISOTROPIC_BASE
let baseGeoInfo: geometryInfoAnisoOutParams=geometryInfoAniso(
normalW,viewDirectionW.xyz,specular_roughness,geometricNormalW
,vec3f(geometry_tangent.x,geometry_tangent.y,specular_roughness_anisotropy),TBN
);
#else
let baseGeoInfo: geometryInfoOutParams=geometryInfo(
normalW,viewDirectionW.xyz,specular_roughness,geometricNormalW
);
#endif
#ifdef FUZZ
let fuzzNormalW=normalize(mix(normalW,coatNormalW,coat_weight));var fuzzTangent=normalize(TBN[0]);fuzzTangent=normalize(fuzzTangent-dot(fuzzTangent,fuzzNormalW)*fuzzNormalW);let fuzzBitangent=cross(fuzzNormalW,fuzzTangent);let fuzzGeoInfo: geometryInfoOutParams=geometryInfo(
fuzzNormalW,viewDirectionW.xyz,fuzz_roughness,geometricNormalW
);
#endif
let coatReflectance: ReflectanceParams=dielectricReflectance(
coat_ior 
,1.0f 
,vec3f(1.0f)
,coat_weight
);
#ifdef THIN_FILM
let thin_film_outside_ior: f32=mix(1.0f,coat_ior,coat_weight);
#endif
let baseDielectricReflectance: ReflectanceParams=dielectricReflectance(
specular_ior 
,mix(1.0f,coat_ior,coat_weight) 
,specular_color
,specular_weight
);let baseConductorReflectance: ReflectanceParams=conductorReflectance(base_color,specular_color,specular_weight);var material_surface_ibl: vec3f=vec3f(0.f,0.f,0.f);
#include<openpbrEnvironmentLighting>
var material_surface_direct: vec3f=vec3f(0.f,0.f,0.f);
#if defined(LIGHT0)
var aggShadow: f32=0.f;var numLights: f32=0.f;
#include<openpbrDirectLightingInit>[0..maxSimultaneousLights]
#include<openpbrDirectLighting>[0..maxSimultaneousLights]
#endif
var material_surface_emission: vec3f=uniforms.vEmissionColor;
#ifdef EMISSION_COLOR
let emissionColorTex: vec3f=textureSample(emissionColorSampler,emissionColorSamplerSampler,uniforms.vEmissionColorUV+uvOffset).rgb;
#ifdef EMISSION_COLOR_GAMMA
material_surface_emission*=toLinearSpace(emissionColorTex.rgb);
#else
material_surface_emission*=emissionColorTex.rgb;
#endif
material_surface_emission*= uniforms.vEmissionColorInfos.y;
#endif
material_surface_emission*=uniforms.vLightingIntensity.y;
#define CUSTOM_FRAGMENT_BEFORE_FINALCOLORCOMPOSITION
var finalColor: vec4f=vec4f(material_surface_ibl+material_surface_direct+material_surface_emission,alpha);
#define CUSTOM_FRAGMENT_BEFORE_FOG
finalColor=max(finalColor,vec4f(0.0));
#include<logDepthFragment>
#include<fogFragment>(color,finalColor)
#include<pbrBlockImageProcessing>
#define CUSTOM_FRAGMENT_BEFORE_FRAGCOLOR
#ifdef PREPASS
#include<pbrBlockPrePass>
#endif
#if !defined(PREPASS) && !defined(ORDER_INDEPENDENT_TRANSPARENCY)
fragmentOutputs.color=finalColor;
#endif
#include<oitFragment>
#if ORDER_INDEPENDENT_TRANSPARENCY
if (fragDepth==nearestDepth) {fragmentOutputs.frontColor=vec4f(fragmentOutputs.frontColor.rgb+finalColor.rgb*finalColor.a*alphaMultiplier,1.0-alphaMultiplier*(1.0-finalColor.a));} else {fragmentOutputs.backColor+=finalColor;}
#endif
#include<pbrDebug>
#define CUSTOM_FRAGMENT_MAIN_END
}
`;e.ShadersStoreWGSL[r]||(e.ShadersStoreWGSL[r]=E);const Ie={name:r,shader:E};export{Ie as openpbrPixelShaderWGSL};
