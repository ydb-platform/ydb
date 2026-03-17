import{S as e}from"./index-BZt0m9TU.js";import"./vertexColorMixing-ntFhQt35.js";import"./openpbrUboDeclaration-D16XfuIq.js";import"./mainUVVaryingDeclaration-C4Vp_S5r.js";import"./helperFunctions-B2gYs5dd.js";import"./pbrBRDFFunctions-nbmf1Y5u.js";import"./bakedVertexAnimation-BuWxqXit.js";import"./instancesDeclaration-DMHYiRGq.js";import"./harmonicsFunctions-JxcDxFWf.js";import"./clipPlaneVertex-CHAAXtfr.js";import"./logDepthVertex-C6DbuQZI.js";import"./shadowsVertex-C4igYguG.js";import"./morphTargetsVertex-BsrYdCoc.js";import"./logDepthDeclaration-DBCLquxJ.js";import"./index-xGeN4i2A.js";import"./svelte/svelte_svelte.js";import"./svelte/svelte_animate.js";import"./svelte/svelte_attachments.js";import"./svelte/svelte_easing.js";import"./svelte/svelte_internal_client.js";import"./svelte/svelte_internal_flags_async.js";import"./svelte/svelte_internal_flags_legacy.js";import"./svelte/svelte_internal_flags_tracing.js";import"./svelte/svelte_internal_server.js";import"./svelte/svelte_legacy.js";import"./svelte/svelte_motion.js";import"./svelte/svelte_reactivity.js";import"./svelte/svelte_reactivity_window.js";import"./svelte/svelte_server.js";import"./svelte/svelte_store.js";import"./svelte/svelte_transition.js";import"./svelte/svelte_events.js";import"./sceneUboDeclaration-DZIv4upk.js";import"./meshUboDeclaration-BD5u1Aes.js";const n="openpbrVertexDeclaration",a=`uniform mat4 view;uniform mat4 viewProjection;uniform vec4 vEyePosition;
#ifdef MULTIVIEW
mat4 viewProjectionR;
#endif
#ifdef BASE_COLOR
uniform vec2 vBaseColorInfos;uniform mat4 baseColorMatrix;
#endif
#ifdef BASE_WEIGHT
uniform mat4 baseWeightMatrix;uniform vec2 vBaseWeightInfos;
#endif
uniform float vBaseDiffuseRoughness;
#ifdef BASE_DIFFUSE_ROUGHNESS
uniform mat4 baseDiffuseRoughnessMatrix;uniform vec2 vBaseDiffuseRoughnessInfos;
#endif
#ifdef AMBIENT_OCCLUSION
uniform vec2 vAmbientOcclusionInfos;uniform mat4 ambientOcclusionMatrix;
#endif
#ifdef EMISSION_COLOR
uniform vec2 vEmissionColorInfos;uniform mat4 emissionColorMatrix;
#endif
#ifdef LIGHTMAP
uniform vec2 vLightmapInfos;uniform mat4 lightmapMatrix;
#endif
#ifdef BASE_METALNESS
uniform vec2 vBaseMetalnessInfos;uniform mat4 baseMetalnessMatrix;
#endif
#ifdef SPECULAR_WEIGHT
uniform vec2 vSpecularWeightInfos;uniform mat4 specularWeightMatrix;
#endif
#ifdef SPECULAR_COLOR
uniform vec2 vSpecularColorInfos;uniform mat4 specularColorMatrix;
#endif
#ifdef SPECULAR_ROUGHNESS
uniform vec2 vSpecularRoughnessInfos;uniform mat4 specularRoughnessMatrix;
#endif
#ifdef SPECULAR_ROUGHNESS_ANISOTROPY
uniform vec2 vSpecularRoughnessAnisotropyInfos;uniform mat4 specularRoughnessAnisotropyMatrix;
#endif
#ifdef COAT_WEIGHT
uniform vec2 vCoatWeightInfos;uniform mat4 coatWeightMatrix;
#endif
#ifdef COAT_COLOR
uniform vec2 vCoatColorInfos;uniform mat4 coatColorMatrix;
#endif
#ifdef COAT_ROUGHNESS
uniform vec2 vCoatRoughnessInfos;uniform mat4 coatRoughnessMatrix;
#endif
#ifdef COAT_ROUGHNESS_ANISOTROPY
uniform vec2 vCoatRoughnessAnisotropyInfos;uniform mat4 coatRoughnessAnisotropyMatrix;
#endif
#ifdef COAT_IOR
uniform vec2 vCoatIorInfos;uniform mat4 coatIorMatrix;
#endif
#ifdef COAT_DARKENING
uniform vec2 vCoatDarkeningInfos;uniform mat4 coatDarkeningMatrix;
#endif
#ifdef FUZZ_WEIGHT
uniform vec2 vFuzzWeightInfos;uniform mat4 fuzzWeightMatrix;
#endif
#ifdef FUZZ_COLOR
uniform vec2 vFuzzColorInfos;uniform mat4 fuzzColorMatrix;
#endif
#ifdef FUZZ_ROUGHNESS
uniform vec2 vFuzzRoughnessInfos;uniform mat4 fuzzRoughnessMatrix;
#endif
#ifdef GEOMETRY_NORMAL
uniform vec2 vGeometryNormalInfos;uniform mat4 geometryNormalMatrix;
#endif
#ifdef GEOMETRY_TANGENT
uniform vec2 vGeometryTangentInfos;uniform mat4 geometryTangentMatrix;
#endif
#ifdef GEOMETRY_COAT_NORMAL
uniform vec2 vGeometryCoatNormalInfos;uniform mat4 geometryCoatNormalMatrix;
#endif
#ifdef THIN_FILM_WEIGHT
uniform vec2 vThinFilmWeightInfos;uniform mat4 thinFilmWeightMatrix;
#endif
#ifdef THIN_FILM_THICKNESS
uniform vec2 vThinFilmThicknessInfos;uniform mat4 thinFilmThicknessMatrix;
#endif
#ifdef GEOMETRY_OPACITY
uniform mat4 geometryOpacityMatrix;uniform vec2 vGeometryOpacityInfos;
#endif
#ifdef POINTSIZE
uniform float pointSize;
#endif
uniform vec4 cameraInfo;
#ifdef REFLECTION
uniform vec2 vReflectionInfos;uniform mat4 reflectionMatrix;
#endif
#ifdef NORMAL
#if defined(USESPHERICALFROMREFLECTIONMAP) && defined(USESPHERICALINVERTEX)
#ifdef USESPHERICALFROMREFLECTIONMAP
#ifdef SPHERICAL_HARMONICS
uniform vec3 vSphericalL00;uniform vec3 vSphericalL1_1;uniform vec3 vSphericalL10;uniform vec3 vSphericalL11;uniform vec3 vSphericalL2_2;uniform vec3 vSphericalL2_1;uniform vec3 vSphericalL20;uniform vec3 vSphericalL21;uniform vec3 vSphericalL22;
#else
uniform vec3 vSphericalX;uniform vec3 vSphericalY;uniform vec3 vSphericalZ;uniform vec3 vSphericalXX_ZZ;uniform vec3 vSphericalYY_ZZ;uniform vec3 vSphericalZZ;uniform vec3 vSphericalXY;uniform vec3 vSphericalYZ;uniform vec3 vSphericalZX;
#endif
#endif
#endif
#endif
#ifdef DETAIL
uniform vec4 vDetailInfos;uniform mat4 detailMatrix;
#endif
#include<decalVertexDeclaration>
#define ADDITIONAL_VERTEX_DECLARATION
`;e.IncludesShadersStore[n]||(e.IncludesShadersStore[n]=a);const o="openpbrNormalMapVertexDeclaration",E=`#if defined(GEOMETRY_NORMAL) || defined(PARALLAX) || defined(GEOMETRY_COAT_NORMAL) || defined(ANISOTROPIC) || defined(FUZZ)
#if defined(TANGENT) && defined(NORMAL) 
varying mat3 vTBN;
#endif
#endif
`;e.IncludesShadersStore[o]||(e.IncludesShadersStore[o]=E);const r="openpbrNormalMapVertex",_=`#if defined(GEOMETRY_NORMAL) || defined(PARALLAX) || defined(GEOMETRY_COAT_NORMAL) || defined(ANISOTROPIC) || defined(FUZZ)
#if defined(TANGENT) && defined(NORMAL)
vec3 tbnNormal=normalize(normalUpdated);vec3 tbnTangent=normalize(tangentUpdated.xyz);vec3 tbnBitangent=cross(tbnNormal,tbnTangent)*tangentUpdated.w;vTBN=mat3(finalWorld)*mat3(tbnTangent,tbnBitangent,tbnNormal);
#endif
#endif
`;e.IncludesShadersStore[r]||(e.IncludesShadersStore[r]=_);const i="openpbrVertexShader",t=`#define OPENPBR_VERTEX_SHADER
#define CUSTOM_VERTEX_EXTENSION
precision highp float;
#include<__decl__openpbrVertex>
#define CUSTOM_VERTEX_BEGIN
attribute vec3 position;
#ifdef NORMAL
attribute vec3 normal;
#endif
#ifdef TANGENT
attribute vec4 tangent;
#endif
#ifdef UV1
attribute vec2 uv;
#endif
#include<uvAttributeDeclaration>[2..7]
#include<mainUVVaryingDeclaration>[1..7]
#ifdef VERTEXCOLOR
attribute vec4 color;
#endif
#include<helperFunctions>
#include<pbrBRDFFunctions>
#include<bonesDeclaration>
#include<bakedVertexAnimationDeclaration>
#include<instancesDeclaration>
#include<prePassVertexDeclaration>
#include<samplerVertexDeclaration>(_DEFINENAME_,BASE_COLOR,_VARYINGNAME_,BaseColor)
#include<samplerVertexDeclaration>(_DEFINENAME_,BASE_WEIGHT,_VARYINGNAME_,BaseWeight)
#include<samplerVertexDeclaration>(_DEFINENAME_,BASE_DIFFUSE_ROUGHNESS,_VARYINGNAME_,BaseDiffuseRoughness)
#include<samplerVertexDeclaration>(_DEFINENAME_,BASE_METALNESS,_VARYINGNAME_,BaseMetalness)
#include<samplerVertexDeclaration>(_DEFINENAME_,SPECULAR_WEIGHT,_VARYINGNAME_,SpecularWeight)
#include<samplerVertexDeclaration>(_DEFINENAME_,SPECULAR_COLOR,_VARYINGNAME_,SpecularColor)
#include<samplerVertexDeclaration>(_DEFINENAME_,SPECULAR_ROUGHNESS,_VARYINGNAME_,SpecularRoughness)
#include<samplerVertexDeclaration>(_DEFINENAME_,SPECULAR_ROUGHNESS_ANISOTROPY,_VARYINGNAME_,SpecularRoughnessAnisotropy)
#include<samplerVertexDeclaration>(_DEFINENAME_,COAT_WEIGHT,_VARYINGNAME_,CoatWeight)
#include<samplerVertexDeclaration>(_DEFINENAME_,COAT_COLOR,_VARYINGNAME_,CoatColor)
#include<samplerVertexDeclaration>(_DEFINENAME_,COAT_ROUGHNESS,_VARYINGNAME_,CoatRoughness)
#include<samplerVertexDeclaration>(_DEFINENAME_,COAT_ROUGHNESS_ANISOTROPY,_VARYINGNAME_,CoatRoughnessAnisotropy)
#include<samplerVertexDeclaration>(_DEFINENAME_,COAT_DARKENING,_VARYINGNAME_,CoatDarkening)
#include<samplerVertexDeclaration>(_DEFINENAME_,FUZZ_WEIGHT,_VARYINGNAME_,FuzzWeight)
#include<samplerVertexDeclaration>(_DEFINENAME_,FUZZ_COLOR,_VARYINGNAME_,FuzzColor)
#include<samplerVertexDeclaration>(_DEFINENAME_,FUZZ_ROUGHNESS,_VARYINGNAME_,FuzzRoughness)
#include<samplerVertexDeclaration>(_DEFINENAME_,GEOMETRY_NORMAL,_VARYINGNAME_,GeometryNormal)
#include<samplerVertexDeclaration>(_DEFINENAME_,GEOMETRY_TANGENT,_VARYINGNAME_,GeometryTangent)
#include<samplerVertexDeclaration>(_DEFINENAME_,GEOMETRY_COAT_NORMAL,_VARYINGNAME_,GeometryCoatNormal)
#include<samplerVertexDeclaration>(_DEFINENAME_,GEOMETRY_OPACITY,_VARYINGNAME_,GeometryOpacity)
#include<samplerVertexDeclaration>(_DEFINENAME_,EMISSION_COLOR,_VARYINGNAME_,EmissionColor)
#include<samplerVertexDeclaration>(_DEFINENAME_,THIN_FILM_WEIGHT,_VARYINGNAME_,ThinFilmWeight)
#include<samplerVertexDeclaration>(_DEFINENAME_,THIN_FILM_THICKNESS,_VARYINGNAME_,ThinFilmThickness)
#include<samplerVertexDeclaration>(_DEFINENAME_,AMBIENT_OCCLUSION,_VARYINGNAME_,AmbientOcclusion)
#include<samplerVertexDeclaration>(_DEFINENAME_,DECAL,_VARYINGNAME_,Decal)
#include<samplerVertexDeclaration>(_DEFINENAME_,DETAIL,_VARYINGNAME_,Detail)
varying vec3 vPositionW;
#if DEBUGMODE>0
varying vec4 vClipSpacePosition;
#endif
#ifdef NORMAL
varying vec3 vNormalW;
#if defined(USESPHERICALFROMREFLECTIONMAP) && defined(USESPHERICALINVERTEX)
varying vec3 vEnvironmentIrradiance;
#include<harmonicsFunctions>
#endif
#endif
#if defined(VERTEXCOLOR) || defined(INSTANCESCOLOR) && defined(INSTANCES)
varying vec4 vColor;
#endif
#include<openpbrNormalMapVertexDeclaration>
#include<clipPlaneVertexDeclaration>
#include<fogVertexDeclaration>
#include<__decl__lightVxFragment>[0..maxSimultaneousLights]
#include<morphTargetsVertexGlobalDeclaration>
#include<morphTargetsVertexDeclaration>[0..maxSimultaneousMorphTargets]
#ifdef REFLECTIONMAP_SKYBOX
varying vec3 vPositionUVW;
#endif
#if defined(REFLECTIONMAP_EQUIRECTANGULAR_FIXED) || defined(REFLECTIONMAP_MIRROREDEQUIRECTANGULAR_FIXED)
varying vec3 vDirectionW;
#endif
#include<logDepthDeclaration>
#define CUSTOM_VERTEX_DEFINITIONS
void main(void) {
#define CUSTOM_VERTEX_MAIN_BEGIN
vec3 positionUpdated=position;
#ifdef NORMAL
vec3 normalUpdated=normal;
#endif
#ifdef TANGENT
vec4 tangentUpdated=tangent;
#endif
#ifdef UV1
vec2 uvUpdated=uv;
#endif
#ifdef UV2
vec2 uv2Updated=uv2;
#endif
#ifdef VERTEXCOLOR
vec4 colorUpdated=color;
#endif
#include<morphTargetsVertexGlobal>
#include<morphTargetsVertex>[0..maxSimultaneousMorphTargets]
#ifdef REFLECTIONMAP_SKYBOX
vPositionUVW=positionUpdated;
#endif
#define CUSTOM_VERTEX_UPDATE_POSITION
#define CUSTOM_VERTEX_UPDATE_NORMAL
#include<instancesVertex>
#if defined(PREPASS) && ((defined(PREPASS_VELOCITY) || defined(PREPASS_VELOCITY_LINEAR)) && !defined(BONES_VELOCITY_ENABLED)
vCurrentPosition=viewProjection*finalWorld*vec4(positionUpdated,1.0);vPreviousPosition=previousViewProjection*finalPreviousWorld*vec4(positionUpdated,1.0);
#endif
#include<bonesVertex>
#include<bakedVertexAnimation>
vec4 worldPos=finalWorld*vec4(positionUpdated,1.0);vPositionW=vec3(worldPos);
#ifdef PREPASS
#include<prePassVertex>
#endif
#ifdef NORMAL
mat3 normalWorld=mat3(finalWorld);
#if defined(INSTANCES) && defined(THIN_INSTANCES)
vNormalW=normalUpdated/vec3(dot(normalWorld[0],normalWorld[0]),dot(normalWorld[1],normalWorld[1]),dot(normalWorld[2],normalWorld[2]));vNormalW=normalize(normalWorld*vNormalW);
#else
#ifdef NONUNIFORMSCALING
normalWorld=transposeMat3(inverseMat3(normalWorld));
#endif
vNormalW=normalize(normalWorld*normalUpdated);
#endif
#if defined(USESPHERICALFROMREFLECTIONMAP) && defined(USESPHERICALINVERTEX)
#if BASE_DIFFUSE_MODEL != BRDF_DIFFUSE_MODEL_LAMBERT && BASE_DIFFUSE_MODEL != BRDF_DIFFUSE_MODEL_LEGACY
vec3 viewDirectionW=normalize(vEyePosition.xyz-vPositionW);
#if !defined(NATIVE) && !defined(WEBGPU)
bool bbb=any(isnan(position));if (bbb) { }
#endif
float NdotV=max(dot(vNormalW,viewDirectionW),0.0);vec3 roughNormal=mix(vNormalW,viewDirectionW,(0.5*(1.0-NdotV))*vBaseDiffuseRoughness);vec3 reflectionVector=vec3(reflectionMatrix*vec4(roughNormal,0)).xyz;
#else
vec3 reflectionVector=vec3(reflectionMatrix*vec4(vNormalW,0)).xyz;
#endif
#ifdef REFLECTIONMAP_OPPOSITEZ
reflectionVector.z*=-1.0;
#endif
vEnvironmentIrradiance=computeEnvironmentIrradiance(reflectionVector);
#endif
#endif
#define CUSTOM_VERTEX_UPDATE_WORLDPOS
#ifdef MULTIVIEW
if (gl_ViewID_OVR==0u) {gl_Position=viewProjection*worldPos;} else {gl_Position=viewProjectionR*worldPos;}
#else
gl_Position=viewProjection*worldPos;
#endif
#if DEBUGMODE>0
vClipSpacePosition=gl_Position;
#endif
#if defined(REFLECTIONMAP_EQUIRECTANGULAR_FIXED) || defined(REFLECTIONMAP_MIRROREDEQUIRECTANGULAR_FIXED)
vDirectionW=normalize(vec3(finalWorld*vec4(positionUpdated,0.0)));
#endif
#ifndef UV1
vec2 uvUpdated=vec2(0.,0.);
#endif
#ifndef UV2
vec2 uv2Updated=vec2(0.,0.);
#endif
#ifdef MAINUV1
vMainUV1=uvUpdated;
#endif
#ifdef MAINUV2
vMainUV2=uv2Updated;
#endif
#include<uvVariableDeclaration>[3..7]
#include<samplerVertexImplementation>(_DEFINENAME_,BASE_COLOR,_VARYINGNAME_,BaseColor,_MATRIXNAME_,baseColor,_INFONAME_,BaseColorInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,BASE_WEIGHT,_VARYINGNAME_,BaseWeight,_MATRIXNAME_,baseWeight,_INFONAME_,BaseWeightInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,BASE_DIFFUSE_ROUGHNESS,_VARYINGNAME_,BaseDiffuseRoughness,_MATRIXNAME_,baseDiffuseRoughness,_INFONAME_,BaseDiffuseRoughnessInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,BASE_METALNESS,_VARYINGNAME_,BaseMetalness,_MATRIXNAME_,baseMetalness,_INFONAME_,BaseMetalnessInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,SPECULAR_WEIGHT,_VARYINGNAME_,SpecularWeight,_MATRIXNAME_,specularWeight,_INFONAME_,SpecularWeightInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,SPECULAR_COLOR,_VARYINGNAME_,SpecularColor,_MATRIXNAME_,specularColor,_INFONAME_,SpecularColorInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,SPECULAR_ROUGHNESS,_VARYINGNAME_,SpecularRoughness,_MATRIXNAME_,specularRoughness,_INFONAME_,SpecularRoughnessInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,SPECULAR_ROUGHNESS_ANISOTROPY,_VARYINGNAME_,SpecularRoughnessAnisotropy,_MATRIXNAME_,specularRoughnessAnisotropy,_INFONAME_,SpecularRoughnessAnisotropyInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,COAT_WEIGHT,_VARYINGNAME_,CoatWeight,_MATRIXNAME_,coatWeight,_INFONAME_,CoatWeightInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,COAT_COLOR,_VARYINGNAME_,CoatColor,_MATRIXNAME_,coatColor,_INFONAME_,CoatColorInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,COAT_ROUGHNESS,_VARYINGNAME_,CoatRoughness,_MATRIXNAME_,coatRoughness,_INFONAME_,CoatRoughnessInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,COAT_ROUGHNESS_ANISOTROPY,_VARYINGNAME_,CoatRoughnessAnisotropy,_MATRIXNAME_,coatRoughnessAnisotropy,_INFONAME_,CoatRoughnessAnisotropyInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,COAT_DARKENING,_VARYINGNAME_,CoatDarkening,_MATRIXNAME_,coatDarkening,_INFONAME_,CoatDarkeningInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,FUZZ_WEIGHT,_VARYINGNAME_,FuzzWeight,_MATRIXNAME_,fuzzWeight,_INFONAME_,FuzzWeightInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,FUZZ_COLOR,_VARYINGNAME_,FuzzColor,_MATRIXNAME_,fuzzColor,_INFONAME_,FuzzColorInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,FUZZ_ROUGHNESS,_VARYINGNAME_,FuzzRoughness,_MATRIXNAME_,fuzzRoughness,_INFONAME_,FuzzRoughnessInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,GEOMETRY_NORMAL,_VARYINGNAME_,GeometryNormal,_MATRIXNAME_,geometryNormal,_INFONAME_,GeometryNormalInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,GEOMETRY_COAT_NORMAL,_VARYINGNAME_,GeometryCoatNormal,_MATRIXNAME_,geometryCoatNormal,_INFONAME_,GeometryCoatNormalInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,GEOMETRY_OPACITY,_VARYINGNAME_,GeometryOpacity,_MATRIXNAME_,geometryOpacity,_INFONAME_,GeometryOpacityInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,GEOMETRY_TANGENT,_VARYINGNAME_,GeometryTangent,_MATRIXNAME_,geometryTangent,_INFONAME_,GeometryTangentInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,EMISSION_COLOR,_VARYINGNAME_,EmissionColor,_MATRIXNAME_,emissionColor,_INFONAME_,EmissionColorInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,THIN_FILM_WEIGHT,_VARYINGNAME_,ThinFilmWeight,_MATRIXNAME_,thinFilmWeight,_INFONAME_,ThinFilmWeightInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,THIN_FILM_THICKNESS,_VARYINGNAME_,ThinFilmThickness,_MATRIXNAME_,thinFilmThickness,_INFONAME_,ThinFilmThicknessInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,AMBIENT_OCCLUSION,_VARYINGNAME_,AmbientOcclusion,_MATRIXNAME_,ambientOcclusion,_INFONAME_,AmbientOcclusionInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,DECAL,_VARYINGNAME_,Decal,_MATRIXNAME_,decal,_INFONAME_,DecalInfos.x)
#include<samplerVertexImplementation>(_DEFINENAME_,DETAIL,_VARYINGNAME_,Detail,_MATRIXNAME_,detail,_INFONAME_,DetailInfos.x)
#include<openpbrNormalMapVertex>
#include<clipPlaneVertex>
#include<fogVertex>
#include<shadowsVertex>[0..maxSimultaneousLights]
#include<vertexColorMixing>
#if defined(POINTSIZE) && !defined(WEBGPU)
gl_PointSize=pointSize;
#endif
#include<logDepthVertex>
#define CUSTOM_VERTEX_MAIN_END
}
`;e.ShadersStore[i]||(e.ShadersStore[i]=t);const b={name:i,shader:t};export{b as openpbrVertexShader};
