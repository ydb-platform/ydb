import{S as r}from"./index-BZt0m9TU.js";import"./index-xGeN4i2A.js";import"./svelte/svelte_svelte.js";import"./svelte/svelte_animate.js";import"./svelte/svelte_attachments.js";import"./svelte/svelte_easing.js";import"./svelte/svelte_internal_client.js";import"./svelte/svelte_internal_flags_async.js";import"./svelte/svelte_internal_flags_legacy.js";import"./svelte/svelte_internal_flags_tracing.js";import"./svelte/svelte_internal_server.js";import"./svelte/svelte_legacy.js";import"./svelte/svelte_motion.js";import"./svelte/svelte_reactivity.js";import"./svelte/svelte_reactivity_window.js";import"./svelte/svelte_server.js";import"./svelte/svelte_store.js";import"./svelte/svelte_transition.js";import"./svelte/svelte_events.js";const e="iblShadowSpatialBlurPixelShader",o=`precision highp sampler2D;
#define PI 3.1415927
varying vec2 vUV;uniform sampler2D depthSampler;uniform sampler2D worldNormalSampler;uniform sampler2D voxelTracingSampler;uniform vec4 blurParameters;
#define stridef blurParameters.x
#define worldScale blurParameters.y
const float weights[5]=float[5](0.0625,0.25,0.375,0.25,0.0625);const int nbWeights=5;vec2 max2(vec2 v,vec2 w) {return vec2(max(v.x,w.x),max(v.y,w.y));}
void main(void)
{vec2 gbufferRes=vec2(textureSize(depthSampler,0));ivec2 gbufferPixelCoord=ivec2(vUV*gbufferRes);vec2 shadowRes=vec2(textureSize(voxelTracingSampler,0));ivec2 shadowPixelCoord=ivec2(vUV*shadowRes);vec3 N=texelFetch(worldNormalSampler,gbufferPixelCoord,0).xyz;if (length(N)<0.01) {glFragColor=vec4(1.0,1.0,0.0,1.0);return;}
float depth=-texelFetch(depthSampler,gbufferPixelCoord,0).x;vec4 X=vec4(0.0);for(int y=0; y<nbWeights; ++y) {for(int x=0; x<nbWeights; ++x) {ivec2 gBufferCoords=gbufferPixelCoord+int(stridef)*ivec2(x-(nbWeights>>1),y-(nbWeights>>1));ivec2 shadowCoords=shadowPixelCoord+int(stridef)*ivec2(x-(nbWeights>>1),y-(nbWeights>>1));vec4 T=texelFetch(voxelTracingSampler,shadowCoords,0);float ddepth=-texelFetch(depthSampler,gBufferCoords,0).x-depth;vec3 dN=texelFetch(worldNormalSampler,gBufferCoords,0).xyz-N;float w=weights[x]*weights[y] *
exp2(max(-1000.0/(worldScale*worldScale),-0.5) *
(ddepth*ddepth) -
1e1*dot(dN,dN));X+=vec4(w*T.x,w*T.y,w*T.z,w);}}
gl_FragColor=vec4(X.x/X.w,X.y/X.w,X.z/X.w,1.0);}`;r.ShadersStore[e]||(r.ShadersStore[e]=o);const y={name:e,shader:o};export{y as iblShadowSpatialBlurPixelShader};
