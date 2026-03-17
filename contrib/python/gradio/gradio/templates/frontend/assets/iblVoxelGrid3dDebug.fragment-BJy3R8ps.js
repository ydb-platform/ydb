import{S as o}from"./index-BZt0m9TU.js";import"./index-xGeN4i2A.js";import"./svelte/svelte_svelte.js";import"./svelte/svelte_animate.js";import"./svelte/svelte_attachments.js";import"./svelte/svelte_easing.js";import"./svelte/svelte_internal_client.js";import"./svelte/svelte_internal_flags_async.js";import"./svelte/svelte_internal_flags_legacy.js";import"./svelte/svelte_internal_flags_tracing.js";import"./svelte/svelte_internal_server.js";import"./svelte/svelte_legacy.js";import"./svelte/svelte_motion.js";import"./svelte/svelte_reactivity.js";import"./svelte/svelte_reactivity_window.js";import"./svelte/svelte_server.js";import"./svelte/svelte_store.js";import"./svelte/svelte_transition.js";import"./svelte/svelte_events.js";const e="iblVoxelGrid3dDebugPixelShader",r=`precision highp sampler3D;varying vec2 vUV;uniform sampler3D voxelTexture;uniform sampler2D voxelSlabTexture;uniform sampler2D textureSampler;uniform vec4 sizeParams;
#define offsetX sizeParams.x
#define offsetY sizeParams.y
#define widthScale sizeParams.z
#define heightScale sizeParams.w
uniform float mipNumber;void main(void) {vec2 uv =
vec2((offsetX+vUV.x)*widthScale,(offsetY+vUV.y)*heightScale);vec4 background=texture2D(textureSampler,vUV);vec4 voxelSlab=texture2D(voxelSlabTexture,vUV);ivec3 size=textureSize(voxelTexture,int(mipNumber));float dimension=ceil(sqrt(float(size.z)));vec2 samplePos=fract(uv.xy*vec2(dimension));int sampleIndex=int(floor(uv.x*float(dimension)) +
floor(uv.y*float(dimension))*dimension);float mip_separator=0.0;if (samplePos.x<0.01 || samplePos.y<0.01) {mip_separator=1.0;}
bool outBounds=sampleIndex>size.z-1 ? true : false;sampleIndex=clamp(sampleIndex,0,size.z-1);ivec2 samplePosInt=ivec2(samplePos.xy*vec2(size.xy));vec3 voxel=texelFetch(voxelTexture,
ivec3(samplePosInt.x,samplePosInt.y,sampleIndex),
int(mipNumber))
.rgb;if (uv.x<0.0 || uv.x>1.0 || uv.y<0.0 || uv.y>1.0) {gl_FragColor.rgba=background;} else {if (outBounds) {voxel=vec3(0.15,0.0,0.0);} else {if (voxel.r>0.001) {voxel.g=1.0;}
voxel.r+=mip_separator;}
glFragColor.rgb=mix(background.rgb,voxelSlab.rgb,voxelSlab.a)+voxel;glFragColor.a=1.0;}}`;o.ShadersStore[e]||(o.ShadersStore[e]=r);const P={name:e,shader:r};export{P as iblVoxelGrid3dDebugPixelShader};
