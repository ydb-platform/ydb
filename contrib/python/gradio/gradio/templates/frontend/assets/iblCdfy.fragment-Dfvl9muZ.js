import{S as o}from"./index-BZt0m9TU.js";import"./helperFunctions-B2gYs5dd.js";import"./index-xGeN4i2A.js";import"./svelte/svelte_svelte.js";import"./svelte/svelte_animate.js";import"./svelte/svelte_attachments.js";import"./svelte/svelte_easing.js";import"./svelte/svelte_internal_client.js";import"./svelte/svelte_internal_flags_async.js";import"./svelte/svelte_internal_flags_legacy.js";import"./svelte/svelte_internal_flags_tracing.js";import"./svelte/svelte_internal_server.js";import"./svelte/svelte_legacy.js";import"./svelte/svelte_motion.js";import"./svelte/svelte_reactivity.js";import"./svelte/svelte_reactivity_window.js";import"./svelte/svelte_server.js";import"./svelte/svelte_store.js";import"./svelte/svelte_transition.js";import"./svelte/svelte_events.js";const e="iblCdfyPixelShader",i=`precision highp sampler2D;precision highp samplerCube;
#include<helperFunctions>
#define PI 3.1415927
varying vec2 vUV;
#ifdef IBL_USE_CUBE_MAP
uniform samplerCube iblSource;
#else
uniform sampler2D iblSource;
#endif
uniform int iblHeight;
#ifdef IBL_USE_CUBE_MAP
float fetchCube(vec2 uv) {vec3 direction=equirectangularToCubemapDirection(uv);return sin(PI*uv.y)*dot(textureCubeLodEXT(iblSource,direction,0.0).rgb,LuminanceEncodeApprox);}
#else
float fetchPanoramic(ivec2 Coords,float envmapHeight) {return sin(PI*(float(Coords.y)+0.5)/envmapHeight) *
dot(texelFetch(iblSource,Coords,0).rgb,LuminanceEncodeApprox);}
#endif
void main(void) {ivec2 coords=ivec2(gl_FragCoord.x,gl_FragCoord.y);float cdfy=0.0;for (int y=1; y<=coords.y; y++) {
#ifdef IBL_USE_CUBE_MAP
vec2 uv=vec2(vUV.x,(float(y-1)+0.5)/float(iblHeight));cdfy+=fetchCube(uv);
#else
cdfy+=fetchPanoramic(ivec2(coords.x,y-1),float(iblHeight));
#endif
}
gl_FragColor=vec4(cdfy,0.0,0.0,1.0);}`;o.ShadersStore[e]||(o.ShadersStore[e]=i);const x={name:e,shader:i};export{x as iblCdfyPixelShader};
