import{S as r}from"./index-BZt0m9TU.js";import"./index-xGeN4i2A.js";import"./svelte/svelte_svelte.js";import"./svelte/svelte_animate.js";import"./svelte/svelte_attachments.js";import"./svelte/svelte_easing.js";import"./svelte/svelte_internal_client.js";import"./svelte/svelte_internal_flags_async.js";import"./svelte/svelte_internal_flags_legacy.js";import"./svelte/svelte_internal_flags_tracing.js";import"./svelte/svelte_internal_server.js";import"./svelte/svelte_legacy.js";import"./svelte/svelte_motion.js";import"./svelte/svelte_reactivity.js";import"./svelte/svelte_reactivity_window.js";import"./svelte/svelte_server.js";import"./svelte/svelte_store.js";import"./svelte/svelte_transition.js";import"./svelte/svelte_events.js";const e="iblShadowAccumulationPixelShader",o=`#ifdef GL_ES
precision mediump float;
#endif
varying vec2 vUV;uniform vec4 accumulationParameters;
#define remanence accumulationParameters.x
#define resetb accumulationParameters.y
#define sceneSize accumulationParameters.z
uniform sampler2D motionSampler;uniform sampler2D positionSampler;uniform sampler2D spatialBlurSampler;uniform sampler2D oldAccumulationSampler;uniform sampler2D prevPositionSampler;vec2 max2(vec2 v,vec2 w) { return vec2(max(v.x,w.x),max(v.y,w.y)); }
void main(void) {bool reset=bool(resetb);vec2 gbufferRes=vec2(textureSize(motionSampler,0));ivec2 gbufferPixelCoord=ivec2(vUV*gbufferRes);vec2 shadowRes=vec2(textureSize(spatialBlurSampler,0));ivec2 shadowPixelCoord=ivec2(vUV*shadowRes);vec4 LP=texelFetch(positionSampler,gbufferPixelCoord,0);if (0.0==LP.w) {gl_FragColor=vec4(1.0,0.0,0.0,1.0);return;}
vec2 velocityColor=texelFetch(motionSampler,gbufferPixelCoord,0).xy;vec2 prevCoord=vUV+velocityColor;vec3 PrevLP=texture(prevPositionSampler,prevCoord).xyz;vec4 PrevShadows=texture(oldAccumulationSampler,prevCoord);vec3 newShadows=texelFetch(spatialBlurSampler,shadowPixelCoord,0).xyz;PrevShadows.a =
!reset && all(lessThan(abs(prevCoord-vec2(0.5)),vec2(0.5))) &&
distance(LP.xyz,PrevLP)<5e-2*sceneSize
? max(PrevShadows.a/(1.0+PrevShadows.a),1.0-remanence)
: 1.0;PrevShadows=max(vec4(0.0),PrevShadows);gl_FragColor =
vec4(mix(PrevShadows.x,newShadows.x,PrevShadows.a),
mix(PrevShadows.y,newShadows.y,PrevShadows.a),
mix(PrevShadows.z,newShadows.z,PrevShadows.a),PrevShadows.a);}`;r.ShadersStore[e]||(r.ShadersStore[e]=o);const b={name:e,shader:o};export{b as iblShadowAccumulationPixelShader};
