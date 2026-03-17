import{S as r}from"./index-BZt0m9TU.js";import"./index-xGeN4i2A.js";import"./svelte/svelte_svelte.js";import"./svelte/svelte_animate.js";import"./svelte/svelte_attachments.js";import"./svelte/svelte_easing.js";import"./svelte/svelte_internal_client.js";import"./svelte/svelte_internal_flags_async.js";import"./svelte/svelte_internal_flags_legacy.js";import"./svelte/svelte_internal_flags_tracing.js";import"./svelte/svelte_internal_server.js";import"./svelte/svelte_legacy.js";import"./svelte/svelte_motion.js";import"./svelte/svelte_reactivity.js";import"./svelte/svelte_reactivity_window.js";import"./svelte/svelte_server.js";import"./svelte/svelte_store.js";import"./svelte/svelte_transition.js";import"./svelte/svelte_events.js";const o="ssaoCombinePixelShader",e=`uniform sampler2D textureSampler;uniform sampler2D originalColor;uniform vec4 viewport;varying vec2 vUV;
#define CUSTOM_FRAGMENT_DEFINITIONS
void main(void) {
#define CUSTOM_FRAGMENT_MAIN_BEGIN
vec2 uv=viewport.xy+vUV*viewport.zw;vec4 ssaoColor=texture2D(textureSampler,uv);vec4 sceneColor=texture2D(originalColor,uv);gl_FragColor=sceneColor*ssaoColor;
#define CUSTOM_FRAGMENT_MAIN_END
}
`;r.ShadersStore[o]||(r.ShadersStore[o]=e);const T={name:o,shader:e};export{T as ssaoCombinePixelShader};
