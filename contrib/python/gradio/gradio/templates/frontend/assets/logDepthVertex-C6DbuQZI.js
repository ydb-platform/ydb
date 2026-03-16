import{S as e}from"./index-BZt0m9TU.js";const t="fogVertexDeclaration",r=`#ifdef FOG
varying vec3 vFogDistance;
#endif
`;e.IncludesShadersStore[t]||(e.IncludesShadersStore[t]=r);const o="fogVertex",s=`#ifdef FOG
vFogDistance=(view*worldPos).xyz;
#endif
`;e.IncludesShadersStore[o]||(e.IncludesShadersStore[o]=s);const n="logDepthVertex",a=`#ifdef LOGARITHMICDEPTH
vFragmentDepth=1.0+gl_Position.w;gl_Position.z=log2(max(0.000001,vFragmentDepth))*logarithmicDepthConstant;
#endif
`;e.IncludesShadersStore[n]||(e.IncludesShadersStore[n]=a);
