#pragma once

// strrpbrk would suffice but is not portable.
#ifdef __cplusplus
extern "C" {
#endif

int context_len( char const* );
int utf8str_codepoint_len( char const*, int );

#ifdef __cplusplus
}
#endif
