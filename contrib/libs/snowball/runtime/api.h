#ifndef SNOWBALL_API_H_INCLUDED
#define SNOWBALL_API_H_INCLUDED

typedef unsigned char symbol;

/* Or replace 'char' above with 'short' for 16 bit characters.

   More precisely, replace 'char' with whatever type guarantees the
   character width you need.
*/

struct SN_env {
    symbol * p;
    int c; int l; int lb; int bra; int ket;
    int af;
#ifdef __cplusplus
    SN_env() : p(), c(), l(), lb(), bra(), ket(), af() { }
#endif
};

#ifdef __cplusplus
extern "C" {
#endif

extern struct SN_env * SN_new_env(int alloc_size);
extern void SN_delete_env(struct SN_env * z);

extern int SN_set_current(struct SN_env * z, int size, const symbol * s);

#ifdef __cplusplus
}
#endif

#endif
