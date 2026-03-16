/*
 * This file is part of Python Twofish
 * a Python bridge to the C Twofish library by Niels Ferguson
 *
 * Released under The BSD 3-Clause License
 * Copyright (c) 2013 Keybase
 *
 * Bridge C module
 */

#include <Python.h>
#include "twofish.h"

#if !defined(DL_EXPORT)

#if defined(HAVE_DECLSPEC_DLL)
    #define DL_EXPORT(type) __declspec(dllexport) type
#else
    #define DL_EXPORT(type) type
#endif

#endif

/* Exported trampolines */
DL_EXPORT(void) exp_Twofish_initialise() {
    Twofish_initialise();
}

DL_EXPORT(void) exp_Twofish_prepare_key(uint8_t key[], int key_len, Twofish_key * xkey) {
    Twofish_prepare_key(key, key_len, xkey);
}

DL_EXPORT(void) exp_Twofish_encrypt(Twofish_key * xkey, uint8_t p[16], uint8_t c[16]) {
    Twofish_encrypt(xkey, p, c);
}

DL_EXPORT(void) exp_Twofish_decrypt(Twofish_key * xkey, uint8_t c[16], uint8_t p[16]) {
    Twofish_decrypt(xkey, c, p);
}

/*
We need a stub init_twofish function so the module will link as a proper module.
Do not import _twofish from python; it will not work since _twofish is not a *real* module
*/
PyMODINIT_FUNC init_twofish(void) { }
PyMODINIT_FUNC PyInit__twofish(void) { }
