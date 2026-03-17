/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */

#ifndef NCXML_H
#define NCXML_H

#ifdef DLL_NETCDF
  #ifdef DLL_EXPORT /* define when building the library */
    #define DECLSPEC __declspec(dllexport)
  #else
    #define DECLSPEC __declspec(dllimport)
  #endif
#else
  #define DECLSPEC
#endif

typedef void* ncxml_t;
typedef void* ncxml_attr_t;
typedef void* ncxml_doc_t;

#if defined(__cplusplus)
extern "C" {
#endif

DECLSPEC void ncxml_initialize(void);
DECLSPEC void ncxml_finalize(void);
DECLSPEC ncxml_doc_t ncxml_parse(char* contents, size_t len);
DECLSPEC void ncxml_free(ncxml_doc_t doc0);
DECLSPEC ncxml_t ncxml_root(ncxml_doc_t doc);
DECLSPEC const char* ncxml_name(ncxml_t xml0);
DECLSPEC char* ncxml_attr(ncxml_t xml0, const char* key);
DECLSPEC ncxml_t ncxml_child(ncxml_t xml0, const char* name);
DECLSPEC ncxml_t ncxml_next(ncxml_t xml0, const char* name);
DECLSPEC char* ncxml_text(ncxml_t xml0);
DECLSPEC int ncxml_attr_pairs(ncxml_t xml0, char*** pairsp);
/* Nameless versions of child and next */
DECLSPEC ncxml_t ncxml_child_first(ncxml_t xml0);
DECLSPEC ncxml_t ncxml_child_next(ncxml_t xml0);

#if defined(__cplusplus)
}
#endif

#endif /*NCXML_H*/
