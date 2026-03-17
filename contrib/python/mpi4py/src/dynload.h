/* Author:  Lisandro Dalcin
 * Contact: dalcinl@gmail.com
 */

#ifndef PyMPI_DYNLOAD_H
#define PyMPI_DYNLOAD_H

#if HAVE_DLFCN_H
  #include <dlfcn.h>
#else
  #if defined(__linux) || defined(__linux__)
    #define RTLD_LAZY     0x00001
    #define RTLD_NOW      0x00002
    #define RTLD_LOCAL    0x00000
    #define RTLD_GLOBAL   0x00100
    #define RTLD_NOLOAD   0x00004
    #define RTLD_NODELETE 0x01000
    #define RTLD_DEEPBIND 0x00008
  #elif defined(__sun) || defined(__sun__)
    #define RTLD_LAZY     0x00001
    #define RTLD_NOW      0x00002
    #define RTLD_LOCAL    0x00000
    #define RTLD_GLOBAL   0x00100
    #define RTLD_NOLOAD   0x00004
    #define RTLD_NODELETE 0x01000
    #define RTLD_FIRST    0x02000
  #elif defined(__APPLE__)
    #define RTLD_LAZY     0x1
    #define RTLD_NOW      0x2
    #define RTLD_LOCAL    0x4
    #define RTLD_GLOBAL   0x8
    #define RTLD_NOLOAD   0x10
    #define RTLD_NODELETE 0x80
    #define RTLD_FIRST    0x100
  #elif defined(__CYGWIN__)
    #define RTLD_LAZY     1
    #define RTLD_NOW      2
    #define RTLD_LOCAL    0
    #define RTLD_GLOBAL   4
  #endif
  #if defined(__cplusplus)
  extern "C" {
  #endif
  extern void *dlopen(const char *, int);
  extern void *dlsym(void *, const char *);
  extern int   dlclose(void *);
  extern char *dlerror(void);
  #if defined(__cplusplus)
  }
  #endif
#endif

#ifndef RTLD_LAZY
#define RTLD_LAZY 1
#endif
#ifndef RTLD_NOW
#define RTLD_NOW RTLD_LAZY
#endif
#ifndef RTLD_LOCAL
#define RTLD_LOCAL 0
#endif
#ifndef RTLD_GLOBAL
#define RTLD_GLOBAL RTLD_LOCAL
#endif

#endif /* !PyMPI_DYNLOAD_H */

/*
  Local variables:
  c-basic-offset: 2
  indent-tabs-mode: nil
  End:
*/
