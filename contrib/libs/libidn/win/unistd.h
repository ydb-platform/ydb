#ifndef _LIBIDN_WIN_UNISTD_H 
#define _LIBIDN_WIN_UNISTD_H 
 
#ifndef _SSIZE_T_DEFINED 
#  if defined(_WIN64) 
#    define _SSIZE_T_DEFINED 
#    define ssize_t __int64 
#  else 
#    define _SSIZE_T_DEFINED 
#    define ssize_t int 
#  endif 
#endif 
 
#endif // _LIBIDN_WIN_UNISTD_H 
