/*********************************************************************
  *   Copyright 2018, UCAR/Unidata
  *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
  *********************************************************************/

#ifndef D4CURLFUNCTIONS_H
#define D4CURLFUNCTIONS_H

/* Aliases to older names */
#ifndef HAVE_CURLOPT_KEYPASSWD
#define CURLOPT_KEYPASSWD CURLOPT_SSLKEYPASSWD
#endif
#ifndef HAVE_CURLINFO_RESPONSE_CODE
#define CURLINFO_RESPONSE_CODE CURLINFO_HTTP_CODE
#endif

enum CURLFLAGTYPE {CF_UNKNOWN=0,CF_OTHER=1,CF_STRING=2,CF_LONG=3};
struct CURLFLAG {
    const char* name;
    int flag;
    int value;
    enum CURLFLAGTYPE type;
};

extern ncerror NCD4_set_curlopt(NCD4INFO* state, int flag, void* value);

extern ncerror NCD4_set_flags_perfetch(NCD4INFO*);
extern ncerror NCD4_set_flags_perlink(NCD4INFO*);

extern ncerror NCD4_set_curlflag(NCD4INFO*,int);

extern void NCD4_curl_debug(NCD4INFO* state);

extern struct CURLFLAG* NCD4_curlflagbyname(const char* name);
extern void NCD4_curl_protocols(NCD4INFO*);
extern ncerror NCD4_get_rcproperties(NCD4INFO* state);

#endif /*D4CURLFUNCTIONS_H*/
