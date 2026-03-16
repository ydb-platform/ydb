#ifndef YDEBUG_H
#define YDEBUG_H

#ifdef DEBUG_CALL
#ifndef _MSC_VER
#define ydprintf(fmt, args...) fprintf(stderr, "[&] [yappi-dbg] " fmt "\n", ## args)
#else
#define ydprintf(fmt, ...) fprintf(stderr, "[&] [yappi-dbg] " fmt "\n", __VA_ARGS__)
#endif
#else
#ifndef _MSC_VER
#define ydprintf(fmt, args...)
#else
#define ydprintf(fmt, ...)
#endif
#endif

#ifndef _MSC_VER
#define yerr(fmt, args...) fprintf(stderr, "[*]	[yappi-err]	" fmt "\n", ## args)
#define yinfo(fmt, args...) fprintf(stderr, "[+] [yappi-info] " fmt "\n", ## args)
#define yprint(fmt, args...) fprintf(stderr, fmt,  ## args)
#else
#define yerr(fmt, ...) fprintf(stderr, "[*]	[yappi-err]	" fmt "\n", __VA_ARGS__)
#define yinfo(fmt, ...) fprintf(stderr, "[+] [yappi-info] " fmt "\n", __VA_ARGS__)
#define yprint(fmt, ...) fprintf(stderr, fmt, __VA_ARGS__)
#endif

#endif
