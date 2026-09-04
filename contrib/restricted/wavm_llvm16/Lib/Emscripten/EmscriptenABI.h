#pragma once

#include <climits>
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/WASI/WASIABI.h"

namespace WAVM { namespace Emscripten { namespace emabi {

	typedef U32 Address;
	static constexpr Address addressMin = 0;
	static constexpr Address addressMax = UINT32_MAX;

	typedef U32 Size;
	static constexpr Size sizeMin = 0;
	static constexpr Size sizeMax = UINT32_MAX;

	typedef I32 Int;
	static constexpr Int intMin = INT32_MIN;
	static constexpr Int intMax = INT32_MAX;

	typedef I32 Long;
	static constexpr Long longMin = INT32_MIN;
	static constexpr Long longMax = INT32_MAX;

	typedef I32 FD;

	struct iovec
	{
		Address address;
		Size numBytes;
	};

	typedef Long time_t;

	struct tm
	{
		Int tm_sec;
		Int tm_min;
		Int tm_hour;
		Int tm_mday;
		Int tm_mon;
		Int tm_year;
		Int tm_wday;
		Int tm_yday;
		Int tm_isdst;
		Long __tm_gmtoff;
		Address __tm_zone;
	};

	typedef U32 pthread_t;
	typedef U32 pthread_key_t;

	typedef Int Result;
	static constexpr Int resultMin = intMin;
	static constexpr Int resultMax = intMax;

	typedef uint32_t __wasi_clockid_t;
#define __WASI_CLOCK_REALTIME (UINT32_C(0))
#define __WASI_CLOCK_MONOTONIC (UINT32_C(1))
#define __WASI_CLOCK_PROCESS_CPUTIME_ID (UINT32_C(2))
#define __WASI_CLOCK_THREAD_CPUTIME_ID (UINT32_C(3))

	// Emscripten uses the WASI ABI error codes for errors that are common to both ABIs.
	static constexpr Result esuccess = __WASI_ESUCCESS;
	static constexpr Result eperm = -__WASI_EPERM;
	static constexpr Result enoent = -__WASI_ENOENT;
	static constexpr Result esrch = -__WASI_ESRCH;
	static constexpr Result eintr = -__WASI_EINTR;
	static constexpr Result eio = -__WASI_EIO;
	static constexpr Result enxio = -__WASI_ENXIO;
	static constexpr Result e2big = -__WASI_E2BIG;
	static constexpr Result enoexec = -__WASI_ENOEXEC;
	static constexpr Result ebadf = -__WASI_EBADF;
	static constexpr Result echild = -__WASI_ECHILD;
	static constexpr Result eagain = -__WASI_EAGAIN;
	static constexpr Result enomem = -__WASI_ENOMEM;
	static constexpr Result eacces = -__WASI_EACCES;
	static constexpr Result efault = -__WASI_EFAULT;
	static constexpr Result ebusy = -__WASI_EBUSY;
	static constexpr Result eexist = -__WASI_EEXIST;
	static constexpr Result exdev = -__WASI_EXDEV;
	static constexpr Result enodev = -__WASI_ENODEV;
	static constexpr Result enotdir = -__WASI_ENOTDIR;
	static constexpr Result eisdir = -__WASI_EISDIR;
	static constexpr Result einval = -__WASI_EINVAL;
	static constexpr Result enfile = -__WASI_ENFILE;
	static constexpr Result emfile = -__WASI_EMFILE;
	static constexpr Result enotty = -__WASI_ENOTTY;
	static constexpr Result etxtbsy = -__WASI_ETXTBSY;
	static constexpr Result efbig = -__WASI_EFBIG;
	static constexpr Result enospc = -__WASI_ENOSPC;
	static constexpr Result espipe = -__WASI_ESPIPE;
	static constexpr Result erofs = -__WASI_EROFS;
	static constexpr Result emlink = -__WASI_EMLINK;
	static constexpr Result epipe = -__WASI_EPIPE;
	static constexpr Result edom = -__WASI_EDOM;
	static constexpr Result erange = -__WASI_ERANGE;
	static constexpr Result edeadlk = -__WASI_EDEADLK;
	static constexpr Result enametoolong = -__WASI_ENAMETOOLONG;
	static constexpr Result enolck = -__WASI_ENOLCK;
	static constexpr Result enosys = -__WASI_ENOSYS;
	static constexpr Result enotempty = -__WASI_ENOTEMPTY;
	static constexpr Result eloop = -__WASI_ELOOP;
	static constexpr Result enomsg = -__WASI_ENOMSG;
	static constexpr Result eidrm = -__WASI_EIDRM;
	static constexpr Result enolink = -__WASI_ENOLINK;
	static constexpr Result eproto = -__WASI_EPROTO;
	static constexpr Result emultihop = -__WASI_EMULTIHOP;
	static constexpr Result ebadmsg = -__WASI_EBADMSG;
	static constexpr Result eoverflow = -__WASI_EOVERFLOW;
	static constexpr Result eilseq = -__WASI_EILSEQ;
	static constexpr Result enotsock = -__WASI_ENOTSOCK;
	static constexpr Result enotsup = -__WASI_ENOTSUP;
	static constexpr Result edestaddrreq = -__WASI_EDESTADDRREQ;
	static constexpr Result emsgsize = -__WASI_EMSGSIZE;
	static constexpr Result eprototype = -__WASI_EPROTOTYPE;
	static constexpr Result enoprotoopt = -__WASI_ENOPROTOOPT;
	static constexpr Result eprotonosupport = -__WASI_EPROTONOSUPPORT;
	static constexpr Result eafnosupport = -__WASI_EAFNOSUPPORT;
	static constexpr Result eaddrinuse = -__WASI_EADDRINUSE;
	static constexpr Result eaddrnotavail = -__WASI_EADDRNOTAVAIL;
	static constexpr Result enetdown = -__WASI_ENETDOWN;
	static constexpr Result enetunreach = -__WASI_ENETUNREACH;
	static constexpr Result enetreset = -__WASI_ENETRESET;
	static constexpr Result econnaborted = -__WASI_ECONNABORTED;
	static constexpr Result econnreset = -__WASI_ECONNRESET;
	static constexpr Result enobufs = -__WASI_ENOBUFS;
	static constexpr Result eisconn = -__WASI_EISCONN;
	static constexpr Result enotconn = -__WASI_ENOTCONN;
	static constexpr Result etimedout = -__WASI_ETIMEDOUT;
	static constexpr Result econnrefused = -__WASI_ECONNREFUSED;
	static constexpr Result ehostunreach = -__WASI_EHOSTUNREACH;
	static constexpr Result ealready = -__WASI_EALREADY;
	static constexpr Result einprogress = -__WASI_EINPROGRESS;
	static constexpr Result estale = -__WASI_ESTALE;
	static constexpr Result edquot = -__WASI_EDQUOT;
	static constexpr Result ecanceled = -__WASI_ECANCELED;
	static constexpr Result eownerdead = -__WASI_EOWNERDEAD;
	static constexpr Result enotrecoverable = -__WASI_ENOTRECOVERABLE;
	static constexpr Result enostr = -100;
	static constexpr Result ebfont = -101;
	static constexpr Result ebadslt = -102;
	static constexpr Result ebadrqc = -103;
	static constexpr Result enoano = -104;
	static constexpr Result enotblk = -105;
	static constexpr Result echrng = -106;
	static constexpr Result el3hlt = -107;
	static constexpr Result el3rst = -108;
	static constexpr Result elnrng = -109;
	static constexpr Result eunatch = -110;
	static constexpr Result enocsi = -111;
	static constexpr Result el2hlt = -112;
	static constexpr Result ebade = -113;
	static constexpr Result ebadr = -114;
	static constexpr Result exfull = -115;
	static constexpr Result enodata = -116;
	static constexpr Result etime = -117;
	static constexpr Result enosr = -118;
	static constexpr Result enonet = -119;
	static constexpr Result enopkg = -120;
	static constexpr Result eremote = -121;
	static constexpr Result eadv = -122;
	static constexpr Result esrmnt = -123;
	static constexpr Result ecomm = -124;
	static constexpr Result edotdot = -125;
	static constexpr Result enotuniq = -126;
	static constexpr Result ebadfd = -127;
	static constexpr Result eremchg = -128;
	static constexpr Result elibacc = -129;
	static constexpr Result elibbad = -130;
	static constexpr Result elibscn = -131;
	static constexpr Result elibmax = -132;
	static constexpr Result elibexec = -133;
	static constexpr Result erestart = -134;
	static constexpr Result estrpipe = -135;
	static constexpr Result eusers = -136;
	static constexpr Result esocktnosupport = -137;
	static constexpr Result eopnotsupp = -138;
	static constexpr Result epfnosupport = -139;
	static constexpr Result eshutdown = -140;
	static constexpr Result etoomexternrefs = -141;
	static constexpr Result ehostdown = -142;
	static constexpr Result euclean = -143;
	static constexpr Result enotnam = -144;
	static constexpr Result enavail = -145;
	static constexpr Result eisnam = -146;
	static constexpr Result eremoteio = -147;
	static constexpr Result enomedium = -148;
	static constexpr Result emediumtype = -149;
	static constexpr Result enokey = -150;
	static constexpr Result ekeyexpired = -151;
	static constexpr Result ekeyrevoked = -152;
	static constexpr Result ekeyrejected = -153;
	static constexpr Result erfkill = -154;
	static constexpr Result ehwpoison = -155;
	static constexpr Result el2nsync = -156;
}}}
