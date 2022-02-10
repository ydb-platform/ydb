#include "types.h"
#include <util/string/printf.h>
#include <util/generic/vector.h>
#include <errno.h>

namespace NActors {

    TVector<const char*> TDisconnectReason::Reasons = {
        "EndOfStream",
        "CloseOnIdle",
        "LostConnection",
        "DeadPeer",
        "NewSession",
        "HandshakeFailTransient",
        "HandshakeFailPermanent",
        "UserRequest",
        "Debug",
        "ChecksumError",
        "FormatError",
        "EventTooLarge",
        "QueueOverload",
        "E2BIG",
        "EACCES",
        "EADDRINUSE",
        "EADDRNOTAVAIL",
        "EADV",
        "EAFNOSUPPORT",
        "EAGAIN",
        "EALREADY",
        "EBADE",
        "EBADF",
        "EBADFD",
        "EBADMSG",
        "EBADR",
        "EBADRQC",
        "EBADSLT",
        "EBFONT",
        "EBUSY",
        "ECANCELED",
        "ECHILD",
        "ECHRNG",
        "ECOMM",
        "ECONNABORTED",
        "ECONNREFUSED",
        "ECONNRESET",
        "EDEADLK",
        "EDEADLOCK",
        "EDESTADDRREQ",
        "EDOM",
        "EDOTDOT",
        "EDQUOT",
        "EEXIST",
        "EFAULT",
        "EFBIG",
        "EHOSTDOWN",
        "EHOSTUNREACH",
        "EHWPOISON",
        "EIDRM",
        "EILSEQ",
        "EINPROGRESS",
        "EINTR",
        "EINVAL",
        "EIO",
        "EISCONN",
        "EISDIR",
        "EISNAM",
        "EKEYEXPIRED",
        "EKEYREJECTED",
        "EKEYREVOKED",
        "EL2HLT",
        "EL2NSYNC",
        "EL3HLT",
        "EL3RST",
        "ELIBACC",
        "ELIBBAD",
        "ELIBEXEC",
        "ELIBMAX",
        "ELIBSCN",
        "ELNRNG",
        "ELOOP",
        "EMEDIUMTYPE",
        "EMFILE",
        "EMLINK",
        "EMSGSIZE",
        "EMULTIHOP",
        "ENAMETOOLONG",
        "ENAVAIL",
        "ENETDOWN",
        "ENETRESET",
        "ENETUNREACH",
        "ENFILE",
        "ENOANO",
        "ENOBUFS",
        "ENOCSI",
        "ENODATA",
        "ENODEV",
        "ENOENT",
        "ENOEXEC",
        "ENOKEY",
        "ENOLCK",
        "ENOLINK",
        "ENOMEDIUM",
        "ENOMEM",
        "ENOMSG",
        "ENONET",
        "ENOPKG",
        "ENOPROTOOPT",
        "ENOSPC",
        "ENOSR",
        "ENOSTR",
        "ENOSYS",
        "ENOTBLK",
        "ENOTCONN",
        "ENOTDIR",
        "ENOTEMPTY",
        "ENOTNAM",
        "ENOTRECOVERABLE",
        "ENOTSOCK",
        "ENOTTY",
        "ENOTUNIQ",
        "ENXIO",
        "EOPNOTSUPP",
        "EOVERFLOW",
        "EOWNERDEAD",
        "EPERM",
        "EPFNOSUPPORT",
        "EPIPE",
        "EPROTO",
        "EPROTONOSUPPORT",
        "EPROTOTYPE",
        "ERANGE",
        "EREMCHG",
        "EREMOTE",
        "EREMOTEIO",
        "ERESTART",
        "ERFKILL",
        "EROFS",
        "ESHUTDOWN",
        "ESOCKTNOSUPPORT",
        "ESPIPE",
        "ESRCH",
        "ESRMNT",
        "ESTALE",
        "ESTRPIPE",
        "ETIME",
        "ETIMEDOUT",
        "ETOOMANYREFS",
        "ETXTBSY",
        "EUCLEAN",
        "EUNATCH",
        "EUSERS",
        "EWOULDBLOCK",
        "EXDEV",
        "EXFULL",
    };

    TDisconnectReason TDisconnectReason::FromErrno(int err) {
        switch (err) {
#define REASON(ERRNO) case ERRNO: return TDisconnectReason(TString(#ERRNO))
#if defined(E2BIG)
            REASON(E2BIG);
#endif
#if defined(EACCES)
            REASON(EACCES);
#endif
#if defined(EADDRINUSE)
            REASON(EADDRINUSE);
#endif
#if defined(EADDRNOTAVAIL)
            REASON(EADDRNOTAVAIL);
#endif
#if defined(EADV)
            REASON(EADV);
#endif
#if defined(EAFNOSUPPORT)
            REASON(EAFNOSUPPORT);
#endif
#if defined(EAGAIN)
            REASON(EAGAIN);
#endif
#if defined(EALREADY)
            REASON(EALREADY);
#endif
#if defined(EBADE)
            REASON(EBADE);
#endif
#if defined(EBADF)
            REASON(EBADF);
#endif
#if defined(EBADFD)
            REASON(EBADFD);
#endif
#if defined(EBADMSG)
            REASON(EBADMSG);
#endif
#if defined(EBADR)
            REASON(EBADR);
#endif
#if defined(EBADRQC)
            REASON(EBADRQC);
#endif
#if defined(EBADSLT)
            REASON(EBADSLT);
#endif
#if defined(EBFONT)
            REASON(EBFONT);
#endif
#if defined(EBUSY)
            REASON(EBUSY);
#endif
#if defined(ECANCELED)
            REASON(ECANCELED);
#endif
#if defined(ECHILD)
            REASON(ECHILD);
#endif
#if defined(ECHRNG)
            REASON(ECHRNG);
#endif
#if defined(ECOMM)
            REASON(ECOMM);
#endif
#if defined(ECONNABORTED)
            REASON(ECONNABORTED);
#endif
#if defined(ECONNREFUSED)
            REASON(ECONNREFUSED);
#endif
#if defined(ECONNRESET)
            REASON(ECONNRESET);
#endif
#if defined(EDEADLK)
            REASON(EDEADLK);
#endif
#if defined(EDEADLOCK) && (!defined(EDEADLK) || EDEADLOCK != EDEADLK)
            REASON(EDEADLOCK);
#endif
#if defined(EDESTADDRREQ)
            REASON(EDESTADDRREQ);
#endif
#if defined(EDOM)
            REASON(EDOM);
#endif
#if defined(EDOTDOT)
            REASON(EDOTDOT);
#endif
#if defined(EDQUOT)
            REASON(EDQUOT);
#endif
#if defined(EEXIST)
            REASON(EEXIST);
#endif
#if defined(EFAULT)
            REASON(EFAULT);
#endif
#if defined(EFBIG)
            REASON(EFBIG);
#endif
#if defined(EHOSTDOWN)
            REASON(EHOSTDOWN);
#endif
#if defined(EHOSTUNREACH)
            REASON(EHOSTUNREACH);
#endif
#if defined(EHWPOISON)
            REASON(EHWPOISON);
#endif
#if defined(EIDRM)
            REASON(EIDRM);
#endif
#if defined(EILSEQ)
            REASON(EILSEQ);
#endif
#if defined(EINPROGRESS)
            REASON(EINPROGRESS);
#endif
#if defined(EINTR)
            REASON(EINTR);
#endif
#if defined(EINVAL)
            REASON(EINVAL);
#endif
#if defined(EIO)
            REASON(EIO);
#endif
#if defined(EISCONN)
            REASON(EISCONN);
#endif
#if defined(EISDIR)
            REASON(EISDIR);
#endif
#if defined(EISNAM)
            REASON(EISNAM);
#endif
#if defined(EKEYEXPIRED)
            REASON(EKEYEXPIRED);
#endif
#if defined(EKEYREJECTED)
            REASON(EKEYREJECTED);
#endif
#if defined(EKEYREVOKED)
            REASON(EKEYREVOKED);
#endif
#if defined(EL2HLT)
            REASON(EL2HLT);
#endif
#if defined(EL2NSYNC)
            REASON(EL2NSYNC);
#endif
#if defined(EL3HLT)
            REASON(EL3HLT);
#endif
#if defined(EL3RST)
            REASON(EL3RST);
#endif
#if defined(ELIBACC)
            REASON(ELIBACC);
#endif
#if defined(ELIBBAD)
            REASON(ELIBBAD);
#endif
#if defined(ELIBEXEC)
            REASON(ELIBEXEC);
#endif
#if defined(ELIBMAX)
            REASON(ELIBMAX);
#endif
#if defined(ELIBSCN)
            REASON(ELIBSCN);
#endif
#if defined(ELNRNG)
            REASON(ELNRNG);
#endif
#if defined(ELOOP)
            REASON(ELOOP);
#endif
#if defined(EMEDIUMTYPE)
            REASON(EMEDIUMTYPE);
#endif
#if defined(EMFILE)
            REASON(EMFILE);
#endif
#if defined(EMLINK)
            REASON(EMLINK);
#endif
#if defined(EMSGSIZE)
            REASON(EMSGSIZE);
#endif
#if defined(EMULTIHOP)
            REASON(EMULTIHOP);
#endif
#if defined(ENAMETOOLONG)
            REASON(ENAMETOOLONG);
#endif
#if defined(ENAVAIL)
            REASON(ENAVAIL);
#endif
#if defined(ENETDOWN)
            REASON(ENETDOWN);
#endif
#if defined(ENETRESET)
            REASON(ENETRESET);
#endif
#if defined(ENETUNREACH)
            REASON(ENETUNREACH);
#endif
#if defined(ENFILE)
            REASON(ENFILE);
#endif
#if defined(ENOANO)
            REASON(ENOANO);
#endif
#if defined(ENOBUFS)
            REASON(ENOBUFS);
#endif
#if defined(ENOCSI)
            REASON(ENOCSI);
#endif
#if defined(ENODATA)
            REASON(ENODATA);
#endif
#if defined(ENODEV)
            REASON(ENODEV);
#endif
#if defined(ENOENT)
            REASON(ENOENT);
#endif
#if defined(ENOEXEC)
            REASON(ENOEXEC);
#endif
#if defined(ENOKEY)
            REASON(ENOKEY);
#endif
#if defined(ENOLCK)
            REASON(ENOLCK);
#endif
#if defined(ENOLINK)
            REASON(ENOLINK);
#endif
#if defined(ENOMEDIUM)
            REASON(ENOMEDIUM);
#endif
#if defined(ENOMEM)
            REASON(ENOMEM);
#endif
#if defined(ENOMSG)
            REASON(ENOMSG);
#endif
#if defined(ENONET)
            REASON(ENONET);
#endif
#if defined(ENOPKG)
            REASON(ENOPKG);
#endif
#if defined(ENOPROTOOPT)
            REASON(ENOPROTOOPT);
#endif
#if defined(ENOSPC)
            REASON(ENOSPC);
#endif
#if defined(ENOSR)
            REASON(ENOSR);
#endif
#if defined(ENOSTR)
            REASON(ENOSTR);
#endif
#if defined(ENOSYS)
            REASON(ENOSYS);
#endif
#if defined(ENOTBLK)
            REASON(ENOTBLK);
#endif
#if defined(ENOTCONN)
            REASON(ENOTCONN);
#endif
#if defined(ENOTDIR)
            REASON(ENOTDIR);
#endif
#if defined(ENOTEMPTY)
            REASON(ENOTEMPTY);
#endif
#if defined(ENOTNAM)
            REASON(ENOTNAM);
#endif
#if defined(ENOTRECOVERABLE)
            REASON(ENOTRECOVERABLE);
#endif
#if defined(ENOTSOCK)
            REASON(ENOTSOCK);
#endif
#if defined(ENOTTY)
            REASON(ENOTTY);
#endif
#if defined(ENOTUNIQ)
            REASON(ENOTUNIQ);
#endif
#if defined(ENXIO)
            REASON(ENXIO);
#endif
#if defined(EOPNOTSUPP)
            REASON(EOPNOTSUPP);
#endif
#if defined(EOVERFLOW)
            REASON(EOVERFLOW);
#endif
#if defined(EOWNERDEAD)
            REASON(EOWNERDEAD);
#endif
#if defined(EPERM)
            REASON(EPERM);
#endif
#if defined(EPFNOSUPPORT)
            REASON(EPFNOSUPPORT);
#endif
#if defined(EPIPE)
            REASON(EPIPE);
#endif
#if defined(EPROTO)
            REASON(EPROTO);
#endif
#if defined(EPROTONOSUPPORT)
            REASON(EPROTONOSUPPORT);
#endif
#if defined(EPROTOTYPE)
            REASON(EPROTOTYPE);
#endif
#if defined(ERANGE)
            REASON(ERANGE);
#endif
#if defined(EREMCHG)
            REASON(EREMCHG);
#endif
#if defined(EREMOTE)
            REASON(EREMOTE);
#endif
#if defined(EREMOTEIO)
            REASON(EREMOTEIO);
#endif
#if defined(ERESTART)
            REASON(ERESTART);
#endif
#if defined(ERFKILL)
            REASON(ERFKILL);
#endif
#if defined(EROFS)
            REASON(EROFS);
#endif
#if defined(ESHUTDOWN)
            REASON(ESHUTDOWN);
#endif
#if defined(ESOCKTNOSUPPORT)
            REASON(ESOCKTNOSUPPORT);
#endif
#if defined(ESPIPE)
            REASON(ESPIPE);
#endif
#if defined(ESRCH)
            REASON(ESRCH);
#endif
#if defined(ESRMNT)
            REASON(ESRMNT);
#endif
#if defined(ESTALE)
            REASON(ESTALE);
#endif
#if defined(ESTRPIPE)
            REASON(ESTRPIPE);
#endif
#if defined(ETIME)
            REASON(ETIME);
#endif
#if defined(ETIMEDOUT)
            REASON(ETIMEDOUT);
#endif
#if defined(ETOOMANYREFS)
            REASON(ETOOMANYREFS);
#endif
#if defined(ETXTBSY)
            REASON(ETXTBSY);
#endif
#if defined(EUCLEAN)
            REASON(EUCLEAN);
#endif
#if defined(EUNATCH)
            REASON(EUNATCH);
#endif
#if defined(EUSERS)
            REASON(EUSERS);
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || EWOULDBLOCK != EAGAIN)
            REASON(EWOULDBLOCK);
#endif
#if defined(EXDEV)
            REASON(EXDEV);
#endif
#if defined(EXFULL)
            REASON(EXFULL);
#endif
            default:
                return TDisconnectReason(Sprintf("errno=%d", errno));
        }
    }

} // NActors
