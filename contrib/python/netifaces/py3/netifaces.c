#include <Python.h>

/* Before Python 2.6, PyUnicode_FromString doesn't exist */
#if PY_MAJOR_VERSION < 2 || (PY_MAJOR_VERSION == 2 && PY_MINOR_VERSION < 6)
PyObject *PyUnicode_FromString(const char *s)
{
  Py_ssize_t len = strlen(s);
  if (!len) {
    Py_UNICODE uc = 0;
    return PyUnicode_FromUnicode(&uc, 0);
  }
  return PyUnicode_DecodeUTF8(s, len, NULL);
}
#endif

/* Python 3 compatibility */
#if PY_MAJOR_VERSION >= 3
#define PyInt_FromLong PyLong_FromLong

#define MODULE_ERROR            NULL
#define MODULE_RETURN(v)       return (v)
#define MODULE_INIT(name)       PyMODINIT_FUNC PyInit_##name(void)
#define MODULE_DEF(name,doc,methods)                        \
  static struct PyModuleDef moduledef = {                   \
    PyModuleDef_HEAD_INIT, (name), (doc), -1, (methods), };
#define MODULE_CREATE(obj,name,doc,methods) \
  obj = PyModule_Create(&moduledef);
#else /* PY_MAJOR_VERSION < 3 */
#define MODULE_ERROR
#define MODULE_RETURN(v)
#define MODULE_INIT(name)       void init##name(void)
#define MODULE_DEF(name,doc,methods)
#define MODULE_CREATE(obj,name,doc,methods) \
  obj = Py_InitModule3((name), (methods), (doc));
#endif

#ifndef WIN32

#  include <sys/types.h>
#  include <sys/socket.h>
#  include <net/if.h>
#  include <netdb.h>

#  if HAVE_PF_ROUTE
#    include <net/route.h>
#  endif

#  if HAVE_SYSCTL_CTL_NET
#    include <sys/sysctl.h>
#    include <net/route.h>
#  endif

/* RTNL_FAMILY_MAX not there yet in old kernels, see linux commit 25239ce */
#  if HAVE_PF_NETLINK
#    include <asm/types.h>
#    include <linux/netlink.h>
#    include <linux/rtnetlink.h>
#    if !defined(RTNL_FAMILY_MAX)
#      include <linux/net.h>
#      define RTNL_FAMILY_MAX NPROTO
#    endif
#    include <arpa/inet.h>
#  endif

#  if HAVE_GETIFADDRS
#    if HAVE_IPV6_SOCKET_IOCTLS
#      include <sys/ioctl.h>
#      include <netinet/in.h>
#      error #include <netinet/in_var.h>
#    endif
#  endif

#  if HAVE_SOCKET_IOCTLS
#    include <sys/ioctl.h>
#    include <netinet/in.h>
#    include <arpa/inet.h>
#    if defined(__sun)
#      include <unistd.h>
#      include <stropts.h>
#      include <sys/sockio.h>
#    endif
#  endif /* HAVE_SOCKET_IOCTLS */

/* For logical interfaces support we convert all names to same name prefixed
   with l */
#if HAVE_SIOCGLIFNUM
#define CNAME(x) l##x
#else
#define CNAME(x) x
#endif

#if HAVE_NET_IF_DL_H
#  include <net/if_dl.h>
#endif

/* For the benefit of stupid platforms (Linux), include all the sockaddr
   definitions we can lay our hands on. It can also be useful for the benefit
   of another stupid platform (FreeBSD, see PR 152036). */
#include <netinet/in.h>
#  if HAVE_NETASH_ASH_H
#    include <netash/ash.h>
#  endif
#  if HAVE_NETATALK_AT_H
#    include <netatalk/at.h>
#  endif
#  if HAVE_NETAX25_AX25_H
#    include <netax25/ax25.h>
#  endif
#  if HAVE_NETECONET_EC_H
#    include <neteconet/ec.h>
#  endif
#  if HAVE_NETIPX_IPX_H
#    include <netipx/ipx.h>
#  endif
#  if HAVE_NETPACKET_PACKET_H
#    include <netpacket/packet.h>
#  endif
#  if HAVE_NETROSE_ROSE_H
#    include <netrose/rose.h>
#  endif
#  if HAVE_LINUX_IRDA_H
#    error #include <linux/irda.h>
#  endif
#  if HAVE_LINUX_ATM_H
#    include <linux/atm.h>
#  endif
#  if HAVE_LINUX_LLC_H
#    include <linux/llc.h>
#  endif
#  if HAVE_LINUX_TIPC_H
#    include <linux/tipc.h>
#  endif
#  if HAVE_LINUX_DN_H
#    error #include <linux/dn.h>
#  endif

/* Map address families to sizes of sockaddr structs */
static int af_to_len(int af) 
{
  switch (af) {
  case AF_INET: return sizeof (struct sockaddr_in);
#if defined(AF_INET6) && HAVE_SOCKADDR_IN6
  case AF_INET6: return sizeof (struct sockaddr_in6);
#endif
#if defined(AF_AX25) && HAVE_SOCKADDR_AX25
#  if defined(AF_NETROM)
  case AF_NETROM: /* I'm assuming this is carried over x25 */
#  endif
  case AF_AX25: return sizeof (struct sockaddr_ax25);
#endif
#if defined(AF_IPX) && HAVE_SOCKADDR_IPX
  case AF_IPX: return sizeof (struct sockaddr_ipx);
#endif
#if defined(AF_APPLETALK) && HAVE_SOCKADDR_AT
  case AF_APPLETALK: return sizeof (struct sockaddr_at);
#endif
#if defined(AF_ATMPVC) && HAVE_SOCKADDR_ATMPVC
  case AF_ATMPVC: return sizeof (struct sockaddr_atmpvc);
#endif
#if defined(AF_ATMSVC) && HAVE_SOCKADDR_ATMSVC
  case AF_ATMSVC: return sizeof (struct sockaddr_atmsvc);
#endif
#if defined(AF_X25) && HAVE_SOCKADDR_X25
  case AF_X25: return sizeof (struct sockaddr_x25);
#endif
#if defined(AF_ROSE) && HAVE_SOCKADDR_ROSE
  case AF_ROSE: return sizeof (struct sockaddr_rose);
#endif
#if defined(AF_DECnet) && HAVE_SOCKADDR_DN
  case AF_DECnet: return sizeof (struct sockaddr_dn);
#endif
#if defined(AF_PACKET) && HAVE_SOCKADDR_LL
  case AF_PACKET: return sizeof (struct sockaddr_ll);
#endif
#if defined(AF_ASH) && HAVE_SOCKADDR_ASH
  case AF_ASH: return sizeof (struct sockaddr_ash);
#endif
#if defined(AF_ECONET) && HAVE_SOCKADDR_EC
  case AF_ECONET: return sizeof (struct sockaddr_ec);
#endif
#if defined(AF_IRDA) && HAVE_SOCKADDR_IRDA
  case AF_IRDA: return sizeof (struct sockaddr_irda);
#endif
#if defined(AF_LINK) && HAVE_SOCKADDR_DL
  case AF_LINK: return sizeof (struct sockaddr_dl);
#endif
  }
  return sizeof (struct sockaddr);
}

#if !HAVE_SOCKADDR_SA_LEN
#define SA_LEN(sa)      af_to_len(sa->sa_family)
#if HAVE_SIOCGLIFNUM
#define SS_LEN(sa)      af_to_len(sa->ss_family)
#else
#define SS_LEN(sa)      SA_LEN(sa)
#endif
#else
#define SA_LEN(sa)      sa->sa_len
#endif /* !HAVE_SOCKADDR_SA_LEN */

#  if HAVE_GETIFADDRS
#    include <ifaddrs.h>
#  endif /* HAVE_GETIFADDRS */

#  if !HAVE_GETIFADDRS && (!HAVE_SOCKET_IOCTLS || !HAVE_SIOCGIFCONF)
/* If the platform doesn't define, what we need, barf.  If you're seeing this,
   it means you need to write suitable code to retrieve interface information
   on your system. */
#    error You need to add code for your platform.
#  endif

#else /* defined(WIN32) */

#define _WIN32_WINNT 0x0501

#  include <winsock2.h>
#  include <ws2tcpip.h>
#  include <iphlpapi.h>
#  include <netioapi.h>

#endif /* defined(WIN32) */

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

/* On systems without AF_LINK (Windows, for instance), define it anyway, but
   give it a crazy value.  On Linux, which has AF_PACKET but not AF_LINK,
   define AF_LINK as the latter instead. */
#ifndef AF_LINK
#  ifdef AF_PACKET
#    define AF_LINK  AF_PACKET
#  else
#    define AF_LINK  -1000
#  endif
#  define HAVE_AF_LINK 0
#else
#  define HAVE_AF_LINK 1
#endif

/* -- Utility Functions ----------------------------------------------------- */

#if !defined(WIN32)
#if  !HAVE_GETNAMEINFO
#undef getnameinfo
#undef NI_NUMERICHOST

#define getnameinfo our_getnameinfo
#define NI_NUMERICHOST 1

/* A very simple getnameinfo() for platforms without */
static int
getnameinfo (const struct sockaddr *addr, int addr_len,
             char *buffer, int buflen,
             char *buf2, int buf2len,
             int flags)
{
  switch (addr->sa_family) {
  case AF_INET:
    {
      const struct sockaddr_in *sin = (struct sockaddr_in *)addr;
      const unsigned char *bytes = (unsigned char *)&sin->sin_addr.s_addr;
      char tmpbuf[20];

      sprintf (tmpbuf, "%d.%d.%d.%d",
               bytes[0], bytes[1], bytes[2], bytes[3]);

      strncpy (buffer, tmpbuf, buflen);
    }
    break;
#ifdef AF_INET6
  case AF_INET6:
    {
      const struct sockaddr_in6 *sin = (const struct sockaddr_in6 *)addr;
      const unsigned char *bytes = sin->sin6_addr.s6_addr;
      int n;
      char tmpbuf[80], *ptr = tmpbuf;
      int done_double_colon = FALSE;
      int colon_mode = FALSE;

      for (n = 0; n < 8; ++n) {
        unsigned char b1 = bytes[2 * n];
        unsigned char b2 = bytes[2 * n + 1];

        if (b1) {
          if (colon_mode) {
            colon_mode = FALSE;
            *ptr++ = ':';
          }
          sprintf (ptr, "%x%02x", b1, b2);
          ptr += strlen (ptr);
          *ptr++ = ':';
        } else if (b2) {
          if (colon_mode) {
            colon_mode = FALSE;
            *ptr++ = ':';
          }
          sprintf (ptr, "%x", b2);
          ptr += strlen (ptr);
          *ptr++ = ':';
        } else {
          if (!colon_mode) {
            if (done_double_colon) {
              *ptr++ = '0';
              *ptr++ = ':';
            } else {
              if (n == 0)
                *ptr++ = ':';
              colon_mode = TRUE;
              done_double_colon = TRUE;
            }
          }
        }
      }
      if (colon_mode) {
        colon_mode = FALSE;
        *ptr++ = ':';
        *ptr++ = '\0';
      } else {
        *--ptr = '\0';
      }

      strncpy (buffer, tmpbuf, buflen);
    }
    break;
#endif /* AF_INET6 */
  default:
    return -1;
  }

  return 0;
}
#endif

static int
string_from_sockaddr (struct sockaddr *addr,
                      char *buffer,
                      size_t buflen)
{
  struct sockaddr* bigaddr = 0;
  int failure;
  struct sockaddr* gniaddr;
  socklen_t gnilen;

  if (!addr || addr->sa_family == AF_UNSPEC)
    return -1;

  if (SA_LEN(addr) < af_to_len(addr->sa_family)) {
    /* Sometimes ifa_netmask can be truncated. So let's detruncate it.  FreeBSD
       PR: kern/152036: getifaddrs(3) returns truncated sockaddrs for netmasks
       -- http://www.freebsd.org/cgi/query-pr.cgi?pr=152036 */
    gnilen = af_to_len(addr->sa_family);
    bigaddr = calloc(1, gnilen);
    if (!bigaddr)
      return -1;
    memcpy(bigaddr, addr, SA_LEN(addr));
#if HAVE_SOCKADDR_SA_LEN
    bigaddr->sa_len = gnilen;
#endif
    gniaddr = bigaddr;
  } else {
    gnilen = SA_LEN(addr);
    gniaddr = addr;
  }

  failure = getnameinfo (gniaddr, gnilen,
                         buffer, buflen,
                         NULL, 0,
                         NI_NUMERICHOST);

  if (bigaddr) {
    free(bigaddr);
    bigaddr = 0;
  }

  if (failure) {
    size_t n, len;
    char *ptr;
    const char *data;
      
    len = SA_LEN(addr);

#if HAVE_AF_LINK
    /* BSD-like systems have AF_LINK */
    if (addr->sa_family == AF_LINK) {
      struct sockaddr_dl *dladdr = (struct sockaddr_dl *)addr;
      len = dladdr->sdl_alen;
      data = LLADDR(dladdr);
    } else {
#endif
#if defined(AF_PACKET)
      /* Linux has AF_PACKET instead */
      if (addr->sa_family == AF_PACKET) {
        struct sockaddr_ll *lladdr = (struct sockaddr_ll *)addr;
        len = lladdr->sll_halen;
        data = (const char *)lladdr->sll_addr;
      } else {
#endif
        /* We don't know anything about this sockaddr, so just display
           the entire data area in binary. */
        len -= (sizeof (struct sockaddr) - sizeof (addr->sa_data));
        data = addr->sa_data;
#if defined(AF_PACKET)
      }
#endif
#if HAVE_AF_LINK
    }
#endif

    if (buflen < 3 * len)
      return -1;

    ptr = buffer;
    buffer[0] = '\0';

    for (n = 0; n < len; ++n) {
      sprintf (ptr, "%02x:", data[n] & 0xff);
      ptr += 3;
    }
    if (len)
      *--ptr = '\0';
  }

  if (!buffer[0])
    return -1;

  return 0;
}

/* Tries to format in CIDR form where possible; falls back to using
   string_from_sockaddr(). */
static int
string_from_netmask (struct sockaddr *addr,
                     char *buffer,
                     size_t buflen)
{
#ifdef AF_INET6
  if (addr && addr->sa_family == AF_INET6) {
    struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)addr;
    unsigned n = 16;
    unsigned zeroes = 0;
    unsigned prefix;
    unsigned bytes;
    char *bufptr = buffer;
    char *bufend = buffer + buflen;
    char pfxbuf[16];

    while (n--) {
      unsigned char byte = sin6->sin6_addr.s6_addr[n];

      /* We need to count the rightmost zeroes */
      unsigned char x = byte;
      unsigned zx = 8;

      x &= -x;
      if (x)
        --zx;
      if (x & 0x0f)
        zx -= 4;
      if (x & 0x33)
        zx -= 2;
      if (x & 0x55)
        zx -= 1;

      zeroes += zx;

      if (byte)
        break;
    }

    prefix = 128 - zeroes;
    bytes = 2 * ((prefix + 15) / 16);

    for (n = 0; n < bytes; ++n) {
      unsigned char byte = sin6->sin6_addr.s6_addr[n];
      char ch1, ch2;

      if (n && !(n & 1)) {
        if (bufptr < bufend)
          *bufptr++ = ':';
      }

      ch1 = '0' + (byte >> 4);
      if (ch1 > '9')
        ch1 += 'a' - '0' - 10;
      ch2 = '0' + (byte & 0xf);
      if (ch2 > '9')
        ch2 += 'a' - '0' - 10;

      if (bufptr < bufend)
        *bufptr++ = ch1;
      if (bufptr < bufend)
        *bufptr++ = ch2;
    }

    if (bytes < 16) {
      if (bufend - bufptr > 2) {
        *bufptr++ = ':';
        *bufptr++ = ':';
      }
    }

    sprintf (pfxbuf, "/%u", prefix);

    if (bufend - bufptr > (int)strlen(pfxbuf))
      strcpy (bufptr, pfxbuf);

    if (buflen)
      buffer[buflen - 1] = '\0';

    return 0;
  }
#endif

  return string_from_sockaddr(addr, buffer, buflen);
}
#endif /* !defined(WIN32) */

#if defined(WIN32)
static int
compare_bits (const void *pva,
              const void *pvb,
              unsigned bits)
{
  const unsigned char *pa = (const unsigned char *)pva;
  const unsigned char *pb = (const unsigned char *)pvb;
  unsigned char a, b;
  static unsigned char masks[] = {
    0x00, 0x80, 0xc0, 0xe0, 0xf0, 0xf8, 0xfc, 0xfe
  };
  while (bits >= 8) {
    a = *pa++;
    b = *pb++;
    if (a < b)
      return -1;
    else if (a > b)
      return +1;
    bits -= 8;
  }

  if (bits) {
    a = *pa++ & masks[bits];
    b = *pb++ & masks[bits];
    if (a < b)
      return -1;
    else if (a > b)
      return +1;
  }

  return 0;
}

static PyObject *
netmask_from_prefix (unsigned prefix)
{
  char buffer[256];
  char *bufptr = buffer;
  char *bufend = buffer + sizeof(buffer);
  unsigned bytes = 2 * ((prefix + 15) / 16);
  static const unsigned char masks[] = {
    0x00, 0x80, 0xc0, 0xe0, 0xf0, 0xf8, 0xfc, 0xfe
  };
  unsigned n;
  unsigned left = prefix;
  char pfxbuf[16];

  for (n = 0; n < bytes; ++n) {
    unsigned char byte;
    char ch1, ch2;

    if (left >= 8) {
      byte = 0xff;
      left -= 8;
    } else {
      byte = masks[left];
      left = 0;
    }

    if (n && !(n & 1)) {
      if (bufptr < bufend)
        *bufptr++ = ':';
    }

    ch1 = '0' + (byte >> 4);
    if (ch1 > '9')
      ch1 += 'a' - '0' - 10;
    ch2 = '0' + (byte & 0xf);
    if (ch2 > '9')
      ch2 += 'a' - '0' - 10;

    if (bufptr < bufend)
      *bufptr++ = ch1;
    if (bufptr < bufend)
      *bufptr++ = ch2;
  }

  if (bytes < 16) {
    if (bufend - bufptr > 2) {
      *bufptr++ = ':';
      *bufptr++ = ':';
    }
  }

  sprintf (pfxbuf, "/%u", prefix);

  if ((size_t)(bufend - bufptr) > strlen(pfxbuf))
    strcpy (bufptr, pfxbuf);

  buffer[sizeof(buffer) - 1] = '\0';

  return PyUnicode_FromString(buffer);
}

/* We dynamically bind to WSAAddressToStringW or WSAAddressToStringA
   depending on which is available, as the latter is deprecated and
   the former doesn't exist on all Windows versions on which this code
   might run. */
typedef INT (WSAAPI *WSAAddressToStringWPtr)(LPSOCKADDR, DWORD, LPWSAPROTOCOL_INFOW, LPWSTR, LPDWORD);
typedef INT (WSAAPI *WSAAddressToStringAPtr)(LPSOCKADDR, DWORD, LPWSAPROTOCOL_INFOA, LPSTR, LPDWORD);

static WSAAddressToStringWPtr
get_address_to_string_w(void) {
  static int ptr_is_set;
  static WSAAddressToStringWPtr ptr;

  if (!ptr_is_set) {
    HMODULE hmod = LoadLibrary ("ws2_32.dll");
    ptr = (WSAAddressToStringWPtr)GetProcAddress (hmod, "WSAAddressToStringW");
    if (!ptr)
      FreeLibrary (hmod);
    ptr_is_set = 1;
  }

  return ptr;
}

static WSAAddressToStringAPtr
get_address_to_string_a(void) {
  static int ptr_is_set;
  static WSAAddressToStringAPtr ptr;

  if (!ptr_is_set) {
    HMODULE hmod = LoadLibrary ("ws2_32.dll");
    ptr = (WSAAddressToStringAPtr)GetProcAddress (hmod, "WSAAddressToStringA");
    if (!ptr)
      FreeLibrary (hmod);
    ptr_is_set = 1;
  }

  return ptr;
}

static PyObject *
string_from_address(SOCKADDR *addr, DWORD addrlen)
{
  WSAAddressToStringWPtr AddressToStringW = get_address_to_string_w();

  if (AddressToStringW) {
    wchar_t buffer[256];
    DWORD dwLen = sizeof(buffer) / sizeof(wchar_t);
    INT iRet;

    iRet = AddressToStringW (addr, addrlen, NULL, buffer, &dwLen);

    if (iRet == 0)
      return PyUnicode_FromWideChar (buffer, dwLen - 1);
  } else {
    char buffer[256];
    DWORD dwLen = sizeof(buffer);
    WSAAddressToStringAPtr AddressToStringA = get_address_to_string_a();
    INT iRet;

    iRet = AddressToStringA (addr, addrlen, NULL, buffer, &dwLen);

    if (iRet == 0)
      return PyUnicode_FromString (buffer);
  }

  return NULL;
}
#endif

static int
add_to_family (PyObject *result, int family, PyObject *obj)
{
  PyObject *py_family;
  PyObject *list;

  if (!PyObject_Size (obj))
    return TRUE;

  py_family = PyInt_FromLong (family);
  list = PyDict_GetItem (result, py_family);

  if (!py_family) {
    Py_DECREF (obj);
    Py_XDECREF (list);
    return FALSE;
  }

  if (!list) {
    list = PyList_New (1);
    if (!list) {
      Py_DECREF (obj);
      Py_DECREF (py_family);
      return FALSE;
    }

    PyList_SET_ITEM (list, 0, obj);
    PyDict_SetItem (result, py_family, list);
    Py_DECREF (list);
  } else {
    PyList_Append (list, obj);
    Py_DECREF (obj);
  }

  return TRUE;
}

/* -- ifaddresses() --------------------------------------------------------- */

static PyObject *
ifaddrs (PyObject *self, PyObject *args)
{
  const char *ifname;
  PyObject *result;
  int found = FALSE;
#if defined(WIN32)
  PIP_ADAPTER_ADDRESSES pAdapterAddresses = NULL, pInfo = NULL;
  ULONG ulBufferLength = 0;
  DWORD dwRet;
  PIP_ADAPTER_UNICAST_ADDRESS pUniAddr;
#elif HAVE_GETIFADDRS
  struct ifaddrs *addrs = NULL;
  struct ifaddrs *addr = NULL;
#endif

  if (!PyArg_ParseTuple (args, "s", &ifname))
    return NULL;

  result = PyDict_New ();

  if (!result)
    return NULL;

#if defined(WIN32)
  /* .. Win32 ............................................................... */

  /* First, retrieve the adapter information.  We do this in a loop, in
     case someone adds or removes adapters in the meantime. */
  do {
    dwRet = GetAdaptersAddresses (AF_UNSPEC, GAA_FLAG_INCLUDE_PREFIX, NULL,
                                  pAdapterAddresses, &ulBufferLength);

    if (dwRet == ERROR_BUFFER_OVERFLOW) {
      if (pAdapterAddresses)
        free (pAdapterAddresses);
      pAdapterAddresses = (PIP_ADAPTER_ADDRESSES)malloc (ulBufferLength);

      if (!pAdapterAddresses) {
        Py_DECREF (result);
        PyErr_SetString (PyExc_MemoryError, "Not enough memory");
        return NULL;
      }
    }
  } while (dwRet == ERROR_BUFFER_OVERFLOW);

  /* If we failed, then fail in Python too */
  if (dwRet != ERROR_SUCCESS && dwRet != ERROR_NO_DATA) {
    Py_DECREF (result);
    if (pAdapterAddresses)
      free (pAdapterAddresses);

    PyErr_SetString (PyExc_OSError,
                     "Unable to obtain adapter information.");
    return NULL;
  }

  for (pInfo = pAdapterAddresses; pInfo; pInfo = pInfo->Next) {
    char buffer[256];

    if (strcmp (pInfo->AdapterName, ifname) != 0)
      continue;

    found = TRUE;

    /* Do the physical address */
    if (256 >= 3 * pInfo->PhysicalAddressLength) {
      PyObject *hwaddr, *dict;
      char *ptr = buffer;
      unsigned n;

      *ptr = '\0';
      for (n = 0; n < pInfo->PhysicalAddressLength; ++n) {
        sprintf (ptr, "%02x:", pInfo->PhysicalAddress[n] & 0xff);
        ptr += 3;
      }
      *--ptr = '\0';

      hwaddr = PyUnicode_FromString (buffer);
      dict = PyDict_New ();

      if (!dict) {
        Py_XDECREF (hwaddr);
        Py_DECREF (result);
        free (pAdapterAddresses);
        return NULL;
      }

      PyDict_SetItemString (dict, "addr", hwaddr);
      Py_DECREF (hwaddr);

      if (!add_to_family (result, AF_LINK, dict)) {
        Py_DECREF (result);
        free (pAdapterAddresses);
        return NULL;
      }
    }

    for (pUniAddr = pInfo->FirstUnicastAddress;
         pUniAddr;
         pUniAddr = pUniAddr->Next) {
      PyObject *addr;
      PyObject *mask = NULL;
      PyObject *bcast = NULL;
      PIP_ADAPTER_PREFIX pPrefix;
      short family = pUniAddr->Address.lpSockaddr->sa_family;

      addr = string_from_address (pUniAddr->Address.lpSockaddr,
                                  pUniAddr->Address.iSockaddrLength);

      if (!addr)
        continue;

      /* Find the netmask, where possible */
      if (family == AF_INET) {
        struct sockaddr_in *pAddr
          = (struct sockaddr_in *)pUniAddr->Address.lpSockaddr;
        int prefix_len = -1;
        struct sockaddr_in maskAddr, bcastAddr;
        unsigned toDo;
        unsigned wholeBytes, remainingBits;
        unsigned char *pMaskBits, *pBcastBits;
        PIP_ADAPTER_PREFIX pBest = NULL;

        for (pPrefix = pInfo->FirstPrefix;
             pPrefix;
             pPrefix = pPrefix->Next) {
          struct sockaddr_in *pPrefixAddr
            = (struct sockaddr_in *)pPrefix->Address.lpSockaddr;

          if (pPrefixAddr->sin_family != AF_INET
              || (prefix_len >= 0
		  && pPrefix->PrefixLength < (unsigned)prefix_len)
	      || (prefix_len >= 0 && pPrefix->PrefixLength == 32))
            continue;

          if (compare_bits (&pPrefixAddr->sin_addr,
                            &pAddr->sin_addr,
                            pPrefix->PrefixLength) == 0) {
            prefix_len = pPrefix->PrefixLength;
            pBest = pPrefix;
          }
        }

        if (!pBest)
          continue;

        if (prefix_len < 0)
          prefix_len = 32;

        memcpy (&maskAddr,
                pBest->Address.lpSockaddr,
                sizeof (maskAddr));
        memcpy (&bcastAddr,
                pBest->Address.lpSockaddr,
                sizeof (bcastAddr));

        wholeBytes = prefix_len >> 3;
        remainingBits = prefix_len & 7;

        toDo = wholeBytes;
        pMaskBits = (unsigned char *)&maskAddr.sin_addr;

        while (toDo--)
          *pMaskBits++ = 0xff;

        toDo = 4 - wholeBytes;

        pBcastBits = (unsigned char *)&bcastAddr.sin_addr + wholeBytes;

        if (remainingBits) {
          static const unsigned char masks[] = {
            0x00, 0x80, 0xc0, 0xe0, 0xf0, 0xf8, 0xfc, 0xfe
          };
          *pMaskBits++ = masks[remainingBits];
          *pBcastBits &= masks[remainingBits];
          *pBcastBits++ |= ~masks[remainingBits];
          --toDo;
        }

        while (toDo--) {
          *pMaskBits++ = 0;
          *pBcastBits++ = 0xff;
        }

        mask = string_from_address ((SOCKADDR *)&maskAddr,
                                    sizeof (maskAddr));
        bcast = string_from_address ((SOCKADDR *)&bcastAddr,
                                     sizeof (bcastAddr));
      } else if (family == AF_INET6) {
        struct sockaddr_in6 *pAddr
          = (struct sockaddr_in6 *)pUniAddr->Address.lpSockaddr;
        int prefix_len = -1;
        struct sockaddr_in6 bcastAddr;
        unsigned toDo;
        unsigned wholeBytes, remainingBits;
        unsigned char *pBcastBits;
        PIP_ADAPTER_PREFIX pBest = NULL;

        for (pPrefix = pInfo->FirstPrefix;
             pPrefix;
             pPrefix = pPrefix->Next) {
          struct sockaddr_in6 *pPrefixAddr
            = (struct sockaddr_in6 *)pPrefix->Address.lpSockaddr;

          if (pPrefixAddr->sin6_family != AF_INET6
              || (prefix_len >= 0
		  && pPrefix->PrefixLength < (unsigned)prefix_len)
	      || (prefix_len >= 0 && pPrefix->PrefixLength == 128))
            continue;

          if (compare_bits (&pPrefixAddr->sin6_addr,
                            &pAddr->sin6_addr,
                            pPrefix->PrefixLength) == 0) {
            prefix_len = pPrefix->PrefixLength;
            pBest = pPrefix;
          }
        }

        if (!pBest)
          continue;

        if (prefix_len < 0)
          prefix_len = 128;

        memcpy (&bcastAddr,
                pBest->Address.lpSockaddr,
                sizeof (bcastAddr));

        wholeBytes = prefix_len >> 3;
        remainingBits = prefix_len & 7;

        toDo = 16 - wholeBytes;

        pBcastBits = (unsigned char *)&bcastAddr.sin6_addr + wholeBytes;

        if (remainingBits) {
          static const unsigned char masks[] = {
            0x00, 0x80, 0xc0, 0xe0, 0xf0, 0xf8, 0xfc, 0xfe
          };
          *pBcastBits &= masks[remainingBits];
          *pBcastBits++ |= ~masks[remainingBits];
          --toDo;
        }

        while (toDo--)
          *pBcastBits++ = 0xff;

        mask = netmask_from_prefix (prefix_len);
        bcast = string_from_address ((SOCKADDR *)&bcastAddr, sizeof(bcastAddr));
      }

      {
        PyObject *dict;

        dict = PyDict_New ();

        if (!dict) {
          Py_XDECREF (addr);
          Py_XDECREF (mask);
          Py_XDECREF (bcast);
          Py_DECREF (result);
          free (pAdapterAddresses);
          return NULL;
        }

        if (addr)
          PyDict_SetItemString (dict, "addr", addr);
        if (mask)
          PyDict_SetItemString (dict, "netmask", mask);
        if (bcast)
          PyDict_SetItemString (dict, "broadcast", bcast);

        Py_XDECREF (addr);
        Py_XDECREF (mask);
        Py_XDECREF (bcast);

        if (!add_to_family (result, family, dict)) {
          Py_DECREF (result);
          free ((void *)pAdapterAddresses);
          return NULL;
        }
      }
    }
  }

  free ((void *)pAdapterAddresses);
#elif HAVE_GETIFADDRS
  /* .. UNIX, with getifaddrs() ............................................. */

  if (getifaddrs (&addrs) < 0) {
    Py_DECREF (result);
    PyErr_SetFromErrno (PyExc_OSError);
    return NULL;
  }

  for (addr = addrs; addr; addr = addr->ifa_next) {
    char buffer[256];
    PyObject *pyaddr = NULL, *netmask = NULL, *braddr = NULL, *flags = NULL;

    if (addr->ifa_name == NULL || strcmp (addr->ifa_name, ifname) != 0)
      continue;
 
    /* We mark the interface as found, even if there are no addresses;
       this results in sensible behaviour for these few cases. */
    found = TRUE;

    /* Sometimes there are records without addresses (e.g. in the case of a
       dial-up connection via ppp, which on Linux can have a link address
       record with no actual address).  We skip these as they aren't useful.
       Thanks to Christian Kauhaus for reporting this issue. */
    if (!addr->ifa_addr)
      continue;
      
#if HAVE_IPV6_SOCKET_IOCTLS
    /* For IPv6 addresses we try to get the flags. */
    if (addr->ifa_addr->sa_family == AF_INET6) {
      struct sockaddr_in6 *sin;
      struct in6_ifreq ifr6;
      
      int sock6 = socket (AF_INET6, SOCK_DGRAM, 0);

      if (sock6 < 0) {
        Py_DECREF (result);
        PyErr_SetFromErrno (PyExc_OSError);
        freeifaddrs (addrs);
        return NULL;
      }
      
      sin = (struct sockaddr_in6 *)addr->ifa_addr;
      strncpy (ifr6.ifr_name, addr->ifa_name, IFNAMSIZ);
      ifr6.ifr_addr = *sin;
      
      if (ioctl (sock6, SIOCGIFAFLAG_IN6, &ifr6) >= 0) {
        flags = PyLong_FromUnsignedLong (ifr6.ifr_ifru.ifru_flags6);
      }

      close (sock6);
    }
#endif /* HAVE_IPV6_SOCKET_IOCTLS */

    if (string_from_sockaddr (addr->ifa_addr, buffer, sizeof (buffer)) == 0)
      pyaddr = PyUnicode_FromString (buffer);

    if (string_from_netmask (addr->ifa_netmask, buffer, sizeof (buffer)) == 0)
      netmask = PyUnicode_FromString (buffer);

    if (string_from_sockaddr (addr->ifa_broadaddr, buffer, sizeof (buffer)) == 0)
      braddr = PyUnicode_FromString (buffer);

    /* Cygwin's implementation of getaddrinfo() is buggy and returns broadcast
       addresses for 169.254.0.0/16.  Nix them here. */
    if (addr->ifa_addr->sa_family == AF_INET) {
      struct sockaddr_in *sin = (struct sockaddr_in *)addr->ifa_addr;

      if ((ntohl(sin->sin_addr.s_addr) & 0xffff0000) == 0xa9fe0000) {
        Py_XDECREF (braddr);
        braddr = NULL;
      }
    }

    {
      PyObject *dict = PyDict_New();

      if (!dict) {
        Py_XDECREF (pyaddr);
        Py_XDECREF (netmask);
        Py_XDECREF (braddr);
        Py_XDECREF (flags);
        Py_DECREF (result);
        freeifaddrs (addrs);
        return NULL;
      }

      if (pyaddr)
        PyDict_SetItemString (dict, "addr", pyaddr);
      if (netmask)
        PyDict_SetItemString (dict, "netmask", netmask);

      if (braddr) {
        if (addr->ifa_flags & (IFF_POINTOPOINT | IFF_LOOPBACK))
          PyDict_SetItemString (dict, "peer", braddr);
        else
          PyDict_SetItemString (dict, "broadcast", braddr);
      }
      
      if (flags)
        PyDict_SetItemString (dict, "flags", flags);

      Py_XDECREF (pyaddr);
      Py_XDECREF (netmask);
      Py_XDECREF (braddr);
      Py_XDECREF (flags);

      if (!add_to_family (result, addr->ifa_addr->sa_family, dict)) {
        Py_DECREF (result);
        freeifaddrs (addrs);
        return NULL;
      }
    }
  }

  freeifaddrs (addrs);
#elif HAVE_SOCKET_IOCTLS
  /* .. UNIX, with SIOC ioctls() ............................................ */
  
  int sock = socket(AF_INET, SOCK_DGRAM, 0);

  if (sock < 0) {
    Py_DECREF (result);
    PyErr_SetFromErrno (PyExc_OSError);
    return NULL;
  }

  struct CNAME(ifreq) ifr;
  PyObject *addr = NULL, *netmask = NULL, *braddr = NULL, *dstaddr = NULL;
  int is_p2p = FALSE;
  char buffer[256];

  strncpy (ifr.CNAME(ifr_name), ifname, IFNAMSIZ);

#if HAVE_SIOCGIFHWADDR
  if (ioctl (sock, SIOCGIFHWADDR, &ifr) == 0) {
    found = TRUE;

    if (string_from_sockaddr ((struct sockaddr *)&ifr.CNAME(ifr_addr), buffer, sizeof (buffer)) == 0) {
      PyObject *hwaddr = PyUnicode_FromString (buffer);
      PyObject *dict = PyDict_New ();

      if (!hwaddr || !dict) {
        Py_XDECREF (hwaddr);
        Py_XDECREF (dict);
        Py_XDECREF (result);
        close (sock);
        return NULL;
      }

      PyDict_SetItemString (dict, "addr", hwaddr);
      Py_DECREF (hwaddr);

      if (!add_to_family (result, AF_LINK, dict)) {
        Py_DECREF (result);
        close (sock);
        return NULL;
      }
    }
  }
#endif

#if HAVE_SIOCGIFADDR
#if HAVE_SIOCGLIFNUM
  if (ioctl (sock, SIOCGLIFADDR, &ifr) == 0) {
#else
  if (ioctl (sock, SIOCGIFADDR, &ifr) == 0) {
#endif
    found = TRUE;

    if (string_from_sockaddr ((struct sockaddr *)&ifr.CNAME(ifr_addr), buffer, sizeof (buffer)) == 0)
      addr = PyUnicode_FromString (buffer);
  }
#endif

#if HAVE_SIOCGIFNETMASK
#if HAVE_SIOCGLIFNUM
  if (ioctl (sock, SIOCGLIFNETMASK, &ifr) == 0) {
#else
  if (ioctl (sock, SIOCGIFNETMASK, &ifr) == 0) {
#endif
    found = TRUE;

    if (string_from_sockaddr ((struct sockaddr *)&ifr.CNAME(ifr_addr), buffer, sizeof (buffer)) == 0)
      netmask = PyUnicode_FromString (buffer);
  }
#endif

#if HAVE_SIOCGIFFLAGS
#if HAVE_SIOCGLIFNUM
  if (ioctl (sock, SIOCGLIFFLAGS, &ifr) == 0) {
#else
  if (ioctl (sock, SIOCGIFFLAGS, &ifr) == 0) {
#endif
    found = TRUE;

    if (ifr.CNAME(ifr_flags) & IFF_POINTOPOINT)
      is_p2p = TRUE;
  }
#endif

#if HAVE_SIOCGIFBRDADDR
#if HAVE_SIOCGLIFNUM
  if (!is_p2p && ioctl (sock, SIOCGLIFBRDADDR, &ifr) == 0) {
#else
  if (!is_p2p && ioctl (sock, SIOCGIFBRDADDR, &ifr) == 0) {
#endif
    found = TRUE;

    if (string_from_sockaddr ((struct sockaddr *)&ifr.CNAME(ifr_addr), buffer, sizeof (buffer)) == 0)
      braddr = PyUnicode_FromString (buffer);
  }
#endif

#if HAVE_SIOCGIFDSTADDR
#if HAVE_SIOCGLIFNUM
  if (is_p2p && ioctl (sock, SIOCGLIFBRDADDR, &ifr) == 0) {
#else
  if (is_p2p && ioctl (sock, SIOCGIFBRDADDR, &ifr) == 0) {
#endif
    found = TRUE;

    if (string_from_sockaddr ((struct sockaddr *)&ifr.CNAME(ifr_addr), buffer, sizeof (buffer)) == 0)
      dstaddr = PyUnicode_FromString (buffer);
  }
#endif

  {
    PyObject *dict = PyDict_New();

    if (!dict) {
      Py_XDECREF (addr);
      Py_XDECREF (netmask);
      Py_XDECREF (braddr);
      Py_XDECREF (dstaddr);
      Py_DECREF (result);
      close (sock);
      return NULL;
    }

    if (addr)
      PyDict_SetItemString (dict, "addr", addr);
    if (netmask)
      PyDict_SetItemString (dict, "netmask", netmask);
    if (braddr)
      PyDict_SetItemString (dict, "broadcast", braddr);
    if (dstaddr)
      PyDict_SetItemString (dict, "peer", dstaddr);

    Py_XDECREF (addr);
    Py_XDECREF (netmask);
    Py_XDECREF (braddr);
    Py_XDECREF (dstaddr);

    if (!add_to_family (result, AF_INET, dict)) {
      Py_DECREF (result);
      close (sock);
      return NULL;
    }
  }

  close (sock);
#endif /* HAVE_SOCKET_IOCTLS */

  if (found)
    return result;
  else {
    Py_DECREF (result);
    PyErr_SetString (PyExc_ValueError, 
                     "You must specify a valid interface name.");
    return NULL;
  }
}

/* -- interfaces() ---------------------------------------------------------- */

static PyObject *
interfaces (PyObject *self)
{
  PyObject *result;

#if defined(WIN32)
  /* .. Win32 ............................................................... */

  PIP_ADAPTER_ADDRESSES pAdapterAddresses = NULL, pInfo = NULL;
  ULONG ulBufferLength = 0;
  DWORD dwRet;

  /* First, retrieve the adapter information */
  do {
    dwRet = GetAdaptersAddresses(AF_UNSPEC, 0, NULL,
                                 pAdapterAddresses, &ulBufferLength);

    if (dwRet == ERROR_BUFFER_OVERFLOW) {
      if (pAdapterAddresses)
        free (pAdapterAddresses);
      pAdapterAddresses = (PIP_ADAPTER_ADDRESSES)malloc (ulBufferLength);

      if (!pAdapterAddresses) {
        PyErr_SetString (PyExc_MemoryError, "Not enough memory");
        return NULL;
      }
    }
  } while (dwRet == ERROR_BUFFER_OVERFLOW);

  /* If we failed, then fail in Python too */
  if (dwRet != ERROR_SUCCESS && dwRet != ERROR_NO_DATA) {
    if (pAdapterAddresses)
      free (pAdapterAddresses);

    PyErr_SetString (PyExc_OSError,
                     "Unable to obtain adapter information.");
    return NULL;
  }

  result = PyList_New(0);

  if (dwRet == ERROR_NO_DATA) {
    free (pAdapterAddresses);
    return result;
  }

  for (pInfo = pAdapterAddresses; pInfo; pInfo = pInfo->Next) {
    PyObject *ifname = (PyObject *)PyUnicode_FromString (pInfo->AdapterName);

    PyList_Append (result, ifname);
    Py_DECREF (ifname);
  }

  free (pAdapterAddresses);
#elif HAVE_GETIFADDRS
  /* .. UNIX, with getifaddrs() ............................................. */

  const char *prev_name = NULL;
  struct ifaddrs *addrs = NULL;
  struct ifaddrs *addr = NULL;

  result = PyList_New (0);

  if (getifaddrs (&addrs) < 0) {
    Py_DECREF (result);
    PyErr_SetFromErrno (PyExc_OSError);
    return NULL;
  }

  for (addr = addrs; addr; addr = addr->ifa_next) {
    if (addr->ifa_name == NULL)
      continue;

    if (!prev_name || strncmp (addr->ifa_name, prev_name, IFNAMSIZ) != 0) {
      PyObject *ifname = PyUnicode_FromString (addr->ifa_name);
    
      if (!PySequence_Contains (result, ifname))
        PyList_Append (result, ifname);
      Py_DECREF (ifname);
      prev_name = addr->ifa_name;
    }
  }

  freeifaddrs (addrs);
#elif HAVE_SIOCGIFCONF
  /* .. UNIX, with SIOC ioctl()s ............................................ */

  const char *prev_name = NULL;
  int fd = socket (AF_INET, SOCK_DGRAM, 0);
  struct CNAME(ifconf) ifc;
  int len = -1;

  if (fd < 0) {
    PyErr_SetFromErrno (PyExc_OSError);
    return NULL;
  }

  // Try to find out how much space we need
#if HAVE_SIOCGSIZIFCONF
  if (ioctl (fd, SIOCGSIZIFCONF, &len) < 0)
    len = -1;
#elif HAVE_SIOCGLIFNUM
  { struct lifnum lifn;
    lifn.lifn_family = AF_UNSPEC;
    lifn.lifn_flags = LIFC_NOXMIT | LIFC_TEMPORARY | LIFC_ALLZONES;
    ifc.lifc_family = AF_UNSPEC;
    ifc.lifc_flags = LIFC_NOXMIT | LIFC_TEMPORARY | LIFC_ALLZONES;
    if (ioctl (fd, SIOCGLIFNUM, (char *)&lifn) < 0)
      len = -1;
    else
      len = lifn.lifn_count;
  }
#endif

  // As a last resort, guess
  if (len < 0)
    len = 64;

  ifc.CNAME(ifc_len) = (int)(len * sizeof (struct CNAME(ifreq)));
  ifc.CNAME(ifc_buf) = malloc (ifc.CNAME(ifc_len));

  if (!ifc.CNAME(ifc_buf)) {
    PyErr_SetString (PyExc_MemoryError, "Not enough memory");
    close (fd);
    return NULL;
  }

#if HAVE_SIOCGLIFNUM
  if (ioctl (fd, SIOCGLIFCONF, &ifc) < 0) {
#else
  if (ioctl (fd, SIOCGIFCONF, &ifc) < 0) {
#endif
    free (ifc.CNAME(ifc_req));
    PyErr_SetFromErrno (PyExc_OSError);
    close (fd);
    return NULL;
  }

  result = PyList_New (0);
  struct CNAME(ifreq) *pfreq = ifc.CNAME(ifc_req);
  struct CNAME(ifreq) *pfreqend = (struct CNAME(ifreq) *)((char *)pfreq
                                                          + ifc.CNAME(ifc_len));
  while (pfreq < pfreqend) {
    if (!prev_name || strncmp (prev_name, pfreq->CNAME(ifr_name), IFNAMSIZ) != 0) {
      PyObject *name = PyUnicode_FromString (pfreq->CNAME(ifr_name));

      if (!PySequence_Contains (result, name))
        PyList_Append (result, name);
      Py_XDECREF (name);

      prev_name = pfreq->CNAME(ifr_name);
    }

#if !HAVE_SOCKADDR_SA_LEN
    ++pfreq;
#else
    /* On some platforms, the ifreq struct can *grow*(!) if the socket address
       is very long.  Mac OS X is such a platform. */
    {
      size_t len = sizeof (struct CNAME(ifreq));
      if (pfreq->ifr_addr.sa_len > sizeof (struct sockaddr))
        len = len - sizeof (struct sockaddr) + pfreq->ifr_addr.sa_len;
        pfreq = (struct CNAME(ifreq) *)((char *)pfreq + len);
    }
#endif
  }

  free (ifc.CNAME(ifc_buf));
  close (fd);
#endif /* HAVE_SIOCGIFCONF */

  return result;
}

/* -- gateways() ------------------------------------------------------------ */

static PyObject *
gateways (PyObject *self)
{
  PyObject *result, *defaults;

#if defined(WIN32)
  /* .. Win32 ............................................................... */

  /* We try to access GetIPForwardTable2() and FreeMibTable() through
     function pointers so that this code will still run on machines
     running Windows versions prior to Vista.  On those systems, we
     fall back to the older GetIPForwardTable() API (and can only find
     IPv4 gateways as a result).

     We also fall back to the older API if the newer code fails for
     some reason. */

  HANDLE hIpHelper;
  typedef NTSTATUS (WINAPI *PGETIPFORWARDTABLE2)(ADDRESS_FAMILY Family,
						 PMIB_IPFORWARD_TABLE2 *pTable);
  typedef VOID (WINAPI *PFREEMIBTABLE)(PVOID Memory);

  PGETIPFORWARDTABLE2 pGetIpForwardTable2;
  PFREEMIBTABLE pFreeMibTable;

  hIpHelper = GetModuleHandle (TEXT("iphlpapi.dll"));

  result = NULL;
  pGetIpForwardTable2 = (PGETIPFORWARDTABLE2)
    GetProcAddress (hIpHelper, "GetIpForwardTable2");
  pFreeMibTable = (PFREEMIBTABLE)
    GetProcAddress (hIpHelper, "FreeMibTable");

  if (pGetIpForwardTable2) {
    PMIB_IPFORWARD_TABLE2 table;
    DWORD dwErr = pGetIpForwardTable2 (AF_UNSPEC, &table);

    if (dwErr == NO_ERROR) {
      DWORD n;
      ULONG lBestInetMetric = ~(ULONG)0, lBestInet6Metric = ~(ULONG)0;

      result = PyDict_New();
      defaults = PyDict_New();
      PyDict_SetItemString (result, "default", defaults);
      Py_DECREF(defaults);

      /* This prevents a crash on PyPy */
      defaults = PyDict_GetItemString (result, "default");

      for (n = 0; n < table->NumEntries; ++n) {
	MIB_IFROW ifRow;
	PyObject *ifname;
	PyObject *gateway;
	PyObject *isdefault;
	PyObject *tuple, *deftuple = NULL;
	WCHAR *pwcsName;
	DWORD dwFamily = table->Table[n].NextHop.si_family;
	BOOL bBest = FALSE;

	if (table->Table[n].DestinationPrefix.PrefixLength)
	  continue;

	switch (dwFamily) {
	case AF_INET:
	  if (!table->Table[n].NextHop.Ipv4.sin_addr.s_addr)
	    continue;
	  break;
	case AF_INET6:
	  if (memcmp (&table->Table[n].NextHop.Ipv6.sin6_addr,
		      &in6addr_any,
		      sizeof (struct in6_addr)) == 0)
	    continue;
	  break;
	default:
	  continue;
	}

	memset (&ifRow, 0, sizeof (ifRow));
	ifRow.dwIndex = table->Table[n].InterfaceIndex;
	if (GetIfEntry (&ifRow) != NO_ERROR)
	  continue;

        gateway = string_from_address ((SOCKADDR *)&table->Table[n].NextHop,
                                       sizeof (table->Table[n].NextHop));

        if (!gateway)
          continue;

	/* Strip the prefix from the interface name */
	pwcsName = ifRow.wszName;
	if (_wcsnicmp (L"\\DEVICE\\TCPIP_", pwcsName, 14) == 0)
	  pwcsName += 14;

	switch (dwFamily) {
	case AF_INET:
          bBest = table->Table[n].Metric < lBestInetMetric;
          lBestInetMetric = table->Table[n].Metric;
	  break;
	case AF_INET6:
          bBest = table->Table[n].Metric < lBestInet6Metric;
          lBestInet6Metric = table->Table[n].Metric;
	  break;
	}

	ifname = PyUnicode_FromWideChar (pwcsName, wcslen (pwcsName));
	isdefault = bBest ? Py_True : Py_False;

	tuple = PyTuple_Pack (3, gateway, ifname, isdefault);

	if (PyObject_IsTrue (isdefault))
	  deftuple = PyTuple_Pack (2, gateway, ifname);

	Py_DECREF (gateway);
	Py_DECREF (ifname);

	if (tuple && !add_to_family (result, dwFamily, tuple)) {
	  Py_DECREF (deftuple);
	  Py_DECREF (result);
	  free (table);
	  return NULL;
	}

	if (deftuple) {
	  PyObject *pyfamily = PyInt_FromLong (dwFamily);

	  PyDict_SetItem (defaults, pyfamily, deftuple);

	  Py_DECREF (pyfamily);
	  Py_DECREF (deftuple);
	}
      }

      pFreeMibTable (table);
    }
  }

  if (!result) {
    PMIB_IPFORWARDTABLE table = NULL;
    DWORD dwRet;
    DWORD dwSize = 0;
    DWORD n;
    DWORD dwBestMetric = ~(DWORD)0;

    do {
      dwRet = GetIpForwardTable (table, &dwSize, FALSE);

      if (dwRet == ERROR_INSUFFICIENT_BUFFER) {
        PMIB_IPFORWARDTABLE tbl = (PMIB_IPFORWARDTABLE)realloc (table, dwSize);

        if (!tbl) {
          free (table);
          PyErr_NoMemory();
          return NULL;
        }

	table = tbl;
      }
    } while (dwRet == ERROR_INSUFFICIENT_BUFFER);

    if (dwRet != NO_ERROR) {
      free (table);
      PyErr_SetFromWindowsErr (dwRet);
      return NULL;
    }

    result = PyDict_New();
    defaults = PyDict_New();
    PyDict_SetItemString (result, "default", defaults);
    Py_DECREF(defaults);

    /* This prevents a crash on PyPy */
    defaults = PyDict_GetItemString (result, "default");

    for (n = 0; n < table->dwNumEntries; ++n) {
      MIB_IFROW ifRow;
      PyObject *ifname;
      PyObject *gateway;
      PyObject *isdefault;
      PyObject *tuple, *deftuple = NULL;
      DWORD dwGateway;
      char gwbuf[16];
      WCHAR *pwcsName;
      BOOL bBest;

      if (table->table[n].dwForwardDest
          || !table->table[n].dwForwardNextHop
          || table->table[n].dwForwardType != MIB_IPROUTE_TYPE_INDIRECT)
        continue;

      memset (&ifRow, 0, sizeof (ifRow));
      ifRow.dwIndex = table->table[n].dwForwardIfIndex;
      if (GetIfEntry (&ifRow) != NO_ERROR)
        continue;

      dwGateway = ntohl (table->table[n].dwForwardNextHop);

      sprintf (gwbuf, "%u.%u.%u.%u",
	       (dwGateway >> 24) & 0xff,
	       (dwGateway >> 16) & 0xff,
	       (dwGateway >> 8) & 0xff,
	       dwGateway & 0xff);

      /* Strip the prefix from the interface name */
      pwcsName = ifRow.wszName;
      if (_wcsnicmp (L"\\DEVICE\\TCPIP_", pwcsName, 14) == 0)
	pwcsName += 14;

      bBest = table->table[n].dwForwardMetric1 < dwBestMetric;
      if (bBest)
        dwBestMetric = table->table[n].dwForwardMetric1;

      ifname = PyUnicode_FromWideChar (pwcsName, wcslen (pwcsName));
      gateway = PyUnicode_FromString (gwbuf);
      isdefault = bBest ? Py_True : Py_False;

      tuple = PyTuple_Pack (3, gateway, ifname, isdefault);

      if (PyObject_IsTrue (isdefault))
        deftuple = PyTuple_Pack (2, gateway, ifname);

      Py_DECREF (gateway);
      Py_DECREF (ifname);

      if (tuple && !add_to_family (result, AF_INET, tuple)) {
        Py_DECREF (deftuple);
        Py_DECREF (result);
        free (table);
        return NULL;
      }

      if (deftuple) {
        PyObject *pyfamily = PyInt_FromLong (AF_INET);

        PyDict_SetItem (defaults, pyfamily, deftuple);

        Py_DECREF (pyfamily);
        Py_DECREF (deftuple);
      }
    }
  }
#elif defined(HAVE_PF_NETLINK)
  /* .. Linux (PF_NETLINK socket) ........................................... */

  /* PF_NETLINK is pretty poorly documented and it looks to be quite easy to
     get wrong.  This *appears* to be the right way to do it, even though a
     lot of the code out there on the 'Net is very different! */

  struct routing_msg {
    struct nlmsghdr hdr;
    struct rtmsg    rt;
    char            data[0];
  } *pmsg, *msgbuf;
  int s;
  int seq = 0;
  ssize_t ret;
  struct sockaddr_nl sanl;
  static const struct sockaddr_nl sanl_kernel = { .nl_family = AF_NETLINK };
  socklen_t sanl_len;
  int pagesize = getpagesize();
  int bufsize = pagesize < 8192 ? pagesize : 8192;
  int is_multi = 0;
  int interrupted = 0;
  int def_priorities[RTNL_FAMILY_MAX];

  memset(def_priorities, 0xff, sizeof(def_priorities));

  result = PyDict_New();
  defaults = PyDict_New();
  PyDict_SetItemString (result, "default", defaults);
  Py_DECREF (defaults);

  /* This prevents a crash on PyPy */
  defaults = PyDict_GetItemString (result, "default");

  msgbuf = (struct routing_msg *)malloc (bufsize);

  if (!msgbuf) {
    PyErr_NoMemory ();
    Py_DECREF (result);
    return NULL;
  }

  s = socket (PF_NETLINK, SOCK_RAW, NETLINK_ROUTE);

  if (s < 0) {
    PyErr_SetFromErrno (PyExc_OSError);
    Py_DECREF (result);
    free (msgbuf);
    return NULL;
  }

  sanl.nl_family = AF_NETLINK;
  sanl.nl_groups = 0;
  sanl.nl_pid = 0;

  if (bind (s, (struct sockaddr *)&sanl, sizeof (sanl)) < 0) {
    PyErr_SetFromErrno (PyExc_OSError);
    Py_DECREF (result);
    free (msgbuf);
    close (s);
    return NULL;
  }

  sanl_len = sizeof (sanl);
  if (getsockname (s, (struct sockaddr *)&sanl, &sanl_len) < 0) {
    PyErr_SetFromErrno  (PyExc_OSError);
    Py_DECREF (result);
    free (msgbuf);
    close (s);
    return NULL;
  }

  do {
    interrupted = 0;

    pmsg = msgbuf;
    memset (pmsg, 0, sizeof (struct routing_msg));
    pmsg->hdr.nlmsg_len = NLMSG_LENGTH(sizeof(struct rtmsg));
    pmsg->hdr.nlmsg_flags = NLM_F_DUMP | NLM_F_REQUEST;
    pmsg->hdr.nlmsg_seq = ++seq;
    pmsg->hdr.nlmsg_type = RTM_GETROUTE;
    pmsg->hdr.nlmsg_pid = 0;

    pmsg->rt.rtm_family = 0;

    if (sendto (s, pmsg, pmsg->hdr.nlmsg_len, 0,
                (struct sockaddr *)&sanl_kernel, sizeof(sanl_kernel)) < 0) {
      PyErr_SetFromErrno  (PyExc_OSError);
      Py_DECREF (result);
      free (msgbuf);
      close (s);
      return NULL;
    }

    do {
      struct sockaddr_nl sanl_from;
      struct iovec iov = { msgbuf, bufsize };
      struct msghdr msghdr = {
        &sanl_from,
        sizeof(sanl_from),
        &iov,
        1,
        NULL,
        0,
        0
      };
      int nllen;

      ret = recvmsg (s, &msghdr, 0);

      if (msghdr.msg_flags & MSG_TRUNC) {
        PyErr_SetString (PyExc_OSError, "netlink message truncated");
        Py_DECREF (result);
        free (msgbuf);
        close (s);
        return NULL;
      }

      if (ret < 0) {
        PyErr_SetFromErrno (PyExc_OSError);
        Py_DECREF (result);
        free (msgbuf);
        close (s);
        return NULL;
      }

      nllen = ret;
      pmsg = msgbuf;
      while (NLMSG_OK (&pmsg->hdr, nllen)) {
        void *dst = NULL;
        void *gw = NULL;
        int ifndx = -1;
        struct rtattr *attrs, *attr;
        int len;
        int priority;

        /* Ignore messages not for us */
        if (pmsg->hdr.nlmsg_seq != seq || pmsg->hdr.nlmsg_pid != sanl.nl_pid)
          goto next;

        /* This is only defined on Linux kernel versions 3.1 and higher */
#ifdef NLM_F_DUMP_INTR
        if (pmsg->hdr.nlmsg_flags & NLM_F_DUMP_INTR) {
          /* The dump was interrupted by a signal; we need to go round again */
          interrupted = 1;
          is_multi = 0;
          break;
        }
#endif

        is_multi = pmsg->hdr.nlmsg_flags & NLM_F_MULTI;

        if (pmsg->hdr.nlmsg_type == NLMSG_DONE) {
          is_multi = interrupted = 0;
          break;
        }

        if (pmsg->hdr.nlmsg_type == NLMSG_ERROR) {
          struct nlmsgerr *perr = (struct nlmsgerr *)&pmsg->rt;
          errno = -perr->error;
          PyErr_SetFromErrno (PyExc_OSError);
          Py_DECREF (result);
          free (msgbuf);
          close (s);
          return NULL;
        }

        attr = attrs = RTM_RTA(&pmsg->rt);
        len = RTM_PAYLOAD(&pmsg->hdr);
        priority = -1;
        while (RTA_OK(attr, len)) {
          switch (attr->rta_type) {
          case RTA_GATEWAY:
            gw = RTA_DATA(attr);
            break;
          case RTA_DST:
            dst = RTA_DATA(attr);
            break;
          case RTA_OIF:
            ifndx = *(int *)RTA_DATA(attr);
            break;
          case RTA_PRIORITY:
            priority = *(int *)RTA_DATA(attr);
            break;
          default:
            break;
          }

          attr = RTA_NEXT(attr, len);
        }

        static const unsigned char ipv4_default[4] = {};
        static const unsigned char ipv6_default[16] = {};

        /* We're looking for gateways with no destination */
        if ((!dst
            || (pmsg->rt.rtm_family == AF_INET && !memcmp(dst, ipv4_default, sizeof(ipv4_default)))
            || (pmsg->rt.rtm_family == AF_INET6 && !memcmp(dst, ipv6_default, sizeof(ipv6_default)))
        ) && gw && ifndx >= 0) {
          char buffer[256];
          char ifnamebuf[IF_NAMESIZE];
          char *ifname;
          const char *addr;
          PyObject *pyifname;
          PyObject *pyaddr;
          PyObject *isdefault;
          PyObject *tuple = NULL, *deftuple = NULL;

          ifname = if_indextoname (ifndx, ifnamebuf);

          if (!ifname)
            goto next;

          addr = inet_ntop (pmsg->rt.rtm_family, gw, buffer, sizeof (buffer));

          if (!addr)
            goto next;

          /* We set isdefault to True if this route came from the main table;
             this should correspond with the way most people set up alternate
             routing tables on Linux. */

          isdefault = pmsg->rt.rtm_table == RT_TABLE_MAIN ? Py_True : Py_False;

          /* Priority starts at 0, having none means we use kernel default (0) */
          if (priority < 0) {
            priority = 0;
          }

          /* Try to pick the active default route based on priority (which
             is displayed in the UI as "metric", confusingly) */
          if (pmsg->rt.rtm_family < RTNL_FAMILY_MAX) {
            /* If no active default route found, or metric is lower */
            if (def_priorities[pmsg->rt.rtm_family] == -1
                || priority < def_priorities[pmsg->rt.rtm_family])
              /* Set new default */
              def_priorities[pmsg->rt.rtm_family] = priority;
            else
              /* Leave default, but unset isdefault for iface tuple */
              isdefault = Py_False;
          }

          pyifname = PyUnicode_FromString (ifname);
          pyaddr = PyUnicode_FromString (buffer);

          tuple = PyTuple_Pack (3, pyaddr, pyifname, isdefault);

          if (PyObject_IsTrue (isdefault))
            deftuple = PyTuple_Pack (2, pyaddr, pyifname);

          Py_DECREF (pyaddr);
          Py_DECREF (pyifname);

          if (tuple && !add_to_family (result, pmsg->rt.rtm_family, tuple)) {
            Py_XDECREF (deftuple);
            Py_DECREF (result);
            free (msgbuf);
            close (s);
            return NULL;
          }

          if (deftuple) {
            PyObject *pyfamily = PyInt_FromLong (pmsg->rt.rtm_family);

            PyDict_SetItem (defaults, pyfamily, deftuple);

            Py_DECREF (pyfamily);
            Py_DECREF (deftuple);
          }
        }

      next:
	pmsg = (struct routing_msg *)NLMSG_NEXT(&pmsg->hdr, nllen);
      }
    } while (is_multi);
  } while (interrupted);

  free (msgbuf);
  close (s);
#elif defined(HAVE_SYSCTL_CTL_NET)
  /* .. UNIX, via sysctl() .................................................. */

  int mib[] = { CTL_NET, PF_ROUTE, 0, 0, NET_RT_FLAGS,
                RTF_UP | RTF_GATEWAY };
  size_t len;
  char *buffer = NULL, *ptr, *end;
  int ret;
  char ifnamebuf[IF_NAMESIZE];
  char *ifname;

  result = PyDict_New();
  defaults = PyDict_New();
  PyDict_SetItemString (result, "default", defaults);
  Py_DECREF (defaults);

  /* This prevents a crash on PyPy */
  defaults = PyDict_GetItemString (result, "default");

  /* Remembering that the routing table may change while we're reading it,
     we need to do this in a loop until we succeed. */
  do {
    if (sysctl (mib, 6, 0, &len, 0, 0) < 0) {
      PyErr_SetFromErrno (PyExc_OSError);
      free (buffer);
      Py_DECREF (result);
      return NULL;
    }

    ptr = realloc(buffer, len);
    if (!ptr) {
      PyErr_NoMemory();
      free (buffer);
      Py_DECREF (result);
      return NULL;
    }

    buffer = ptr;

    ret = sysctl (mib, 6, buffer, &len, 0, 0);
  } while (ret != 0 && (errno == ENOMEM || errno == EINTR));

  if (ret < 0) {
    PyErr_SetFromErrno (PyExc_OSError);
    free (buffer);
    Py_DECREF (result);
    return NULL;
  }

  ptr = buffer;
  end = buffer + len;

  while (ptr + sizeof (struct rt_msghdr) <= end) {
    struct rt_msghdr *msg = (struct rt_msghdr *)ptr;
    char *msgend = (char *)msg + msg->rtm_msglen;
    int addrs = msg->rtm_addrs;
    int addr = RTA_DST;
    PyObject *pyifname;

    if (msgend > end)
      break;

    ifname = if_indextoname (msg->rtm_index, ifnamebuf);

    if (!ifname) {
      ptr = msgend;
      continue;
    }

    pyifname = PyUnicode_FromString (ifname);

    ptr = (char *)(msg + 1);
    while (ptr + sizeof (struct sockaddr) <= msgend && addrs) {
      struct sockaddr *sa = (struct sockaddr *)ptr;
      int len = SA_LEN(sa);

      if (!len)
        len = 4;
      else
        len = (len + 3) & ~3;

      if (ptr + len > msgend)
        break;

      while (!(addrs & addr))
        addr <<= 1;

      addrs &= ~addr;

      if (addr == RTA_DST) {
        if (sa->sa_family == AF_INET) {
          struct sockaddr_in *sin = (struct sockaddr_in *)sa;
          if (sin->sin_addr.s_addr != INADDR_ANY)
            break;
#ifdef AF_INET6
        } else if (sa->sa_family == AF_INET6) {
          struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)sa;
          if (memcmp (&sin6->sin6_addr, &in6addr_any, sizeof (in6addr_any)) != 0)
            break;
#endif
        } else {
          break;
        }
      }

      if (addr == RTA_GATEWAY) {
        char strbuf[256];
        PyObject *tuple = NULL;
        PyObject *deftuple = NULL;

        if (string_from_sockaddr (sa, strbuf, sizeof(strbuf)) == 0) {
          PyObject *pyaddr = PyUnicode_FromString (strbuf);
#ifdef RTF_IFSCOPE
          PyObject *isdefault = PyBool_FromLong (!(msg->rtm_flags & RTF_IFSCOPE));
#else
          Py_INCREF(Py_True);
          PyObject *isdefault = Py_True;
#endif
          tuple = PyTuple_Pack (3, pyaddr, pyifname, isdefault);

          if (PyObject_IsTrue (isdefault))
            deftuple = PyTuple_Pack (2, pyaddr, pyifname);

          Py_DECREF (pyaddr);
          Py_DECREF (isdefault);
        }

        if (tuple && !add_to_family (result, sa->sa_family, tuple)) {
          Py_DECREF (deftuple);
          Py_DECREF (result);
          Py_DECREF (pyifname);
          free (buffer);
          return NULL;
        }

        if (deftuple) {
          PyObject *pyfamily = PyInt_FromLong (sa->sa_family);

          PyDict_SetItem (defaults, pyfamily, deftuple);

          Py_DECREF (pyfamily);
          Py_DECREF (deftuple);
        }
      }

      /* These are aligned on a 4-byte boundary */
      ptr += len;
    }

    Py_DECREF (pyifname);
    ptr = msgend;
  }

  free (buffer);
#elif defined(HAVE_PF_ROUTE)
  /* .. UNIX, via PF_ROUTE socket ........................................... */

  /* The PF_ROUTE code will only retrieve gateway information for AF_INET and
     AF_INET6.  This is because it would need to loop through all possible
     values, and the messages it needs to send in each case are potentially
     different.  It is also very likely to return a maximum of one gateway
     in each case (since we can't read the entire routing table this way, we
     can only ask about routes). */

  int pagesize = getpagesize();
  int bufsize = pagesize < 8192 ? 8192 : pagesize;
  struct rt_msghdr *pmsg;
  int s;
  int seq = 0;
  int pid = getpid();
  ssize_t ret;
  struct sockaddr_in *sin_dst, *sin_netmask;
  struct sockaddr_dl *sdl_ifp;
  struct sockaddr_in6 *sin6_dst;
  size_t msglen;
  char ifnamebuf[IF_NAMESIZE];
  char *ifname;
  int skip;

  result = PyDict_New();
  defaults = PyDict_New();
  PyDict_SetItemString (result, "default", defaults);
  Py_DECREF(defaults);

  pmsg = (struct rt_msghdr *)malloc (bufsize);

  if (!pmsg) {
    PyErr_NoMemory();
    return NULL;
  }

  s = socket (PF_ROUTE, SOCK_RAW, 0);

  if (s < 0) {
    PyErr_SetFromErrno (PyExc_OSError);
    free (pmsg);
    return NULL;
  }

  msglen = (sizeof (struct rt_msghdr)
            + 2 * sizeof (struct sockaddr_in) 
            + sizeof (struct sockaddr_dl));
  memset (pmsg, 0, msglen);
  
  /* AF_INET first */
  pmsg->rtm_msglen = msglen;
  pmsg->rtm_type = RTM_GET;
  pmsg->rtm_index = 0;
  pmsg->rtm_flags = RTF_UP | RTF_GATEWAY;
  pmsg->rtm_version = RTM_VERSION;
  pmsg->rtm_seq = ++seq;
  pmsg->rtm_pid = 0;
  pmsg->rtm_addrs = RTA_DST | RTA_NETMASK | RTA_IFP;

  sin_dst = (struct sockaddr_in *)(pmsg + 1);
  sin_netmask = (struct sockaddr_in *)(sin_dst + 1);
  sdl_ifp = (struct sockaddr_dl *)(sin_netmask + 1);

  sin_dst->sin_family = AF_INET;
  sin_netmask->sin_family = AF_INET;
  sdl_ifp->sdl_family = AF_LINK;

#if HAVE_SOCKADDR_SA_LEN
  sin_dst->sin_len = sizeof (struct sockaddr_in);
  sin_netmask->sin_len = sizeof (struct sockaddr_in);
  sdl_ifp->sdl_len = sizeof (struct sockaddr_dl);
#endif

  skip = 0;
  if (send (s, pmsg, msglen, 0) < 0) {
    if (errno == ESRCH)
      skip = 1;
    else {
      PyErr_SetFromErrno (PyExc_OSError);
      close (s);
      free (pmsg);
      return NULL;
    }
  }

  while (!skip && !(pmsg->rtm_flags & RTF_DONE)) {
    char *ptr;
    char *msgend;
    int addrs;
    int addr;
    struct sockaddr_in *dst = NULL;
    struct sockaddr_in *gw = NULL;
    struct sockaddr_dl *ifp = NULL;
    PyObject *tuple = NULL;
    PyObject *deftuple = NULL;

    do {
      ret = recv (s, pmsg, bufsize, 0);
    } while ((ret < 0 && errno == EINTR)
             || (ret > 0 && (pmsg->rtm_seq != seq || pmsg->rtm_pid != pid)));

    if (ret < 0) {
      PyErr_SetFromErrno (PyExc_OSError);
      close (s);
      free (pmsg);
      return NULL;
    }

    if (pmsg->rtm_errno != 0) {
      if (pmsg->rtm_errno == ESRCH)
        skip = 1;
      else {
        errno = pmsg->rtm_errno;
        PyErr_SetFromErrno (PyExc_OSError);
        close (s);
        free (pmsg);
        return NULL;
      }
    }

    if (skip)
      break;

    ptr = (char *)(pmsg + 1);
    msgend = (char *)pmsg + pmsg->rtm_msglen;
    addrs = pmsg->rtm_addrs;
    addr = RTA_DST;
    while (ptr + sizeof (struct sockaddr) <= msgend && addrs) {
      struct sockaddr *sa = (struct sockaddr *)ptr;
      int len = SA_LEN(sa);

      if (!len)
        len = 4;
      else
        len = (len + 3) & ~3;

      if (ptr + len > msgend)
        break;

      while (!(addrs & addr))
        addr <<= 1;

      addrs &= ~addr;

      switch (addr) {
      case RTA_DST:
        dst = (struct sockaddr_in *)sa;
        break;
      case RTA_GATEWAY:
        gw = (struct sockaddr_in *)sa;
        break;
      case RTA_IFP:
        ifp = (struct sockaddr_dl *)sa;
        break;
      }

      ptr += len;
    }

    if ((dst && dst->sin_family != AF_INET)
        || (gw && gw->sin_family != AF_INET)
        || (ifp && ifp->sdl_family != AF_LINK)) {
      dst = gw = NULL;
      ifp = NULL;
    }

    if (dst && dst->sin_addr.s_addr == INADDR_ANY)
        dst = NULL;

    if (!dst && gw && ifp) {
      char buffer[256];

      if (ifp->sdl_index)
        ifname = if_indextoname (ifp->sdl_index, ifnamebuf);
      else {
        memcpy (ifnamebuf, ifp->sdl_data, ifp->sdl_nlen);
        ifnamebuf[ifp->sdl_nlen] = '\0';
        ifname = ifnamebuf;
      }

      if (string_from_sockaddr ((struct sockaddr *)gw,
                                buffer, sizeof(buffer)) == 0) {
        PyObject *pyifname = PyUnicode_FromString (ifname);
        PyObject *pyaddr = PyUnicode_FromString (buffer);
#ifdef RTF_IFSCOPE
        PyObject *isdefault = PyBool_FromLong (!(pmsg->rtm_flags & RTF_IFSCOPE));
#else
        PyObject *isdefault = Py_True;
	Py_INCREF(isdefault);
#endif

        tuple = PyTuple_Pack (3, pyaddr, pyifname, isdefault);

        if (PyObject_IsTrue (isdefault))
          deftuple = PyTuple_Pack (2, pyaddr, pyifname);

        Py_DECREF (pyaddr);
        Py_DECREF (pyifname);
        Py_DECREF (isdefault);
      }

      if (tuple && !add_to_family (result, AF_INET, tuple)) {
        Py_DECREF (deftuple);
        Py_DECREF (result);
        free (pmsg);
        return NULL;
      }

      if (deftuple) {
        PyObject *pyfamily = PyInt_FromLong (AF_INET);

        PyDict_SetItem (defaults, pyfamily, deftuple);

        Py_DECREF (pyfamily);
        Py_DECREF (deftuple);
      }
    }
  }

  /* The code below is very similar to, but not identical to, the code above.
     We could probably refactor some of it, but take care---there are subtle
     differences! */

#ifdef AF_INET6
  /* AF_INET6 now */
  msglen = (sizeof (struct rt_msghdr)
            + sizeof (struct sockaddr_in6)
            + sizeof (struct sockaddr_dl));
  memset (pmsg, 0, msglen);

  pmsg->rtm_msglen = msglen;
  pmsg->rtm_type = RTM_GET;
  pmsg->rtm_index = 0;
  pmsg->rtm_flags = RTF_UP | RTF_GATEWAY;
  pmsg->rtm_version = RTM_VERSION;
  pmsg->rtm_seq = ++seq;
  pmsg->rtm_pid = 0;
  pmsg->rtm_addrs = RTA_DST | RTA_IFP;

  sin6_dst = (struct sockaddr_in6 *)(pmsg + 1);
  sdl_ifp = (struct sockaddr_dl *)(sin6_dst + 1);

  sin6_dst->sin6_family = AF_INET6;
  sin6_dst->sin6_addr = in6addr_any;
  sdl_ifp->sdl_family = AF_LINK;

#if HAVE_SOCKADDR_SA_LEN
  sin6_dst->sin6_len = sizeof (struct sockaddr_in6);
  sdl_ifp->sdl_len = sizeof (struct sockaddr_dl);
#endif

  skip = 0;
  if (send (s, pmsg, msglen, 0) < 0) {
    if (errno == ESRCH)
      skip = 1;
    else {
      PyErr_SetFromErrno (PyExc_OSError);
      close (s);
      free (pmsg);
      return NULL;
    }
  }

  while (!skip && !(pmsg->rtm_flags & RTF_DONE)) {
    char *ptr;
    char *msgend;
    int addrs;
    int addr;
    struct sockaddr_in6 *dst = NULL;
    struct sockaddr_in6 *gw = NULL;
    struct sockaddr_dl *ifp = NULL;
    PyObject *tuple = NULL;
    PyObject *deftuple = NULL;

    do {
      ret = recv (s, pmsg, bufsize, 0);
    } while ((ret < 0 && errno == EINTR)
             || (ret > 0 && (pmsg->rtm_seq != seq || pmsg->rtm_pid != pid)));

    if (ret < 0) {
      PyErr_SetFromErrno (PyExc_OSError);
      close (s);
      free (pmsg);
      return NULL;
    }

    if (pmsg->rtm_errno != 0) {
      if (pmsg->rtm_errno == ESRCH)
        skip = 1;
      else {
        errno = pmsg->rtm_errno;
        PyErr_SetFromErrno (PyExc_OSError);
        close (s);
        free (pmsg);
        return NULL;
      }
    }

    if (skip)
      break;

    ptr = (char *)(pmsg + 1);
    msgend = (char *)pmsg + pmsg->rtm_msglen;
    addrs = pmsg->rtm_addrs;
    addr = RTA_DST;
    while (ptr + sizeof (struct sockaddr) <= msgend && addrs) {
      struct sockaddr *sa = (struct sockaddr *)ptr;
      int len = SA_LEN(sa);

      if (!len)
        len = 4;
      else
        len = (len + 3) & ~3;

      if (ptr + len > msgend)
        break;

      while (!(addrs & addr))
        addr <<= 1;

      addrs &= ~addr;

      switch (addr) {
      case RTA_DST:
        dst = (struct sockaddr_in6 *)sa;
        break;
      case RTA_GATEWAY:
        gw = (struct sockaddr_in6 *)sa;
        break;
      case RTA_IFP:
        ifp = (struct sockaddr_dl *)sa;
        break;
      }

      ptr += len;
    }

    if ((dst && dst->sin6_family != AF_INET6)
        || (gw && gw->sin6_family != AF_INET6)
        || (ifp && ifp->sdl_family != AF_LINK)) {
      dst = gw = NULL;
      ifp = NULL;
    }

    if (dst && memcmp (&dst->sin6_addr, &in6addr_any,
                       sizeof(struct in6_addr)) == 0)
        dst = NULL;

    if (!dst && gw && ifp) {
      char buffer[256];

      if (ifp->sdl_index)
        ifname = if_indextoname (ifp->sdl_index, ifnamebuf);
      else {
        memcpy (ifnamebuf, ifp->sdl_data, ifp->sdl_nlen);
        ifnamebuf[ifp->sdl_nlen] = '\0';
        ifname = ifnamebuf;
      }

      if (string_from_sockaddr ((struct sockaddr *)gw,
                                buffer, sizeof(buffer)) == 0) {
        PyObject *pyifname = PyUnicode_FromString (ifname);
        PyObject *pyaddr = PyUnicode_FromString (buffer);
#ifdef RTF_IFSCOPE
        PyObject *isdefault = PyBool_FromLong (!(pmsg->rtm_flags & RTF_IFSCOPE));
#else
        PyObject *isdefault = Py_True;
	Py_INCREF (isdefault);
#endif

        tuple = PyTuple_Pack (3, pyaddr, pyifname, isdefault);

        if (PyObject_IsTrue (isdefault))
          deftuple = PyTuple_Pack (2, pyaddr, pyifname);

        Py_DECREF (pyaddr);
        Py_DECREF (pyifname);
        Py_DECREF (isdefault);
      }

      if (tuple && !add_to_family (result, AF_INET6, tuple)) {
        Py_DECREF (deftuple);
        Py_DECREF (result);
        free (pmsg);
        return NULL;
      }

      if (deftuple) {
        PyObject *pyfamily = PyInt_FromLong (AF_INET6);

        PyDict_SetItem (defaults, pyfamily, deftuple);

        Py_DECREF (pyfamily);
        Py_DECREF (deftuple);
      }
    }
  }
#endif /* AF_INET6 */

  free (pmsg);
#else
  /* If we don't know how to implement this on your platform, we raise an
     exception. */
  PyErr_SetString (PyExc_OSError,
                   "Unable to obtain gateway information on your platform.");
#endif

  return result;
}

/* -- Python Module --------------------------------------------------------- */

static PyMethodDef methods[] = {
  { "ifaddresses", (PyCFunction)ifaddrs, METH_VARARGS,
    "Obtain information about the specified network interface.\n"
"\n"
"Returns a dict whose keys are equal to the address family constants,\n"
"e.g. netifaces.AF_INET, and whose values are a list of addresses in\n"
"that family that are attached to the network interface." },
  { "interfaces", (PyCFunction)interfaces, METH_NOARGS,
    "Obtain a list of the interfaces available on this machine." },
  { "gateways", (PyCFunction)gateways, METH_NOARGS,
    "Obtain a list of the gateways on this machine.\n"
"\n"
"Returns a dict whose keys are equal to the address family constants,\n"
"e.g. netifaces.AF_INET, and whose values are a list of tuples of the\n"
"format (<address>, <interface>, <is_default>).\n"
"\n"
"There is also a special entry with the key 'default', which you can use\n"
"to quickly obtain the default gateway for a particular address family.\n"
"\n"
"There may in general be multiple gateways; different address\n"
"families may have different gateway settings (e.g. AF_INET vs AF_INET6)\n"
"and on some systems it's also possible to have interface-specific\n"
"default gateways.\n" },
  { NULL, NULL, 0, NULL }
};

MODULE_DEF("netifaces", NULL, methods);

MODULE_INIT(netifaces)
{
  PyObject *address_family_dict;
  PyObject *m;

#ifdef WIN32
  WSADATA wsad;
  
  WSAStartup(MAKEWORD (2, 2), &wsad);
#endif

  MODULE_CREATE(m, "netifaces", NULL, methods);
  if (!m)
    return MODULE_ERROR;

  /* Address families (auto-detect using #ifdef) */
  address_family_dict = PyDict_New();
#ifdef AF_UNSPEC  
  PyModule_AddIntConstant (m, "AF_UNSPEC", AF_UNSPEC);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_UNSPEC),
          PyUnicode_FromString("AF_UNSPEC"));
#endif
#ifdef AF_UNIX
  PyModule_AddIntConstant (m, "AF_UNIX", AF_UNIX);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_UNIX),
          PyUnicode_FromString("AF_UNIX"));
#endif
#ifdef AF_FILE
  PyModule_AddIntConstant (m, "AF_FILE", AF_FILE);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_FILE),
          PyUnicode_FromString("AF_FILE"));
#endif
#ifdef AF_INET
  PyModule_AddIntConstant (m, "AF_INET", AF_INET);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_INET),
          PyUnicode_FromString("AF_INET"));
#endif
#ifdef AF_AX25
  PyModule_AddIntConstant (m, "AF_AX25", AF_AX25);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_AX25),
          PyUnicode_FromString("AF_AX25"));
#endif
#ifdef AF_IMPLINK  
  PyModule_AddIntConstant (m, "AF_IMPLINK", AF_IMPLINK);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_IMPLINK),
          PyUnicode_FromString("AF_IMPLINK"));
#endif
#ifdef AF_PUP  
  PyModule_AddIntConstant (m, "AF_PUP", AF_PUP);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_PUP),
          PyUnicode_FromString("AF_PUP"));
#endif
#ifdef AF_CHAOS
  PyModule_AddIntConstant (m, "AF_CHAOS", AF_CHAOS);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_CHAOS),
          PyUnicode_FromString("AF_CHAOS"));
#endif
#ifdef AF_NS
  PyModule_AddIntConstant (m, "AF_NS", AF_NS);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_NS),
          PyUnicode_FromString("AF_NS"));
#endif
#ifdef AF_ISO
  PyModule_AddIntConstant (m, "AF_ISO", AF_ISO);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_ISO),
          PyUnicode_FromString("AF_ISO"));
#endif
#ifdef AF_ECMA
  PyModule_AddIntConstant (m, "AF_ECMA", AF_ECMA);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_ECMA),
          PyUnicode_FromString("AF_ECMA"));
#endif
#ifdef AF_DATAKIT
  PyModule_AddIntConstant (m, "AF_DATAKIT", AF_DATAKIT);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_DATAKIT),
          PyUnicode_FromString("AF_DATAKIT"));
#endif
#ifdef AF_CCITT
  PyModule_AddIntConstant (m, "AF_CCITT", AF_CCITT);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_CCITT),
          PyUnicode_FromString("AF_CCITT"));
#endif
#ifdef AF_SNA
  PyModule_AddIntConstant (m, "AF_SNA", AF_SNA);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_SNA),
          PyUnicode_FromString("AF_SNA"));
#endif
#ifdef AF_DECnet
  PyModule_AddIntConstant (m, "AF_DECnet", AF_DECnet);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_DECnet),
          PyUnicode_FromString("AF_DECnet"));
#endif
#ifdef AF_DLI
  PyModule_AddIntConstant (m, "AF_DLI", AF_DLI);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_DLI),
          PyUnicode_FromString("AF_DLI"));
#endif
#ifdef AF_LAT
  PyModule_AddIntConstant (m, "AF_LAT", AF_LAT);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_LAT),
          PyUnicode_FromString("AF_LAT"));
#endif
#ifdef AF_HYLINK
  PyModule_AddIntConstant (m, "AF_HYLINK", AF_HYLINK);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_HYLINK),
          PyUnicode_FromString("AF_HYLINK"));
#endif
#ifdef AF_APPLETALK
  PyModule_AddIntConstant (m, "AF_APPLETALK", AF_APPLETALK);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_APPLETALK),
          PyUnicode_FromString("AF_APPLETALK"));
#endif
#ifdef AF_ROUTE
  PyModule_AddIntConstant (m, "AF_ROUTE", AF_ROUTE);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_ROUTE),
          PyUnicode_FromString("AF_ROUTE"));
#endif
#ifdef AF_LINK
  PyModule_AddIntConstant (m, "AF_LINK", AF_LINK);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_LINK),
          PyUnicode_FromString("AF_LINK"));
#endif
#ifdef AF_PACKET
  PyModule_AddIntConstant (m, "AF_PACKET", AF_PACKET);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_PACKET),
          PyUnicode_FromString("AF_PACKET"));
#endif
#ifdef AF_COIP
  PyModule_AddIntConstant (m, "AF_COIP", AF_COIP);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_COIP),
          PyUnicode_FromString("AF_COIP"));
#endif
#ifdef AF_CNT
  PyModule_AddIntConstant (m, "AF_CNT", AF_CNT);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_CNT),
          PyUnicode_FromString("AF_CNT"));
#endif
#ifdef AF_IPX
  PyModule_AddIntConstant (m, "AF_IPX", AF_IPX);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_IPX),
          PyUnicode_FromString("AF_IPX"));
#endif
#ifdef AF_SIP
  PyModule_AddIntConstant (m, "AF_SIP", AF_SIP);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_SIP),
          PyUnicode_FromString("AF_SIP"));
#endif
#ifdef AF_NDRV
  PyModule_AddIntConstant (m, "AF_NDRV", AF_NDRV);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_NDRV),
          PyUnicode_FromString("AF_NDRV"));
#endif
#ifdef AF_ISDN
  PyModule_AddIntConstant (m, "AF_ISDN", AF_ISDN);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_ISDN),
          PyUnicode_FromString("AF_ISDN"));
#endif
#ifdef AF_INET6
  PyModule_AddIntConstant (m, "AF_INET6", AF_INET6);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_INET6),
          PyUnicode_FromString("AF_INET6"));
#endif
#ifdef AF_NATM
  PyModule_AddIntConstant (m, "AF_NATM", AF_NATM);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_NATM),
          PyUnicode_FromString("AF_NATM"));
#endif
#ifdef AF_SYSTEM
  PyModule_AddIntConstant (m, "AF_SYSTEM", AF_SYSTEM);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_SYSTEM),
          PyUnicode_FromString("AF_SYSTEM"));
#endif
#ifdef AF_NETBIOS
  PyModule_AddIntConstant (m, "AF_NETBIOS", AF_NETBIOS);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_NETBIOS),
          PyUnicode_FromString("AF_NETBIOS"));
#endif
#ifdef AF_NETBEUI
  PyModule_AddIntConstant (m, "AF_NETBEUI", AF_NETBEUI);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_NETBEUI),
          PyUnicode_FromString("AF_NETBEUI"));
#endif
#ifdef AF_PPP
  PyModule_AddIntConstant (m, "AF_PPP", AF_PPP);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_PPP),
          PyUnicode_FromString("AF_PPP"));
#endif
#ifdef AF_ATM
  PyModule_AddIntConstant (m, "AF_ATM", AF_ATM);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_ATM),
          PyUnicode_FromString("AF_ATM"));
#endif
#ifdef AF_ATMPVC
  PyModule_AddIntConstant (m, "AF_ATMPVC", AF_ATMPVC);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_ATMPVC),
          PyUnicode_FromString("AF_ATMPVC"));
#endif
#ifdef AF_ATMSVC
  PyModule_AddIntConstant (m, "AF_ATMSVC", AF_ATMSVC);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_ATMSVC),
          PyUnicode_FromString("AF_ATMSVC"));
#endif
#ifdef AF_NETGRAPH
  PyModule_AddIntConstant (m, "AF_NETGRAPH", AF_NETGRAPH);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_NETGRAPH),
          PyUnicode_FromString("AF_NETGRAPH"));
#endif
#ifdef AF_VOICEVIEW
  PyModule_AddIntConstant (m, "AF_VOICEVIEW", AF_VOICEVIEW);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_VOICEVIEW),
          PyUnicode_FromString("AF_VOICEVIEW"));
#endif
#ifdef AF_FIREFOX
  PyModule_AddIntConstant (m, "AF_FIREFOX", AF_FIREFOX);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_FIREFOX),
          PyUnicode_FromString("AF_FIREFOX"));
#endif
#ifdef AF_UNKNOWN1
  PyModule_AddIntConstant (m, "AF_UNKNOWN1", AF_UNKNOWN1);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_UNKNOWN1),
          PyUnicode_FromString("AF_UNKNOWN1"));
#endif
#ifdef AF_BAN
  PyModule_AddIntConstant (m, "AF_BAN", AF_BAN);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_BAN),
          PyUnicode_FromString("AF_BAN"));
#endif
#ifdef AF_CLUSTER
  PyModule_AddIntConstant (m, "AF_CLUSTER", AF_CLUSTER);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_CLUSTER),
          PyUnicode_FromString("AF_CLUSTER"));
#endif
#ifdef AF_12844
  PyModule_AddIntConstant (m, "AF_12844", AF_12844);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_12844),
          PyUnicode_FromString("AF_12844"));
#endif
#ifdef AF_IRDA
  PyModule_AddIntConstant (m, "AF_IRDA", AF_IRDA);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_IRDA),
          PyUnicode_FromString("AF_IRDA"));
#endif
#ifdef AF_NETDES
  PyModule_AddIntConstant (m, "AF_NETDES", AF_NETDES);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_NETDES),
          PyUnicode_FromString("AF_NETDES"));
#endif
#ifdef AF_NETROM
  PyModule_AddIntConstant (m, "AF_NETROM", AF_NETROM);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_NETROM),
          PyUnicode_FromString("AF_NETROM"));
#endif
#ifdef AF_BRIDGE
  PyModule_AddIntConstant (m, "AF_BRIDGE", AF_BRIDGE);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_BRIDGE),
          PyUnicode_FromString("AF_BRIDGE"));
#endif
#ifdef AF_X25
  PyModule_AddIntConstant (m, "AF_X25", AF_X25);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_X25),
          PyUnicode_FromString("AF_X25"));
#endif
#ifdef AF_ROSE
  PyModule_AddIntConstant (m, "AF_ROSE", AF_ROSE);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_ROSE),
          PyUnicode_FromString("AF_ROSE"));
#endif
#ifdef AF_SECURITY
  PyModule_AddIntConstant (m, "AF_SECURITY", AF_SECURITY);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_SECURITY),
          PyUnicode_FromString("AF_SECURITY"));
#endif
#ifdef AF_KEY
  PyModule_AddIntConstant (m, "AF_KEY", AF_KEY);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_KEY),
          PyUnicode_FromString("AF_KEY"));
#endif
#ifdef AF_NETLINK
  PyModule_AddIntConstant (m, "AF_NETLINK", AF_NETLINK);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_NETLINK),
          PyUnicode_FromString("AF_NETLINK"));
#endif
#ifdef AF_ASH
  PyModule_AddIntConstant (m, "AF_ASH", AF_ASH);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_ASH),
          PyUnicode_FromString("AF_ASH"));
#endif
#ifdef AF_ECONET
  PyModule_AddIntConstant (m, "AF_ECONET", AF_ECONET);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_ECONET),
          PyUnicode_FromString("AF_ECONET"));
#endif
#ifdef AF_SNA
  PyModule_AddIntConstant (m, "AF_SNA", AF_SNA);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_SNA),
          PyUnicode_FromString("AF_SNA"));
#endif
#ifdef AF_PPPOX
  PyModule_AddIntConstant (m, "AF_PPPOX", AF_PPPOX);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_PPPOX),
          PyUnicode_FromString("AF_PPPOX"));
#endif
#ifdef AF_WANPIPE
  PyModule_AddIntConstant (m, "AF_WANPIPE", AF_WANPIPE);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_WANPIPE),
          PyUnicode_FromString("AF_WANPIPE"));
#endif
#ifdef AF_BLUETOOTH
  PyModule_AddIntConstant (m, "AF_BLUETOOTH", AF_BLUETOOTH);
  PyDict_SetItem(address_family_dict, PyInt_FromLong(AF_BLUETOOTH),
          PyUnicode_FromString("AF_BLUETOOTH"));
#endif
#ifdef IN6_IFF_AUTOCONF
  PyModule_AddIntConstant (m, "IN6_IFF_AUTOCONF", IN6_IFF_AUTOCONF);
#endif
#ifdef IN6_IFF_TEMPORARY
  PyModule_AddIntConstant (m, "IN6_IFF_TEMPORARY", IN6_IFF_TEMPORARY);
#endif
#ifdef IN6_IFF_DYNAMIC
  PyModule_AddIntConstant (m, "IN6_IFF_DYNAMIC", IN6_IFF_DYNAMIC);
#endif
#ifdef IN6_IFF_OPTIMISTIC
  PyModule_AddIntConstant (m, "IN6_IFF_OPTIMISTIC", IN6_IFF_OPTIMISTIC);
#endif
#ifdef IN6_IFF_SECURED
  PyModule_AddIntConstant (m, "IN6_IFF_SECURED", IN6_IFF_SECURED);
#endif
  PyModule_AddObject(m, "address_families", address_family_dict);

  // Add-in the version number from setup.py
#undef STR
#undef _STR
#define _STR(x) #x
#define STR(x)  _STR(x)

  PyModule_AddStringConstant(m, "version", STR(NETIFACES_VERSION));

  MODULE_RETURN(m);
}
