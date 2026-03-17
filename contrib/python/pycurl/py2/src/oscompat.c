#include "pycurl.h"

#if defined(WIN32)
PYCURL_INTERNAL int
dup_winsock(int sock, const struct curl_sockaddr *address)
{
    int rv;
    WSAPROTOCOL_INFO pi;

    rv = WSADuplicateSocket(sock, GetCurrentProcessId(), &pi);
    if (rv) {
        return CURL_SOCKET_BAD;
    }

    /* not sure if WSA_FLAG_OVERLAPPED is needed, but it does not seem to hurt */
    return (int) WSASocket(address->family, address->socktype, address->protocol, &pi, 0, WSA_FLAG_OVERLAPPED);
}
#endif

#if defined(WIN32) && ((_WIN32_WINNT < 0x0600) || (NTDDI_VERSION < NTDDI_VISTA))
/*
 * Only Winsock on Vista+ has inet_ntop().
 */
PYCURL_INTERNAL const char *
pycurl_inet_ntop (int family, void *addr, char *string, size_t string_size)
{
    SOCKADDR *sa;
    int       sa_len;
    /* both size_t and DWORD should be unsigned ints */
    DWORD string_size_dword = (DWORD) string_size;

    if (family == AF_INET6) {
        struct sockaddr_in6 sa6;
        memset(&sa6, 0, sizeof(sa6));
        sa6.sin6_family = AF_INET6;
        memcpy(&sa6.sin6_addr, addr, sizeof(sa6.sin6_addr));
        sa = (SOCKADDR*) &sa6;
        sa_len = sizeof(sa6);
    } else if (family == AF_INET) {
        struct sockaddr_in sa4;
        memset(&sa4, 0, sizeof(sa4));
        sa4.sin_family = AF_INET;
        memcpy(&sa4.sin_addr, addr, sizeof(sa4.sin_addr));
        sa = (SOCKADDR*) &sa4;
        sa_len = sizeof(sa4);
    } else {
        errno = EAFNOSUPPORT;
        return NULL;
    }
    if (WSAAddressToString(sa, sa_len, NULL, string, &string_size_dword))
        return NULL;
    return string;
}
#endif

/* vi:ts=4:et:nowrap
 */
