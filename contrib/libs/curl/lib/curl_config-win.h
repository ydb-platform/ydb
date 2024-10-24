/* lib/curl_config.h.in.  Generated somehow by cmake.  */

/* when building libcurl itself */
/* #undef BUILDING_LIBCURL */

/* Location of default ca bundle */
/* #undef CURL_CA_BUNDLE */

/* define "1" to use built-in ca store of TLS backend */
/* #undef CURL_CA_FALLBACK */

/* Location of default ca path */
/* #undef CURL_CA_PATH */

/* to disable cookies support */
/* #undef CURL_DISABLE_COOKIES */

/* to disable cryptographic authentication */
/* #undef CURL_DISABLE_CRYPTO_AUTH */

/* to disable DICT */
/* #undef CURL_DISABLE_DICT */

/* disable DoH */
/* #undef CURL_DISABLE_DOH */

/* to disable FILE */
/* #undef CURL_DISABLE_FILE */

/* to disable FTP */
/* #undef CURL_DISABLE_FTP */

/* to disable GOPHER */
/* #undef CURL_DISABLE_GOPHER */

/* disable HTTP authentication */
/* #undef CURL_DISABLE_HTTP_AUTH */

/* to disable IMAP */
/* #undef CURL_DISABLE_IMAP */

/* to disable HTTP */
/* #undef CURL_DISABLE_HTTP */

/* to disable LDAP */
#define CURL_DISABLE_LDAP 1

/* to disable LDAPS */
#define CURL_DISABLE_LDAPS 1

/* to disable --libcurl C code generation option */
/* #undef CURL_DISABLE_LIBCURL_OPTION */

/* disable mime API */
/* #undef CURL_DISABLE_MIME */

/* disable netrc parsing */
/* #undef CURL_DISABLE_NETRC */

/* if the OpenSSL configuration won't be loaded automatically */
/* #undef CURL_DISABLE_OPENSSL_AUTO_LOAD_CONFIG */

/* disable date parsing */
/* #undef CURL_DISABLE_PARSEDATE */

/* to disable POP3 */
/* #undef CURL_DISABLE_POP3 */

/* disable progress-meter */
/* #undef CURL_DISABLE_PROGRESS_METER */

/* to disable proxies */
/* #undef CURL_DISABLE_PROXY */

/* to disable RTSP */
/* #undef CURL_DISABLE_RTSP */

/* to disable SMB */
/* #undef CURL_DISABLE_SMB */

/* disable DNS shuffling */
/* #undef CURL_DISABLE_SHUFFLE_DNS */

/* to disable SMTP */
/* #undef CURL_DISABLE_SMTP */

/* to disable TELNET */
/* #undef CURL_DISABLE_TELNET */

/* to disable TFTP */
/* #undef CURL_DISABLE_TFTP */

/* to disable verbose strings */
/* #undef CURL_DISABLE_VERBOSE_STRINGS */

/* to make a symbol visible */
/* #undef CURL_EXTERN_SYMBOL */
/* Ensure using CURL_EXTERN_SYMBOL is possible */
#ifndef CURL_EXTERN_SYMBOL
#define CURL_EXTERN_SYMBOL
#endif

/* Use Windows LDAP implementation */
/* #undef USE_WIN32_LDAP */

/* when not building a shared library */
#define CURL_STATICLIB 1

/* your Entropy Gathering Daemon socket pathname */
/* #undef EGD_SOCKET */

/* Define if you want to enable IPv6 support */
#define ENABLE_IPV6 1

/* Define to the type qualifier of arg 1 for getnameinfo. */
/* #undef GETNAMEINFO_QUAL_ARG1 */

/* Define to the type of arg 1 for getnameinfo. */
/* #undef GETNAMEINFO_TYPE_ARG1 */

/* Define to the type of arg 2 for getnameinfo. */
/* #undef GETNAMEINFO_TYPE_ARG2 */

/* Define to the type of args 4 and 6 for getnameinfo. */
/* #undef GETNAMEINFO_TYPE_ARG46 */

/* Define to the type of arg 7 for getnameinfo. */
/* #undef GETNAMEINFO_TYPE_ARG7 */

/* Specifies the number of arguments to getservbyport_r */
/* #undef GETSERVBYPORT_R_ARGS */

/* Specifies the size of the buffer to pass to getservbyport_r */
/* #undef GETSERVBYPORT_R_BUFSIZE */

/* Define to 1 if you have the alarm function. */
/* #undef HAVE_ALARM */

/* Define to 1 if you have the <alloca.h> header file. */
/* #undef HAVE_ALLOCA_H */

/* Define to 1 if you have the <arpa/inet.h> header file. */
/* #undef HAVE_ARPA_INET_H */

/* Define to 1 if you have the <arpa/tftp.h> header file. */
/* #undef HAVE_ARPA_TFTP_H */

/* Define to 1 if you have the <assert.h> header file. */
#define HAVE_ASSERT_H 1

/* Define to 1 if you have the `basename' function. */
/* #undef HAVE_BASENAME */

/* Define to 1 if bool is an available type. */
#define HAVE_BOOL_T 1

/* Define to 1 if you have the __builtin_available function. */
/* #undef HAVE_BUILTIN_AVAILABLE */

/* Define to 1 if you have the clock_gettime function and monotonic timer. */
/* #undef HAVE_CLOCK_GETTIME_MONOTONIC */

/* Define to 1 if you have the `closesocket' function. */
#define HAVE_CLOSESOCKET 1

/* Define to 1 if you have the `CRYPTO_cleanup_all_ex_data' function. */
/* #undef HAVE_CRYPTO_CLEANUP_ALL_EX_DATA */

/* Define to 1 if you have the <crypto.h> header file. */
/* #undef HAVE_CRYPTO_H */

/* Define to 1 if you have the <des.h> header file. */
/* #undef HAVE_DES_H */

/* Define to 1 if you have the <dlfcn.h> header file. */
/* #undef HAVE_DLFCN_H */

/* Define to 1 if you have the `ENGINE_load_builtin_engines' function. */
/* #undef HAVE_ENGINE_LOAD_BUILTIN_ENGINES */

/* Define to 1 if you have the <errno.h> header file. */
#define HAVE_ERRNO_H 1

/* Define to 1 if you have the <err.h> header file. */
/* #undef HAVE_ERR_H */

/* Define to 1 if you have the fcntl function. */
/* #undef HAVE_FCNTL */

/* Define to 1 if you have the <fcntl.h> header file. */
#define HAVE_FCNTL_H 1

/* Define to 1 if you have a working fcntl O_NONBLOCK function. */
/* #undef HAVE_FCNTL_O_NONBLOCK */

/* Define to 1 if you have the `fork' function. */
/* #undef HAVE_FORK */

/* Define to 1 if you have the freeaddrinfo function. */
#define HAVE_FREEADDRINFO 1

/* Define to 1 if you have the freeifaddrs function. */
/* #undef HAVE_FREEIFADDRS */

/* Define to 1 if you have the ftruncate function. */
/* #undef HAVE_FTRUNCATE */

/* Define to 1 if you have a working getaddrinfo function. */
#define HAVE_GETADDRINFO 1

/* Define to 1 if you have the `geteuid' function. */
/* #undef HAVE_GETEUID */

/* Define to 1 if you have the gethostbyaddr function. */
#define HAVE_GETHOSTBYADDR 1

/* Define to 1 if you have the gethostbyaddr_r function. */
/* #undef HAVE_GETHOSTBYADDR_R */

/* gethostbyaddr_r() takes 5 args */
/* #undef HAVE_GETHOSTBYADDR_R_5 */

/* gethostbyaddr_r() takes 7 args */
/* #undef HAVE_GETHOSTBYADDR_R_7 */

/* gethostbyaddr_r() takes 8 args */
/* #undef HAVE_GETHOSTBYADDR_R_8 */

/* Define to 1 if you have the gethostbyname function. */
#define HAVE_GETHOSTBYNAME 1

/* Define to 1 if you have the gethostbyname_r function. */
/* #undef HAVE_GETHOSTBYNAME_R */

/* gethostbyname_r() takes 3 args */
/* #undef HAVE_GETHOSTBYNAME_R_3 */

/* gethostbyname_r() takes 5 args */
/* #undef HAVE_GETHOSTBYNAME_R_5 */

/* gethostbyname_r() takes 6 args */
/* #undef HAVE_GETHOSTBYNAME_R_6 */

/* Define to 1 if you have the gethostname function. */
#define HAVE_GETHOSTNAME 1

/* Define to 1 if you have a working getifaddrs function. */
/* #undef HAVE_GETIFADDRS */

/* Define to 1 if you have the getnameinfo function. */
/* #undef HAVE_GETNAMEINFO */

/* Define to 1 if you have the `getpass_r' function. */
/* #undef HAVE_GETPASS_R */

/* Define to 1 if you have the getpeername function. */
#define HAVE_GETPEERNAME 1

/* Define to 1 if you have the `getppid' function. */
/* #undef HAVE_GETPPID */

/* Define to 1 if you have the `getprotobyname' function. */
#define HAVE_GETPROTOBYNAME 1

/* Define to 1 if you have the `getpwuid' function. */
/* #undef HAVE_GETPWUID */

/* Define to 1 if you have the `getpwuid_r' function. */
/* #undef HAVE_GETPWUID_R */

/* Define to 1 if you have the `getrlimit' function. */
/* #undef HAVE_GETRLIMIT */

/* Define to 1 if you have the getservbyport_r function. */
/* #undef HAVE_GETSERVBYPORT_R */

/* Define to 1 if you have the getsockname function. */
#define HAVE_GETSOCKNAME 1

/* Define to 1 if you have the `gettimeofday' function. */
/* #undef HAVE_GETTIMEOFDAY */

/* Define to 1 if you have a working glibc-style strerror_r function. */
/* #undef HAVE_GLIBC_STRERROR_R */

/* Define to 1 if you have a working gmtime_r function. */
/* #undef HAVE_GMTIME_R */

/* if you have the gssapi libraries */
/* #undef HAVE_GSSAPI */

/* Define to 1 if you have the <gssapi/gssapi_generic.h> header file. */
/* #undef HAVE_GSSAPI_GSSAPI_GENERIC_H */

/* Define to 1 if you have the <gssapi/gssapi.h> header file. */
/* #undef HAVE_GSSAPI_GSSAPI_H */

/* Define to 1 if you have the <gssapi/gssapi_krb5.h> header file. */
/* #undef HAVE_GSSAPI_GSSAPI_KRB5_H */

/* if you have the GNU gssapi libraries */
/* #undef HAVE_GSSGNU */

/* if you have the Heimdal gssapi libraries */
/* #undef HAVE_GSSHEIMDAL */

/* if you have the MIT gssapi libraries */
/* #undef HAVE_GSSMIT */

/* Define to 1 if you have the `idna_strerror' function. */
/* #undef HAVE_IDNA_STRERROR */

/* Define to 1 if you have the `idn_free' function. */
/* #undef HAVE_IDN_FREE */

/* Define to 1 if you have the <idn-free.h> header file. */
/* #undef HAVE_IDN_FREE_H */

/* Define to 1 if you have the <ifaddrs.h> header file. */
/* #undef HAVE_IFADDRS_H */

/* Define to 1 if you have the `inet_addr' function. */
#define HAVE_INET_ADDR 1

/* Define to 1 if you have the inet_ntoa_r function. */
/* #undef HAVE_INET_NTOA_R */

/* inet_ntoa_r() takes 2 args */
/* #undef HAVE_INET_NTOA_R_2 */

/* inet_ntoa_r() takes 3 args */
/* #undef HAVE_INET_NTOA_R_3 */

/* Define to 1 if you have a IPv6 capable working inet_ntop function. */
/* #undef HAVE_INET_NTOP */

/* Define to 1 if you have a IPv6 capable working inet_pton function. */
#define HAVE_INET_PTON 1

/* Define to 1 if you have the <inttypes.h> header file. */
/* #undef HAVE_INTTYPES_H */

/* Define to 1 if you have the ioctl function. */
/* #undef HAVE_IOCTL */

/* Define to 1 if you have the ioctlsocket function. */
#define HAVE_IOCTLSOCKET 1

/* Define to 1 if you have the IoctlSocket camel case function. */
/* #undef HAVE_IOCTLSOCKET_CAMEL */

/* Define to 1 if you have a working IoctlSocket camel case FIONBIO function.
   */
#define HAVE_IOCTLSOCKET_CAMEL_FIONBIO 1

/* Define to 1 if you have a working ioctlsocket FIONBIO function. */
#define HAVE_IOCTLSOCKET_FIONBIO 1

/* Define to 1 if you have a working ioctl FIONBIO function. */
/* #undef HAVE_IOCTL_FIONBIO */

/* Define to 1 if you have a working ioctl SIOCGIFADDR function. */
/* #undef HAVE_IOCTL_SIOCGIFADDR */

/* Define to 1 if you have the <io.h> header file. */
#define HAVE_IO_H 1

/* if you have the Kerberos4 libraries (including -ldes) */
/* #undef HAVE_KRB4 */

/* Define to 1 if you have the `krb_get_our_ip_for_realm' function. */
/* #undef HAVE_KRB_GET_OUR_IP_FOR_REALM */

/* Define to 1 if you have the <krb.h> header file. */
/* #undef HAVE_KRB_H */

/* Define to 1 if you have the lber.h header file. */
/* #undef HAVE_LBER_H */

/* Define to 1 if you have the ldapssl.h header file. */
/* #undef HAVE_LDAPSSL_H */

/* Define to 1 if you have the ldap.h header file. */
/* #undef HAVE_LDAP_H */

/* Use LDAPS implementation */
/* #undef HAVE_LDAP_SSL */

/* Define to 1 if you have the ldap_ssl.h header file. */
/* #undef HAVE_LDAP_SSL_H */

/* Define to 1 if you have the `ldap_url_parse' function. */
/* #undef HAVE_LDAP_URL_PARSE */

/* Define to 1 if you have the <libgen.h> header file. */
/* #undef HAVE_LIBGEN_H */

/* Define to 1 if you have the `idn' library (-lidn). */
/* #undef HAVE_LIBIDN */

/* Define to 1 if you have the `resolv' library (-lresolv). */
/* #undef HAVE_LIBRESOLV */

/* Define to 1 if you have the `resolve' library (-lresolve). */
/* #undef HAVE_LIBRESOLVE */

/* Define to 1 if you have the `socket' library (-lsocket). */
/* #undef HAVE_LIBSOCKET */

/* Define to 1 if you have the `ssh2' library (-lssh2). */
/* #undef HAVE_LIBSSH2 */

/* Define to 1 if libssh2 provides `libssh2_version'. */
/* #undef HAVE_LIBSSH2_VERSION */

/* Define to 1 if libssh2 provides `libssh2_init'. */
/* #undef HAVE_LIBSSH2_INIT */

/* Define to 1 if libssh2 provides `libssh2_exit'. */
/* #undef HAVE_LIBSSH2_EXIT */

/* Define to 1 if libssh2 provides `libssh2_scp_send64'. */
/* #undef HAVE_LIBSSH2_SCP_SEND64 */

/* Define to 1 if libssh2 provides `libssh2_session_handshake'. */
/* #undef HAVE_LIBSSH2_SESSION_HANDSHAKE */

/* Define to 1 if you have the <libssh2.h> header file. */
/* #undef HAVE_LIBSSH2_H */

/* Define to 1 if you have the `ssl' library (-lssl). */
#define HAVE_LIBSSL 1

/* if zlib is available */
#define HAVE_LIBZ 1

/* if brotli is available */
#define HAVE_BROTLI 1

/* if your compiler supports LL */
#define HAVE_LL 1

/* Define to 1 if you have the <locale.h> header file. */
#define HAVE_LOCALE_H 1

/* Define to 1 if you have a working localtime_r function. */
/* #undef HAVE_LOCALTIME_R */

/* Define to 1 if the compiler supports the 'long long' data type. */
#define HAVE_LONGLONG 1

/* Define to 1 if you have the malloc.h header file. */
#define HAVE_MALLOC_H 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the MSG_NOSIGNAL flag. */
/* #undef HAVE_MSG_NOSIGNAL */

/* Define to 1 if you have the <netdb.h> header file. */
/* #undef HAVE_NETDB_H */

/* Define to 1 if you have the <netinet/in.h> header file. */
/* #undef HAVE_NETINET_IN_H */

/* Define to 1 if you have the <netinet/tcp.h> header file. */
/* #undef HAVE_NETINET_TCP_H */

/* Define to 1 if you have the <net/if.h> header file. */
/* #undef HAVE_NET_IF_H */

/* Define to 1 if you have the <nghttp2/nghttp2.h> header file. */
#define HAVE_NGHTTP2_NGHTTP2_H 1

/* Define to 1 if you have the <nghttp3/nghttp3.h> header file. */
/* #undef HAVE_NGHTTP3_NGHTTP3_H */

/* Define to 1 if you have the <ngtcp2/ngtcp2_crypto.h> header file. */
/* #undef HAVE_NGTCP2_NGTCP2_CRYPTO_H */

/* Define to 1 if you have the <ngtcp2/ngtcp2.h> header file. */
/* #undef HAVE_NGTCP2_NGTCP2_H */

/* if you have an old MIT gssapi library, lacking GSS_C_NT_HOSTBASED_SERVICE */
/* #undef HAVE_OLD_GSSMIT */

/* Define to 1 if you have the <openssl/crypto.h> header file. */
#define HAVE_OPENSSL_CRYPTO_H 1

/* Define to 1 if you have the <openssl/engine.h> header file. */
/* #undef HAVE_OPENSSL_ENGINE_H */

/* Define to 1 if you have the <openssl/err.h> header file. */
#define HAVE_OPENSSL_ERR_H 1

/* Define to 1 if you have the <openssl/pem.h> header file. */
#define HAVE_OPENSSL_PEM_H 1

/* Define to 1 if you have the <openssl/pkcs12.h> header file. */
/* #undef HAVE_OPENSSL_PKCS12_H */

/* Define to 1 if you have the <openssl/rsa.h> header file. */
#define HAVE_OPENSSL_RSA_H 1

/* Define to 1 if you have the <openssl/ssl.h> header file. */
/* #undef HAVE_OPENSSL_SSL_H */

/* Define to 1 if you have the `OpenSSL_version' function. */
#define HAVE_OPENSSL_VERSION 1

/* Define to 1 if you have the <openssl/x509.h> header file. */
#define HAVE_OPENSSL_X509_H 1

/* Define to 1 if you have the <pem.h> header file. */
/* #undef HAVE_PEM_H */

/* Define to 1 if you have the `perror' function. */
#define HAVE_PERROR 1

/* Define to 1 if you have the `pipe' function. */
/* #undef HAVE_PIPE */

/* Define to 1 if you have a working poll function. */
/* #undef HAVE_POLL */

/* If you have a fine poll */
/* #undef HAVE_POLL_FINE */

/* Define to 1 if you have the <poll.h> header file. */
/* #undef HAVE_POLL_H */

/* Define to 1 if you have a working POSIX-style strerror_r function. */
/* #undef HAVE_POSIX_STRERROR_R */

/* Define to 1 if you have the <proto/bsdsocket.h> header file. */
/* #undef HAVE_PROTO_BSDSOCKET_H */

/* Define to 1 if you have the <pthread.h> header file */
/* #undef HAVE_PTHREAD_H */

/* Define to 1 if you have the <pwd.h> header file. */
/* #undef HAVE_PWD_H */

/* Define to 1 if you have the <quiche.h> header file. */
/* #undef HAVE_QUICHE_H */

/* Define to 1 if you have the `RAND_egd' function. */
/* #undef HAVE_RAND_EGD */

/* Define to 1 if you have the `RAND_screen' function. */
/* #undef HAVE_RAND_SCREEN */

/* Define to 1 if you have the `RAND_status' function. */
/* #undef HAVE_RAND_STATUS */

/* Define to 1 if you have the recv function. */
#define HAVE_RECV 1

/* Define to 1 if you have the recvfrom function. */
/* #undef HAVE_RECVFROM */

/* Define to 1 if you have the <rsa.h> header file. */
/* #undef HAVE_RSA_H */

/* Define to 1 if you have the select function. */
#define HAVE_SELECT 1

/* Define to 1 if you have the send function. */
#define HAVE_SEND 1

/* Define to 1 if you have the 'fsetxattr' function. */
/* #undef HAVE_FSETXATTR */

/* fsetxattr() takes 5 args */
/* #undef HAVE_FSETXATTR_5 */

/* fsetxattr() takes 6 args */
/* #undef HAVE_FSETXATTR_6 */

/* Define to 1 if you have the <setjmp.h> header file. */
#define HAVE_SETJMP_H 1

/* Define to 1 if you have the `setlocale' function. */
#define HAVE_SETLOCALE 1

/* Define to 1 if you have the `setmode' function. */
#define HAVE_SETMODE 1

/* Define to 1 if you have the `setrlimit' function. */
/* #undef HAVE_SETRLIMIT */

/* Define to 1 if you have the setsockopt function. */
#define HAVE_SETSOCKOPT 1

/* Define to 1 if you have a working setsockopt SO_NONBLOCK function. */
/* #undef HAVE_SETSOCKOPT_SO_NONBLOCK */

/* Define to 1 if you have the <sgtty.h> header file. */
/* #undef HAVE_SGTTY_H */

/* Define to 1 if you have the sigaction function. */
/* #undef HAVE_SIGACTION */

/* Define to 1 if you have the siginterrupt function. */
/* #undef HAVE_SIGINTERRUPT */

/* Define to 1 if you have the signal function. */
/* #undef HAVE_SIGNAL */

/* Define to 1 if you have the <signal.h> header file. */
#define HAVE_SIGNAL_H 1

/* Define to 1 if you have the sigsetjmp function or macro. */
/* #undef HAVE_SIGSETJMP */

/* Define to 1 if sig_atomic_t is an available typedef. */
#define HAVE_SIG_ATOMIC_T 1

/* Define to 1 if sig_atomic_t is already defined as volatile. */
/* #undef HAVE_SIG_ATOMIC_T_VOLATILE */

/* Define to 1 if struct sockaddr_in6 has the sin6_scope_id member */
/* #undef HAVE_SOCKADDR_IN6_SIN6_SCOPE_ID */

/* Define to 1 if you have the `socket' function. */
#define HAVE_SOCKET 1

/* Define to 1 if you have the `SSLv2_client_method' function. */
/* #undef HAVE_SSLV2_CLIENT_METHOD */

/* Define to 1 if you have the <ssl.h> header file. */
/* #undef HAVE_SSL_H */

/* Define to 1 if you have the <stdbool.h> header file. */
#define HAVE_STDBOOL_H 1

/* Define to 1 if you have the <stdint.h> header file. */
/* #undef HAVE_STDINT_H */

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the strcasecmp function. */
/* #undef HAVE_STRCASECMP */

/* Define to 1 if you have the strcasestr function. */
/* #undef HAVE_STRCASESTR */

/* Define to 1 if you have the strcmpi function. */
/* #undef HAVE_STRCMPI */

/* Define to 1 if you have the strdup function. */
#define HAVE_STRDUP 1

/* Define to 1 if you have the strerror_r function. */
/* #undef HAVE_STRERROR_R */

/* Define to 1 if you have the stricmp function. */
#define HAVE_STRICMP 1

/* Define to 1 if you have the <strings.h> header file. */
/* #undef HAVE_STRINGS_H */

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the strlcat function. */
/* #undef HAVE_STRLCAT */

/* Define to 1 if you have the `strlcpy' function. */
/* #undef HAVE_STRLCPY */

/* Define to 1 if you have the strncasecmp function. */
/* #undef HAVE_STRNCASECMP */

/* Define to 1 if you have the strncmpi function. */
/* #undef HAVE_STRNCMPI */

/* Define to 1 if you have the strnicmp function. */
/* #undef HAVE_STRNICMP */

/* Define to 1 if you have the <stropts.h> header file. */
/* #undef HAVE_STROPTS_H */

/* Define to 1 if you have the strstr function. */
#define HAVE_STRSTR 1

/* Define to 1 if you have the strtok_r function. */
/* #undef HAVE_STRTOK_R */

/* Define to 1 if you have the strtoll function. */
#define HAVE_STRTOLL 1

/* if struct sockaddr_storage is defined */
#define HAVE_STRUCT_SOCKADDR_STORAGE 1

/* Define to 1 if you have the timeval struct. */
#define HAVE_STRUCT_TIMEVAL 1

/* Define to 1 if you have the <sys/filio.h> header file. */
/* #undef HAVE_SYS_FILIO_H */

/* Define to 1 if you have the <sys/ioctl.h> header file. */
/* #undef HAVE_SYS_IOCTL_H */

/* Define to 1 if you have the <sys/param.h> header file. */
/* #undef HAVE_SYS_PARAM_H */

/* Define to 1 if you have the <sys/poll.h> header file. */
/* #undef HAVE_SYS_POLL_H */

/* Define to 1 if you have the <sys/resource.h> header file. */
/* #undef HAVE_SYS_RESOURCE_H */

/* Define to 1 if you have the <sys/select.h> header file. */
/* #undef HAVE_SYS_SELECT_H */

/* Define to 1 if you have the <sys/socket.h> header file. */
/* #undef HAVE_SYS_SOCKET_H */

/* Define to 1 if you have the <sys/sockio.h> header file. */
/* #undef HAVE_SYS_SOCKIO_H */

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
/* #undef HAVE_SYS_TIME_H */

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/uio.h> header file. */
/* #undef HAVE_SYS_UIO_H */

/* Define to 1 if you have the <sys/un.h> header file. */
/* #undef HAVE_SYS_UN_H */

/* Define to 1 if you have the <sys/utime.h> header file. */
#define HAVE_SYS_UTIME_H 1

/* Define to 1 if you have the <termios.h> header file. */
/* #undef HAVE_TERMIOS_H */

/* Define to 1 if you have the <termio.h> header file. */
/* #undef HAVE_TERMIO_H */

/* Define to 1 if you have the <time.h> header file. */
#define HAVE_TIME_H 1

/* Define to 1 if you have the <tld.h> header file. */
/* #undef HAVE_TLD_H */

/* Define to 1 if you have the `tld_strerror' function. */
/* #undef HAVE_TLD_STRERROR */

/* Define to 1 if you have the `uname' function. */
/* #undef HAVE_UNAME */

/* Define to 1 if you have the <unistd.h> header file. */
/* #undef HAVE_UNISTD_H */

/* Define to 1 if you have the `usleep' function. */
#define HAVE_USLEEP 1

/* Define to 1 if you have the `utime' function. */
#define HAVE_UTIME 1

/* Define to 1 if you have the <utime.h> header file. */
/* #undef HAVE_UTIME_H */

/* Define to 1 if compiler supports C99 variadic macro style. */
/* #undef HAVE_VARIADIC_MACROS_C99 */

/* Define to 1 if compiler supports old gcc variadic macro style. */
/* #undef HAVE_VARIADIC_MACROS_GCC */

/* Define to 1 if you have the winber.h header file. */
/* #undef HAVE_WINBER_H */

/* Define to 1 if you have the windows.h header file. */
#define HAVE_WINDOWS_H 1

/* Define to 1 if you have the winldap.h header file. */
/* #undef HAVE_WINLDAP_H */

/* Define to 1 if you have the winsock2.h header file. */
#define HAVE_WINSOCK2_H 1

/* Define to 1 if you have the winsock.h header file. */
#define HAVE_WINSOCK_H 1

/* Define this symbol if your OS supports changing the contents of argv */
/* #undef HAVE_WRITABLE_ARGV */

/* Define to 1 if you have the writev function. */
/* #undef HAVE_WRITEV */

/* Define to 1 if you have the ws2tcpip.h header file. */
#define HAVE_WS2TCPIP_H 1

/* Define to 1 if you have the <x509.h> header file. */
/* #undef HAVE_X509_H */

/* Define if you have the <process.h> header file. */
#define HAVE_PROCESS_H 1

/* if you have the zlib.h header file */
#define HAVE_ZLIB_H 1

/* Define to the sub-directory in which libtool stores uninstalled libraries.
   */
/* #undef LT_OBJDIR */

/* If you lack a fine basename() prototype */
/* #undef NEED_BASENAME_PROTO */

/* Define to 1 if you need the lber.h header file even with ldap.h */
/* #undef NEED_LBER_H */

/* Define to 1 if you need the malloc.h header file even with stdlib.h */
/* #undef NEED_MALLOC_H */

/* Define to 1 if _REENTRANT preprocessor symbol must be defined. */
/* #undef NEED_REENTRANT */

/* cpu-machine-OS */
#define OS "Windows"

/* Name of package */
/* #undef PACKAGE */

/* Define to the address where bug reports for this package should be sent. */
/* #undef PACKAGE_BUGREPORT */

/* Define to the full name of this package. */
/* #undef PACKAGE_NAME */

/* Define to the full name and version of this package. */
/* #undef PACKAGE_STRING */

/* Define to the one symbol short name of this package. */
/* #undef PACKAGE_TARNAME */

/* Define to the version of this package. */
/* #undef PACKAGE_VERSION */

/* a suitable file to read random data from */
/* #undef RANDOM_FILE */

/* Define to the type of arg 1 for recvfrom. */
/* #undef RECVFROM_TYPE_ARG1 */

/* Define to the type pointed by arg 2 for recvfrom. */
/* #undef RECVFROM_TYPE_ARG2 */

/* Define to 1 if the type pointed by arg 2 for recvfrom is void. */
/* #undef RECVFROM_TYPE_ARG2_IS_VOID */

/* Define to the type of arg 3 for recvfrom. */
/* #undef RECVFROM_TYPE_ARG3 */

/* Define to the type of arg 4 for recvfrom. */
/* #undef RECVFROM_TYPE_ARG4 */

/* Define to the type pointed by arg 5 for recvfrom. */
/* #undef RECVFROM_TYPE_ARG5 */

/* Define to 1 if the type pointed by arg 5 for recvfrom is void. */
/* #undef RECVFROM_TYPE_ARG5_IS_VOID */

/* Define to the type pointed by arg 6 for recvfrom. */
/* #undef RECVFROM_TYPE_ARG6 */

/* Define to 1 if the type pointed by arg 6 for recvfrom is void. */
/* #undef RECVFROM_TYPE_ARG6_IS_VOID */

/* Define to the function return type for recvfrom. */
/* #undef RECVFROM_TYPE_RETV */

/* Define to the type of arg 1 for recv. */
#define RECV_TYPE_ARG1 SOCKET

/* Define to the type of arg 2 for recv. */
#define RECV_TYPE_ARG2 char *

/* Define to the type of arg 3 for recv. */
#define RECV_TYPE_ARG3 int

/* Define to the type of arg 4 for recv. */
#define RECV_TYPE_ARG4 int

/* Define to the function return type for recv. */
#define RECV_TYPE_RETV int

/* Define as the return type of signal handlers (`int' or `void'). */
#define RETSIGTYPE void

/* Define to the type qualifier of arg 5 for select. */
/* #undef SELECT_QUAL_ARG5 */

/* Define to the type of arg 1 for select. */
/* #undef SELECT_TYPE_ARG1 */

/* Define to the type of args 2, 3 and 4 for select. */
/* #undef SELECT_TYPE_ARG234 */

/* Define to the type of arg 5 for select. */
/* #undef SELECT_TYPE_ARG5 */

/* Define to the function return type for select. */
/* #undef SELECT_TYPE_RETV */

/* Define to the type qualifier of arg 2 for send. */
#define SEND_QUAL_ARG2 const

/* Define to the type of arg 1 for send. */
#define SEND_TYPE_ARG1 SOCKET

/* Define to the type of arg 2 for send. */
#define SEND_TYPE_ARG2 char *

/* Define to the type of arg 3 for send. */
#define SEND_TYPE_ARG3 int

/* Define to the type of arg 4 for send. */
#define SEND_TYPE_ARG4 int

/* Define to the function return type for send. */
#define SEND_TYPE_RETV int

/* The size of `int', as computed by sizeof. */
#ifndef SIZEOF_INT
#error undefined SIZEOF_INT
#endif

/* The size of `short', as computed by sizeof. */
#ifndef SIZEOF_SHORT
#error undefined SIZEOF_SHORT
#endif

/* The size of `long', as computed by sizeof. */
#ifndef SIZEOF_LONG
#error undefined SIZEOF_LONG
#endif

/* The size of `off_t', as computed by sizeof. */
#define SIZEOF_OFF_T 4

/* The size of `curl_off_t', as computed by sizeof. */
#define SIZEOF_CURL_OFF_T 8

/* The size of `time_t', as computed by sizeof. */
#define SIZEOF_TIME_T 8

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Define to the type of arg 3 for strerror_r. */
/* #undef STRERROR_R_TYPE_ARG3 */

/* Define to 1 if you can safely include both <sys/time.h> and <time.h>. */
/* #undef TIME_WITH_SYS_TIME */

/* to enable alt-svc */
/* #undef USE_ALTSVC */

/* if AmiSSL is in use */
/* #undef USE_AMISSL */

/* Define to enable c-ares support */
#define USE_ARES 1

/* Define to disable non-blocking sockets. */
/* #undef USE_BLOCKING_SOCKETS */

/* if GnuTLS is enabled */
/* #undef USE_GNUTLS */

/* if PolarSSL is enabled */
/* #undef USE_POLARSSL */

/* if DarwinSSL is enabled */
/* #undef USE_DARWINSSL */

/* if libSSH2 is in use */
/* #undef USE_LIBSSH2 */

/* If you want to build curl with the built-in manual */
/* #undef USE_MANUAL */

/* if mbedTLS is enabled */
/* #undef USE_MBEDTLS */

/* if MesaLink is enabled */
/* #undef USE_MESALINK */

/* Define to enable metalink support */
/* #undef USE_METALINK */

/* to enable NGHTTP2  */
/* #undef USE_NGHTTP2 */

/* if nghttp3 is in use */
/* #undef USE_NGHTTP3 */

/* if ngtcp2 is in use */
/* #undef USE_NGTCP2 */

/* if ngtcp2_crypto_openssl is in use */
/* #undef USE_NGTCP2_CRYPTO_OPENSSL */

/* if NSS is enabled */
/* #undef USE_NSS */

/* Use OpenLDAP-specific code */
/* #undef USE_OPENLDAP */

/* if OpenSSL is in use */
#define USE_OPENSSL 1

/* if quiche is in use */
/* #undef USE_QUICHE */

/* to enable Windows SSL  */
/* #undef USE_SCHANNEL */

/* enable Secure Transport */
/* #undef USE_SECTRANSP */

/* Define if you want to enable POSIX threaded DNS lookup */
/* #undef USE_THREADS_POSIX */

/* Define if you want to enable WIN32 threaded DNS lookup */
/* #undef USE_THREADS_WIN32 */

/* Use TLS-SRP authentication */
#define USE_TLS_SRP 1

/* if Unix domain sockets are enabled  */
/* #undef USE_UNIX_SOCKETS */

/* enable multiple SSL backends */
/* #undef CURL_WITH_MULTI_SSL */

/* Define to 1 if you have the `normaliz' (WinIDN) library (-lnormaliz). */
/* #undef USE_WIN32_IDN */

/* Define to 1 if you are building a Windows target with large file support. */
#define USE_WIN32_LARGE_FILES 1

/* Use Windows LDAP implementation */
/* #undef USE_WIN32_LDAP */

/* Define to 1 if you are building a Windows target without large file
   support. */
/* #undef USE_WIN32_SMALL_FILES */

/* to enable SSPI support */
/* #undef USE_WINDOWS_SSPI */

/* if wolfSSL is enabled */
/* #undef USE_WOLFSSL */

/* Version number of package */
/* #undef VERSION */

/* Define to 1 if OS is AIX. */
#ifndef _ALL_SOURCE
#  undef _ALL_SOURCE
#endif

/* Number of bits in a file offset, on hosts where this is settable. */
/* #undef _FILE_OFFSET_BITS */

/* Define for large files, on AIX-style hosts. */
/* #undef _LARGE_FILES */

/* define this if you need it to compile thread-safe code */
/* #undef _THREAD_SAFE */

/* Define to empty if `const' does not conform to ANSI C. */
/* #undef const */

/* Type to use in place of in_addr_t when system does not provide it. */
#define in_addr_t unsigned long

/* Define to `__inline__' or `__inline' if that's what the C compiler
   calls it, or to nothing if 'inline' is not supported under any name.  */
#ifndef __cplusplus
#undef inline
#endif

/* Define to `unsigned int' if <sys/types.h> does not define. */
/* #undef size_t */

/* the signed version of size_t */
#define ssize_t __int64

/* Define to 1 if you have the mach_absolute_time function. */
/* #undef HAVE_MACH_ABSOLUTE_TIME */
