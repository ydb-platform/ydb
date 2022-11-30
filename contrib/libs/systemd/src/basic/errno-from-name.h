/* ANSI-C code produced by gperf version 3.1 */
/* Command-line: /var/empty/gperf-3.1/bin/gperf -L ANSI-C -t --ignore-case -N lookup_errno -H hash_errno_name -p -C src/basic/errno-from-name.gperf  */
/* Computed positions: -k'2-3,5-6' */

#if !((' ' == 32) && ('!' == 33) && ('"' == 34) && ('#' == 35) \
      && ('%' == 37) && ('&' == 38) && ('\'' == 39) && ('(' == 40) \
      && (')' == 41) && ('*' == 42) && ('+' == 43) && (',' == 44) \
      && ('-' == 45) && ('.' == 46) && ('/' == 47) && ('0' == 48) \
      && ('1' == 49) && ('2' == 50) && ('3' == 51) && ('4' == 52) \
      && ('5' == 53) && ('6' == 54) && ('7' == 55) && ('8' == 56) \
      && ('9' == 57) && (':' == 58) && (';' == 59) && ('<' == 60) \
      && ('=' == 61) && ('>' == 62) && ('?' == 63) && ('A' == 65) \
      && ('B' == 66) && ('C' == 67) && ('D' == 68) && ('E' == 69) \
      && ('F' == 70) && ('G' == 71) && ('H' == 72) && ('I' == 73) \
      && ('J' == 74) && ('K' == 75) && ('L' == 76) && ('M' == 77) \
      && ('N' == 78) && ('O' == 79) && ('P' == 80) && ('Q' == 81) \
      && ('R' == 82) && ('S' == 83) && ('T' == 84) && ('U' == 85) \
      && ('V' == 86) && ('W' == 87) && ('X' == 88) && ('Y' == 89) \
      && ('Z' == 90) && ('[' == 91) && ('\\' == 92) && (']' == 93) \
      && ('^' == 94) && ('_' == 95) && ('a' == 97) && ('b' == 98) \
      && ('c' == 99) && ('d' == 100) && ('e' == 101) && ('f' == 102) \
      && ('g' == 103) && ('h' == 104) && ('i' == 105) && ('j' == 106) \
      && ('k' == 107) && ('l' == 108) && ('m' == 109) && ('n' == 110) \
      && ('o' == 111) && ('p' == 112) && ('q' == 113) && ('r' == 114) \
      && ('s' == 115) && ('t' == 116) && ('u' == 117) && ('v' == 118) \
      && ('w' == 119) && ('x' == 120) && ('y' == 121) && ('z' == 122) \
      && ('{' == 123) && ('|' == 124) && ('}' == 125) && ('~' == 126))
/* The character set is not based on ISO-646.  */
#error "gperf generated tables don't work with this execution character set. Please report a bug to <bug-gperf@gnu.org>."
#endif

#line 1 "src/basic/errno-from-name.gperf"

#if __GNUC__ >= 7
_Pragma("GCC diagnostic ignored \"-Wimplicit-fallthrough\"")
#endif
#line 6 "src/basic/errno-from-name.gperf"
struct errno_name { const char* name; int id; };

#define TOTAL_KEYWORDS 134
#define MIN_WORD_LENGTH 3
#define MAX_WORD_LENGTH 15
#define MIN_HASH_VALUE 6
#define MAX_HASH_VALUE 326
/* maximum key range = 321, duplicates = 0 */

#ifndef GPERF_DOWNCASE
#define GPERF_DOWNCASE 1
static unsigned char gperf_downcase[256] =
  {
      0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,
     15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,
     30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,
     45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,
     60,  61,  62,  63,  64,  97,  98,  99, 100, 101, 102, 103, 104, 105, 106,
    107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121,
    122,  91,  92,  93,  94,  95,  96,  97,  98,  99, 100, 101, 102, 103, 104,
    105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
    120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134,
    135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149,
    150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164,
    165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179,
    180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194,
    195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209,
    210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224,
    225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
    240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254,
    255
  };
#endif

#ifndef GPERF_CASE_STRCMP
#define GPERF_CASE_STRCMP 1
static int
gperf_case_strcmp (register const char *s1, register const char *s2)
{
  for (;;)
    {
      unsigned char c1 = gperf_downcase[(unsigned char)*s1++];
      unsigned char c2 = gperf_downcase[(unsigned char)*s2++];
      if (c1 != 0 && c1 == c2)
        continue;
      return (int)c1 - (int)c2;
    }
}
#endif

#ifdef __GNUC__
__inline
#else
#ifdef __cplusplus
inline
#endif
#endif
static unsigned int
hash_errno_name (register const char *str, register size_t len)
{
  static const unsigned short asso_values[] =
    {
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
       30, 110, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327,  25,  70,  65,  10,  10,
      105,  35,  30,   5, 327, 155,  50,  80,   0,   0,
       30, 175,   5,  35,  20, 160,  25, 105, 115,  90,
      327, 327, 327, 327, 327, 327, 327,  25,  70,  65,
       10,  10, 105,  35,  30,   5, 327, 155,  50,  80,
        0,   0,  30, 175,   5,  35,  20, 160,  25, 105,
      115,  90, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327, 327, 327, 327, 327,
      327, 327, 327, 327, 327, 327
    };
  register unsigned int hval = len;

  switch (hval)
    {
      default:
        hval += asso_values[(unsigned char)str[5]];
      /*FALLTHROUGH*/
      case 5:
        hval += asso_values[(unsigned char)str[4]];
      /*FALLTHROUGH*/
      case 4:
      case 3:
        hval += asso_values[(unsigned char)str[2]];
      /*FALLTHROUGH*/
      case 2:
        hval += asso_values[(unsigned char)str[1]];
        break;
    }
  return hval;
}

const struct errno_name *
lookup_errno (register const char *str, register size_t len)
{
  static const struct errno_name wordlist[] =
    {
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0},
#line 119 "src/basic/errno-from-name.gperf"
      {"ENOANO", ENOANO},
      {(char*)0},
#line 97 "src/basic/errno-from-name.gperf"
      {"EIO", EIO},
      {(char*)0},
#line 43 "src/basic/errno-from-name.gperf"
      {"ENOSR", ENOSR},
      {(char*)0},
#line 38 "src/basic/errno-from-name.gperf"
      {"ENOLINK", ENOLINK},
      {(char*)0},
#line 105 "src/basic/errno-from-name.gperf"
      {"EDOM", EDOM},
#line 37 "src/basic/errno-from-name.gperf"
      {"EINTR", EINTR},
#line 122 "src/basic/errno-from-name.gperf"
      {"ENOPROTOOPT", ENOPROTOOPT},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
#line 28 "src/basic/errno-from-name.gperf"
      {"EINPROGRESS", EINPROGRESS},
#line 84 "src/basic/errno-from-name.gperf"
      {"ENOTDIR", ENOTDIR},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 136 "src/basic/errno-from-name.gperf"
      {"ENOENT", ENOENT},
#line 92 "src/basic/errno-from-name.gperf"
      {"EDOTDOT", EDOTDOT},
#line 41 "src/basic/errno-from-name.gperf"
      {"ENETDOWN", ENETDOWN},
#line 69 "src/basic/errno-from-name.gperf"
      {"ENOMEDIUM", ENOMEDIUM},
#line 96 "src/basic/errno-from-name.gperf"
      {"ENOTRECOVERABLE", ENOTRECOVERABLE},
#line 88 "src/basic/errno-from-name.gperf"
      {"ENOSTR", ENOSTR},
#line 54 "src/basic/errno-from-name.gperf"
      {"ENOTNAM", ENOTNAM},
      {(char*)0},
#line 58 "src/basic/errno-from-name.gperf"
      {"ENETRESET", ENETRESET},
      {(char*)0},
#line 51 "src/basic/errno-from-name.gperf"
      {"ENONET", ENONET},
      {(char*)0}, {(char*)0},
#line 23 "src/basic/errno-from-name.gperf"
      {"EADV", EADV},
#line 94 "src/basic/errno-from-name.gperf"
      {"ETIME", ETIME},
#line 76 "src/basic/errno-from-name.gperf"
      {"ENODEV", ENODEV},
#line 49 "src/basic/errno-from-name.gperf"
      {"EREMOTE", EREMOTE},
#line 61 "src/basic/errno-from-name.gperf"
      {"ENOTSOCK", ENOTSOCK},
#line 101 "src/basic/errno-from-name.gperf"
      {"EREMOTEIO", EREMOTEIO},
#line 78 "src/basic/errno-from-name.gperf"
      {"EROFS", EROFS},
#line 56 "src/basic/errno-from-name.gperf"
      {"ENOCSI", ENOCSI},
#line 120 "src/basic/errno-from-name.gperf"
      {"EISCONN", EISCONN},
      {(char*)0}, {(char*)0},
#line 45 "src/basic/errno-from-name.gperf"
      {"EPIPE", EPIPE},
      {(char*)0},
#line 36 "src/basic/errno-from-name.gperf"
      {"ENODATA", ENODATA},
#line 93 "src/basic/errno-from-name.gperf"
      {"EADDRNOTAVAIL", EADDRNOTAVAIL},
#line 35 "src/basic/errno-from-name.gperf"
      {"ETIMEDOUT", ETIMEDOUT},
#line 57 "src/basic/errno-from-name.gperf"
      {"EADDRINUSE", EADDRINUSE},
#line 59 "src/basic/errno-from-name.gperf"
      {"EISDIR", EISDIR},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 141 "src/basic/errno-from-name.gperf"
      {"EOPNOTSUPP", EOPNOTSUPP},
#line 75 "src/basic/errno-from-name.gperf"
      {"EPROTO", EPROTO},
#line 71 "src/basic/errno-from-name.gperf"
      {"ENAVAIL", ENAVAIL},
      {(char*)0}, {(char*)0},
#line 30 "src/basic/errno-from-name.gperf"
      {"EPROTOTYPE", EPROTOTYPE},
#line 116 "src/basic/errno-from-name.gperf"
      {"ESRMNT", ESRMNT},
#line 89 "src/basic/errno-from-name.gperf"
      {"ENAMETOOLONG", ENAMETOOLONG},
#line 31 "src/basic/errno-from-name.gperf"
      {"ERESTART", ERESTART},
#line 109 "src/basic/errno-from-name.gperf"
      {"EHOSTDOWN", EHOSTDOWN},
#line 95 "src/basic/errno-from-name.gperf"
      {"EPROTONOSUPPORT", EPROTONOSUPPORT},
#line 132 "src/basic/errno-from-name.gperf"
      {"EAGAIN", EAGAIN},
      {(char*)0},
#line 113 "src/basic/errno-from-name.gperf"
      {"ENOTCONN", ENOTCONN},
      {(char*)0},
#line 107 "src/basic/errno-from-name.gperf"
      {"ESRCH", ESRCH},
#line 33 "src/basic/errno-from-name.gperf"
      {"ENOMSG", ENOMSG},
#line 14 "src/basic/errno-from-name.gperf"
      {"EDESTADDRREQ", EDESTADDRREQ},
      {(char*)0}, {(char*)0},
#line 85 "src/basic/errno-from-name.gperf"
      {"ECONNRESET", ECONNRESET},
#line 24 "src/basic/errno-from-name.gperf"
      {"ERANGE", ERANGE},
#line 131 "src/basic/errno-from-name.gperf"
      {"ECONNREFUSED", ECONNREFUSED},
      {(char*)0}, {(char*)0},
#line 40 "src/basic/errno-from-name.gperf"
      {"ELOOP", ELOOP},
#line 67 "src/basic/errno-from-name.gperf"
      {"EINVAL", EINVAL},
#line 82 "src/basic/errno-from-name.gperf"
      {"EDEADLK", EDEADLK},
      {(char*)0},
#line 117 "src/basic/errno-from-name.gperf"
      {"EDEADLOCK", EDEADLOCK},
      {(char*)0},
#line 44 "src/basic/errno-from-name.gperf"
      {"ELNRNG", ELNRNG},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
#line 27 "src/basic/errno-from-name.gperf"
      {"ENOMEM", ENOMEM},
      {(char*)0},
#line 130 "src/basic/errno-from-name.gperf"
      {"ESTRPIPE", ESTRPIPE},
#line 53 "src/basic/errno-from-name.gperf"
      {"ENOTEMPTY", ENOTEMPTY},
#line 60 "src/basic/errno-from-name.gperf"
      {"EIDRM", EIDRM},
#line 102 "src/basic/errno-from-name.gperf"
      {"ENOSPC", ENOSPC},
#line 118 "src/basic/errno-from-name.gperf"
      {"ECONNABORTED", ECONNABORTED},
      {(char*)0},
#line 68 "src/basic/errno-from-name.gperf"
      {"ESHUTDOWN", ESHUTDOWN},
#line 22 "src/basic/errno-from-name.gperf"
      {"EBADR", EBADR},
#line 66 "src/basic/errno-from-name.gperf"
      {"ENOKEY", ENOKEY},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 20 "src/basic/errno-from-name.gperf"
      {"EBADE", EBADE},
#line 16 "src/basic/errno-from-name.gperf"
      {"ESPIPE", ESPIPE},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
#line 19 "src/basic/errno-from-name.gperf"
      {"ENOTTY", ENOTTY},
#line 12 "src/basic/errno-from-name.gperf"
      {"EREMCHG", EREMCHG},
#line 34 "src/basic/errno-from-name.gperf"
      {"EALREADY", EALREADY},
      {(char*)0},
#line 86 "src/basic/errno-from-name.gperf"
      {"ENXIO", ENXIO},
#line 42 "src/basic/errno-from-name.gperf"
      {"ESTALE", ESTALE},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 39 "src/basic/errno-from-name.gperf"
      {"EPERM", EPERM},
      {(char*)0},
#line 29 "src/basic/errno-from-name.gperf"
      {"ENOTBLK", ENOTBLK},
      {(char*)0}, {(char*)0},
#line 18 "src/basic/errno-from-name.gperf"
      {"EOWNERDEAD", EOWNERDEAD},
#line 112 "src/basic/errno-from-name.gperf"
      {"ENOSYS", ENOSYS},
#line 103 "src/basic/errno-from-name.gperf"
      {"ENOEXEC", ENOEXEC},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 108 "src/basic/errno-from-name.gperf"
      {"ECHRNG", ECHRNG},
#line 50 "src/basic/errno-from-name.gperf"
      {"ETOOMANYREFS", ETOOMANYREFS},
      {(char*)0}, {(char*)0},
#line 81 "src/basic/errno-from-name.gperf"
      {"E2BIG", E2BIG},
#line 13 "src/basic/errno-from-name.gperf"
      {"EACCES", EACCES},
      {(char*)0}, {(char*)0},
#line 72 "src/basic/errno-from-name.gperf"
      {"EOVERFLOW", EOVERFLOW},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
#line 80 "src/basic/errno-from-name.gperf"
      {"EHWPOISON", EHWPOISON},
#line 123 "src/basic/errno-from-name.gperf"
      {"ECOMM", ECOMM},
#line 32 "src/basic/errno-from-name.gperf"
      {"EISNAM", EISNAM},
#line 79 "src/basic/errno-from-name.gperf"
      {"ELIBACC", ELIBACC},
      {(char*)0}, {(char*)0},
#line 99 "src/basic/errno-from-name.gperf"
      {"EXDEV", EXDEV},
#line 65 "src/basic/errno-from-name.gperf"
      {"EL2HLT", EL2HLT},
#line 129 "src/basic/errno-from-name.gperf"
      {"ELIBBAD", ELIBBAD},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 46 "src/basic/errno-from-name.gperf"
      {"ECHILD", ECHILD},
#line 70 "src/basic/errno-from-name.gperf"
      {"ELIBSCN", ELIBSCN},
#line 104 "src/basic/errno-from-name.gperf"
      {"EMSGSIZE", EMSGSIZE},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 133 "src/basic/errno-from-name.gperf"
      {"ELIBMAX", ELIBMAX},
#line 140 "src/basic/errno-from-name.gperf"
      {"ENOTUNIQ", ENOTUNIQ},
      {(char*)0}, {(char*)0},
#line 111 "src/basic/errno-from-name.gperf"
      {"ENFILE", ENFILE},
#line 125 "src/basic/errno-from-name.gperf"
      {"ERFKILL", ERFKILL},
      {(char*)0},
#line 25 "src/basic/errno-from-name.gperf"
      {"ECANCELED", ECANCELED},
      {(char*)0},
#line 128 "src/basic/errno-from-name.gperf"
      {"EWOULDBLOCK", EWOULDBLOCK},
#line 11 "src/basic/errno-from-name.gperf"
      {"EAFNOSUPPORT", EAFNOSUPPORT},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 98 "src/basic/errno-from-name.gperf"
      {"ENETUNREACH", ENETUNREACH},
#line 114 "src/basic/errno-from-name.gperf"
      {"EPFNOSUPPORT", EPFNOSUPPORT},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 134 "src/basic/errno-from-name.gperf"
      {"EEXIST", EEXIST},
#line 138 "src/basic/errno-from-name.gperf"
      {"EBADSLT", EBADSLT},
#line 91 "src/basic/errno-from-name.gperf"
      {"ELIBEXEC", ELIBEXEC},
      {(char*)0}, {(char*)0},
#line 139 "src/basic/errno-from-name.gperf"
      {"EKEYREVOKED", EKEYREVOKED},
#line 55 "src/basic/errno-from-name.gperf"
      {"EKEYREJECTED", EKEYREJECTED},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 137 "src/basic/errno-from-name.gperf"
      {"ENOPKG", ENOPKG},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
#line 48 "src/basic/errno-from-name.gperf"
      {"EBFONT", EBFONT},
#line 115 "src/basic/errno-from-name.gperf"
      {"ENOTSUP", ENOTSUP},
      {(char*)0}, {(char*)0},
#line 21 "src/basic/errno-from-name.gperf"
      {"EBADF", EBADF},
#line 127 "src/basic/errno-from-name.gperf"
      {"EFAULT", EFAULT},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
#line 100 "src/basic/errno-from-name.gperf"
      {"EDQUOT", EDQUOT},
      {(char*)0},
#line 135 "src/basic/errno-from-name.gperf"
      {"EL2NSYNC", EL2NSYNC},
      {(char*)0},
#line 106 "src/basic/errno-from-name.gperf"
      {"EFBIG", EFBIG},
#line 63 "src/basic/errno-from-name.gperf"
      {"EBADFD", EBADFD},
#line 47 "src/basic/errno-from-name.gperf"
      {"EBADMSG", EBADMSG},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 83 "src/basic/errno-from-name.gperf"
      {"EL3RST", EL3RST},
#line 62 "src/basic/errno-from-name.gperf"
      {"EHOSTUNREACH", EHOSTUNREACH},
      {(char*)0}, {(char*)0},
#line 90 "src/basic/errno-from-name.gperf"
      {"ESOCKTNOSUPPORT", ESOCKTNOSUPPORT},
#line 110 "src/basic/errno-from-name.gperf"
      {"ENOLCK", ENOLCK},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0},
#line 64 "src/basic/errno-from-name.gperf"
      {"EL3HLT", EL3HLT},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
#line 121 "src/basic/errno-from-name.gperf"
      {"EUSERS", EUSERS},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
#line 15 "src/basic/errno-from-name.gperf"
      {"EILSEQ", EILSEQ},
#line 26 "src/basic/errno-from-name.gperf"
      {"ETXTBSY", ETXTBSY},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 124 "src/basic/errno-from-name.gperf"
      {"EMFILE", EMFILE},
#line 10 "src/basic/errno-from-name.gperf"
      {"EUNATCH", EUNATCH},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0},
#line 142 "src/basic/errno-from-name.gperf"
      {"EMEDIUMTYPE", EMEDIUMTYPE},
#line 73 "src/basic/errno-from-name.gperf"
      {"EUCLEAN", EUCLEAN},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
#line 126 "src/basic/errno-from-name.gperf"
      {"ENOBUFS", ENOBUFS},
      {(char*)0},
#line 9 "src/basic/errno-from-name.gperf"
      {"EMULTIHOP", EMULTIHOP},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 87 "src/basic/errno-from-name.gperf"
      {"EBADRQC", EBADRQC},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
#line 17 "src/basic/errno-from-name.gperf"
      {"EMLINK", EMLINK},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0},
#line 77 "src/basic/errno-from-name.gperf"
      {"EKEYEXPIRED", EKEYEXPIRED},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 74 "src/basic/errno-from-name.gperf"
      {"EBUSY", EBUSY},
#line 52 "src/basic/errno-from-name.gperf"
      {"EXFULL", EXFULL}
    };

  if (len <= MAX_WORD_LENGTH && len >= MIN_WORD_LENGTH)
    {
      register unsigned int key = hash_errno_name (str, len);

      if (key <= MAX_HASH_VALUE)
        {
          register const char *s = wordlist[key].name;

          if (s && (((unsigned char)*str ^ (unsigned char)*s) & ~32) == 0 && !gperf_case_strcmp (str, s))
            return &wordlist[key];
        }
    }
  return 0;
}
