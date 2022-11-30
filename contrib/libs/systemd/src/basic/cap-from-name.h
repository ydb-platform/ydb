/* ANSI-C code produced by gperf version 3.1 */
/* Command-line: /var/empty/gperf-3.1/bin/gperf -L ANSI-C -t --ignore-case -N lookup_capability -H hash_capability_name -p -C src/basic/cap-from-name.gperf  */
/* Computed positions: -k'5,8-9' */

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

#line 1 "src/basic/cap-from-name.gperf"

#if __GNUC__ >= 7
_Pragma("GCC diagnostic ignored \"-Wimplicit-fallthrough\"")
#endif
#line 6 "src/basic/cap-from-name.gperf"
struct capability_name { const char* name; int id; };

#define TOTAL_KEYWORDS 38
#define MIN_WORD_LENGTH 8
#define MAX_WORD_LENGTH 20
#define MIN_HASH_VALUE 8
#define MAX_HASH_VALUE 61
/* maximum key range = 54, duplicates = 0 */

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
hash_capability_name (register const char *str, register size_t len)
{
  static const unsigned char asso_values[] =
    {
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62,  5, 20, 20,  0,  0,
       0, 20, 62,  0, 62,  0,  0, 15, 10, 25,
      30, 62,  0,  0,  5,  0, 62,  0, 35, 62,
      62, 62, 62, 62, 62,  0, 62,  5, 20, 20,
       0,  0,  0, 20, 62,  0, 62,  0,  0, 15,
      10, 25, 30, 62,  0,  0,  5,  0, 62,  0,
      35, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
      62, 62, 62, 62, 62, 62
    };
  register unsigned int hval = len;

  switch (hval)
    {
      default:
        hval += asso_values[(unsigned char)str[8]];
      /*FALLTHROUGH*/
      case 8:
        hval += asso_values[(unsigned char)str[7]];
      /*FALLTHROUGH*/
      case 7:
      case 6:
      case 5:
        hval += asso_values[(unsigned char)str[4]];
        break;
    }
  return hval;
}

const struct capability_name *
lookup_capability (register const char *str, register size_t len)
{
  static const struct capability_name wordlist[] =
    {
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
      {(char*)0}, {(char*)0}, {(char*)0}, {(char*)0},
#line 26 "src/basic/cap-from-name.gperf"
      {"CAP_KILL", CAP_KILL},
#line 27 "src/basic/cap-from-name.gperf"
      {"CAP_LEASE", CAP_LEASE},
#line 46 "src/basic/cap-from-name.gperf"
      {"CAP_SETUID", CAP_SETUID},
      {(char*)0},
#line 41 "src/basic/cap-from-name.gperf"
      {"CAP_IPC_LOCK", CAP_IPC_LOCK},
#line 23 "src/basic/cap-from-name.gperf"
      {"CAP_SYS_RAWIO", CAP_SYS_RAWIO},
#line 42 "src/basic/cap-from-name.gperf"
      {"CAP_WAKE_ALARM", CAP_WAKE_ALARM},
#line 10 "src/basic/cap-from-name.gperf"
      {"CAP_FSETID", CAP_FSETID},
#line 18 "src/basic/cap-from-name.gperf"
      {"CAP_SYS_RESOURCE", CAP_SYS_RESOURCE},
#line 25 "src/basic/cap-from-name.gperf"
      {"CAP_SYS_TIME", CAP_SYS_TIME},
#line 36 "src/basic/cap-from-name.gperf"
      {"CAP_SYS_ADMIN", CAP_SYS_ADMIN},
#line 31 "src/basic/cap-from-name.gperf"
      {"CAP_DAC_READ_SEARCH", CAP_DAC_READ_SEARCH},
#line 14 "src/basic/cap-from-name.gperf"
      {"CAP_FOWNER", CAP_FOWNER},
#line 34 "src/basic/cap-from-name.gperf"
      {"CAP_NET_RAW", CAP_NET_RAW},
#line 16 "src/basic/cap-from-name.gperf"
      {"CAP_SYS_NICE", CAP_SYS_NICE},
#line 35 "src/basic/cap-from-name.gperf"
      {"CAP_SYS_TTY_CONFIG", CAP_SYS_TTY_CONFIG},
#line 9 "src/basic/cap-from-name.gperf"
      {"CAP_AUDIT_READ", CAP_AUDIT_READ},
#line 30 "src/basic/cap-from-name.gperf"
      {"CAP_AUDIT_WRITE", CAP_AUDIT_WRITE},
      {(char*)0},
#line 21 "src/basic/cap-from-name.gperf"
      {"CAP_AUDIT_CONTROL", CAP_AUDIT_CONTROL},
#line 24 "src/basic/cap-from-name.gperf"
      {"CAP_NET_ADMIN", CAP_NET_ADMIN},
#line 29 "src/basic/cap-from-name.gperf"
      {"CAP_SYS_MODULE", CAP_SYS_MODULE},
#line 17 "src/basic/cap-from-name.gperf"
      {"CAP_SETGID", CAP_SETGID},
#line 20 "src/basic/cap-from-name.gperf"
      {"CAP_SETFCAP", CAP_SETFCAP},
#line 32 "src/basic/cap-from-name.gperf"
      {"CAP_SYS_BOOT", CAP_SYS_BOOT},
#line 43 "src/basic/cap-from-name.gperf"
      {"CAP_MAC_ADMIN", CAP_MAC_ADMIN},
#line 28 "src/basic/cap-from-name.gperf"
      {"CAP_SYS_CHROOT", CAP_SYS_CHROOT},
#line 12 "src/basic/cap-from-name.gperf"
      {"CAP_SYSLOG", CAP_SYSLOG},
      {(char*)0}, {(char*)0},
#line 22 "src/basic/cap-from-name.gperf"
      {"CAP_IPC_OWNER", CAP_IPC_OWNER},
#line 39 "src/basic/cap-from-name.gperf"
      {"CAP_CHOWN", CAP_CHOWN},
      {(char*)0},
#line 45 "src/basic/cap-from-name.gperf"
      {"CAP_DAC_OVERRIDE", CAP_DAC_OVERRIDE},
      {(char*)0},
#line 40 "src/basic/cap-from-name.gperf"
      {"CAP_SYS_PACCT", CAP_SYS_PACCT},
#line 37 "src/basic/cap-from-name.gperf"
      {"CAP_SYS_PTRACE", CAP_SYS_PTRACE},
      {(char*)0}, {(char*)0},
#line 19 "src/basic/cap-from-name.gperf"
      {"CAP_NET_BROADCAST", CAP_NET_BROADCAST},
      {(char*)0},
#line 44 "src/basic/cap-from-name.gperf"
      {"CAP_MKNOD", CAP_MKNOD},
#line 11 "src/basic/cap-from-name.gperf"
      {"CAP_NET_BIND_SERVICE", CAP_NET_BIND_SERVICE},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 13 "src/basic/cap-from-name.gperf"
      {"CAP_LINUX_IMMUTABLE", CAP_LINUX_IMMUTABLE},
      {(char*)0},
#line 38 "src/basic/cap-from-name.gperf"
      {"CAP_MAC_OVERRIDE", CAP_MAC_OVERRIDE},
#line 15 "src/basic/cap-from-name.gperf"
      {"CAP_BLOCK_SUSPEND", CAP_BLOCK_SUSPEND},
      {(char*)0}, {(char*)0}, {(char*)0},
#line 33 "src/basic/cap-from-name.gperf"
      {"CAP_SETPCAP", CAP_SETPCAP}
    };

  if (len <= MAX_WORD_LENGTH && len >= MIN_WORD_LENGTH)
    {
      register unsigned int key = hash_capability_name (str, len);

      if (key <= MAX_HASH_VALUE)
        {
          register const char *s = wordlist[key].name;

          if (s && (((unsigned char)*str ^ (unsigned char)*s) & ~32) == 0 && !gperf_case_strcmp (str, s))
            return &wordlist[key];
        }
    }
  return 0;
}
