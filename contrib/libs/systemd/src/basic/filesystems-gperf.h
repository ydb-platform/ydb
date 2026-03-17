/* ANSI-C code produced by gperf version 3.1 */
/* Command-line: /var/empty/gperf-3.1/bin/gperf --output-file src/basic/filesystems-gperf.h ../src/basic/filesystems-gperf.gperf  */
/* Computed positions: -k'1-2,$' */

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

#line 2 "../src/basic/filesystems-gperf.gperf"

#include <linux/magic.h>

#include "filesystems.h"
#include "missing_magic.h"
#include "stat-util.h"

struct FilesystemMagic {
        const char *name;
        statfs_f_type_t magic[FILESYSTEM_MAGIC_MAX];
};
#include <string.h>

#define TOTAL_KEYWORDS 94
#define MIN_WORD_LENGTH 3
#define MAX_WORD_LENGTH 13
#define MIN_HASH_VALUE 3
#define MAX_HASH_VALUE 189
/* maximum key range = 187, duplicates = 0 */

#ifdef __GNUC__
__inline
#else
#ifdef __cplusplus
inline
#endif
#endif
static unsigned int
filesystems_gperf_hash (register const char *str, register size_t len)
{
  static const unsigned char asso_values[] =
    {
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190,  15, 190,
        5,  95,  80, 190,  20, 190, 190, 190,   0, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190,  40,  35,  10,
        5,  25,  10,   0,  90,   0,  65,   5,   0,  10,
        5,  50,   0,   0,  45,   0,  25,  55,  20,  15,
       65,  65,  95, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190, 190, 190, 190,
      190, 190, 190, 190, 190, 190, 190
    };
  return len + asso_values[(unsigned char)str[1]+1] + asso_values[(unsigned char)str[0]] + asso_values[(unsigned char)str[len - 1]];
}

const struct FilesystemMagic *
filesystems_gperf_lookup (register const char *str, register size_t len)
{
  static const struct FilesystemMagic wordlist[] =
    {
      {""}, {""}, {""},
#line 69 "../src/basic/filesystems-gperf.gperf"
      {"gfs",             {GFS2_MAGIC}},
      {""}, {""},
#line 115 "../src/basic/filesystems-gperf.gperf"
      {"sockfs",          {SOCKFS_MAGIC}},
#line 109 "../src/basic/filesystems-gperf.gperf"
      {"shiftfs",         {SHIFTFS_MAGIC}},
#line 84 "../src/basic/filesystems-gperf.gperf"
      {"nfs",             {NFS_SUPER_MAGIC}},
#line 70 "../src/basic/filesystems-gperf.gperf"
      {"gfs2",            {GFS2_MAGIC}},
#line 114 "../src/basic/filesystems-gperf.gperf"
      {"smbfs",           {SMB_SUPER_MAGIC}},
      {""},
#line 110 "../src/basic/filesystems-gperf.gperf"
      {"smackfs",         {SMACK_MAGIC}},
#line 81 "../src/basic/filesystems-gperf.gperf"
      {"ncp",             {NCP_SUPER_MAGIC}},
#line 96 "../src/basic/filesystems-gperf.gperf"
      {"proc",            {PROC_SUPER_MAGIC}},
#line 82 "../src/basic/filesystems-gperf.gperf"
      {"ncpfs",           {NCP_SUPER_MAGIC}},
#line 46 "../src/basic/filesystems-gperf.gperf"
      {"cramfs",          {CRAMFS_MAGIC}},
#line 95 "../src/basic/filesystems-gperf.gperf"
      {"ppc-cmm",         {PPC_CMM_MAGIC}},
#line 45 "../src/basic/filesystems-gperf.gperf"
      {"configfs",        {CONFIGFS_MAGIC}},
#line 108 "../src/basic/filesystems-gperf.gperf"
      {"selinuxfs",       {SELINUX_MAGIC}},
#line 107 "../src/basic/filesystems-gperf.gperf"
      {"securityfs",      {SECURITYFS_MAGIC}},
#line 50 "../src/basic/filesystems-gperf.gperf"
      {"devpts",          {DEVPTS_SUPER_MAGIC}},
#line 48 "../src/basic/filesystems-gperf.gperf"
      {"debugfs",         {DEBUGFS_MAGIC}},
#line 52 "../src/basic/filesystems-gperf.gperf"
      {"devtmpfs",        {TMPFS_MAGIC}},
#line 126 "../src/basic/filesystems-gperf.gperf"
      {"v9fs",            {V9FS_MAGIC}},
#line 99 "../src/basic/filesystems-gperf.gperf"
      {"pvfs2",           {ORANGEFS_DEVREQ_MAGIC}},
#line 53 "../src/basic/filesystems-gperf.gperf"
      {"dmabuf",          {DMA_BUF_MAGIC}},
      {""},
#line 56 "../src/basic/filesystems-gperf.gperf"
      {"efs",             {EFS_SUPER_MAGIC}},
#line 106 "../src/basic/filesystems-gperf.gperf"
      {"secretmem",       {SECRETMEM_MAGIC}},
#line 57 "../src/basic/filesystems-gperf.gperf"
      {"erofs",           {EROFS_SUPER_MAGIC_V1}},
#line 49 "../src/basic/filesystems-gperf.gperf"
      {"devmem",          {DEVMEM_MAGIC}},
#line 120 "../src/basic/filesystems-gperf.gperf"
      {"tracefs",         {TRACEFS_MAGIC}},
#line 55 "../src/basic/filesystems-gperf.gperf"
      {"efivarfs",        {EFIVARFS_MAGIC}},
#line 87 "../src/basic/filesystems-gperf.gperf"
      {"nsfs",            {NSFS_MAGIC}},
#line 119 "../src/basic/filesystems-gperf.gperf"
      {"tmpfs",           {TMPFS_MAGIC}},
      {""},
#line 67 "../src/basic/filesystems-gperf.gperf"
      {"fusectl",         {FUSE_CTL_SUPER_MAGIC}},
#line 54 "../src/basic/filesystems-gperf.gperf"
      {"ecryptfs",        {ECRYPTFS_SUPER_MAGIC}},
      {""},
#line 79 "../src/basic/filesystems-gperf.gperf"
      {"msdos",           {MSDOS_SUPER_MAGIC}},
#line 38 "../src/basic/filesystems-gperf.gperf"
      {"cpuset",          {CGROUP_SUPER_MAGIC}},
#line 65 "../src/basic/filesystems-gperf.gperf"
      {"fuseblk",         {FUSE_SUPER_MAGIC}},
#line 27 "../src/basic/filesystems-gperf.gperf"
      {"afs",             {AFS_FS_MAGIC, AFS_SUPER_MAGIC}},
#line 26 "../src/basic/filesystems-gperf.gperf"
      {"affs",            {AFFS_SUPER_MAGIC}},
      {""},
#line 123 "../src/basic/filesystems-gperf.gperf"
      {"vboxsf",          {VBOXSF_SUPER_MAGIC}},
#line 74 "../src/basic/filesystems-gperf.gperf"
      {"iso9660",         {ISOFS_SUPER_MAGIC}},
#line 34 "../src/basic/filesystems-gperf.gperf"
      {"bpf",             {BPF_FS_MAGIC}},
#line 125 "../src/basic/filesystems-gperf.gperf"
      {"vfat",            {MSDOS_SUPER_MAGIC}},
#line 24 "../src/basic/filesystems-gperf.gperf"
      {"apparmorfs",      {AAFS_MAGIC}},
      {""}, {""},
#line 116 "../src/basic/filesystems-gperf.gperf"
      {"squashfs",        {SQUASHFS_MAGIC}},
#line 44 "../src/basic/filesystems-gperf.gperf"
      {"coda",            {CODA_SUPER_MAGIC}},
#line 105 "../src/basic/filesystems-gperf.gperf"
      {"rpc_pipefs",      {RPC_PIPEFS_SUPER_MAGIC}},
#line 97 "../src/basic/filesystems-gperf.gperf"
      {"pstore",          {PSTOREFS_MAGIC}},
      {""},
#line 92 "../src/basic/filesystems-gperf.gperf"
      {"orangefs",        {ORANGEFS_DEVREQ_MAGIC}},
#line 66 "../src/basic/filesystems-gperf.gperf"
      {"fuse",            {FUSE_SUPER_MAGIC}},
#line 91 "../src/basic/filesystems-gperf.gperf"
      {"openpromfs",      {OPENPROM_SUPER_MAGIC}},
      {""},
#line 103 "../src/basic/filesystems-gperf.gperf"
      {"resctrl",         {RDTGROUP_SUPER_MAGIC}},
#line 104 "../src/basic/filesystems-gperf.gperf"
      {"reiserfs",        {REISERFS_SUPER_MAGIC}},
#line 88 "../src/basic/filesystems-gperf.gperf"
      {"ntfs",            {NTFS_SB_MAGIC}},
#line 90 "../src/basic/filesystems-gperf.gperf"
      {"ocfs2",           {OCFS2_SUPER_MAGIC}},
#line 29 "../src/basic/filesystems-gperf.gperf"
      {"autofs",          {AUTOFS_SUPER_MAGIC}},
      {""},
#line 128 "../src/basic/filesystems-gperf.gperf"
      {"xfs",             {XFS_SUPER_MAGIC}},
#line 25 "../src/basic/filesystems-gperf.gperf"
      {"adfs",            {ADFS_SUPER_MAGIC}},
      {""},
#line 94 "../src/basic/filesystems-gperf.gperf"
      {"pipefs",          {PIPEFS_MAGIC}},
      {""}, {""},
#line 101 "../src/basic/filesystems-gperf.gperf"
      {"qnx6",            {QNX6_SUPER_MAGIC}},
#line 75 "../src/basic/filesystems-gperf.gperf"
      {"jffs2",           {JFFS2_SUPER_MAGIC}},
      {""}, {""}, {""},
#line 43 "../src/basic/filesystems-gperf.gperf"
      {"cifs",            {CIFS_SUPER_MAGIC, SMB2_SUPER_MAGIC}},
#line 127 "../src/basic/filesystems-gperf.gperf"
      {"xenfs",           {XENFS_SUPER_MAGIC}},
#line 86 "../src/basic/filesystems-gperf.gperf"
      {"nilfs2",          {NILFS_SUPER_MAGIC}},
      {""}, {""},
#line 31 "../src/basic/filesystems-gperf.gperf"
      {"bdev",            {BDEVFS_MAGIC}},
#line 102 "../src/basic/filesystems-gperf.gperf"
      {"ramfs",           {RAMFS_MAGIC}},
#line 77 "../src/basic/filesystems-gperf.gperf"
      {"mqueue",          {MQUEUE_MAGIC}},
      {""},
#line 122 "../src/basic/filesystems-gperf.gperf"
      {"usbdevfs",        {USBDEVICE_SUPER_MAGIC}},
#line 85 "../src/basic/filesystems-gperf.gperf"
      {"nfs4",            {NFS_SUPER_MAGIC}},
      {""},
#line 30 "../src/basic/filesystems-gperf.gperf"
      {"balloon-kvm",     {BALLOON_KVM_MAGIC}},
      {""},
#line 121 "../src/basic/filesystems-gperf.gperf"
      {"udf",             {UDF_SUPER_MAGIC}},
#line 72 "../src/basic/filesystems-gperf.gperf"
      {"hpfs",            {HPFS_SUPER_MAGIC}},
#line 35 "../src/basic/filesystems-gperf.gperf"
      {"btrfs",           {BTRFS_SUPER_MAGIC}},
#line 71 "../src/basic/filesystems-gperf.gperf"
      {"hostfs",          {HOSTFS_SUPER_MAGIC}},
      {""}, {""},
#line 59 "../src/basic/filesystems-gperf.gperf"
      {"ext2",            {EXT2_SUPER_MAGIC}},
#line 117 "../src/basic/filesystems-gperf.gperf"
      {"sysfs",           {SYSFS_MAGIC}},
#line 130 "../src/basic/filesystems-gperf.gperf"
      {"zonefs",          {ZONEFS_MAGIC}},
#line 28 "../src/basic/filesystems-gperf.gperf"
      {"anon_inodefs",    {ANON_INODE_FS_MAGIC}},
#line 36 "../src/basic/filesystems-gperf.gperf"
      {"btrfs_test_fs",   {BTRFS_TEST_MAGIC}},
#line 112 "../src/basic/filesystems-gperf.gperf"
      {"smb3",            {CIFS_SUPER_MAGIC}},
      {""},
#line 42 "../src/basic/filesystems-gperf.gperf"
      {"cgroup",          {CGROUP_SUPER_MAGIC}},
      {""},
#line 47 "../src/basic/filesystems-gperf.gperf"
      {"dax",             {DAXFS_MAGIC}},
#line 63 "../src/basic/filesystems-gperf.gperf"
      {"f2fs",            {F2FS_SUPER_MAGIC}},
      {""}, {""},
#line 40 "../src/basic/filesystems-gperf.gperf"
      {"cgroup2",         {CGROUP2_SUPER_MAGIC}},
      {""},
#line 39 "../src/basic/filesystems-gperf.gperf"
      {"ceph",            {CEPH_SUPER_MAGIC}},
      {""}, {""}, {""}, {""},
#line 73 "../src/basic/filesystems-gperf.gperf"
      {"hugetlbfs",       {HUGETLBFS_MAGIC}},
#line 62 "../src/basic/filesystems-gperf.gperf"
      {"exfat",           {EXFAT_SUPER_MAGIC}},
#line 33 "../src/basic/filesystems-gperf.gperf"
      {"binfmt_misc",     {BINFMTFS_MAGIC}},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 100 "../src/basic/filesystems-gperf.gperf"
      {"qnx4",            {QNX4_SUPER_MAGIC}},
      {""}, {""},
#line 93 "../src/basic/filesystems-gperf.gperf"
      {"overlay",         {OVERLAYFS_SUPER_MAGIC}},
#line 131 "../src/basic/filesystems-gperf.gperf"
      {"zsmalloc",        {ZSMALLOC_MAGIC}},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 76 "../src/basic/filesystems-gperf.gperf"
      {"minix",           {MINIX_SUPER_MAGIC, MINIX_SUPER_MAGIC2, MINIX2_SUPER_MAGIC, MINIX2_SUPER_MAGIC2, MINIX3_SUPER_MAGIC}},
      {""}, {""}, {""}, {""}, {""},
#line 32 "../src/basic/filesystems-gperf.gperf"
      {"binder",          {BINDERFS_SUPER_MAGIC}},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 89 "../src/basic/filesystems-gperf.gperf"
      {"ntfs3",           {NTFS3_SUPER_MAGIC}},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 61 "../src/basic/filesystems-gperf.gperf"
      {"ext4",            {EXT4_SUPER_MAGIC}},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 129 "../src/basic/filesystems-gperf.gperf"
      {"z3fold",          {Z3FOLD_MAGIC}},
      {""}, {""},
#line 60 "../src/basic/filesystems-gperf.gperf"
      {"ext3",            {EXT3_SUPER_MAGIC}}
    };

  if (len <= MAX_WORD_LENGTH && len >= MIN_WORD_LENGTH)
    {
      register unsigned int key = filesystems_gperf_hash (str, len);

      if (key <= MAX_HASH_VALUE)
        {
          register const char *s = wordlist[key].name;

          if (*str == *s && !strcmp (str + 1, s + 1))
            return &wordlist[key];
        }
    }
  return 0;
}
