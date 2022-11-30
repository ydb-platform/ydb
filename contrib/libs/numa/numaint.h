/* Internal interfaces of libnuma */

extern int numa_sched_setaffinity_v1(pid_t pid, unsigned len, const unsigned long *mask);
extern int numa_sched_getaffinity_v1(pid_t pid, unsigned len, const unsigned long *mask);
extern int numa_sched_setaffinity_v1_int(pid_t pid, unsigned len,const unsigned long *mask);
extern int numa_sched_getaffinity_v1_int(pid_t pid, unsigned len,const unsigned long *mask);
extern int numa_sched_setaffinity_v2(pid_t pid, struct bitmask *mask);
extern int numa_sched_getaffinity_v2(pid_t pid, struct bitmask *mask);
extern int numa_sched_setaffinity_v2_int(pid_t pid, struct bitmask *mask);
extern int numa_sched_getaffinity_v2_int(pid_t pid, struct bitmask *mask);

#define SHM_HUGETLB     04000   /* segment will use huge TLB pages */

#define BITS_PER_LONG (sizeof(unsigned long) * 8)
#define CPU_BYTES(x) (round_up(x, BITS_PER_LONG)/8)
#define CPU_LONGS(x) (CPU_BYTES(x) / sizeof(long))

#define make_internal_alias(x) extern __typeof (x) x##_int __attribute((alias(#x), visibility("hidden")))
#define hidden __attribute__((visibility("hidden")))

enum numa_warn {
	W_nosysfs,
	W_noproc,
	W_badmeminfo,
	W_nosysfs2,
	W_cpumap,
	W_numcpus,
	W_noderunmask,
	W_distance,
	W_memory,
	W_cpuparse,
	W_nodeparse,
	W_blockdev1,
	W_blockdev2,
	W_blockdev3,
	W_blockdev4,
	W_blockdev5,
	W_netlink1,
	W_netlink2,
	W_netlink3,
	W_net1,
	W_net2,
	W_class1,
	W_class2,
	W_pci1,
	W_pci2,
	W_node_parse1,
	W_node_parse2,
	W_nonode,
	W_badchar,
};

#define howmany(x,y) (((x)+((y)-1))/(y))
#define bitsperlong (8 * sizeof(unsigned long))
#define bitsperint (8 * sizeof(unsigned int))
#define longsperbits(n) howmany(n, bitsperlong)
#define bytesperbits(x) ((x+7)/8)
