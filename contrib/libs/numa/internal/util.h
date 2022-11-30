extern void printmask(char *name, struct bitmask *mask);
extern int find_first(struct bitmask *mask);
extern struct bitmask *nodemask(char *s);
extern struct bitmask *cpumask(char *s, int *ncpus);
extern int read_sysctl(char *name);
extern void complain(char *fmt, ...);
extern void nerror(char *fmt, ...);

extern long memsize(char *s);
extern int parse_policy(char *name, char *arg);
extern void print_policies(void);
extern char *policy_name(int policy);

#define err(x) perror("numactl: " x),exit(1)
#define array_len(x) (sizeof(x)/sizeof(*(x)))

#define round_up(x,y) (((x) + (y) - 1) & ~((y)-1))

#if HAVE_ATTRIBUTE_SYMVER
#define SYMVER(a,b) __attribute__ ((symver (b)))
#else
#define SYMVER(a,b) __asm__ (".symver " a "," b);
#endif
