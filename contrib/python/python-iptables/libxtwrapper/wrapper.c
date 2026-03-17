#include <errno.h>
#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/utsname.h>

int kernel_version;

#define LINUX_VERSION(x,y,z)	(0x10000*(x) + 0x100*(y) + z)
void get_kernel_version(void)
{
	static struct utsname uts;
	int x = 0, y = 0, z = 0;

	if (uname(&uts) == -1) {
		fprintf(stderr, "Unable to retrieve kernel version.\n");
        return;
	}

	sscanf(uts.release, "%d.%d.%d", &x, &y, &z);
	kernel_version = LINUX_VERSION(x, y, z);
}

static jmp_buf env;

void throw_exception(int err)
{
    longjmp(env, err);
}

int wrap_parse(int (*fn)(int, char **, int, unsigned int *, void *, void **),
               int i, char **argv, int inv, unsigned int *flags, void *p,
               void **mptr)
{
    int rv = -1;
    int err;

    if ((err = setjmp(env)) == 0) {
        rv = fn(i, argv, inv, flags, p, mptr);
    } else {
        errno = err;
    }

    return rv;
}

struct ipt_ip;
void wrap_save(int (*fn)(const void *, const void *),
               const void *ip, const void *m)
{
    fn(ip, m);
    fprintf(stdout, "\n"); /* make sure something is written to stdout */
    fflush(stdout);
}

int wrap_x6fn(void (*fn)(void *), void *data)
{
    int err;

    if ((err = setjmp(env)) == 0) {
        fn(data);
    } else {
        errno = err;
        return -err;
    }

    return 0;
}

int wrap_uintfn(void (*fn)(unsigned int), unsigned int data)
{
    int err;

    if ((err = setjmp(env)) == 0) {
        fn(data);
    } else {
        errno = err;
        return -err;
    }

    return 0;
}

int wrap_voidfn(void (*fn)(void))
{
    int err;

    if ((err = setjmp(env)) == 0) {
        fn();
    } else {
        errno = err;
        return -err;
    }

    return 0;
}
