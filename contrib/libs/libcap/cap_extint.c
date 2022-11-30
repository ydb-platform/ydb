/*
 * Copyright (c) 1997-8 Andrew G Morgan <morgan@kernel.org>
 *
 * This file deals with exchanging internal and external
 * representations of capability sets.
 */

#include "libcap.h"

/*
 * External representation for capabilities. (exported as a fixed
 * length)
 */
#define CAP_EXT_MAGIC "\220\302\001\121"
#define CAP_EXT_MAGIC_SIZE 4
const static __u8 external_magic[CAP_EXT_MAGIC_SIZE+1] = CAP_EXT_MAGIC;

struct cap_ext_struct {
    __u8 magic[CAP_EXT_MAGIC_SIZE];
    __u8 length_of_capset;
    /*
     * note, we arrange these so the caps are stacked with byte-size
     * resolution
     */
    __u8 bytes[CAP_SET_SIZE][NUMBER_OF_CAP_SETS];
};

/*
 * return size of external capability set
 */

ssize_t cap_size(cap_t caps)
{
    return ssizeof(struct cap_ext_struct);
}

/*
 * Copy the internal (cap_d) capability set into an external
 * representation.  The external representation is portable to other
 * Linux architectures.
 */

ssize_t cap_copy_ext(void *cap_ext, cap_t cap_d, ssize_t length)
{
    struct cap_ext_struct *result = (struct cap_ext_struct *) cap_ext;
    int i;

    /* valid arguments? */
    if (!good_cap_t(cap_d) || length < ssizeof(struct cap_ext_struct)
	|| cap_ext == NULL) {
	errno = EINVAL;
	return -1;
    }

    /* fill external capability set */
    memcpy(&result->magic, external_magic, CAP_EXT_MAGIC_SIZE);
    result->length_of_capset = CAP_SET_SIZE;

    for (i=0; i<NUMBER_OF_CAP_SETS; ++i) {
	size_t j;
	for (j=0; j<CAP_SET_SIZE; ) {
	    __u32 val;

	    val = cap_d->u[j/sizeof(__u32)].flat[i];

	    result->bytes[j++][i] =  val        & 0xFF;
	    result->bytes[j++][i] = (val >>= 8) & 0xFF;
	    result->bytes[j++][i] = (val >>= 8) & 0xFF;
	    result->bytes[j++][i] = (val >> 8)  & 0xFF;
	}
    }

    /* All done: return length of external representation */
    return (ssizeof(struct cap_ext_struct));
}

/*
 * Import an external representation to produce an internal rep.
 * the internal rep should be liberated with cap_free().
 */

cap_t cap_copy_int(const void *cap_ext)
{
    const struct cap_ext_struct *export =
	(const struct cap_ext_struct *) cap_ext;
    cap_t cap_d;
    int set, blen;

    /* Does the external representation make sense? */
    if ((export == NULL)
	|| memcmp(export->magic, external_magic, CAP_EXT_MAGIC_SIZE)) {
	errno = EINVAL;
	return NULL;
    }

    /* Obtain a new internal capability set */
    if (!(cap_d = cap_init()))
       return NULL;

    blen = export->length_of_capset;
    for (set=0; set<NUMBER_OF_CAP_SETS; ++set) {
	unsigned blk;
	int bno = 0;
	for (blk=0; blk<(CAP_SET_SIZE/sizeof(__u32)); ++blk) {
	    __u32 val = 0;

	    if (bno != blen)
		val  = export->bytes[bno++][set];
	    if (bno != blen)
		val |= export->bytes[bno++][set] << 8;
	    if (bno != blen)
		val |= export->bytes[bno++][set] << 16;
	    if (bno != blen)
		val |= export->bytes[bno++][set] << 24;

	    cap_d->u[blk].flat[set] = val;
	}
    }

    /* all done */
    return cap_d;
}

