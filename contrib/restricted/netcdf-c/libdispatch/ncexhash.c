/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See LICENSE.txt for license information.
*/

#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "netcdf.h"
#include "ncexhash.h"
#include "nccrc.h"

/* 0 => no debug */
#define DEBUG 0
#undef DEBUGTRACE
#undef CATCH
#define INLINED

#ifdef CATCH
/* Warning: do not evaluate x more than once */
#define THROW(x) throw(x)
static void breakpoint(void) {}
static int ignore[] = {NC_ENOTFOUND, 0};
static int throw(int x)
{
    int* p;
    if(x != 0) {
	for(p=ignore;*p;p++) {if(x == *p) break;}
	if(*p == 0) breakpoint();
    }
    return x;
}
#else
#define THROW(x) (x)
#endif

/**
@Internal
*/

#ifdef DEBUGTRACE
#define TRACE(x) {fprintf(stderr,">>>> %s\n",x); fflush(stderr);}
#else
#define TRACE(x)
#endif

/* Minimum table size is 2 */
#define MINDEPTH 1

/* Minimum leaf size is 2 entries */
#define MINLEAFLEN 2

#define MAX(a,b) ((a) > (b) ? (a) : (b))

#ifdef INLINED
#define exhashlinkleaf(map,leaf) {\
    if(leaf && map) { leaf->next = map->leaves; map->leaves = leaf; } \
}

#define exhashfreeleaf(map,leaf) { \
    if(leaf) {{if(leaf->entries) free(leaf->entries);} free(leaf);} \
}

#endif /*INLINED*/

static int ncexinitialized = 0;

/* Define a vector of bit masks */
ncexhashkey_t bitmasks[NCEXHASHKEYBITS];

/* Extract the leftmost n bits from h */
#if 1
#define MSB(h,d) (((h) >> (NCEXHASHKEYBITS - (d))) & bitmasks[d])
#else
static ncexhashkey_t
MSB(ncexhashkey_t h, int d)
{
    ncexhashkey_t bm = bitmasks[d];
    ncexhashkey_t hkey = h >> (NCEXHASHKEYBITS - d);
    hkey = hkey & bm;
    return hkey;
}
#endif

/* Provide a mask to get the rightmost bit
   of the left n bits of our hash key */
#define MSBMASK(d) (1 << (NCEXHASHKEYBITS - d))

static int exhashlookup(NCexhashmap* map, ncexhashkey_t hkey, NCexleaf** leafp, int* indexp);
static int exhashlocate(NCexhashmap* map, ncexhashkey_t hkey, NCexleaf** leafp, int* indexp);
static int exhashsplit(NCexhashmap* map, ncexhashkey_t hkey, NCexleaf* leaf);
static int exhashdouble(NCexhashmap* map);
static int exbinsearch(ncexhashkey_t hkey, NCexleaf* leaf, int* indexp);
static void exhashnewentry(NCexhashmap* map, NCexleaf* leaf, ncexhashkey_t hkey, int* indexp);
static int exhashnewleaf(NCexhashmap* map, NCexleaf** leaf);
static void exhashunlinkleaf(NCexhashmap* map, NCexleaf* leaf);

#ifndef INLINED
static void exhashlinkleaf(NCexhashmap* map, NCexleaf* leaf);
static void exhashfreeleaf(NCexhashmap* map, NCexleaf* leaf);
#endif /*INLINED*/

/**************************************************/

static void
ncexinit(void)
{
    int i;
    bitmasks[0] = 0;
    for(i=1;i<NCEXHASHKEYBITS;i++)
	bitmasks[i] = (1ULL << i) - 1;
    ncexinitialized = 1;
}

/**************************************************/

/** Creates a new exhash using DEPTH */
NCexhashmap*
ncexhashnew(int leaflen)
{
    NCexhashmap* map = NULL;
    NCexleaf* leaf[2] = {NULL,NULL};
    NCexleaf** topvector = NULL;
    int i;
    int gdepth;

    TRACE("ncexhashnew");

    if(!ncexinitialized) ncexinit();

    gdepth = MINDEPTH;
    if(leaflen < MINLEAFLEN) leaflen = MINLEAFLEN;
    
    /* Create the table */
    if((map = (NCexhashmap*)calloc(1,sizeof(NCexhashmap))) == NULL)
	goto done;
    map->leaflen = leaflen;
    /* Create the top level vector */
    if((topvector = calloc(1<<gdepth,sizeof(NCexleaf*))) == NULL)
	goto done;
    map->directory = topvector;
    if(exhashnewleaf(map,&leaf[0])) goto done;
    if(exhashnewleaf(map,&leaf[1])) goto done;
    exhashlinkleaf(map,leaf[0]);
    exhashlinkleaf(map,leaf[1]);
    /* Fill in vector */
    for(i=0;i<(1<<gdepth);i++) topvector[i] = (i & 0x1?leaf[1]:leaf[0]);
    topvector = NULL;
    leaf[0] = leaf[1] = NULL;
    map->depth = gdepth;
    assert(map->leaves != NULL);
done:
    if(leaf[0]) {exhashunlinkleaf(map,leaf[0]); exhashfreeleaf(map,leaf[0]);}
    if(leaf[1]) {exhashunlinkleaf(map,leaf[1]); exhashfreeleaf(map,leaf[1]);}
    if(topvector) free(topvector);
    return map;
}

/** Reclaims the exhash structure. */
void
ncexhashmapfree(NCexhashmap* map)
{
    NCexleaf* current = NULL;
    NCexleaf* next= NULL;

    if(map == NULL) return;
    /* Walk the leaf chain to free leaves */
    current = map->leaves; next = NULL;
    while(current) {
	next = current->next;	
	exhashfreeleaf(map,current);
	current = next;	
    }
    nullfree(map->directory);    
    free(map);
}

/** Returns the number of active elements. */
int
ncexhashcount(NCexhashmap* map)
{
    return map->nactive;
}

/* Hash key based API */

/* Lookup by Hash Key */
int
ncexhashget(NCexhashmap* map, ncexhashkey_t hkey, uintptr_t* datap)
{
    int stat = NC_NOERR;
    NCexleaf* leaf;
    NCexentry* entry;
    int index;

    /* Do internal lookup */
    if((stat = exhashlookup(map,hkey,&leaf,&index)))
       return THROW(stat);
    entry = &leaf->entries[index];
    assert(entry->hashkey == hkey);
    if(datap) *datap = entry->data;
    return THROW(stat);
}

/* Insert by Hash Key */
int
ncexhashput(NCexhashmap* map, ncexhashkey_t hkey, uintptr_t data)
{
    int stat = NC_NOERR;
    NCexleaf* leaf;
    NCexentry* entry;
    int index;

    if(map->iterator.walking) return THROW(NC_EPERM);

    /* Do internal lookup */
    if((stat = exhashlookup(map,hkey,&leaf,&index)) == NC_ENOTFOUND) {
        /* We have to add an entry (sigh!) so find where it goes */
        if((stat = exhashlocate(map,hkey,&leaf,&index)))
  	    return THROW(stat);
    }
    entry = &leaf->entries[index];
    entry->hashkey = hkey;
    assert(entry->hashkey == hkey);
    entry->data = data;
    return THROW(stat);
}

static int
exhashlookup(NCexhashmap* map, ncexhashkey_t hkey, NCexleaf** leafp, int* indexp)
{
    int stat = NC_NOERR;
    NCexleaf* leaf;
    ncexhashkey_t offset;
    int index;

    TRACE("exhashlookup");

    /* Extract global depth least significant bits of hkey */
    offset = MSB(hkey,map->depth);
    /* Get corresponding leaf from directory */
    leaf = map->directory[offset];
    if(leafp) *leafp = leaf; /* always return this if possible */
    /* Binary search the leaf entries looking for hkey */
    stat = exbinsearch(hkey,leaf,&index);
    if(indexp) *indexp = index;
#if DEBUG >= 3
    fprintf(stderr,"lookup: found=%s offset=%x leaf=%d index=%d\n",
	(stat == NC_NOERR ? "true" : "false"),
	offset,leaf->uid,index);
#endif
    return THROW(stat);
}

static int
exhashlocate(NCexhashmap* map, ncexhashkey_t hkey, NCexleaf** leafp, int* indexp)
{
    int stat = NC_NOERR;
    ncexhashkey_t offset;
    NCexleaf* leaf = NULL;
    int index = -1;
    int iter;

    TRACE("exhashlocate");

#if DEBUG >= 2
    fprintf(stderr,"locate.before: "); ncexhashprint(map);
#endif

    /* Setup */
    *leafp = NULL;
    *indexp = -1;

    if(map->iterator.walking) return THROW(NC_EPERM);

    /* Repeatedly split and/or double until the target leaf has room */
    for(iter=0;;iter++) {
        /* Extract global depth least significant bits of hkey */
        offset = MSB(hkey,map->depth);
        /* Get corresponding leaf from directory */
        leaf = map->directory[offset];
        /* Is there room in the leaf to add an entry? */
#if DEBUG >= 3
	fprintf(stderr,"locate: iter=%d offset=%x leaf=(%d)%p active=%d\n",iter,offset,leaf->uid,leaf,(int)leaf->active);
#else
	(void)iter;
#endif
       if(leaf->active < map->leaflen) break; /* yes, there is room */
       /* Not Enough room, so we need to split this leaf */
        /* Split this leaf and loop to verify we have room */
#if DEBUG >= 3
        fprintf(stderr,"locate.split.loop: uid=%d\n",leaf->uid);
#endif
        if((stat = exhashsplit(map,hkey,leaf))) return THROW(stat); /* failed */
    }
    /* We now now that there is room in the leaf */
    /* Insert into this leaf */
    exhashnewentry(map,leaf,hkey,&index);
#if DEBUG >= 3
    fprintf(stderr,"locate.inserted: index=%d\n",index);
#endif
#if DEBUG >= 1
    fprintf(stderr,"locate.inserted.after: "); ncexhashprint(map);
#endif
    *leafp = leaf;
    *indexp = index;
    return THROW(stat);
}

static int
exhashdouble(NCexhashmap* map)
{
    NCexleaf** olddir = NULL;
    NCexleaf** newdir = NULL;
    size_t oldcount,newcount;
    ncexhashkey_t iold, inew;

    TRACE("exhashdouble");

    if(map->iterator.walking) return THROW(NC_EPERM);

#if DEBUG >= 1
    fprintf(stderr,"double.before: "); ncexhashprint(map);
#endif

    olddir = map->directory;
    /* Attempt to double the directory count */
    oldcount = (1<<map->depth);
    newcount = 2 * oldcount;
    newdir = (NCexleaf**)malloc(newcount*sizeof(NCexleaf*));
    if(newdir == NULL) return THROW(NC_ENOMEM);
    /* Note that newdir == olddir is possible because realloc */
    /* Walk the old directory from top to bottom to double copy
       into newspace */
    assert(oldcount >= 1 && newcount >= 2);
    iold = oldcount;
    inew = newcount;
    do {
        iold -= 1;
        inew -= 2;
	newdir[inew] = olddir[iold];
	newdir[inew+1] = olddir[iold];
    } while(iold > 0);
    assert(iold == 0 && inew == 0);
    /* Fix up */
    map->directory = newdir;
    map->depth++;
#if DEBUG >= 1
    fprintf(stderr,"double.after: "); ncexhashprint(map);
#endif
    nullfree(olddir);
    return THROW(NC_NOERR);
}

static int
exhashsplit(NCexhashmap* map, ncexhashkey_t hkey, NCexleaf* leaf)
{
    int stat = NC_NOERR;
    NCexleaf* newleaf = NULL;
    NCexleaf* leafptr = leaf;
    NCexleaf entries;
    int i, index;

    TRACE("exhashsplit");

    if(map->iterator.walking) {stat = NC_EPERM; goto done;}

#if DEBUG >= 1
    fprintf(stderr,"split.before: leaf=%d",leaf->uid); ncexhashprint(map);
#endif

    /* Save the old leaf's entries */
    entries = *leaf;
 
    /* bump leaf depth */
    leaf->depth++;

    /* May require doubling of the map directory */
#if DEBUG >= 3
	fprintf(stderr,"split: leaf.depth=%d map.depth=%d\n",leaf->depth,map->depth);
#endif
    if(leaf->depth > map->depth) {
	/* double the directory */
        if((stat = exhashdouble(map))) return THROW(stat); /* failed */
    }
    
    /* Re-build the old leaf; keep same uid */
    if((leaf->entries = (NCexentry*)calloc((size_t)map->leaflen, sizeof(NCexentry))) == NULL)
	{stat = NC_ENOMEM; goto done;}
    leaf->active = 0;

    /* Allocate and link the new leaf */
    if((stat = exhashnewleaf(map,&newleaf))) goto done;
    exhashlinkleaf(map,newleaf);
    newleaf->depth = leaf->depth;
#if DEBUG >= 3
     fprintf(stderr,"split.split: newleaf=");ncexhashprintleaf(map,newleaf);
#endif

    /* Now walk the directory to locate all occurrences of old
       leaf and replace with newleaf in those cases where the
       directory index % 2 == 1 
    */
    for(i=0;i<(1<<map->depth);i++) {
	if(map->directory[i] == leafptr) {
	    if(i % 2 == 1) { /* entries should be newleaf */
#if DEBUG >= 3
		fprintf(stderr,"split.directory[%d]=%d (newleaf)\n",(int)i,newleaf->uid);
#endif
	        map->directory[i] = newleaf;
	    }
	}
    }

#if DEBUG >= 1
    fprintf(stderr,"split.after: leaf=%d newleaf=%d",leaf->uid,newleaf->uid); ncexhashprint(map);
#endif

    newleaf = NULL; /* no longer needed */

    /* Now re-insert the entries */
    /* Should not cause splits or doubles */
    for(i=0;i<entries.active;i++) {
        NCexentry* e = &entries.entries[i];
	switch (stat = exhashlookup(map,e->hashkey,&leaf,&index)) {
	case NC_NOERR: /* Already exists, so fail */
	    stat = NC_EINTERNAL;
	    goto done;
	default:
	    stat = NC_NOERR;
	    break;
	}
	assert(leaf != NULL);
        /* Insert in the proper leaf */
#if DEBUG >= 3
       fprintf(stderr,"split.reinsert: entry.hashkey=%x leaf.uid=%d index=%d\n",
		e->hashkey,leaf->uid,index);
#endif
	leaf->entries[index] = *e;
	leaf->active++;
#if DEBUG >= 1
       fprintf(stderr,"split.reinsert.after: "); ncexhashprint(map);
#endif
    }

done:
    if(stat) { /* unwind */
        nullfree(leaf->entries);
	*leaf = entries; 
    } else {
        nullfree(entries.entries);
    }
    if(newleaf) {
	exhashunlinkleaf(map,newleaf);
	exhashfreeleaf(map,newleaf);
    }
    return THROW(stat);
}

/**
 * @internal Define a binary searcher for hash keys in leaf
 * @param hkey for which to search
 * @param leaf to search
 * @param indexp store index of the match or if no match, the index
 *               where new value should be stored (0 <= *indexp <= leaf->active)
 * @return NC_NOERR if found
 * @return NC_ENOTFOUND if not found, indexp points to insertion location
 * @author Dennis Heimbigner
 */
static int
exbinsearch(ncexhashkey_t hkey, NCexleaf* leaf, int* indexp)
{
    int stat = NC_NOERR;
    int n = leaf->active;
    int L = 0;
    int R = (n - 1);

    if(n == 0) {
	if(indexp) *indexp = 0; /* Insert at 0 */
        return THROW(NC_ENOTFOUND);
    }
    while(L != R) {
	int m = (L + R);
	if((m & 0x1)) /* ceiling */
	    m = (m / 2) + 1;
	else
	    m = (m / 2);
	if(leaf->entries[m].hashkey > hkey)
            R = m - 1;
        else
            L = m;
    }
    /* Return match index or insertion index */
    if(leaf->entries[L].hashkey == hkey) {
	/* L = L; */
	/* stat = NC_NOERR; */
    } else if(leaf->entries[L].hashkey > hkey) {
        /* L = L; */
	stat = NC_ENOTFOUND;
    } else {/* (leaf->entries[m].hashkey < hkey) */
	L = L+1;
	stat = NC_ENOTFOUND;
    }
    if(indexp) *indexp = L;
    return THROW(stat);
}

/* Create new entry at position index */
static void
exhashnewentry(NCexhashmap* map, NCexleaf* leaf, ncexhashkey_t hkey, int* indexp)
{
    int stat;
    int index;

    /* Figure out where the key should be inserted (*indexp + 1)*/
    stat = exbinsearch(hkey,leaf,indexp);
    assert(stat != NC_NOERR); /* already present */
    index = *indexp;
    assert(index >= 0 && index <= leaf->active);
    assert(index == leaf->active || leaf->entries[index].hashkey > hkey);
    if(leaf->active > 0) {
	int dst = leaf->active;
        int src = leaf->active - 1;
        for(;src >= index;src--,dst--)
            leaf->entries[dst] = leaf->entries[src];
    }
#if 0
    leaf->entries[index].hashkey = hkey;
#else
    leaf->entries[index].hashkey = (ncexhashkey_t)0xffffffffffffffff;
#endif
    leaf->entries[index].data = 0;
    leaf->active++;
    map->nactive++;
}

#ifndef INLINED
static void
exhashlinkleaf(NCexhashmap* map, NCexleaf* leaf)
{
    if(leaf && map) {
        assert(!map->iterator.walking);
	leaf->next = map->leaves;
	map->leaves = leaf;
        assert(leaf->next == NULL || leaf->next != leaf);
    }
}

static void
exhashfreeleaf(NCexhashmap* map, NCexleaf* leaf)
{
    assert(!map->iterator.walking);
    if(leaf) {
	nullfree(leaf->entries);
	nullfree(leaf);
    }
}

#endif /*INLINED*/

static void
exhashunlinkleaf(NCexhashmap* map, NCexleaf* leaf)
{
    if(leaf && map && map->leaves) {
        assert(!map->iterator.walking);
	if(map->leaves == leaf) {/* special case */
	    map->leaves = leaf->next;
	} else {
	    NCexleaf* cur;
	    for(cur = map->leaves;cur != NULL;cur=cur->next) {
	        if(cur->next == leaf) {
		    cur->next = leaf->next;
		    break;			
		}
	    }
	}
    }
}

static int
exhashnewleaf(NCexhashmap* map, NCexleaf** leafp)
{
    int stat = NC_NOERR;
    NCexleaf* leaf = NULL;
    assert(!map->iterator.walking);
    if(leafp) {
        if((leaf = calloc(1,sizeof(NCexleaf))) == NULL)
	    goto done;
	assert(map->leaflen > 0);
        if((leaf->entries = calloc((size_t)map->leaflen, sizeof(NCexentry))) == NULL)
	    goto done;	
        leaf->uid = map->uid++;
	*leafp = leaf; leaf = NULL;
    }
done:
    if(leaf) nullfree(leaf->entries);
    nullfree(leaf);
    return stat;
}

/**
 * Remove by Hash Key
 * @param map
 * @param hkey
 * @param datap Store the data of the removed hkey
 * @return NC_NOERR if found
 * @return NC_ENOTFOUND if not found
 * @return NC_EINVAL for other errors
 * @author Dennis Heimbigner
 */

int
ncexhashremove(NCexhashmap* map, ncexhashkey_t hkey, uintptr_t* datap)
{
     int stat = NC_NOERR;
     NCexleaf* leaf;
     int src,dst;

    if(map->iterator.walking) return THROW(NC_EPERM);

    if((stat = exhashlookup(map,hkey,&leaf,&dst)))
       return THROW(stat);
    if(datap) *datap = leaf->entries[dst].data;
    /* Compress out the index'th entry */
    for(src=dst+1;src<leaf->active;src++,dst++)
	leaf->entries[dst] = leaf->entries[src];
    leaf->active--;        
    map->nactive--;
    return THROW(stat);
}

/**
 * Change data associated with key; do not insert if not found
 * @param map
 * @param hkey
 * @param newdata new data
 * @param olddatap Store previous value
 * @return NC_NOERR if found
 * @return NC_ENOTFOUND if not found
 * @return NC_EINVAL for other errors
 * @author Dennis Heimbigner
 */

int
ncexhashsetdata(NCexhashmap* map, ncexhashkey_t hkey, uintptr_t newdata, uintptr_t* olddatap)
{
    int stat = NC_NOERR;
    NCexleaf* leaf = NULL;
    NCexentry* e = NULL;
    int index;

    if(map->iterator.walking) return THROW(NC_EPERM);

    if((stat = exhashlookup(map,hkey,&leaf,&index)))
       return THROW(stat);
    e = &leaf->entries[index];
    if(olddatap) *olddatap = e->data;
    e->data = newdata;
    return THROW(stat);
}

/**
 * Inquire map-related values
 * @param map
 * @param leaflenp Store map->leaflen
 * @param depthp Store map->depth
 * @param nactivep Store map->nactive
 * @param uidp Store map->uid
 * @param walkingp Store map->iterator.walking
 *
 * @return NC_NOERR if found
 * @return NC_EINVAL for other errors
 * @author Dennis Heimbigner
 */
int
ncexhashinqmap(NCexhashmap* map, int* leaflenp, int* depthp, int* nactivep, int* uidp, int* walkingp)
{
    if(map == NULL) return NC_EINVAL;
    if(leaflenp) *leaflenp = map->leaflen;
    if(depthp) *depthp = map->depth;
    if(nactivep) *nactivep = map->nactive;
    if(uidp) *uidp = map->uid;
    if(walkingp) *walkingp = map->iterator.walking;
    return NC_NOERR;
}

/* Return the hash key for specified key; takes key+size*/
ncexhashkey_t
ncexhashkey(const unsigned char* key, size_t size)
{
    return NC_crc64(0,(unsigned char*)key,(unsigned int)size);
}

/* Walk the entries in some order */
/*
@return NC_NOERR if keyp and datap are valid
@return NC_ERANGE if iteration is finished
@return NC_EINVAL for all other errors
*/
int
ncexhashiterate(NCexhashmap* map, ncexhashkey_t* keyp, uintptr_t* datap)
{
    int stat = NC_NOERR;

    if(!map->iterator.walking) {
	map->iterator.leaf = map->leaves;
	map->iterator.index = 0;
	map->iterator.walking = 1;
    }
    for(;;) {
        if(map->iterator.leaf == NULL)
  	    {stat = NC_ERANGE; break;}
	if(map->iterator.index >= map->iterator.leaf->active) {
	    map->iterator.leaf = map->iterator.leaf->next;
	    map->iterator.index = 0;
	} else {
            assert(map->iterator.leaf != NULL && map->iterator.index < map->iterator.leaf->active);
	    /* Return data from this entry */
	    if(keyp) *keyp = map->iterator.leaf->entries[map->iterator.index].hashkey;
	    if(datap) *datap = map->iterator.leaf->entries[map->iterator.index].data;
  	    map->iterator.index++;
	    break;
	}
    }
    if(stat != NC_NOERR) { /* stop */
	map->iterator.walking = 0;
	map->iterator.leaf = NULL;
	map->iterator.index = 0;	
    }
    return THROW(stat);
}
/**************************************************/
/* Debug support */

void
ncexhashprint(NCexhashmap* hm)
{
    int index;

    if(hm == NULL) {fprintf(stderr,"NULL"); fflush(stderr); return;}
    fprintf(stderr,"{depth=%u leaflen=%u",hm->depth,hm->leaflen);
    if(hm->iterator.walking) {
        fprintf(stderr," iterator=(leaf=%p index=%u)",
		hm->iterator.leaf,hm->iterator.index);
    }
    fprintf(stderr,"\n");
    for(size_t dirindex=0;dirindex<(1<<hm->depth);dirindex++) {
	NCexleaf* leaf = hm->directory[dirindex];
	fprintf(stderr,"\tdirectory[%03zu|%sb]=(%04x)[(%u)^%d|%d|",
		dirindex,ncexbinstr(dirindex,hm->depth),
		leaf->active,
		(unsigned)(0xffff & (uintptr_t)leaf),
		leaf->uid, leaf->depth);
	for(index=0;index<leaf->active;index++) {
	    ncexhashkey_t hkey, bits;
	    const char* s;
	    hkey = leaf->entries[index].hashkey;
	    /* Reduce to the leaf->hash MSB */
	    bits = MSB(hkey,hm->depth);
	    s = ncexbinstr(bits,hm->depth);
	    fprintf(stderr,"%s(%s/",(index==0?":":" "),s);
	    bits = MSB(hkey,leaf->depth);
	    s = ncexbinstr(bits,leaf->depth);
	    fprintf(stderr,"%s|0x%llx,%llu)",
		    s,
	  	    (unsigned long long)hkey,
		    (unsigned long long)leaf->entries[index].data);
	}
	fprintf(stderr,"]\n");
    }
    fprintf(stderr,"}\n");
    fflush(stderr);
}

void
ncexhashprintdir(NCexhashmap* map, NCexleaf** dir)
{
    for(unsigned long long dirindex=0;dirindex<(1<<map->depth);dirindex++) {
	NCexleaf* leaf = dir[dirindex];
	fprintf(stderr,"\tdirectory[%03llu|%sb]=%d/%p\n",
		dirindex,ncexbinstr(dirindex,map->depth),leaf->uid,leaf);
    }
    fflush(stderr);
}

void
ncexhashprintleaf(NCexhashmap* map, NCexleaf* leaf)
{
    int index;
    fprintf(stderr,"(%04x)[(%u)^%d|%d|",
	(unsigned)(0xffff & (uintptr_t)leaf),
	leaf->uid, leaf->depth,leaf->active);
    for(index=0;index<leaf->active;index++) {
	ncexhashkey_t hkey, bits;
	const char* s;
	hkey = leaf->entries[index].hashkey;
	/* Reduce to the leaf->hash MSB */
	bits = MSB(hkey,map->depth);
        s = ncexbinstr(bits,map->depth);
        fprintf(stderr,"%s(%s/",(index==0?":":" "),s);
	bits = MSB(hkey,leaf->depth);
	s = ncexbinstr(bits,leaf->depth);
	fprintf(stderr,"%s|0x%llx,%llu)",
        s, (unsigned long long)hkey, (unsigned long long)leaf->entries[index].data);
    }
    fprintf(stderr,"]\n");
}

void
ncexhashprintentry(NCexhashmap* map, NCexentry* entry)
{

    fprintf(stderr,"{0x%llx,%llu)",(unsigned long long)entry->hashkey,(unsigned long long)entry->data);
}

char*
ncexbinstr(ncexhashkey_t hkey, int depth)
{
    int i;
    static char bits[NCEXHASHKEYBITS+1];
    memset(bits,'0',NCEXHASHKEYBITS+1);
    bits[NCEXHASHKEYBITS] = '\0';
    for(i=0;i<depth;i++)
        bits[(depth-1)-i] = ((hkey >> i) & 0x1) == 0 ? '0' : '1';
    bits[depth] = '\0';
    return bits;    
}

void
ncexhashprintstats(NCexhashmap* map)
{
    int nactive;
    NCexleaf* leaf = NULL;
    double leafavg = 0.0;
    double leafload = 0.0;
    unsigned long long dirsize, leafsize, total;
    
    nactive = 0;
    unsigned long long nleaves = 0;
    for(leaf=map->leaves;leaf;leaf=leaf->next) {
        nleaves++;
	nactive += leaf->active;
    }
	
    leafavg = ((double)nactive)/((double)nleaves);
    leafload = leafavg / ((double)map->leaflen);

    if(nactive != map->nactive) {
	fprintf(stderr,"nactive mismatch: map->active=%d actual=%d\n",map->nactive,nactive);
    }
    fprintf(stderr,"|directory|=%llu nleaves=%llu nactive=%d",
	(unsigned long long)(1<<(map->depth)),nleaves,nactive);
    fprintf(stderr," |leaf|=%d nactive/nleaves=%g", map->leaflen, leafavg);
    fprintf(stderr," load=%g",leafload);
    fprintf(stderr,"]\n");
    dirsize = (1ULL<<(map->depth))*((unsigned long long)sizeof(void*));
    leafsize = (nleaves)*((unsigned long long)sizeof(NCexleaf));
    total = dirsize + leafsize;
    fprintf(stderr,"\tsizeof(directory)=%llu sizeof(leaves)=%lld total=%lld\n",
		dirsize,leafsize,total);
}
