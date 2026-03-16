/*
  Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
  See LICENSE.txt for license information.
*/

/** \file \internal
    Internal netcdf-4 functions.

    This file contains functions for manipulating NCxcache objects.

    Warning: This code depends critically on the assumption that
    |void*| == |uintptr_t|

*/

/* 0 => no debug */
#define DEBUG 0
#define CATCH

/* Define this for debug so that table sizes are small */
#define SMALLTABLE

#ifdef CATCH
/* Warning: do not evalue x more than once */
#define THROW(x) throw(x)
static void breakpoint(void) {}
static int ignore[] = {0};
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

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif
#include <assert.h>

#include "nc4internal.h"
#include "ncexhash.h"
#include "ncxcache.h"

#ifdef SMALLTABLE
/* Keep the table sizes small initially */
#define DFALTTABLESIZE 4
#define DFALTLEAFLEN 4
#else
#define DFALTTABLESIZE 32
#define DFALTLEAFLEN 12
#endif

static void insertafter(NCxnode* current, NCxnode* node);
static void unlinknode(NCxnode* node);

#if DEBUG > 0
void verifylru(NCxcache* cache);

static void
xverify(NCxcache* cache)
{
    verifylru(cache);
}

void
verifylru(NCxcache* cache)
{
    NCxnode* p;
    for(p=cache->lru.next;p != &cache->lru;p=p->next) {
        if(p->next == NULL || p->prev == NULL) {
	    xverify(cache);
	}
    }
}
#endif

/* Locate object by hashkey in an NCxcache */
int
ncxcachelookup(NCxcache* NCxcache, ncexhashkey_t hkey, void** op)
{
    int stat = NC_NOERR;
    uintptr_t inode = 0;
    NCxnode* node = NULL;

    if(NCxcache == NULL) return THROW(NC_EINVAL);
    assert(NCxcache->map != NULL);
    if((stat=ncexhashget(NCxcache->map,hkey,&inode)))
        {stat = THROW(NC_ENOOBJECT); goto done;} /* not present */
    node = (void*)inode;
    if(op) *op = node->content;

done:
    return stat;
}

/* Move object to the front of the LRU list */
int
ncxcachetouch(NCxcache* cache, ncexhashkey_t hkey)
{
    int stat = NC_NOERR;
    uintptr_t inode = 0;
    NCxnode* node = NULL;

    if(cache == NULL) return THROW(NC_EINVAL);
    if((stat=ncexhashget(cache->map,hkey,&inode)))
        {stat = THROW(NC_ENOOBJECT); goto done;} /* not present */
    node = (void*)inode;
    /* unlink */
    unlinknode(node);
    /* Relink into front of chain */
    insertafter(&cache->lru,node);
#if DEBUG > 0
verifylru(cache);
#endif

done:
    return stat;
}

/* Add object to the cache */
int
ncxcacheinsert(NCxcache* cache, const ncexhashkey_t hkey, void* o)
{
    int stat = NC_NOERR;
    uintptr_t inode = 0;
    NCxnode* node = NULL;

    if(cache == NULL) return THROW(NC_EINVAL);
    
#ifndef NCXUSER
    node = calloc(1,sizeof(NCxnode));
#else
    node = (NCxnode*)o;
#endif
    node->content = o; /* Cheat and make content point to the node part*/
    inode = (uintptr_t)node;
    stat = ncexhashput(cache->map,hkey,inode);
    if(stat)
	goto done;
    /* link into the LRU chain at front */
    insertafter(&cache->lru,node);
#if DEBUG > 0
verifylru(cache);
#endif
    node = NULL;
done:
#ifndef NCXUSER
    if(node) nullfree(node);
#endif
    return THROW(stat);
}

/* Remove object from the index;*/
int
ncxcacheremove(NCxcache* cache, ncexhashkey_t hkey, void** op)
{
    int stat = NC_NOERR;
    uintptr_t inode = 0;
    NCxnode* node = NULL;

    if(cache == NULL) return THROW(NC_EINVAL);

    /* Remove from the hash map */
    if((stat=ncexhashremove(cache->map,hkey,&inode)))
        {stat = NC_ENOOBJECT; goto done;} /* not present */
    node = (NCxnode*)inode;
    /* unlink */
    unlinknode(node);
#if DEBUG > 0
verifylru(cache);
#endif
    if(op) {
        *op = node->content;
    }
#ifndef NCXUSER
    nullfree(node);
#endif

done:
    return THROW(stat);
}

/* Free a cache */
void
ncxcachefree(NCxcache* cache)
{
    NCxnode* lru = NULL;

    if(cache == NULL) return;
    lru = &cache->lru;

#ifndef NCXUSER
    {
    NCxnode* p = NULL;
    NCxnode* next = NULL;
    /* walk the lru chain */
    next = NULL;
    for(p=lru->next;p != lru;) {
	next = p->next;
	nullfree(p);
	p = next;
    }
    }
#endif

    lru->next = (lru->prev = lru); /*reset*/
    ncexhashmapfree(cache->map);
    free(cache);
}

/* Create a new cache holding at least size objects */
int
ncxcachenew(size_t leaflen, NCxcache** cachep)
{
    int stat = NC_NOERR;
    NCxcache* cache = NULL;

    if(leaflen == 0) leaflen = DFALTLEAFLEN;

    cache = calloc(1,sizeof(NCxcache));
    if(cache == NULL)
        {stat = NC_ENOMEM; goto done;}
    cache->map = ncexhashnew((int)leaflen);
    if(cache->map == NULL)
        {stat = NC_ENOMEM; goto done;}
    cache->lru.next = &cache->lru;
    cache->lru.prev = &cache->lru;
    if(cachep) {*cachep = cache; cache = NULL;}

done:
    ncxcachefree(cache);
    return THROW(stat);
}

void
ncxcacheprint(NCxcache* cache)
{
    int i;
    NCxnode* p = NULL;

    fprintf(stderr,"NCxcache: lru=");
    fprintf(stderr,"{");
    for(i=0,p=cache->lru.next;p != &cache->lru;p=p->next,i++) {
	if(i>0) fprintf(stderr,",");
	fprintf(stderr,"%p:%p",p,p->content);
    }
    fprintf(stderr,"}\n");
    ncexhashprint(cache->map);
}

void*
ncxcachefirst(NCxcache* cache)
{
    if(cache == NULL) return NULL;
    if(ncexhashcount(cache->map) == 0)
        return NULL;
    return cache->lru.next->content;
}

void*
ncxcachelast(NCxcache* cache)
{
    if(cache == NULL) return NULL;
    if(ncexhashcount(cache->map) == 0)
        return NULL;
    return cache->lru.prev->content;
}

/* Insert node after current */
static void
insertafter(NCxnode* current, NCxnode* node)
{
    NCxnode* curnext = current->next;
    current->next = node;
    node->prev = current;
    node->next = curnext;
    curnext->prev = node;
}

/* Remove node from chain */
static void
unlinknode(NCxnode* node)
{
    NCxnode* next;
    NCxnode* prev;
    assert(node != NULL);
    next = node->next;
    prev = node->prev;
    /* repair the chain */
    next->prev = prev;
    prev->next = next;
    node->next = node->prev = NULL;
}


ncexhashkey_t
ncxcachekey(const void* key, size_t size)
{
    return ncexhashkey((unsigned char*)key,size);
}

