/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include <stddef.h>
#undef DEBUG

/* Not sure this has any effect */
#define _LARGEFILE_SOURCE 1
#define _LARGEFILE64_SOURCE 1

#include "zincludes.h"

#include <errno.h>

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif

#ifdef _WIN32
#include <windows.h>
#ifndef S_ISDIR
#define S_ISDIR(mode) ((mode) & _S_IFDIR)
#define S_ISREG(mode) ((mode) & _S_IFREG)
#endif
#if 0
#ifndef __cplusplus
#include <io.h>
#include <iostream>
#endif
#endif
#endif

#include "fbits.h"
#include "ncpathmgr.h"

#define VERIFY

#ifndef O_DIRECTORY
# define O_DIRECTORY  0200000
#endif

/*Mnemonic*/
#define FLAG_ISDIR 1
#define FLAG_CREATE 1
#define SKIPLAST 1
#define WHOLEPATH 0

#define NCZM_FILE_V1 1

#ifdef S_IRUSR
static int NC_DEFAULT_CREATE_PERMS =
           (S_IRUSR|S_IWUSR        |S_IRGRP|S_IWGRP);
static int NC_DEFAULT_RWOPEN_PERMS =
           (S_IRUSR|S_IWUSR        |S_IRGRP|S_IWGRP);
static int NC_DEFAULT_ROPEN_PERMS =
//           (S_IRUSR                |S_IRGRP);
           (S_IRUSR|S_IWUSR        |S_IRGRP|S_IWGRP);
static int NC_DEFAULT_DIR_PERMS =
  	   (S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IWGRP);
#else
static int NC_DEFAULT_CREATE_PERMS = 0660;
static int NC_DEFAULT_RWOPEN_PERMS = 0660;
static int NC_DEFAULT_ROPEN_PERMS = 0660;
static int NC_DEFAULT_DIR_PERMS = 0770;
#endif

/*
Do a simple mapping of our simplified map model
to a file system.

Every dataset is assumed to be rooted at some directory in the
file tree. So, its location is defined by some path to a
directory representing both the dataset and the root group of
that dataset. The root is recognized because it uniquely
contains a "superblock" file name ".nczarr" that provides
general information about a dataset. Nesting a dataset
inside a dataset is prohibited. This can be detected
by looking for an occurrence of a ".nczarr" file in any containing
directory. If such a file is found, then an illegal nested
dataset has been found.

For the object API, the mapping is as follows:
1. Every content-bearing object (e.g. .zgroup or .zarray) is mapped to a file.
   The key constraint is that the content bearing objects are files.
   This means that if a key  points to a content bearing object then
   no other key can have that content bearing key as a suffix.
2. The meta data containing files are assumed to contain
   UTF-8 character data.
3. The chunk containing files are assumed to contain raw unsigned 8-bit byte data.
*/

/* define the var name containing an objects content */
#define ZCONTENT "data"

typedef struct FD {
  int fd;
} FD;

static FD FDNUL = {-1};

/* Define the "subclass" of NCZMAP */
typedef struct ZFMAP {
    NCZMAP map;
    char* root;
} ZFMAP;

/* Forward */
static NCZMAP_API zapi;
static int zfileclose(NCZMAP* map, int delete);
static int zfcreategroup(ZFMAP*, const char* key, int nskip);
static int zflookupobj(ZFMAP*, const char* key, FD* fd);
static int zfparseurl(const char* path0, NCURI** urip);
static int zffullpath(ZFMAP* zfmap, const char* key, char**);
static void zfrelease(ZFMAP* zfmap, FD* fd);
static void zfunlink(const char* canonpath);

static int platformerr(int err);
static int platformcreatefile(int mode, const char* truepath,FD*);
static int platformcreatedir(int, const char* truepath);
static int platformopenfile(int mode, const char* truepath, FD* fd);
static int platformopendir(int mode, const char* truepath);
static int platformdircontent(const char* path, NClist* contents);
static int platformdelete(const char* path, int delroot);
static int platformseek(FD* fd, int pos, size64_t* offset);
static int platformread(FD* fd, size64_t count, void* content);
static int platformwrite(FD* fd, size64_t count, const void* content);
static void platformrelease(FD* fd);
static int platformtestcontentbearing(const char* truepath);

#ifdef VERIFY
static int verify(const char* path, int isdir);
static int verifykey(const char* key, int isdir);
#endif

static int zfinitialized = 0;
static void
zfileinitialize(void)
{
    if(!zfinitialized) {
        ZTRACE(5,NULL);
	const char* env = NULL;
	int perms = 0;
	env = getenv("NC_DEFAULT_CREATE_PERMS");
	if(env != NULL && strlen(env) > 0) {
	    if(sscanf(env,"%d",&perms) == 1) NC_DEFAULT_CREATE_PERMS = perms;
	}
	env = getenv("NC_DEFAULT_DIR_PERMS");
	if(env != NULL && strlen(env) > 0) {
	    if(sscanf(env,"%d",&perms) == 1) NC_DEFAULT_DIR_PERMS = perms;
	}
        zfinitialized = 1;
	(void)ZUNTRACE(NC_NOERR);
    }
}

/* Define the Dataset level API */

/*
@param datasetpath abs path in the file tree of the root of the dataset'
       might be a relative path.
@param mode the netcdf-c mode flags
@param flags extra flags
@param flags extra parameters
@param mapp return the map object in this
*/

static int
zfilecreate(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* canonpath = NULL;
    char* abspath = NULL;
    ZFMAP* zfmap = NULL;
    NCURI* url = NULL;
	
    NC_UNUSED(parameters);
    ZTRACE(5,"path=%s mode=%d flag=%llu",path,mode,flags);

    if(!zfinitialized) zfileinitialize();

    /* Fixup mode flags */
    mode |= (NC_NETCDF4 | NC_WRITE);

    if(!(mode & NC_WRITE))
        {stat = NC_EPERM; goto done;}

    /* path must be a url with file: protocol*/
    if((stat=zfparseurl(path,&url)))
	goto done;
    if(strcasecmp(url->protocol,"file") != 0)
        {stat = NC_EURL; goto done;}

    /* Convert the root path */
    if((canonpath = NCpathcvt(url->path))==NULL)
	{stat = NC_ENOMEM; goto done;}

    /* Make the root path be absolute */
    if((abspath = NCpathabsolute(canonpath)) == NULL)
	{stat = NC_EURL; goto done;}

    /* Build the zmap state */
    if((zfmap = calloc(1,sizeof(ZFMAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zfmap->map.format = NCZM_FILE;
    zfmap->map.url = ncuribuild(url,NULL,NULL,NCURIALL);
    zfmap->map.flags = flags;
    /* create => NC_WRITE */
    zfmap->map.mode = mode;
    zfmap->map.api = &zapi;
    zfmap->root = abspath;
        abspath = NULL;

    /* If NC_CLOBBER, then delete below file tree */
    if(!fIsSet(mode,NC_NOCLOBBER))
	platformdelete(zfmap->root,0);
    
    /* make sure we can access the root directory; create if necessary */
    if((stat = platformcreatedir(zfmap->map.mode, zfmap->root)))
	goto done;

    /* Dataset superblock will be written by higher layer */
     
    if(mapp) *mapp = (NCZMAP*)zfmap;    

done:
    ncurifree(url);
    nullfree(canonpath);
    nullfree(abspath);
    if(stat)
    	zfileclose((NCZMAP*)zfmap,1);
    return ZUNTRACE(stat);
}

/*
@param datasetpath abs path in the file tree of the root of the dataset'
       might be a relative path.
@param mode the netcdf-c mode flags
@param flags extra flags
@param flags extra parameters
@param mapp return the map object in this
*/

static int
zfileopen(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* canonpath = NULL;
    char* abspath = NULL;
    ZFMAP* zfmap = NULL;
    NCURI*url = NULL;
    
    NC_UNUSED(parameters);
    ZTRACE(5,"path=%s mode=%d flags=%llu",path,mode,flags);

    if(!zfinitialized) zfileinitialize();

    /* Fixup mode flags */
    mode = (NC_NETCDF4 | mode);

    /* path must be a url with file: protocol*/
    if((stat=zfparseurl(path,&url)))
	goto done;
    if(strcasecmp(url->protocol,"file") != 0)
        {stat = NC_EURL; goto done;}

    /* Convert the root path */
    if((canonpath = NCpathcvt(url->path))==NULL)
	{stat = NC_ENOMEM; goto done;}

    /* Make the root path be absolute */
    if((abspath = NCpathabsolute(canonpath)) == NULL)
	{stat = NC_EURL; goto done;}

    /* Build the zmap state */
    if((zfmap = calloc(1,sizeof(ZFMAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zfmap->map.format = NCZM_FILE;
    zfmap->map.url = ncuribuild(url,NULL,NULL,NCURIALL);
    zfmap->map.flags = flags;
    zfmap->map.mode = mode;
    zfmap->map.api = (NCZMAP_API*)&zapi;
    zfmap->root = abspath;
	abspath = NULL;
    
    /* Verify root dir exists */
    if((stat = platformopendir(zfmap->map.mode,zfmap->root)))
	goto done;
    
    /* Dataset superblock will be read by higher layer */
    
    if(mapp) *mapp = (NCZMAP*)zfmap;    

done:
    ncurifree(url);
    nullfree(canonpath);
    nullfree(abspath);
    if(stat) zfileclose((NCZMAP*)zfmap,0);
    return ZUNTRACE(stat);
}

static int
zfiletruncate(const char* surl)
{
    int stat = NC_NOERR;
    NCURI* url = NULL;

    ZTRACE(6,"url=%s",surl);
    ncuriparse(surl,&url);
    if(url == NULL) {stat = NC_EURL; goto done;}
    platformdelete(url->path,0); /* leave root; ignore errors */
done:
    ncurifree(url);
    return ZUNTRACE(stat);
}

/**************************************************/
/* Object API */

static int
zfileexists(NCZMAP* map, const char* key)
{
    int stat = NC_NOERR;
    ZFMAP* zfmap = (ZFMAP*)map;
    FD fd = FDNUL;

    ZTRACE(5,"map=%s key=%s",zfmap->map.url,key);
    switch(stat=zflookupobj(zfmap,key,&fd)) {
    case NC_NOERR: break;
    case NC_ENOOBJECT: stat = NC_EEMPTY;
    case NC_EEMPTY: break;
    default: break;
    }
    zfrelease(zfmap,&fd);    
    return ZUNTRACE(stat);
}

static int
zfilelen(NCZMAP* map, const char* key, size64_t* lenp)
{
    int stat = NC_NOERR;
    ZFMAP* zfmap = (ZFMAP*)map;
    size64_t len = 0;
    FD fd = FDNUL;

    ZTRACE(5,"map=%s key=%s",map->url,key);

    switch (stat=zflookupobj(zfmap,key,&fd)) {
    case NC_NOERR:
        /* Get file size */
        if((stat=platformseek(&fd, SEEK_END, &len))) goto done;
	break;
    case NC_ENOOBJECT: stat = NC_EEMPTY;
    case NC_EEMPTY: break;
    default: break;
    }
    zfrelease(zfmap,&fd);
    if(lenp) *lenp = len;

done:
    return ZUNTRACEX(stat,"len=%llu",(lenp?*lenp:777777777777));
}

static int
zfileread(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content)
{
    int stat = NC_NOERR;
    FD fd = FDNUL;
    ZFMAP* zfmap = (ZFMAP*)map; /* cast to true type */
    
    ZTRACE(5,"map=%s key=%s start=%llu count=%llu",map->url,key,start,count);

#ifdef VERIFY
    if(!verifykey(key,!FLAG_ISDIR))
        assert(!"expected file, have dir");
#endif

    switch (stat = zflookupobj(zfmap,key,&fd)) {
    case NC_NOERR:
        if((stat = platformseek(&fd, SEEK_SET, &start))) goto done;
        if((stat = platformread(&fd, count, content))) goto done;
	break;
    case NC_ENOOBJECT: stat = NC_EEMPTY;
    case NC_EEMPTY: break;
    default: break;
    }
    
done:
    zfrelease(zfmap,&fd);
    return ZUNTRACE(stat);
}

static int
zfilewrite(NCZMAP* map, const char* key, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    FD fd = FDNUL;
    ZFMAP* zfmap = (ZFMAP*)map; /* cast to true type */
    char* truepath = NULL;
    size64_t start = 0;

    ZTRACE(5,"map=%s key=%s start=%llu count=%llu",map->url,key,start,count);

#ifdef VERIFY
    if(!verifykey(key,!FLAG_ISDIR))
        assert(!"expected file, have dir");
#endif

    switch (stat = zflookupobj(zfmap,key,&fd)) {
    case NC_ENOOBJECT:
    case NC_EEMPTY:
	stat = NC_NOERR;
	/* Create the directories leading to this */
	if((stat = zfcreategroup(zfmap,key,SKIPLAST))) goto done;
        /* Create truepath */
        if((stat = zffullpath(zfmap,key,&truepath))) goto done;
	/* Create file */
	if((stat = platformcreatefile(zfmap->map.mode,truepath,&fd))) goto done;
	/* Fall thru to write the object */
    case NC_NOERR:
        if((stat = platformseek(&fd, SEEK_SET, &start))) goto done;
        if((stat = platformwrite(&fd, count, content))) goto done;
	break;
    default: break;
    }

done:
    nullfree(truepath);
    zfrelease(zfmap,&fd);
    return ZUNTRACE(stat);
}

static int
zfileclose(NCZMAP* map, int delete)
{
    int stat = NC_NOERR;
    ZFMAP* zfmap = (ZFMAP*)map;

    ZTRACE(5,"map=%s delete=%d",map->url,delete);
    if(zfmap == NULL) return NC_NOERR;
    
    /* Delete the subtree below the root and the root */
    if(delete) {
	stat = platformdelete(zfmap->root,1);
	zfunlink(zfmap->root);
    }
    nczm_clear(map);
    nullfree(zfmap->root);
    zfmap->root = NULL;
    free(zfmap);
    return ZUNTRACE(stat);
}

/*
Return a list of names immediately "below" a specified prefix key.
In theory, the returned list should be sorted in lexical order,
but it possible that it is not.
The prefix key is not included. 
*/
int
zfilesearch(NCZMAP* map, const char* prefixkey, NClist* matches)
{
    int stat = NC_NOERR;
    ZFMAP* zfmap = (ZFMAP*)map;
    char* fullpath = NULL;
    NClist* nextlevel = nclistnew();
    NCbytes* buf = ncbytesnew();

    ZTRACE(5,"map=%s prefixkey=%s",map->url,prefixkey);

    /* Make the root path be true */
    if(prefixkey == NULL || strlen(prefixkey)==0 || strcmp(prefixkey,"/")==0)
	fullpath = strdup(zfmap->root);
    else if((stat = nczm_concat(zfmap->root,prefixkey,&fullpath))) goto done;

    /* get names of the next level path entries */
    switch (stat = platformdircontent(fullpath, nextlevel)) {
    case NC_NOERR: /* ok */
	break;
    case NC_EEMPTY: /* not a dir */
	stat = NC_NOERR;
	goto done;
    case NC_ENOOBJECT:
    default:
	goto done;
    }
    while(nclistlength(nextlevel) > 0) {
	char* segment = nclistremove(nextlevel,0);
	nclistpush(matches,segment);
    }
    
done:
    nclistfreeall(nextlevel);
    ncbytesfree(buf);
    nullfree(fullpath);
    return ZUNTRACEX(stat,"|matches|=%d",(int)nclistlength(matches));
}

/**************************************************/
/* Utilities */

static void
zfunlink(const char* canonpath)
{
    char* local = NULL;
    if((local = NCpathcvt(canonpath))==NULL) goto done;
    unlink(local);
done:
    nullfree(local);
}

/* Lookup a group by parsed path (segments)*/
/* Return NC_EEMPTY if not found, NC_EINVAL if not a directory; create if create flag is set */
static int
zfcreategroup(ZFMAP* zfmap, const char* key, int nskip)
{
    int stat = NC_NOERR;
    size_t i;
    size_t len;
    char* fullpath = NULL;
    NCbytes* path = ncbytesnew();
    NClist* segments = nclistnew();

    ZTRACE(5,"map=%s key=%s nskip=%d",zfmap->map.url,key,nskip);
    if((stat=nczm_split(key,segments)))
	goto done;    
    len = nclistlength(segments);
    if(len >= (size_t)nskip)
	len -= (size_t)nskip; /* leave off last nskip segments */
    else
        len = 0;
    ncbytescat(path,zfmap->root); /* We need path to be absolute */
    for(i=0;i<len;i++) {
	const char* seg = nclistget(segments,i);
	ncbytescat(path,"/");
	ncbytescat(path,seg);
	/* open and optionally create the directory */	
	stat = platformcreatedir(zfmap->map.mode,ncbytescontents(path));
	if(stat) goto done;
    }
done:
    nullfree(fullpath);
    ncbytesfree(path);
    nclistfreeall(segments);
    return ZUNTRACE(stat);
}

/* Lookup an object
@return NC_NOERR if found and is a content-bearing object
@return NC_EEMPTY if exists but is not-content-bearing
@return NC_ENOOBJECT if not found
*/
static int
zflookupobj(ZFMAP* zfmap, const char* key, FD* fd)
{
    int stat = NC_NOERR;
    char* path = NULL;

    ZTRACE(5,"map=%s key=%s",zfmap->map.url,key);

    if((stat = zffullpath(zfmap,key,&path)))
	{goto done;}    

    /* See if this is content-bearing */
    if((stat = platformtestcontentbearing(path)))
	goto done;        

    /* Open the file */
    if((stat = platformopenfile(zfmap->map.mode,path,fd)))
        goto done;

done:
    errno = 0;
    nullfree(path);
    return ZUNTRACE(stat);
}

/* When we are finished accessing object */
static void
zfrelease(ZFMAP* zfmap, FD* fd)
{
    ZTRACE(5,"map=%s fd=%d",zfmap->map.url,(fd?fd->fd:-1));
    platformrelease(fd);
    (void)ZUNTRACE(NC_NOERR);
}

/**************************************************/
/* External API objects */

NCZMAP_DS_API zmap_file = {
    NCZM_FILE_V1,
    0,
    zfilecreate,
    zfileopen,
    zfiletruncate,
};

static NCZMAP_API zapi = {
    NCZM_FILE_V1,
    zfileclose,
    zfileexists,
    zfilelen,
    zfileread,
    zfilewrite,
    zfilesearch,
};

static int
zffullpath(ZFMAP* zfmap, const char* key, char** pathp)
{
    int stat = NC_NOERR;
    size_t klen, pxlen, flen;
    char* path = NULL;

    ZTRACE(6,"map=%s key=%s",zfmap->map.url,key);

    klen = nulllen(key);
    pxlen = strlen(zfmap->root);
    flen = klen+pxlen+1+1;
    if((path = malloc(flen)) == NULL) {stat = NC_ENOMEM; goto done;}
    path[0] = '\0';
    strlcat(path,zfmap->root,flen);
    /* look for special cases */
    if(key != NULL) {
        if(key[0] != '/') strlcat(path,"/",flen);
	if(strcmp(key,"/") != 0)
            strlcat(path,key,flen);
    }
    if(pathp) {*pathp = path; path = NULL;}
done:
    nullfree(path)
    return ZUNTRACEX(stat,"path=%s",(pathp?*pathp:"null"));
}

static int
zfparseurl(const char* path0, NCURI** urip)
{
    int stat = NC_NOERR;
    NCURI* uri = NULL;
    ZTRACE(6,"path0=%s",path0);
    ncuriparse(path0,&uri);
    if(uri == NULL)
	{stat = NC_EURL; goto done;}
    if(urip) {*urip = uri; uri = NULL;}

done:
    ncurifree(uri);
    return ZUNTRACEX(stat,"uri=%p",(urip?(void*)*urip:(void*)urip));
    return stat;
}

/**************************************************/
static int
platformerr(int err)
{
    ZTRACE(6,"err=%d",err);
    switch (err) {
     case ENOENT: err = NC_ENOOBJECT; break; /* File does not exist */
     case ENOTDIR: err = NC_EEMPTY; break; /* no content */
     case EACCES: err = NC_EAUTH; break; /* file permissions */
     case EPERM:  err = NC_EAUTH; break; /* ditto */
     default: break;
     }
     return ZUNTRACE(err);
}

/* Test type of the specified file.
@return NC_NOERR if found and is a content-bearing object (file)
@return NC_EEMPTY if exists but is not-content-bearing (a directory)
@return NC_ENOOBJECT if not found
*/
static int
platformtestcontentbearing(const char* canonpath)
{
    int ret = 0;
    #ifdef _WIN64
        struct _stat64 buf;
    #else
        struct stat buf;
    #endif

    ZTRACE(6,"canonpath=%s",canonpath);

    errno = 0;
    ret = NCstat(canonpath, &buf);
    ZTRACEMORE(6,"\tstat: ret=%d, errno=%d st_mode=%d",ret,errno,buf.st_mode);
    if(ret < 0) {
	ret = platformerr(errno);
    } else if(S_ISDIR(buf.st_mode)) {
        ret = NC_EEMPTY;
    } else
        ret = NC_NOERR;
    errno = 0;
    return ZUNTRACE(ret);
}

/* Create a file */
static int
platformcreatefile(int mode, const char* canonpath, FD* fd)
{
    int stat = NC_NOERR;
    int ioflags = 0;
    int createflags = 0;
    int permissions = NC_DEFAULT_ROPEN_PERMS;

    ZTRACE(6,"canonpath=%s",canonpath);
    
    errno = 0;
    if(!fIsSet((mode_t)mode, NC_WRITE))
        ioflags |= (O_RDONLY);
    else {
        ioflags |= (O_RDWR);
	permissions = NC_DEFAULT_RWOPEN_PERMS;
    }
#ifdef O_BINARY
    fSet(ioflags, O_BINARY);
#endif

    if(fIsSet(mode, NC_NOCLOBBER))
        fSet(createflags, O_EXCL);
    else
	fSet(createflags, O_TRUNC);

    if(fIsSet(mode,NC_WRITE))
        createflags = (ioflags|O_CREAT);

    /* Try to create file (will also NCpathcvt) */
    fd->fd = NCopen3(canonpath, createflags, permissions);
    if(fd->fd < 0) { /* could not create */
	stat = platformerr(errno);
        goto done; /* could not open */
    }
done:
    errno = 0;
    return ZUNTRACEX(stat,"fd=%d",(fd?fd->fd:-1));
}

/* Open a file; fail if it does not exist */
static int
platformopenfile(int mode, const char* canonpath, FD* fd)
{
    int stat = NC_NOERR;
    int ioflags = 0;
    int permissions = 0;

    ZTRACE(6,"canonpath=%s",canonpath);

    errno = 0;
    if(!fIsSet(mode, NC_WRITE)) {
        ioflags |= (O_RDONLY);
	permissions = NC_DEFAULT_ROPEN_PERMS;
    } else {
        ioflags |= (O_RDWR);
	permissions = NC_DEFAULT_RWOPEN_PERMS;
    }
#ifdef O_BINARY
    fSet(ioflags, O_BINARY);
#endif

#ifdef VERIFY
    if(!verify(canonpath,!FLAG_ISDIR))
        assert(!"expected file, have dir");
#endif

    /* Try to open file  (will localize) */
    fd->fd = NCopen3(canonpath, ioflags, permissions);
    if(fd->fd < 0)
        {stat = platformerr(errno); goto done;} /* could not open */
done:
    errno = 0;
    return ZUNTRACEX(stat,"fd=%d",(fd?fd->fd:-1));
}

/* Create a dir */
static int
platformcreatedir(int mode, const char* canonpath)
{
    int ret = NC_NOERR;

    ZTRACE(6,"canonpath=%s",canonpath);

    errno = 0;
    /* Try to access file as if it exists */
    ret = NCaccess(canonpath,ACCESS_MODE_EXISTS);
    if(ret < 0) { /* it does not exist, then it can be anything */
	if(fIsSet(mode,NC_WRITE)) {
	    /* Try to create it */
            /* Create the directory using mkdir */
   	    if(NCmkdir(canonpath,(mode_t)NC_DEFAULT_DIR_PERMS) < 0)
	        {ret = platformerr(errno); goto done;}
	    /* try to access again */
	    ret = NCaccess(canonpath,ACCESS_MODE_EXISTS);
    	    if(ret < 0)
	        {ret = platformerr(errno); goto done;}
	} else
	    {ret = platformerr(errno); goto done;}	
    }

done:
    errno = 0;
    return ZUNTRACE(ret);
}

/* Open a dir; fail if it does not exist */
static int
platformopendir(int mode, const char* canonpath)
{
    int ret = NC_NOERR;

    ZTRACE(6,"canonpath=%s",canonpath);

    errno = 0;
    /* Try to access file as if it exists */
    ret = NCaccess(canonpath,ACCESS_MODE_EXISTS);
    if(ret < 0)
	{ret = platformerr(errno); goto done;}	
done:
    errno = 0;
    return ZUNTRACE(ret);
}

/**
Given a path, return the list of all files+dirs immediately below
the specified path: e.g. X s.t. path/X exists.
There are several possibilities:
1. path does not exist => return NC_ENOTFOUND
2. path is not a directory => return NC_EEMPTY and |contents| == 0
3. path is a directory => return NC_NOERR and |contents| >= 0

@return NC_NOERR if path is a directory
@return NC_EEMPTY if path is not a directory
@return NC_ENOTFOUND if path does not exist
*/

#ifdef _WIN32
static int
platformdircontent(const char* canonpath, NClist* contents)
{
    int ret = NC_NOERR;
    errno = 0;
    WIN32_FIND_DATA FindFileData;
    HANDLE dir = NULL;
    char* ffpath = NULL;
    char* lpath = NULL;
    size_t len;
    char* d = NULL;

    ZTRACE(6,"canonpath=%s",canonpath);

    switch (ret = platformtestcontentbearing(canonpath)) {
    case NC_EEMPTY: ret = NC_NOERR; break; /* directory */    
    case NC_NOERR: ret = NC_EEMPTY; goto done;
    default: goto done;
    }

    /* We need to process the path to make it work with FindFirstFile */
    len = strlen(canonpath);
    /* Need to terminate path with '/''*' */
    ffpath = (char*)malloc(len+2+1);
    memcpy(ffpath,canonpath,len);
    if(canonpath[len-1] != '/') {
	ffpath[len] = '/';	
	len++;
    }
    ffpath[len] = '*'; len++;
    ffpath[len] = '\0';

    /* localize it */
    if((lpath = NCpathcvt(ffpath))==NULL)
	{ret = NC_ENOMEM; goto done;}
    dir = FindFirstFile(lpath, &FindFileData);
    if(dir == INVALID_HANDLE_VALUE) {
	/* Distinguish not-a-directory from no-matching-file */
        switch (GetLastError()) {
	case ERROR_FILE_NOT_FOUND: /* No matching files */ /* fall thru */
	    ret = NC_NOERR;
	    goto done;
	case ERROR_DIRECTORY: /* not a directory */
	default:
            ret = NC_EEMPTY;
	    goto done;
	}
    }
    do {
	const char* name = NULL;
        name = FindFileData.cFileName;
	if(strcmp(name,".")==0 || strcmp(name,"..")==0)
	    continue;
	nclistpush(contents,strdup(name));
    } while(FindNextFile(dir, &FindFileData));

done:
    if(dir) FindClose(dir);
    nullfree(lpath);
    nullfree(ffpath);
    nullfree(d);
    errno = 0;
    return ZUNTRACEX(ret,"|contents|=%d",(int)nclistlength(contents));
}

#else /*!_WIN32*/

static int
platformdircontent(const char* canonpath, NClist* contents)
{
    int ret = NC_NOERR;
    errno = 0;
    DIR* dir = NULL;

    ZTRACE(6,"canonpath=%s",canonpath);

    switch (ret = platformtestcontentbearing(canonpath)) {
    case NC_EEMPTY: ret = NC_NOERR; break; /* directory */    
    case NC_NOERR: ret = NC_EEMPTY; goto done; 
    case NC_ENOOBJECT: ret = NC_EEMPTY; goto done;
    default: goto done;
    }

    dir = NCopendir(canonpath);
    if(dir == NULL)
        {ret = platformerr(errno); goto done;}
    for(;;) {
	const char* name = NULL;
	struct dirent* de = NULL;
	errno = 0;
        de = readdir(dir);
        if(de == NULL)
	    {ret = platformerr(errno); goto done;}
	if(strcmp(de->d_name,".")==0 || strcmp(de->d_name,"..")==0)
	    continue;
	name = de->d_name;
	nclistpush(contents,strdup(name));
    }
done:
    if(dir) NCclosedir(dir);
    errno = 0;
    return ZUNTRACEX(ret,"|contents|=%d",(int)nclistlength(contents));
}
#endif /*_WIN32*/

static int
platformdeleter(NCbytes* canonpath, int depth)
{
    int ret = NC_NOERR;
    size_t i;
    NClist* subfiles = nclistnew();
    size_t tpathlen = ncbyteslength(canonpath);
    char* local = NULL;

    local = ncbytescontents(canonpath);
    ZTRACE(6,"canonpath=%s depth=%d",local,depth);

    ret = platformdircontent(local, subfiles);
#ifdef DEBUG
    {int i;
	fprintf(stderr,"xxx: contents:\n");
	for(i=0;i<nclistlength(contents);i++)
	    fprintf(stderr,"\t%s\n",(const char*)nclistget(contents,i));
	fprintf(stderr,"xxx: end contents\n");
    }
#endif
    switch (ret) {
    case NC_NOERR: /* recurse to remove levels below */
        for(i=0;i<nclistlength(subfiles);i++) {
	    const char* name = nclistget(subfiles,i);
            /* append name to current path */
            ncbytescat(canonpath, "/");
            ncbytescat(canonpath, name);
            /* recurse */
            if ((ret = platformdeleter(canonpath,depth+1))) goto done;
            ncbytessetlength(canonpath,tpathlen); /* reset */
	    ncbytesnull(canonpath);
	    local = ncbytescontents(canonpath);
	}
	if(depth > 0) {
#ifdef DEBUG
fprintf(stderr,"xxx: remove:  %s\n",canonpath);
#endif
            if(NCrmdir(local) < 0) { /* kill this dir */
#ifdef DEBUG
fprintf(stderr,"xxx: remove: errno=%d|%s\n",errno,nc_strerror(errno));
#endif
		ret = errno;
		goto done;
	    }
	}
	break;    
    case NC_EEMPTY: /* Not a directory */
	ret = NC_NOERR;
#ifdef DEBUG
fprintf(stderr,"xxx: remove:  %s\n",canonpath);
#endif
            if(NCremove(local) < 0) {/* kill this file */
#ifdef DEBUG
fprintf(stderr,"xxx: remove: errno=%d|%s\n",errno,nc_strerror(errno));
#endif
	        ret = errno;
	        goto done;
	    }
	break;
    case NC_ENOTFOUND:
    default:
	goto done;
    }

done:
    errno = 0;
    nclistfreeall(subfiles);
    ncbytessetlength(canonpath,tpathlen);
    ncbytesnull(canonpath);
    return ZUNTRACE(ret);
}

/* Deep file/dir deletion; depth first */
static int
platformdelete(const char* rootpath, int delroot)
{
    int stat = NC_NOERR;
    NCbytes* canonpath = ncbytesnew();

    ZTRACE(6,"rootpath=%s delroot=%d",rootpath,delroot);
    
    if(rootpath == NULL || strlen(rootpath) == 0) goto done;
    ncbytescat(canonpath,rootpath);
    if(rootpath[strlen(rootpath)-1] == '/') /* elide trailing '/' */
	ncbytessetlength(canonpath,ncbyteslength(canonpath)-1);
    /* See if file even exists */
    stat = NCaccess(ncbytescontents(canonpath),F_OK);
    if(stat < 0) {
	stat = errno;
	goto done;
    }
    if((stat = platformdeleter(canonpath,0))) goto done;
    if(delroot) {
        if(NCrmdir(rootpath) < 0) { /* kill this dir */
    	    stat = errno;
	    goto done;
	 }
    }
done:
    ncbytesfree(canonpath);
    errno = 0;
    return ZUNTRACE(stat);
}

static int
platformseek(FD* fd, int pos, size64_t* sizep)
{
    int ret = NC_NOERR;
    size64_t size, newsize;
    struct stat statbuf;    
    
    assert(fd && fd->fd >= 0);
    
    ZTRACE(6,"fd=%d pos=%d",(fd?fd->fd:-1),pos);

    errno = 0;
    ret = NCfstat(fd->fd, &statbuf);    
    if(ret < 0)
	{ret = platformerr(errno); goto done;}
    if(sizep) size = *sizep; else size = 0;
    newsize = (size64_t)lseek(fd->fd,(off_t)size,pos);
    if(sizep) *sizep = newsize;
done:
    errno = 0;
    return ZUNTRACEX(ret,"sizep=%llu",*sizep);
}

static int
platformread(FD* fd, size64_t count, void* content)
{
    int stat = NC_NOERR;
    size_t need = count;
    unsigned char* readpoint = content;

    assert(fd && fd->fd >= 0);

    ZTRACE(6,"fd=%d count=%llu",(fd?fd->fd:-1),count);

    while(need > 0) {
        ssize_t red;
        if((red = read(fd->fd,readpoint,need)) <= 0)
	    {stat = errno; goto done;}
        need -= red;
	readpoint += red;
    }
done:
    errno = 0;
    return ZUNTRACE(stat);
}

static int
platformwrite(FD* fd, size64_t count, const void* content)
{
    int ret = NC_NOERR;
    size_t need = count;
    unsigned char* writepoint = (unsigned char*)content;

    assert(fd && fd->fd >= 0);
    
    ZTRACE(6,"fd=%d count=%llu",(fd?fd->fd:-1),count);

    while(need > 0) {
        ssize_t red = 0;
        if((red = write(fd->fd,(void*)writepoint,need)) <= 0)	
	    {ret = NC_EACCESS; goto done;}
        need -= red;
	writepoint += red;
    }
done:
    return ZUNTRACE(ret);
}

#if 0
static int
platformcwd(char** cwdp)
{
    char buf[4096];
    char* cwd = NULL;
    cwd = NCcwd(buf,sizeof(buf));
    if(cwd == NULL) return errno;
    if(cwdp) *cwdp = strdup(buf);
    return NC_NOERR;
}
#endif

/* When we are finished accessing FD; essentially
   equivalent to closing the file descriptor.
*/
static void
platformrelease(FD* fd)
{
    ZTRACE(6,"fd=%d",(fd?fd->fd:-1));
    if(fd->fd >=0) NCclose(fd->fd);
    fd->fd = -1;
    (void)ZUNTRACE(NC_NOERR);
}

#if 0
/* Close FD => return typ to FDNONE */
*/
static void
platformclose(FD* fd)
{
    if(fd->typ == FDFILE) {
        if(fd->fd >=0) close(fd->u,fd);
	fd->fd = -1;
    } else if(fd->type == FDDIR) {
	if(fd->u.dir) NCclosedir(fd->u,dir);
    }
    fd->typ = FDNONE;
}
#endif


#ifdef VERIFY
static int
verify(const char* path, int isdir)
{
    int ret = 0;
    #ifdef _WIN64
        struct _stat64 buf;
    #else
        struct stat buf;
    #endif 

    ret = NCaccess(path,ACCESS_MODE_EXISTS);
    if(ret < 0)
        return 1; /* If it does not exist, then it can be anything */
    ret = NCstat(path,&buf);
    if(ret < 0) abort();
    if(isdir && S_ISDIR(buf.st_mode)) return 1;
    if(!isdir && S_ISREG(buf.st_mode)) return 1;           
    return 0;
}

static int
verifykey(const char* key, int isdir)
{
    int ret = 0;
    #ifdef _WIN64
        struct _stat64 buf;
    #else
        struct stat buf;
    #endif 

    if(key[0] == '/') key++; /* Want relative name */

    ret = NCaccess(key,ACCESS_MODE_EXISTS);
    if(ret < 0)
        return 1; /* If it does not exist, then it can be anything */
    ret = NCstat(key,&buf);
    if(ret < 0) abort();
    if(isdir && S_ISDIR(buf.st_mode)) return 1;
    if(!isdir && S_ISREG(buf.st_mode)) return 1;           
    return 0;
}
#endif

#if 0
/* Return NC_EINVAL if path does not exist; els 1/0 in isdirp and local path in canonpathp */
static int
testifdir(const char* path, int* isdirp, char** canonpathp)
{
    int ret = NC_NOERR;
    char* tmp = NULL;
    char* canonpath = NULL;
    struct stat statbuf;

    /* Make path be windows compatible */
    if((ret = nczm_fixpath(path,&tmp))) goto done;
    if((canonpath = NCpathcvt(tmp))==NULL) {ret = NC_ENOMEM; goto done;}

    errno = 0;
    ret = NCstat(canonpath, &statbuf);
    if(ret < 0) {
        if(errno == ENOENT)
	    ret = NC_ENOTFOUND;  /* path does not exist */
	else
	    ret = platformerr(errno);
	goto done;
    }
    /* Check for being a directory */
    if(isdirp) {
        if(S_ISDIR(statbuf.st_mode)) {*isdirp = 1;} else {*isdirp = 0;}
    }
    if(canonpathp) {*canonpathp = canonpath; canonpath = NULL;}
done:
    errno = 0;
    nullfree(tmp);
    nullfree(canonpath);
    return ZUNTRACE(ret);    
}
#endif /* 0 */
