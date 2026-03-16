/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef _MSC_VER
#include <io.h>
#endif

#include "netcdf.h"
#include "nc4internal.h"
#include "ncuri.h"
#include "nclist.h"
#include "ncbytes.h"
#include "ncrc.h"
#include "nclog.h"
#include "ncs3sdk.h"
#include "ncutil.h"

#undef AWSDEBUG

/* Alternate .aws directory location */
#define NC_TEST_AWS_DIR "NC_TEST_AWS_DIR"

enum URLFORMAT {UF_NONE=0, UF_VIRTUAL=1, UF_PATH=2, UF_S3=3, UF_OTHER=4};

/* Read these files in order and later overriding earlier */
static const char* awsconfigfiles[] = {".aws/config",".aws/credentials",NULL};
#define NCONFIGFILES (sizeof(awsconfigfiles)/sizeof(char*))

/**************************************************/
/* Forward */

static int endswith(const char* s, const char* suffix);
static void freeprofile(struct AWSprofile* profile);
static void freeentry(struct AWSentry* e);
static int awsparse(const char* text, NClist* profiles);

extern void awsprofiles(void);

/**************************************************/
/* Capture environmental Info */

EXTERNL void
NC_s3sdkenvironment(void)
{
    /* Get various environment variables as defined by the AWS sdk */
    NCglobalstate* gs = NC_getglobalstate();
    if(getenv(AWS_ENV_REGION)!=NULL)
        gs->aws.default_region = nulldup(getenv(AWS_ENV_REGION));
    else if(getenv(AWS_ENV_DEFAULT_REGION)!=NULL)
        gs->aws.default_region = nulldup(getenv(AWS_ENV_DEFAULT_REGION));
    else if(gs->aws.default_region == NULL)
        gs->aws.default_region = nulldup(AWS_GLOBAL_DEFAULT_REGION);
    gs->aws.access_key_id = nulldup(getenv(AWS_ENV_ACCESS_KEY_ID));
    gs->aws.config_file = nulldup(getenv(AWS_ENV_CONFIG_FILE));
    gs->aws.profile = nulldup(getenv(AWS_ENV_PROFILE));
    gs->aws.secret_access_key = nulldup(getenv(AWS_ENV_SECRET_ACCESS_KEY));
}

/**************************************************/
/* Generic S3 Utilities */

/*
Rebuild an S3 url into a canonical path-style url.
If region is not in the host, then use specified region
if provided, otherwise us-east-1.
@param url      (in) the current url
@param s3       (in/out) NCS3INFO structure
@param pathurlp (out) the resulting pathified url string
*/

int
NC_s3urlrebuild(NCURI* url, NCS3INFO* s3, NCURI** newurlp)
{
    size_t i;
    int stat = NC_NOERR;
    NClist* hostsegments = NULL;
    NClist* pathsegments = NULL;
    NCbytes* buf = ncbytesnew();
    NCURI* newurl = NULL;
    char* bucket = NULL;
    char* host = NULL;
    char* path = NULL;
    char* region = NULL;
    NCS3SVC svc = NCS3UNK;
    
    if(url == NULL)
        {stat = NC_EURL; goto done;}

    /* Parse the hostname */
    hostsegments = nclistnew();
    /* split the hostname by "." */
    if((stat = NC_split_delim(url->host,'.',hostsegments))) goto done;

    /* Parse the path*/
    pathsegments = nclistnew();
    /* split the path by "/" */
    if((stat = NC_split_delim(url->path,'/',pathsegments))) goto done;

	/* Distinguish path-style from virtual-host style from s3: and from other.
	Virtual:
		(1) https://<bucket-name>.s3.<region>.amazonaws.com/<path>
		(2) https://<bucket-name>.s3.amazonaws.com/<path> -- region defaults (to us-east-1)
	Path:
		(3) https://s3.<region>.amazonaws.com/<bucket-name>/<path>
		(4) https://s3.amazonaws.com/<bucket-name>/<path> -- region defaults to us-east-1
	S3:
		(5) s3://<bucket-name>/<path>
	Google:
		(6) https://storage.googleapis.com/<bucket-name>/<path>
		(7) gs3://<bucket-name>/<path>
	Other:
		(8) https://<host>/<bucket-name>/<path>
		(9) https://<bucket-name>.s3.<region>.domain.example.com/<path>
		(10)https://s3.<region>.example.com/<bucket>/<path>
	*/
	if(url->host == NULL || strlen(url->host) == 0)
        {stat = NC_EURL; goto done;}

    /* Reduce the host to standard form such as s3.amazonaws.com by pulling out the
       region and bucket from the host */
    if(strcmp(url->protocol,"s3")==0 && nclistlength(hostsegments)==1) { /* Format (5) */
	bucket = nclistremove(hostsegments,0);
	/* region unknown at this point */
	/* Host will be set to canonical form later */
	svc = NCS3;
    } else if(strcmp(url->protocol,"gs3")==0 && nclistlength(hostsegments)==1) { /* Format (7) */
	bucket = nclistremove(hostsegments,0);
	/* region unknown at this point */
	/* Host will be set to canonical form later */
	svc = NCS3GS;
    } else if(endswith(url->host,AWSHOST)) { /* Virtual or path */
	svc = NCS3;
	/* If we find a bucket as part of the host, then remove it */
	switch (nclistlength(hostsegments)) {
	default: stat = NC_EURL; goto done;
	case 3: /* Format (4) */ 
	    /* region unknown at this point */
    	    /* bucket unknown at this point */
	    break;
	case 4: /* Format (2) or (3) */
            if(strcasecmp(nclistget(hostsegments,0),"s3")!=0) { /* Presume format (2) */
	        /* region unknown at this point */
	        bucket = nclistremove(hostsegments,0); /* Make canonical */
            } else if(strcasecmp(nclistget(hostsegments,0),"s3")==0) { /* Format (3) */
	        region = nclistremove(hostsegments,1); /* Make canonical */
	        /* bucket unknown at this point */
	    } else /* ! Format (2) and ! Format (3) => error */
	        {stat = NC_EURL; goto done;}
	    break;
	case 5: /* Format (1) */
            if(strcasecmp(nclistget(hostsegments,1),"s3")!=0)
	        {stat = NC_EURL; goto done;}
	    /* Make canonical */
	    region = nclistremove(hostsegments,2);
    	    bucket = nclistremove(hostsegments,0);
	    break;
	}
    } else if(strcasecmp(url->host,GOOGLEHOST)==0) { /* Google (6) */
        if((host = strdup(url->host))==NULL)
	    {stat = NC_ENOMEM; goto done;}
        /* region is unknown */
	/* bucket is unknown at this point */
	svc = NCS3GS;
    } else { /* Presume Formats (8),(9),(10) */
	if (nclistlength(hostsegments) > 3 && strcasecmp(nclistget(hostsegments, 1), "s3") == 0){
		bucket = nclistremove(hostsegments, 0);
		region = nclistremove(hostsegments, 2);
		host = strdup(url->host + sizeof(bucket) + 1);
	}else{
		if (nclistlength(hostsegments) > 2 && strcasecmp(nclistget(hostsegments, 0), "s3") == 0){
			region = nclistremove(hostsegments, 1);
		}
		if ((host = strdup(url->host)) == NULL){
			stat = NC_ENOMEM;
			goto done;
		}
	}
    }

    /* region = (1) from url, (2) s3->region, (3) default */
    if(region == NULL && s3 != NULL)
	region = nulldup(s3->region);
    if(region == NULL) {
        const char* region0 = NULL;
	/* Get default region */
	if((stat = NC_getdefaults3region(url,&region0))) goto done;
	region = (char*)nulldup(region0);
    }
    if(region == NULL) {stat = NC_ES3; goto done;}

    /* bucket = (1) from url, (2) s3->bucket */
    if(bucket == NULL && nclistlength(pathsegments) > 0) {
	bucket = nclistremove(pathsegments,0); /* Get from the URL path; will reinsert below */
    }
    if(bucket == NULL && s3 != NULL)
	bucket = nulldup(s3->bucket);
    if(bucket == NULL) {stat = NC_ES3; goto done;}

    if(svc == NCS3) {
        /* Construct the revised host */
	ncbytesclear(buf);
        ncbytescat(buf,"s3");
	assert(region != NULL);
        ncbytescat(buf,".");
	ncbytescat(buf,region);
        ncbytescat(buf,AWSHOST);
	nullfree(host);
        host = ncbytesextract(buf);
    } else if(svc == NCS3GS) {
	nullfree(host);
	host = strdup(GOOGLEHOST);
    }

    ncbytesclear(buf);

    /* Construct the revised path */
    if(bucket != NULL) {
        ncbytescat(buf,"/");
        ncbytescat(buf,bucket);
    }
    for(i=0;i<nclistlength(pathsegments);i++) {
	ncbytescat(buf,"/");
	ncbytescat(buf,nclistget(pathsegments,i));
    }
    path = ncbytesextract(buf);

    /* clone the url so we can modify it*/
    if((newurl=ncuriclone(url))==NULL) {stat = NC_ENOMEM; goto done;}

    /* Modify the URL to canonical form */
    ncurisetprotocol(newurl,"https");
    assert(host != NULL);
    ncurisethost(newurl,host);
    assert(path != NULL);
    ncurisetpath(newurl,path);

    /* Add "s3" to the mode list */
    NC_addmodetag(newurl,"s3");

    /* Rebuild the url->url */
    ncurirebuild(newurl);
    /* return various items */
#ifdef AWSDEBUG
    fprintf(stderr,">>> NC_s3urlrebuild: final=%s bucket=|%s| region=|%s|\n",newurl->uri,bucket,region);
#endif
    if(newurlp) {*newurlp = newurl; newurl = NULL;}
    if(s3 != NULL) {
        s3->bucket = bucket; bucket = NULL;
        s3->region = region; region = NULL;
        s3->svc = svc;
    }
done:
    nullfree(region);
    nullfree(bucket)
    nullfree(host)
    nullfree(path)
    ncurifree(newurl);
    ncbytesfree(buf);
    nclistfreeall(hostsegments);
    nclistfreeall(pathsegments);
    return stat;
}

static int
endswith(const char* s, const char* suffix)
{
    if(s == NULL || suffix == NULL) return 0;
    size_t ls = strlen(s);
    size_t lsf = strlen(suffix);
    ssize_t delta = (ssize_t)(ls - lsf);
    if(delta < 0) return 0;
    if(memcmp(s+delta,suffix,lsf)!=0) return 0;
    return 1;
}

/**************************************************/
/* S3 utilities */

EXTERNL int
NC_s3urlprocess(NCURI* url, NCS3INFO* s3, NCURI** newurlp)
{
    int stat = NC_NOERR;
    NCURI* url2 = NULL;
    NClist* pathsegments = NULL;
    const char* profile0 = NULL;

    if(url == NULL || s3 == NULL)
        {stat = NC_EURL; goto done;}
    /* Get current profile */
    if((stat = NC_getactives3profile(url,&profile0))) goto done;
    if(profile0 == NULL) profile0 = "no";
    s3->profile = strdup(profile0);

    /* Rebuild the URL to path format and get a usable region and optional bucket*/
    if((stat = NC_s3urlrebuild(url,s3,&url2))) goto done;
    if(url2->port){
	char hostport[8192];
	snprintf(hostport,sizeof(hostport),"%s:%s",url2->host,url2->port);
	s3->host = strdup(hostport);
    }else{
        s3->host = strdup(url2->host);
    }
    /* construct the rootkey minus the leading bucket */
    pathsegments = nclistnew();
    if((stat = NC_split_delim(url2->path,'/',pathsegments))) goto done;
    if(nclistlength(pathsegments) > 0) {
	char* seg = nclistremove(pathsegments,0);
        nullfree(seg);
    }
    if((stat = NC_join(pathsegments,&s3->rootkey))) goto done;
    if(newurlp) {*newurlp = url2; url2 = NULL;}

done:
    ncurifree(url2);
    nclistfreeall(pathsegments);
    return stat;
}

int
NC_s3clone(NCS3INFO* s3, NCS3INFO** news3p)
{
    NCS3INFO* news3 = NULL;
    if(s3 && news3p) {
	if((news3 = (NCS3INFO*)calloc(1,sizeof(NCS3INFO)))==NULL)
           return NC_ENOMEM;
	if((news3->host = nulldup(s3->host))==NULL) return NC_ENOMEM;
	if((news3->region = nulldup(s3->region))==NULL) return NC_ENOMEM;
	if((news3->bucket = nulldup(s3->bucket))==NULL) return NC_ENOMEM;
	if((news3->rootkey = nulldup(s3->rootkey))==NULL) return NC_ENOMEM;
	if((news3->profile = nulldup(s3->profile))==NULL) return NC_ENOMEM;
    }
    if(news3p) {*news3p = news3; news3 = NULL;}
    else {NC_s3clear(news3); nullfree(news3);}
    return NC_NOERR;
}

int
NC_s3clear(NCS3INFO* s3)
{
    if(s3) {
	nullfree(s3->host); s3->host = NULL;
	nullfree(s3->region); s3->region = NULL;
	nullfree(s3->bucket); s3->bucket = NULL;
	nullfree(s3->rootkey); s3->rootkey = NULL;
	nullfree(s3->profile); s3->profile = NULL;
    }
    return NC_NOERR;
}

/*
Check if a url has indicators that signal an S3 or Google S3 url or ZoH S3 url.
The rules are as follows:
1. If the protocol is "s3" or "gs3" or "zoh", then return (true,s3|gs3|zoh).
2. If the mode contains "s3" or "gs3" or "zoh", then return (true,s3|gs3|zoh).
3. Check the host name:
3.1 If the host ends with ".amazonaws.com", then return (true,s3).
3.1 If the host is "storage.googleapis.com", then return (true,gs3).
4. Otherwise return (false,unknown).
*/

int
NC_iss3(NCURI* uri, NCS3SVC* svcp)
{
    int iss3 = 0;
    NCS3SVC svc = NCS3UNK;

    if(uri == NULL) goto done; /* not a uri */
    /* is the protocol "s3" or "gs3" or "zoh" ? */
    if(strcasecmp(uri->protocol,"s3")==0) {iss3 = 1; svc = NCS3; goto done;}
    if(strcasecmp(uri->protocol,"gs3")==0) {iss3 = 1; svc = NCS3GS; goto done;}
#ifdef NETCDF_ENABLE_ZOH
    if(strcasecmp(uri->protocol,"zoh")==0) {iss3 = 1; svc = NCS3ZOH; goto done;}
#endif
    /* Is "s3" or "gs3" in the mode list? */
    if(NC_testmode(uri,"s3")) {iss3 = 1; svc = NCS3; goto done;}
    if(NC_testmode(uri,"gs3")) {iss3 = 1; svc = NCS3GS; goto done;}    
    /* Last chance; see if host looks s3'y */
    if(uri->host != NULL) {
        if(endswith(uri->host,AWSHOST)) {iss3 = 1; svc = NCS3; goto done;}
        if(strcasecmp(uri->host,GOOGLEHOST)==0) {iss3 = 1; svc = NCS3GS; goto done;}
    }    
    if(svcp) *svcp = svc;
done:
    return iss3;
}

/**************************************************/
/**
The .aws/config and .aws/credentials files
are in INI format (https://en.wikipedia.org/wiki/INI_file).
This format is not well defined, so the grammar used
here is restrictive. Here, the term "profile" is the same
as the INI term "section".

The grammar used is as follows:

Grammar:

inifile: profilelist ;
profilelist: profile | profilelist profile ;
profile: '[' profilename ']' EOL entries ;
entries: empty | entries entry ;
entry:  WORD = WORD EOL ;
profilename: WORD ;
Lexical:
WORD    sequence of printable characters - [ \[\]=]+
EOL	'\n' | ';'

Note:
1. The semicolon at beginning of a line signals a comment.
2. # comments are not allowed
3. Duplicate profiles or keys are ignored.
4. Escape characters are not supported.
*/

#define AWS_EOF (-1)
#define AWS_ERR (0)
#define AWS_WORD (0x10001)
#define AWS_EOL (0x10002)

typedef struct AWSparser {
    char* text;
    char* pos;
    size_t yylen; /* |yytext| */
    NCbytes* yytext;
    int token; /* last token found */
    int pushback; /* allow 1-token pushback */
} AWSparser;

#ifdef LEXDEBUG
static const char*
tokenname(int token)
{
    static char num[32];
    switch(token) {
    case AWS_EOF: return "EOF";
    case AWS_ERR: return "ERR";
    case AWS_WORD: return "WORD";
    default: snprintf(num,sizeof(num),"%d",token); return num;
    }
    return "UNKNOWN";
}
#endif

/*
@param text of the aws credentials file
@param profiles list of form struct AWSprofile (see ncauth.h)
*/

#define LBR '['
#define RBR ']'

static void
freeprofile(struct AWSprofile* profile)
{
    if(profile) {
#ifdef AWSDEBUG
fprintf(stderr,">>> freeprofile: %s\n",profile->name);
#endif
	for(size_t i=0;i<nclistlength(profile->entries);i++) {
	    struct AWSentry* e = (struct AWSentry*)nclistget(profile->entries,i);
	    freeentry(e);
	}
        nclistfree(profile->entries);
	nullfree(profile->name);
	nullfree(profile);
    }
}

void
NC_s3freeprofilelist(NClist* profiles)
{
    if(profiles) {
	for(size_t i=0;i<nclistlength(profiles);i++) {
	    struct AWSprofile* p = (struct AWSprofile*)nclistget(profiles,i);
	    freeprofile(p);
	}
	nclistfree(profiles);
    }
}

const char*
NC_s3dumps3info(NCS3INFO* info)
{
    static char text[8192];
    snprintf(text,sizeof(text),"host=%s region=%s bucket=%s rootkey=%s profile=%s",
		(info->host?info->host:"null"),
		(info->region?info->region:"null"),
		(info->bucket?info->bucket:"null"),
		(info->rootkey?info->rootkey:"null"),
		(info->profile?info->profile:"null"));
    return text;
}

/* Find, load, and parse the aws config &/or credentials file */
int
NC_aws_load_credentials(NCglobalstate* gstate)
{
    int stat = NC_NOERR;
    NClist* profiles = nclistnew();
    NCbytes* buf = ncbytesnew();
    char path[8192];
    const char* aws_root = getenv(NC_TEST_AWS_DIR);
    const char* awscfg_local[NCONFIGFILES + 1]; /* +1 for the env variable */
    const char** awscfg = NULL;

    /* add a "no" credentials */
    {
	struct AWSprofile* noprof = (struct AWSprofile*)calloc(1,sizeof(struct AWSprofile));
	noprof->name = strdup("no");
	noprof->entries = nclistnew();
	nclistpush(profiles,noprof); noprof = NULL;
    }

    awscfg = awsconfigfiles;
    if((awscfg_local[0] = NC_getglobalstate()->aws.config_file)!=NULL) {
	memcpy(&awscfg_local[1],awsconfigfiles,sizeof(char*)*NCONFIGFILES);
	awscfg = awscfg_local;
    }
    for(;*awscfg;awscfg++) {
        /* Construct the path ${HOME}/<file> or Windows equivalent. */
	const char* cfg = *awscfg;

        snprintf(path,sizeof(path),"%s%s%s",
	    (aws_root?aws_root:gstate->home),
	    (*cfg == '/'?"":"/"),
	    cfg);
	ncbytesclear(buf);
        if((stat=NC_readfile(path,buf))) {
            nclog(NCLOGWARN, "Could not open file: %s",path);
        } else {
            /* Parse the credentials file */
	    const char* text = ncbytescontents(buf);
            if((stat = awsparse(text,profiles))) goto done;
	}
    }
  
    /* If there is no default credentials, then try to synthesize one
       from various environment variables */
    {
	size_t i;
        struct AWSprofile* dfalt = NULL;
        struct AWSentry* entry = NULL;
        NCglobalstate* gs = NC_getglobalstate();
	/* Verify that we can build a default */
        if(gs->aws.access_key_id != NULL && gs->aws.secret_access_key != NULL) {
	    /* Kill off any previous default profile */
	    for(i=nclistlength(profiles)-1;i>=0;i--) {/* walk backward because we are removing entries */
		struct AWSprofile* prof = (struct AWSprofile*)nclistget(profiles,i);
		if(strcasecmp(prof->name,"default")==0) {
		    nclistremove(profiles,i);
		    freeprofile(prof);
		}
	    }
	    /* Build new default profile */
	    if((dfalt = (struct AWSprofile*)calloc(1,sizeof(struct AWSprofile)))==NULL) {stat = NC_ENOMEM; goto done;}
	    dfalt->name = strdup("default");
	    dfalt->entries = nclistnew();
	    /* Save the new default profile */
	    nclistpush(profiles,dfalt); dfalt = NULL;
	    /* Create the entries for default */
	    if((entry = (struct AWSentry*)calloc(1,sizeof(struct AWSentry)))==NULL) {stat = NC_ENOMEM; goto done;}
	    entry->key = strdup(AWS_PROF_ACCESS_KEY_ID);
	    entry->value = strdup(gs->aws.access_key_id);
	    nclistpush(dfalt->entries,entry); entry = NULL;
	    if((entry = (struct AWSentry*)calloc(1,sizeof(struct AWSentry)))==NULL) {stat = NC_ENOMEM; goto done;}
	    entry->key = strdup(AWS_PROF_SECRET_ACCESS_KEY);
	    entry->value = strdup(gs->aws.secret_access_key);
	    nclistpush(dfalt->entries,entry); entry = NULL;
	}
    }

    if(gstate->rcinfo->s3profiles)
        NC_s3freeprofilelist(gstate->rcinfo->s3profiles);
    gstate->rcinfo->s3profiles = profiles; profiles = NULL;

#ifdef AWSDEBUG
    awsprofiles();
#endif

done:
    ncbytesfree(buf);
    NC_s3freeprofilelist(profiles);
    return stat;
}

/* Lookup a profile by name;
@param profilename to lookup
@param profilep return the matching profile; null if profile not found
@return NC_NOERR if no error
@return other error
*/

int
NC_authgets3profile(const char* profilename, struct AWSprofile** profilep)
{
    int stat = NC_NOERR;
    NCglobalstate* gstate = NC_getglobalstate();

    for(size_t i=0;i<nclistlength(gstate->rcinfo->s3profiles);i++) {
	struct AWSprofile* profile = (struct AWSprofile*)nclistget(gstate->rcinfo->s3profiles,i);
	if(strcmp(profilename,profile->name)==0)
	    {if(profilep) {*profilep = profile; goto done;}}
    }
    if(profilep) *profilep = NULL; /* not found */
done:
    return stat;
}

/**
@param profile name of profile
@param key key to search for in profile
@param value place to store the value if key is found; NULL if not found
@return NC_NOERR if key is found, Some other error otherwise.
*/

int
NC_s3profilelookup(const char* profile, const char* key, const char** valuep)
{
    int stat = NC_NOERR;
    struct AWSprofile* awsprof = NULL;
    const char* value = NULL;

    if(profile == NULL) return NC_ES3;
    if((stat=NC_authgets3profile(profile,&awsprof))==NC_NOERR && awsprof != NULL) {
        for(size_t i=0;i<nclistlength(awsprof->entries);i++) {
	    struct AWSentry* entry = (struct AWSentry*)nclistget(awsprof->entries,i);
	    if(strcasecmp(entry->key,key)==0) {
		value = entry->value;
	        break;
	    }
	}
    }
    if(valuep) *valuep = value;
    return stat;
}
/**
 * Get the credentials for a given profile or load them from environment.
 @param profile name to use to look for credentials
 @param region return region from profile or env
 @param accessid return accessid from progile or env
 @param accesskey return accesskey from profile or env
 */
void NC_s3getcredentials(const char *profile, const char **region, const char** accessid, const char** accesskey) {
    if(profile != NULL && strcmp(profile,"no") != 0) {
        NC_s3profilelookup(profile, AWS_PROF_ACCESS_KEY_ID, accessid);
        NC_s3profilelookup(profile, AWS_PROF_SECRET_ACCESS_KEY, accesskey);
        NC_s3profilelookup(profile, AWS_PROF_REGION, region);
    }
    else
    { // We load from env if not in profile
        NCglobalstate* gstate = NC_getglobalstate();
        if(gstate->aws.access_key_id != NULL && accessid){
            *accessid = gstate->aws.access_key_id;
        }
        if (gstate->aws.secret_access_key != NULL && accesskey){
            *accesskey = gstate->aws.secret_access_key;
        }
        if(gstate->aws.default_region != NULL && region){
            *region = gstate->aws.default_region;
        }
    }
}


/**************************************************/
/*
Get the current active profile. The priority order is as follows:
1. aws.profile key in mode flags
2. aws.profile in .rc entries
3. AWS_PROFILE env variable
4. "default"
5. "no" -- meaning do not use any profile => no secret key

@param uri uri with mode flags, may be NULL
@param profilep return profile name here or NULL if none found
@return NC_NOERR if no error.
@return NC_EINVAL if something else went wrong.
*/

int
NC_getactives3profile(NCURI* uri, const char** profilep)
{
    int stat = NC_NOERR;
    const char* profile = NULL;
    struct AWSprofile* ap = NULL;
    struct NCglobalstate* gs = NC_getglobalstate();

    if (uri != NULL) {
	profile = ncurifragmentlookup(uri,AWS_FRAG_PROFILE);
	if(profile == NULL)
		profile = NC_rclookupx(uri,AWS_RC_PROFILE);
    }

    if(profile == NULL && gs->aws.profile != NULL) {
        if((stat=NC_authgets3profile(gs->aws.profile,&ap))) goto done;
	if(ap) profile = nulldup(gs->aws.profile);
    }

    if(profile == NULL) {
        if((stat=NC_authgets3profile("default",&ap))) goto done;
	if(ap) profile = "default";
    }

    if(profile == NULL) {
        if((stat=NC_authgets3profile("no",&ap))) goto done;
	if(ap) profile = "no";
    }

#ifdef AWSDEBUG
    fprintf(stderr,">>> activeprofile = %s\n",(profile?profile:"null"));
#endif
    if(profilep) *profilep = profile;
done:
    return stat;
}

/*
Get the current default region. The search order is as follows:
1. aws.region key in mode flags
2. aws.region in .rc entries
3. aws_region key in current profile (only if profiles are being used)
4. NCglobalstate.aws.default_region

@param uri uri with mode flags, may be NULL
@param regionp return region name here or NULL if none found
@return NC_NOERR if no error.
@return NC_EINVAL if something else went wrong.
*/

int
NC_getdefaults3region(NCURI* uri, const char** regionp)
{
    int stat = NC_NOERR;
    const char* region = NULL;
    const char* profile = NULL;

    region = ncurifragmentlookup(uri,AWS_FRAG_REGION);
    if(region == NULL)
        region = NC_rclookupx(uri,AWS_RC_REGION);
    if(region == NULL) {/* See if we can find a profile */
        if(NC_getactives3profile(uri,&profile)==NC_NOERR) {
	    if(profile)
	        (void)NC_s3profilelookup(profile,AWS_PROF_REGION,&region);
	}
    }
    if(region == NULL)
	region = (NC_getglobalstate()->aws.default_region ? NC_getglobalstate()->aws.default_region : "us-east-1"); /* Force use of the Amazon default */
#ifdef AWSDEBUG
    fprintf(stderr,">>> activeregion = |%s|\n",region);
#endif
    if(regionp) *regionp = region;
    return stat;
}

/**
The .aws/config and .aws/credentials files
are in INI format (https://en.wikipedia.org/wiki/INI_file).
This format is not well defined, so the grammar used
here is restrictive. Here, the term "profile" is the same
as the INI term "section".

The grammar used is as follows:

Grammar:

inifile: profilelist ;
profilelist: profile | profilelist profile ;
profile: '[' profilename ']' EOL entries ;
entries: empty | entries entry ;
entry:  WORD = WORD EOL ;
profilename: WORD ;
Lexical:
WORD    sequence of printable characters - [ \[\]=]+
EOL	'\n' | ';'

Note:
1. The semicolon at beginning of a line signals a comment.
2. # comments are not allowed
3. Duplicate profiles or keys are ignored.
4. Escape characters are not supported.
*/

#define AWS_EOF (-1)
#define AWS_ERR (0)
#define AWS_WORD (0x10001)
#define AWS_EOL (0x10002)

#ifdef LEXDEBUG
static const char*
tokenname(int token)
{
    static char num[32];
    switch(token) {
    case AWS_EOF: return "EOF";
    case AWS_ERR: return "ERR";
    case AWS_WORD: return "WORD";
    default: snprintf(num,sizeof(num),"%d",token); return num;
    }
    return "UNKNOWN";
}
#endif

static int
awslex(AWSparser* parser)
{
    int token = 0;
    char* start;
    size_t count;

    parser->token = AWS_ERR;
    ncbytesclear(parser->yytext);
    ncbytesnull(parser->yytext);

    if(parser->pushback != AWS_ERR) {
	token = parser->pushback;
	parser->pushback = AWS_ERR;
	goto done;
    }

    while(token == 0) { /* avoid need to goto when retrying */
	char c = *parser->pos;
	if(c == '\0') {
	    token = AWS_EOF;
	} else if(c == '\n') {
	    parser->pos++;
	    token = AWS_EOL;
	} else if(c <= ' ' || c == '\177') {
	    parser->pos++;
	    continue; /* ignore whitespace */
	} else if(c == ';') {
	    char* p = parser->pos - 1;
	    if(*p == '\n') {
	        /* Skip comment */
	        do {p++;} while(*p != '\n' && *p != '\0');
	        parser->pos = p;
	        token = (*p == '\n'?AWS_EOL:AWS_EOF);
	    } else {
	        token = ';';
	        ncbytesappend(parser->yytext,';');
		parser->pos++;
	    }
	} else if(c == '[' || c == ']' || c == '=') {
	    ncbytesappend(parser->yytext,c);
    	    ncbytesnull(parser->yytext);
	    token = c;
	    parser->pos++;
	} else { /*Assume a word*/
	    start = parser->pos;
	    for(;;) {
		c = *parser->pos++;
	        if(c <= ' ' || c == '\177' || c == '[' || c == ']' || c == '=') break; /* end of word */
	    }
	    /* Pushback last char */
	    parser->pos--;
	    count = (size_t)(parser->pos - start);
	    ncbytesappendn(parser->yytext,start,count);
	    ncbytesnull(parser->yytext);
	    token = AWS_WORD;
	}
#ifdef LEXDEBUG
fprintf(stderr,"%s(%d): |%s|\n",tokenname(token),token,ncbytescontents(parser->yytext));
#endif
    } /*for(;;)*/

done:
    parser->token = token;
    return token;
}

/*
@param text of the aws credentials file
@param profiles list of form struct AWSprofile (see ncauth.h)
*/

#define LBR '['
#define RBR ']'

static int
awsparse(const char* text, NClist* profiles)
{
    int stat = NC_NOERR;
    size_t len;
    AWSparser* parser = NULL;
    struct AWSprofile* profile = NULL;
    int token;
    char* key = NULL;
    char* value = NULL;

    if(text == NULL) text = "";

    parser = calloc(1,sizeof(AWSparser));
    if(parser == NULL)
	{stat = (NC_ENOMEM); goto done;}
    len = strlen(text);
    parser->text = (char*)malloc(len+1+1+1); /* double nul term plus leading EOL */
    if(parser->text == NULL)
	{stat = (NCTHROW(NC_EINVAL)); goto done;}
    parser->pos = parser->text;
    parser->pos[0] = '\n'; /* So we can test for comment unconditionally */
    parser->pos++;
    strcpy(parser->text+1,text);
    parser->pos += len;
    /* Double nul terminate */
    parser->pos[0] = '\0';
    parser->pos[1] = '\0';
    parser->pos = &parser->text[0]; /* reset */
    parser->yytext = ncbytesnew();
    parser->pushback = AWS_ERR;

    /* Do not need recursion, use simple loops */
    for(;;) {
        token = awslex(parser); /* make token always be defined */
	if(token ==  AWS_EOF) break; /* finished */
	if(token ==  AWS_EOL) {continue;} /* blank line */
	if(token != LBR) {stat = NCTHROW(NC_EINVAL); goto done;}
	/* parse [profile name] or [name] */
        token = awslex(parser);
	if(token != AWS_WORD) {stat = NCTHROW(NC_EINVAL); goto done;}
	assert(profile == NULL);
	if((profile = (struct AWSprofile*)calloc(1,sizeof(struct AWSprofile)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	profile->name = ncbytesextract(parser->yytext);
	if(strncmp("profile", profile->name, sizeof("profile")) == 0 ) {
		token =  awslex(parser);
		if(token != AWS_WORD) {stat = NCTHROW(NC_EINVAL); goto done;}
		nullfree(profile->name);
		profile->name = ncbytesextract(parser->yytext);
	}
	profile->entries = nclistnew();
        token = awslex(parser);
	if(token != RBR) {stat = NCTHROW(NC_EINVAL); goto done;}
#ifdef PARSEDEBUG
fprintf(stderr,">>> parse: profile=%s\n",profile->name);
#endif
	/* The fields can be in any order */
	for(;;) {
	    struct AWSentry* entry = NULL;
            token = awslex(parser);
	    if(token == AWS_EOL) {
	        continue; /* ignore empty lines */
	    } else if(token == AWS_EOF) {
	        break;
	    } else if(token == LBR) {/* start of next profile */
	        parser->pushback = token;
		break;
	    } else if(token ==  AWS_WORD) {
	    	key = ncbytesextract(parser->yytext);
		token = awslex(parser);
	        if(token != '=') {stat = NCTHROW(NC_EINVAL); goto done;}
	        token = awslex(parser);
		if(token != AWS_EOL && token != AWS_WORD) {stat = NCTHROW(NC_EINVAL); goto done;}
	        value = ncbytesextract(parser->yytext);
	        if((entry = (struct AWSentry*)calloc(1,sizeof(struct AWSentry)))==NULL)
	            {stat = NC_ENOMEM; goto done;}
	        entry->key = key; key = NULL;
    	        entry->value = value; value = NULL;
#ifdef PARSEDEBUG
fprintf(stderr,">>> parse: entry=(%s,%s)\n",entry->key,entry->value);
#endif
		nclistpush(profile->entries,entry); entry = NULL;
		if(token == AWS_WORD) token = awslex(parser); /* finish the line */
	    } else
	        {stat = NCTHROW(NC_EINVAL); goto done;}
	}

	/* If this profile already exists, then overwrite old one */
	for(size_t i=0;i<nclistlength(profiles);i++) {
	    struct AWSprofile* p = (struct AWSprofile*)nclistget(profiles,i);
	    if(strcasecmp(p->name,profile->name)==0) {
		// Keep unique parameters from previous (incomplete!?) profile
		for (size_t j=0;j<nclistlength(p->entries);j++){
			struct AWSentry* old = (struct AWSentry*)nclistget(p->entries,j);
			int add = 1;
			for (size_t z=0;z<nclistlength(profile->entries);z++){
				struct AWSentry* new = (struct AWSentry*)nclistget(profile->entries,z);
				add &= (strcasecmp(old->key,new->key)!=0);
			}
			if(add){
			    nclistpush(profile->entries, nclistremove(p->entries,j--));
			}
		}
		nclistset(profiles,i,profile);
                profile = NULL;
		/* reclaim old one */
		freeprofile(p);
		break;
	    }
	}
	if(profile) nclistpush(profiles,profile);
	profile = NULL;
    }

done:
    if(profile) freeprofile(profile);
    nullfree(key);
    nullfree(value);
    if(parser != NULL) {
	nullfree(parser->text);
	ncbytesfree(parser->yytext);
	free(parser);
    }
    return (stat);
}

static void
freeentry(struct AWSentry* e)
{
    if(e) {
#ifdef AWSDEBUG
fprintf(stderr,">>> freeentry: key=%s value=%s\n",e->key,e->value);
#endif
        nullfree(e->key);
        nullfree(e->value);
        nullfree(e);
    }
}

/* Provide profile-related  dumper(s) */
void
awsdumpprofile(struct AWSprofile* p)
{
    size_t j;
    if(p == NULL) {
        fprintf(stderr,"    <NULL>");
	goto done;
    }
    fprintf(stderr,"    [%s]",p->name);
    if(p->entries == NULL) {
	fprintf(stderr,"<NULL>");
	goto done;
    }
    for(j=0;j<nclistlength(p->entries);j++) {
        struct AWSentry* e = (struct AWSentry*)nclistget(p->entries,j);
	fprintf(stderr," %s=%s",e->key,e->value);
    }
done:
    fprintf(stderr,"\n");
}

void
awsdumpprofiles(NClist* profiles)
{
    size_t i;
    NCglobalstate* gs = NC_getglobalstate();
    for(i=0;i<nclistlength(gs->rcinfo->s3profiles);i++) {
	struct AWSprofile* p = (struct AWSprofile*)nclistget(profiles,i);
	awsdumpprofile(p);
    }
}

void
awsprofiles(void)
{
    NCglobalstate* gs = NC_getglobalstate();
    fprintf(stderr,">>> profiles from global->rcinfo->s3profiles:\n");
    awsdumpprofiles(gs->rcinfo->s3profiles);
}

