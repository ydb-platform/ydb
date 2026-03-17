/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information.
*/

#include "config.h"

#include <stdlib.h>
#include <stdio.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#include "ocinternal.h"
#include "ocdebug.h"
#include "ncutil.h"

#define MAXLEVEL 1

/*Forward*/
static void dumpocnode1(OCnode* node, int depth);
static void dumpdimensions(OCnode* node);
static void dumpattvalue(OCtype nctype, char** aset, int index);

static const char* sindent = 
	"                                                                                                     ";

static const char*
dent(int n)
{
    if(n > 100) n = 100;
    return sindent+(100-n);
}

/* support [dd] leader*/
static const char*
dent2(int n) {return dent(n+4);}

static void
tabto(int pos, NCbytes* buffer)
{
    int bol,len,pad;
    len = (int)ncbyteslength(buffer);
    /* find preceding newline */
    for(bol=len-1;;bol--) {
	int c = ncbytesget(buffer,(size_t)bol);
	if(c < 0) break;
	if(c == '\n') {bol++; break;}
    }
    len = (len - bol);
    pad = (pos - len);
    while(pad-- > 0) ncbytescat(buffer," ");
}

void
ocdumpnode(OCnode* node)
{
    if(node != NULL) {
        dumpocnode1(node,0);
    } else {
	fprintf(stdout,"<NULL>\n");
    }
    fflush(stdout);
}

static void
dumpocnode1(OCnode* node, int depth)
{
    unsigned int n;
    switch (node->octype) {
    case OC_Atomic: {
        fprintf(stdout,"[%2d]%s ",depth,dent(depth));
	if(node->name == NULL) OCPANIC("prim without name");
	fprintf(stdout,"%s %s",octypetostring(node->etype),node->name);
	dumpdimensions(node);
	fprintf(stdout," &%p",node);
	fprintf(stdout,"\n");
    } break;

    case OC_Dataset: {
        fprintf(stdout,"[%2d]%s ",depth,dent(depth));
	fprintf(stdout,"dataset %s\n",
		(node->name?node->name:""));
	for(n=0;n<nclistlength(node->subnodes);n++) {
	    dumpocnode1((OCnode*)nclistget(node->subnodes,n),depth+1);
	}
    } break;

    case OC_Structure: {
        fprintf(stdout,"[%2d]%s ",depth,dent(depth));
	fprintf(stdout,"struct %s",
		(node->name?node->name:""));
	dumpdimensions(node);
	fprintf(stdout," &%p",node);
	fprintf(stdout,"\n");
	for(n=0;n<nclistlength(node->subnodes);n++) {
	    dumpocnode1((OCnode*)nclistget(node->subnodes,n),depth+1);
	}
    } break;

    case OC_Sequence: {
        fprintf(stdout,"[%2d]%s ",depth,dent(depth));
	fprintf(stdout,"sequence %s",
		(node->name?node->name:""));
	dumpdimensions(node);
	fprintf(stdout," &%p",node);
	fprintf(stdout,"\n");
	for(n=0;n<nclistlength(node->subnodes);n++) {
	    dumpocnode1((OCnode*)nclistget(node->subnodes,n),depth+1);
	}
    } break;

    case OC_Grid: {
	unsigned int i;
        fprintf(stdout,"[%2d]%s ",depth,dent(depth));
	fprintf(stdout,"grid %s",
		(node->name?node->name:""));
	dumpdimensions(node);
	fprintf(stdout," &%p",node);
	fprintf(stdout,"\n");
	fprintf(stdout,"%sarray:\n",dent2(depth+1));
	dumpocnode1((OCnode*)nclistget(node->subnodes,0),depth+2);
	fprintf(stdout,"%smaps:\n",dent2(depth+1));
	for(i=1;i<nclistlength(node->subnodes);i++) {
	    dumpocnode1((OCnode*)nclistget(node->subnodes,i),depth+2);
	}
    } break;

    case OC_Attribute: {
        fprintf(stdout,"[%2d]%s ",depth,dent(depth));
	if(node->name == NULL) OCPANIC("Attribute without name");
	fprintf(stdout,"%s %s",octypetostring(node->etype),node->name);
	for(n=0;n<nclistlength(node->att.values);n++) {
	    char* value = (char*)nclistget(node->att.values,n);
	    if(n > 0) fprintf(stdout,",");
	    fprintf(stdout," %s",value);
	}
	fprintf(stdout," &%p",node);
	fprintf(stdout,"\n");
    } break;

    case OC_Attributeset: {
        fprintf(stdout,"[%2d]%s ",depth,dent(depth));
	fprintf(stdout,"%s:\n",node->name?node->name:"Attributes");
	for(n=0;n<nclistlength(node->subnodes);n++) {
	    dumpocnode1((OCnode*)nclistget(node->subnodes,n),depth+1);
	}
    } break;

    default:
	OCPANIC1("encountered unexpected node type: %x",node->octype);
    }

    if(node->attributes != NULL) {
	unsigned int i;
	for(i=0;i<nclistlength(node->attributes);i++) {
	    OCattribute* att = (OCattribute*)nclistget(node->attributes,i);
	    fprintf(stdout,"%s[%s=",dent2(depth+2),att->name);
	    if(att->nvalues == 0)
		OCPANIC("Attribute.nvalues == 0");
	    if(att->nvalues == 1) {
		dumpattvalue(att->etype,att->values,0);
	    } else {
		int j;
	        fprintf(stdout,"{");
		for(j=0;j<att->nvalues;j++) {
		    if(j>0) fprintf(stdout,", ");
		    dumpattvalue(att->etype,att->values,j);
		}
	        fprintf(stdout,"}");
	    }
	    fprintf(stdout,"]\n");
	}
    }
}

static void
dumpdimensions(OCnode* node)
{
    unsigned int i;
    for(i=0;i<node->array.rank;i++) {
        OCnode* dim = (OCnode*)nclistget(node->array.dimensions,i);
        fprintf(stdout,"[%s=%lu]",
			(dim->name?dim->name:"?"),
			(unsigned long)dim->dim.declsize);
    }
}

static void
dumpattvalue(OCtype nctype, char** strings, int index)
{
    if(nctype == OC_String || nctype == OC_URL) {
        fprintf(stdout,"\"%s\"",strings[index]);
    } else {
        fprintf(stdout,"%s",strings[index]);
    }
}

void
ocdumpslice(OCslice* slice)
{
    fprintf(stdout,"[");
    fprintf(stdout,"%lu",(unsigned long)slice->first);
    if(slice->stride > 1) fprintf(stdout,":%lu",(unsigned long)slice->stride);
    fprintf(stdout,":%lu",(unsigned long)(slice->first+slice->count)-1);
    fprintf(stdout,"]");
}

void
ocdumpclause(OCprojectionclause* ref)
{
    unsigned int i;
    NClist* path = nclistnew();
    occollectpathtonode(ref->node,path);
    for(i=0;i<nclistlength(path);i++) {
        NClist* sliceset;
	OCnode* node = (OCnode*)nclistget(path,i);
	if(node->tree != NULL) continue; /* leave off the root node*/
	fprintf(stdout,"%s%s",(i>0?PATHSEPARATOR:""),node->name);
	sliceset = (NClist*)nclistget(ref->indexsets,i);
	if(sliceset != NULL) {
	    unsigned int j;
	    for(j=0;j<nclistlength(sliceset);j++) {
	        OCslice* slice = (OCslice*)nclistget(sliceset,j);
	        ocdumpslice(slice);
	    }
	}
    }
}


static void
addfield(char* field, size_t llen, char* line, int align)
{
    int len,rem;
    strlcat(line,"|",llen);
    strlcat(line,field,llen);
    len = (int)strlen(field);
    rem = (align - len);
    while(rem-- > 0) strlcat(line," ",llen);
}

static void
dumpfield(size_t index, char* n8, int isxdr)
{
    char line[1024];
    char tmp[32];

    union {
	unsigned int uv;
	int sv;
	char cv[4];
	float fv;
    } form;
    union {
	char cv[8];
        unsigned long long ll;
        double d;
    } dform;

    line[0] = '\0';

    /* offset */
    snprintf(tmp,sizeof(tmp),"%6zd",index);
    addfield(tmp,sizeof(line),line,5);

    memcpy(form.cv,n8,4);

    /* straight hex*/
    snprintf(tmp,sizeof(tmp),"%08x",form.uv);
    addfield(tmp,sizeof(line),line,8);

    if(isxdr) {swapinline32(&form.uv);}

    /* unsigned integer */
    snprintf(tmp,sizeof(tmp),"%12u",form.uv);
    addfield(tmp,sizeof(line),line,12);

    /* signed integer */
    snprintf(tmp,sizeof(tmp),"%12d",form.sv);
    addfield(tmp,sizeof(line),line,12);

    /* float */
    snprintf(tmp,sizeof(tmp),"%#g",form.fv);
    addfield(tmp,sizeof(line),line,12);

    /* char[4] */
    {
        /* use raw form (i.e. n8)*/
        int i;
	tmp[0] = '\0';
        for(i=0;i<4;i++) {
	    char stmp[64];
	    unsigned int c = (n8[i] & 0xff);
	    if(c < ' ' || c > 126)
                snprintf(stmp,sizeof(stmp),"\\%02x",c);
	    else
                snprintf(stmp,sizeof(stmp),"%c",c);
	    strlcat(tmp,stmp,sizeof(tmp));
        }
    }

    addfield(tmp,sizeof(line),line,16);

    /* double */
    memcpy(dform.cv,n8,(size_t)(2*XDRUNIT));
    if(isxdr) xxdrntohdouble(dform.cv,&dform.d);
    snprintf(tmp,sizeof(tmp),"%#g",dform.d);
    addfield(tmp,sizeof(line),line,12);

    fprintf(stdout,"%s\n",line);
}

static void
typedmemorydump(char* memory, size_t len, int fromxdr)
{
    unsigned int i,rem;
    char line[1024];
    char* pmem;
    char mem[8];

    assert(memory[len] == 0);

    /* build the header*/
    line[0] = '\0';
    addfield("offset",sizeof(line),line,6);
    addfield("hex",sizeof(line),line,8);
    addfield("uint",sizeof(line),line,12);
    addfield("int",sizeof(line),line,12);
    addfield("float",sizeof(line),line,12);
    addfield("char[4]",sizeof(line),line,16);
    addfield("double",sizeof(line),line,12);
    strlcat(line,"\n",sizeof(line));
    fprintf(stdout,"%s",line);

    size_t count = (len / sizeof(int));
    rem = (len % sizeof(int));

    for(pmem=memory,i=0;i<count;i++,pmem+=4) {
	memset(mem,0,8);
	if(i<(count-1))
	    memcpy(mem,pmem,8);
	else
	    memcpy(mem,pmem,4);
	dumpfield(i*sizeof(unsigned int),mem,fromxdr);
    }
    if(rem > 0) {
	memset(mem,0,8);
	memcpy(mem,pmem,4);
	dumpfield(i*sizeof(unsigned int),mem,fromxdr);
    }
    fflush(stdout);
}

static void
simplememorydump(char* memory, size_t len, int fromxdr)
{
    unsigned int i,rem;
    int* imemory;
    char tmp[32];
    char line[1024];

    assert(memory[len] == 0);

    /* build the header*/
    line[0] = '\0';
    addfield("offset",sizeof(line),line,6);
    addfield("XDR (hex)",sizeof(line),line,9);
    addfield("!XDR (hex)",sizeof(line),line,10);
    fprintf(stdout,"%s\n",line);

    size_t count = (len / sizeof(int));
    rem = (len % sizeof(int));
    if(rem != 0)
	fprintf(stderr,"ocdump: |mem|%%4 != 0\n");
    imemory = (int*)memory;

    for(i=0;i<count;i++) {
	unsigned int vx = (unsigned int)imemory[i];
	unsigned int v = vx;
	if(!xxdr_network_order) swapinline32(&v);
        line[0] = '\0';
        snprintf(tmp,sizeof(tmp),"%6d",i);
        addfield(tmp,sizeof(line),line,6);
        snprintf(tmp,sizeof(tmp),"%08x",vx);
        addfield(tmp,sizeof(line),line,9);
        snprintf(tmp,sizeof(tmp),"%08x",v);
        addfield(tmp,sizeof(line),line,10);
        fprintf(stdout,"%s\n",line);
    }
    fflush(stdout);
}

void
ocdumpmemory(char* memory, size_t len, int xdrencoded, int level)
{
    if(level > MAXLEVEL) level = MAXLEVEL;
    switch (level) {
    case 1: /* Do a multi-type dump */
        typedmemorydump(memory,len,xdrencoded);
	break;
    case 0: /* Dump a simple linear list of the contents of the memory as 32-bit hex and decimal */
    default:
        simplememorydump(memory,len,xdrencoded);
	break;
    }
}

static OCerror
ocreadfile(FILE* file, off_t datastart, char** memp, size_t* lenp)
{
    char* mem = NULL;
    size_t len;
    struct stat stats;
    long pos;
    OCerror stat = OC_NOERR;

    pos = ftell(file);
    if(pos < 0) {
      fprintf(stderr,"ocreadfile: ftell error.\n");
      stat = OC_ERCFILE;
      goto done;
    }

    fseek(file,0,SEEK_SET);
    if(fseek(file,(long)datastart,SEEK_SET) < 0) {
	fprintf(stderr,"ocreadfile: fseek error.\n");
	stat = OC_ERCFILE;
	goto done;
    }

    if(fstat(fileno(file),&stats) < 0) {
	fprintf(stderr,"ocreadfile: fstat error.\n");
	stat = OC_ERCFILE;
	goto done;
    }
    len = (size_t)stats.st_size;
    len -= (size_t)datastart;

    mem = (char*)calloc(len+1,1);
    if(mem == NULL) {stat = OC_ENOMEM; goto done;}

    /* Read only the data part */
    size_t red = fread(mem,1,len,file);
    if(red < len) {
	fprintf(stderr,"ocreadfile: short file\n");
	stat = OC_ERCFILE;
	goto done;
    }

    if(fseek(file,pos,SEEK_SET) < 0) {; /* leave it as we found it*/
      fprintf(stderr,"ocreadfile: fseek error.\n");
      stat = OC_ERCFILE;
      goto done;
    }
    if(memp) {*memp = mem; mem = NULL;}
    if(lenp) *lenp = len;

 done:
    if(mem != NULL)
      free(mem);
    return OCTHROW(stat);
}

void
ocdd(OCstate* state, OCnode* root, int xdrencoded, int level)
{
    char* mem;
    size_t len;
    if(root->tree->data.file != NULL) {
        if(!ocreadfile(root->tree->data.file,
                       root->tree->data.bod,
                       &mem,
                       &len)) {
          /* ocreadfile allocates memory that must be freed. */
          if(mem != NULL) free(mem);
          fprintf(stderr,"ocdd could not read data file\n");
          return;
        }
        ocdumpmemory(mem,len,xdrencoded,level);
        free(mem);
    } else {
        mem = root->tree->data.memory;
        mem += root->tree->data.bod;
        len = (size_t)root->tree->data.datasize;
        len -= (size_t)root->tree->data.bod;
        ocdumpmemory(mem,len,xdrencoded,level);
    }
}

void
ocdumpdata(OCstate* state, OCdata* data, NCbytes* buffer, int frominstance)
{
    char tmp[1024];
    OCnode* pattern = data->pattern;
    char* smode = NULL;

    snprintf(tmp,sizeof(tmp),"%p:",data);
    ncbytescat(buffer,tmp);
    if(!frominstance) {
        ncbytescat(buffer," node=");
        ncbytescat(buffer,pattern->name);
    }
    snprintf(tmp,sizeof(tmp)," xdroffset=%ld",(unsigned long)data->xdroffset);
    ncbytescat(buffer,tmp);
    if(data->pattern->octype == OC_Atomic) {
        snprintf(tmp,sizeof(tmp)," xdrsize=%ld",(unsigned long)data->xdrsize);
        ncbytescat(buffer,tmp);
    }
    if(ociscontainer(pattern->octype)) {
        snprintf(tmp,sizeof(tmp)," ninstances=%d",(int)data->ninstances);
        ncbytescat(buffer,tmp);
    } else if(pattern->etype == OC_String || pattern->etype == OC_URL) {
        snprintf(tmp,sizeof(tmp)," nstrings=%d",(int)data->nstrings);
        ncbytescat(buffer,tmp);
    }
    ncbytescat(buffer," container=");
    snprintf(tmp,sizeof(tmp),"%p",data->container);
    ncbytescat(buffer,tmp);
    ncbytescat(buffer," mode=");
    ncbytescat(buffer,(smode=ocdtmodestring(data->datamode,0)));
    nullfree(smode);
}

/*
Depth Offset   Index Flags Size Type      Name
0123456789012345678901234567890123456789012345
0         1         2         3         4
[001] 00000000 0000  FS    0000 Structure person
*/
static const int tabstops[] = {0,6,15,21,27,32,42};
static const char* header =
"Depth Offset   Index Flags Size Type     Name\n";

void
ocdumpdatatree(OCstate* state, OCdata* data, NCbytes* buffer, int depth)
{
    size_t i,rank;
    OCnode* pattern;
    char tmp[1024];
    size_t crossproduct;
    int tabstop = 0;
    const char* typename;
    char* smode = NULL;

    /* If this is the first call, then dump a header line */
    if(depth == 0) {
	ncbytescat(buffer,header);
    }

    /* get info about the pattern */
    pattern = data->pattern;
    rank = pattern->array.rank;

    /* get total dimension size */
    if(rank > 0)
        crossproduct = octotaldimsize(pattern->array.rank,pattern->array.sizes);

    /* Dump the depth first */
    snprintf(tmp,sizeof(tmp),"[%03d]",depth);
    ncbytescat(buffer,tmp);

    tabto(tabstops[++tabstop],buffer);

    snprintf(tmp,sizeof(tmp),"%08lu",(unsigned long)data->xdroffset);
    ncbytescat(buffer,tmp);

    tabto(tabstops[++tabstop],buffer);

    /* Dump the Index wrt to parent, if defined */
    if(fisset(data->datamode,OCDT_FIELD)
       || fisset(data->datamode,OCDT_ELEMENT)
       || fisset(data->datamode,OCDT_RECORD)) {
        snprintf(tmp,sizeof(tmp),"%04lu ",(unsigned long)data->index);
        ncbytescat(buffer,tmp);
    }

    tabto(tabstops[++tabstop],buffer);

    /* Dump the mode flags in compact form */
    ncbytescat(buffer,(smode=ocdtmodestring(data->datamode,1)));
    nullfree(smode);

    tabto(tabstops[++tabstop],buffer);

    /* Dump the size or ninstances */
    if(fisset(data->datamode,OCDT_ARRAY)
       || fisset(data->datamode,OCDT_SEQUENCE)) {
        snprintf(tmp,sizeof(tmp),"%04lu",(unsigned long)data->ninstances);
    } else {
        snprintf(tmp,sizeof(tmp),"%04lu",(unsigned long)data->xdrsize);
    }
    ncbytescat(buffer,tmp);

    tabto(tabstops[++tabstop],buffer);

    if(pattern->octype == OC_Atomic) {
	typename = octypetoddsstring(pattern->etype);
    } else { /*must be container*/
	typename = octypetoddsstring(pattern->octype);
    }
    ncbytescat(buffer,typename);

    tabto(tabstops[++tabstop],buffer);

    ncbytescat(buffer,pattern->name);

    if(rank > 0) {
	snprintf(tmp,sizeof(tmp),"[%lu]",(unsigned long)crossproduct);
	ncbytescat(buffer,tmp);
    }
    ncbytescat(buffer,"\n");

    /* dump the sub-instance, which might be fields, records, or elements */
    if(!fisset(data->datamode,OCDT_ATOMIC)) {
        for(i=0;i<data->ninstances;i++)
	    ocdumpdatatree(state,data->instances[i],buffer,depth+1);
    }
}

void
ocdumpdatapath(OCstate* state, OCdata* data, NCbytes* buffer)
{
    int i;
    OCdata* path[1024];
    char tmp[1024];
    OCdata* pathdata;
    OCnode* pattern;
    int isrecord;

    path[0] = data;
    for(i=1;;i++) {
	OCdata* next = path[i-1];
	if(next->container == NULL) break;
	path[i] = next->container;
    }
    /* Path is in reverse order */
    for(i=i-1;i>=0;i--) {
	pathdata = path[i];
	pattern = pathdata->pattern;
	ncbytescat(buffer,"/");
	ncbytescat(buffer,pattern->name);
	/* Check the mode of the next step in path */
	if(i > 0) {
	    OCdata* next = path[i-1];
	    if(fisset(next->datamode,OCDT_FIELD)
		|| fisset(next->datamode,OCDT_ELEMENT)
		|| fisset(next->datamode,OCDT_RECORD)) {
		snprintf(tmp,sizeof(tmp),".%lu",(unsigned long)next->index);
	        ncbytescat(buffer,tmp);
	    }
	}
	if(pattern->octype == OC_Atomic) {
	    if(pattern->array.rank > 0) {
	        size_t xproduct = octotaldimsize(pattern->array.rank,pattern->array.sizes);
	        snprintf(tmp,sizeof(tmp),"[0..%lu]",xproduct-1);
	        ncbytescat(buffer,tmp);
	    }
	}
	isrecord = 0;
	if(pattern->octype == OC_Sequence) {
	    /* Is this a record or a sequence ? */
	    isrecord = (fisset(pathdata->datamode,OCDT_RECORD) ? 1 : 0);
	}
    }
    /* Add suffix to path */
    if(ociscontainer(pattern->octype)) {
        /* add the container type, except distinguish record and sequence */
	ncbytescat(buffer,":");
	if(isrecord)
	    ncbytescat(buffer,"Record");
	else
	    ncbytescat(buffer,octypetoddsstring(pattern->octype));
    } else if(ocisatomic(pattern->octype)) {
	/* add the atomic etype */
	ncbytescat(buffer,":");
	ncbytescat(buffer,octypetoddsstring(pattern->etype));
    }
    snprintf(tmp,sizeof(tmp),"->0x%p",pathdata);
    ncbytescat(buffer,tmp);
}
