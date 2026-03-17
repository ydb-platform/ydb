/* Copyright 2018, UCAR/Unidata.
   See the COPYRIGHT file for more information.
*/

#include "d4includes.h"
#include <stddef.h>

/*
Provide a simple dump of binary data
*/

/**************************************************/

void
NCD4_dumpbytes(size_t size, const void* data0, int swap)
{
    size_t extended;
    void* data = NULL;
    char* pos = NULL;
    size_t i;

    extended = size + 8;
    data = calloc(1,extended); /* provide some space to simplify the code */
    memcpy(data,data0,size);
    fprintf(stderr,"        C  XU8  U8  I8   XU32       U32          I32           I16     I64                  F32\t\t\tF64\n");
    for(i=0,pos=data;i<size; pos++,i++) {
	struct {
	    unsigned char u8[1];
	      signed char i8[1];
	    unsigned short u16[1];
	             short i16[1];
	    unsigned int u32[1];
	             int i32[1];
	    unsigned long long u64[1];
	             long long i64[1];
	    float f32[1];
	    double f64[1];
	    char s[8];
	} v;
	v.s[0] = *((char*)pos);
	v.s[1] = '\0';
	v.u8[0] = *((unsigned char*)pos);
	v.i8[0] = *((signed char*)pos);
        v.u16[0] = *((unsigned short*)pos);
        v.i16[0] = *((short*)pos);
        v.u32[0] = *((unsigned int*)pos);
        v.i32[0] = *((int*)pos);
        v.u64[0] = *((unsigned long long*)pos);
        v.i64[0] = *((long long*)pos);
        v.f32[0] = *((float*)pos);
        v.f64[0] = *((double*)pos);
	if(swap) {
	    swapinline16(v.u16);
	    swapinline32(v.u32);
	    swapinline64(v.u64);
	    swapinline16(v.i16);
	    swapinline32(v.i32);
	    swapinline64(v.i64);
	    swapinline32(v.f32);
	    swapinline64(v.f64);
        }
        if(v.s[0] == '\r') strcpy(v.s,"\\r");
        else if(v.s[0] == '\n') strcpy(v.s,"\\n");
        else if(v.s[0] < ' ' || v.s[0] >= 0x7f) v.s[0] = '?';
        fprintf(stderr,"[%04lu]", (unsigned long)i);
        fprintf(stderr," '%s'",v.s);
        fprintf(stderr," %03x  %03u %04d", v.u8[0], v.u8[0], v.i8[0]);
        fprintf(stderr," 0x%08x %012u %013d", v.u32[0], v.u32[0], v.i32[0]);
        fprintf(stderr," %07d", v.i16[0]);
        fprintf(stderr," %020lld", v.i64[0]);
        fprintf(stderr," %4.4g\t\t%4.4lg", v.f32[0], v.f64[0]);
        fprintf(stderr,"\n");
	fflush(stderr);
    }
    nullfree(data);
}

void
NCD4_tagdump(size_t size, const void* data0, int swap, const char* tag)
{
    fprintf(stderr,"++++++++++ %s ++++++++++\n",tag);
    NCD4_dumpbytes(size,data0,swap);
    fprintf(stderr,"++++++++++ %s ++++++++++\n",tag);
    fflush(stderr);
}

/* Dump the variables in a group */
void
NCD4_dumpvars(NCD4node* group)
{
    size_t i;
    fprintf(stderr,"%s.vars:\n",group->name);
    for(i=0;i<nclistlength(group->vars);i++) {
	NCD4node* var = (NCD4node*)nclistget(group->vars,i);
	NCD4node* type;

	switch (var->subsort) {
	default:
	    type = var->basetype;
	    fprintf(stderr,"<%s name=\"%s\"/>\n",type->name,var->name);
	    break;
	case NC_STRUCT:
	    fprintf(stderr,"<%s name=\"%s\"/>\n","Struct",var->name);
	    break;
	case NC_SEQ:
	    fprintf(stderr,"<%s name=\"%s\"/>\n","Sequence",var->name);
	    break;
        }
    }
    fflush(stderr);
}

union ATOMICS*
NCD4_dumpatomic(NCD4node* var, void* data)
{
    union ATOMICS* p = (union ATOMICS*)data;
    return p;
}
