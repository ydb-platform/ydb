/*
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"


#if OPAL_WANT_MEMCHECKER

#include "opal/mca/memchecker/memchecker.h"
#include "opal/mca/memchecker/base/base.h"

int opal_memchecker_base_runindebugger(void)
{
    return opal_memchecker_base_module->runindebugger();
}

int opal_memchecker_base_isaddressable(void * p, size_t len)
{
    return opal_memchecker_base_module->isaddressable(p, len);
}


int opal_memchecker_base_isdefined(void * p, size_t len)
{
    return opal_memchecker_base_module->isdefined(p, len);
}


int opal_memchecker_base_mem_noaccess(void * p, size_t len)
{
    return opal_memchecker_base_module->mem_noaccess(p, len);
}


int opal_memchecker_base_mem_undefined(void * p, size_t len)
{
    return opal_memchecker_base_module->mem_undefined(p, len);
}


int opal_memchecker_base_mem_defined(void * p, size_t len)
{
    return opal_memchecker_base_module->mem_defined(p, len);
}


int opal_memchecker_base_mem_defined_if_addressable(void * p, size_t len)
{
    return opal_memchecker_base_module->mem_defined_if_addressable(p, len);
}


int opal_memchecker_base_create_block(void * p, size_t len, char * description)
{
    return opal_memchecker_base_module->create_block(p, len, description);
}


int opal_memchecker_base_discard_block(void * p)
{
    return opal_memchecker_base_module->discard_block(p);
}


int opal_memchecker_base_leakcheck(void)
{
    return opal_memchecker_base_module->leakcheck();
}

int opal_memchecker_base_get_vbits(void * p, char * vbits, size_t len)
{
    return opal_memchecker_base_module->get_vbits(p, vbits, len);
}

int opal_memchecker_base_set_vbits(void * p, char * vbits, size_t len)
{
    return opal_memchecker_base_module->set_vbits(p, vbits, len);
}

#endif /* OPAL_WANT_MEMCHECKER */
