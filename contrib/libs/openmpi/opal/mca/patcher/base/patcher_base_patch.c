/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/mca/patcher/patcher.h"
#include "opal/mca/patcher/base/base.h"
#include "opal/util/sys_limits.h"
#include "opal/prefetch.h"
#include <sys/mman.h>

static void mca_patcher_base_patch_construct (mca_patcher_base_patch_t *patch)
{
    patch->patch_symbol = NULL;
    patch->patch_data_size = 0;
}

static void mca_patcher_base_patch_destruct (mca_patcher_base_patch_t *patch)
{
    free (patch->patch_symbol);
}

OBJ_CLASS_INSTANCE(mca_patcher_base_patch_t, opal_list_item_t,
                   mca_patcher_base_patch_construct,
                   mca_patcher_base_patch_destruct);

#if defined(__PPC__)

// PowerPC instructions used in patching
// Reference: "PowerPC User Instruction Set Architecture"
static unsigned int addis(unsigned int RT, unsigned int RS, unsigned int UI) {
    return (15<<26) + (RT<<21) + (RS<<16) + (UI&0xffff);
}
static unsigned int ori(unsigned int RT, unsigned int RS, unsigned int UI) {
    return (24<<26) + (RS<<21) + (RT<<16) + (UI&0xffff);
}
static unsigned int oris(unsigned int RT, unsigned int RS, unsigned int UI) {
    return (25<<26) + (RS<<21) + (RT<<16) + (UI&0xffff);
}
static unsigned int mtspr(unsigned int SPR, unsigned int RS) {
    return (31<<26) + (RS<<21) + ((SPR&0x1f)<<16) + ((SPR>>5)<<11) + (467<<1);
}
static unsigned int bcctr(unsigned int BO, unsigned int BI, unsigned int BH) {
    return (19<<26) + (BO<<21) + (BI<<16) + (BH<<11) + (528<<1);
}
static unsigned int rldicr(unsigned int RT, unsigned int RS, unsigned int SH, unsigned int MB)
{
    return (30<<26) + (RS<<21) + (RT<<16) + ((SH&0x1f)<<11) + ((SH>>5)<<1)
        + ((MB&0x1f)<<6) + ((MB>>5)<<5) + (1<<2);
}

static int PatchLoadImm (uintptr_t addr, unsigned int reg, size_t value)
{
#if defined(__PPC64__)
    *(unsigned int *) (addr + 0) = addis ( reg, 0,   (value >> 48));
    *(unsigned int *) (addr + 4) = ori   ( reg, reg, (value >> 32));
    *(unsigned int *) (addr + 8) = rldicr( reg, reg, 32, 31);
    *(unsigned int *) (addr +12) = oris  ( reg, reg, (value >> 16));
    *(unsigned int *) (addr +16) = ori   ( reg, reg, (value >>  0));
    return 20;
#else
    *(unsigned int *) (addr + 0) = addis ( reg, 0,   (value >> 16));
    *(unsigned int *) (addr + 4) = ori   ( reg, reg, (value >>  0));
    return 8;
#endif
}

#endif

static void flush_and_invalidate_cache (unsigned long a)
{
#if OPAL_ASSEMBLY_ARCH == OPAL_IA32
    static int have_clflush = -1;

    if (OPAL_UNLIKELY(-1 == have_clflush)) {
        int32_t cpuid1, cpuid2, tmp;
        const int32_t level = 1;

        /* cpuid clobbers ebx but it must be restored for -fPIC so save
         * then restore ebx */
        __asm__ volatile ("xchgl %%ebx, %2\n"
                          "cpuid\n"
                          "xchgl %%ebx, %2\n":
                          "=a" (cpuid1), "=d" (cpuid2), "=r" (tmp) :
                          "a" (level) :
                          "ecx");
        /* clflush is in edx bit 19 */
        have_clflush = !!(cpuid2 & (1 << 19));
    }

    if (have_clflush) {
        /* does not work with AMD processors */
        __asm__ volatile("mfence;clflush %0;mfence" : :"m" (*(char*)a));
    }
#elif OPAL_ASSEMBLY_ARCH == OPAL_X86_64
    __asm__ volatile("mfence;clflush %0;mfence" : :"m" (*(char*)a));
#elif OPAL_ASSEMBLY_ARCH == OPAL_IA64
    __asm__ volatile ("fc %0;; sync.i;; srlz.i;;" : : "r"(a) : "memory");
#elif OPAL_ASSEMBLY_ARCH == OPAL_ARM64
    __asm__ volatile ("dc cvau, %0\n\t"
                      "dsb ish\n\t"
                      "ic ivau, %0\n\t"
                      "dsb ish\n\t"
                      "isb":: "r" (a));
#endif
}

// modify protection of memory range
static void ModifyMemoryProtection (uintptr_t addr, size_t length, int prot)
{
    long      page_size = opal_getpagesize ();
    uintptr_t base = (addr & ~(page_size-1));
    uintptr_t bound = ((addr + length + page_size-1) & ~(page_size-1));

    length = bound - base;

#if defined(__PPC__)
    /* NTH: is a loop necessary here? */
    do {
        if (mprotect((void *)base, page_size, prot))
            perror("MemHook: mprotect failed");
        base += page_size;
    } while (base < bound);
#else
    if (mprotect((void *) base, length, prot)) {
            perror("MemHook: mprotect failed");
    }
#endif
}

static inline void apply_patch (unsigned char *patch_data, uintptr_t address, size_t data_size)
{
    ModifyMemoryProtection (address, data_size, PROT_EXEC|PROT_READ|PROT_WRITE);
    memcpy ((void *) address, patch_data, data_size);
#if HAVE___CLEAR_CACHE
    /* do not allow global declaration of compiler intrinsic */
    void __clear_cache(void* beg, void* end);

    __clear_cache ((void *) address, (void *) (address + data_size));
#else
    size_t offset_jump = 16;

#if OPAL_ASSEMBLY_ARCH == OPAL_ARM64
    offset_jump = 32;
#endif

    /* align the address */
    address &= ~(offset_jump - 1);

    for (size_t i = 0 ; i < data_size ; i += offset_jump) {
        flush_and_invalidate_cache (address + i);
    }

#endif

    ModifyMemoryProtection (address, data_size, PROT_EXEC|PROT_READ);
}

static void mca_base_patcher_patch_unapply_binary (mca_patcher_base_patch_t *patch)
{
    apply_patch (patch->patch_orig_data, patch->patch_orig, patch->patch_data_size);
}

void mca_base_patcher_patch_apply_binary (mca_patcher_base_patch_t *patch)
{
    memcpy (patch->patch_orig_data, (void *) patch->patch_orig, patch->patch_data_size);
    apply_patch (patch->patch_data, patch->patch_orig, patch->patch_data_size);
    patch->patch_restore = mca_base_patcher_patch_unapply_binary;
}


int mca_patcher_base_patch_hook (mca_patcher_base_module_t *module, uintptr_t hook_addr)
{
#if (OPAL_ASSEMBLY_ARCH == OPAL_POWERPC64)
    mca_patcher_base_patch_t *hook_patch;
    const unsigned int nop = 0x60000000;

    hook_patch = OBJ_NEW(mca_patcher_base_patch_t);
    if (OPAL_UNLIKELY(NULL == hook_patch)) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    // locate reserved code space in hook function
    for (unsigned int *nop_addr = (unsigned int *)hook_addr ; ; nop_addr++) {
        if (nop_addr[0] == nop && nop_addr[1] == nop && nop_addr[2] == nop
                && nop_addr[3] == nop && nop_addr[4] == nop) {
            hook_patch->patch_orig = (uintptr_t) nop_addr;
            break;
        }
    }

    // generate code to restore TOC
    unsigned long toc;

    asm volatile ("std 2, %0" : "=m" (toc));

    hook_patch->patch_data_size = PatchLoadImm((uintptr_t)hook_patch->patch_data, 2, toc);

    /* put the hook patch on the patch list so it will be undone on finalize */
    opal_list_append (&module->patch_list, &hook_patch->super);

    mca_base_patcher_patch_apply_binary (hook_patch);
#endif

    return OPAL_SUCCESS;
}
