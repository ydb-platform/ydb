//==================================================================================
// lookup_type_cache reimplemented for YQL
//==================================================================================

#include <util/system/compiler.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

extern "C" {
#include "postgres.h"
#include "varatt.h"
#include "catalog/pg_operator_d.h"
#include "access/hash.h"
#include "access/toast_compression.h"
#include "access/tupdesc.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
}

#undef Max

#include "type_cache.h"
#include "utils.h"

namespace NYql {

typedef struct HTAB HTAB;

static __thread HTAB *YQL_TypeCacheHash = nullptr;

HTAB *init_type_cache() {
    if (CacheMemoryContext == nullptr)
        CreateCacheMemoryContext();

    HASHCTL ctl = {
            .keysize = sizeof(Oid),
            .entrysize = sizeof(TypeCacheEntry)
    };

    return hash_create("Type information cache", 64, &ctl,
                       HASH_ELEM | HASH_BLOBS);
}

extern "C" {
extern bool array_element_has_equality(TypeCacheEntry *typentry);
extern bool array_element_has_compare(TypeCacheEntry *typentry);
extern bool array_element_has_hashing(TypeCacheEntry *typentry);
extern bool array_element_has_extended_hashing(TypeCacheEntry *typentry);

/*
 * lookup_type_cache
 *
 * Fetch the type cache entry for the specified datatype, and make sure that
 * all the fields requested by bits in 'flags' are valid.
 *
 * The the caller needs to check whether the fields are InvalidOid or not.
 */
TypeCacheEntry*
lookup_type_cache(Oid type_id, int flags) {
    if (Y_UNLIKELY(YQL_TypeCacheHash == nullptr)) {
        YQL_TypeCacheHash = init_type_cache();
    }

    // Try to look up an existing entry
    auto typentry = static_cast<TypeCacheEntry*>(hash_search(YQL_TypeCacheHash,
                                                             &type_id,
                                                             HASH_FIND, nullptr));

    if (typentry == nullptr) {
        const auto &typeDesc = NYql::NPg::LookupType(type_id);

        bool found;

        typentry = static_cast<TypeCacheEntry*>(hash_search(YQL_TypeCacheHash,
                                                            (void *) &type_id,
                                                            HASH_ENTER, &found));

        Y_ASSERT(!found);            // it wasn't there a moment ago

        memset(typentry, 0, sizeof(TypeCacheEntry));

        // These fields can never change, by definition
        typentry->type_id = type_id;
        // PG 14 seems to use type_id_hash in syscache invalidation callback only, we don't touch the field
        // since YQL doesn't use the callback
        // typentry->type_id_hash = 0; // GetSysCacheHashValue1(TYPEOID, ObjectIdGetDatum(type_id));

        typentry->typlen = typeDesc.TypeLen;
        typentry->typbyval = typeDesc.PassByValue;
        typentry->typalign = typeDesc.TypeAlign;
        // TODO: fill depending on the typstorage, if toasting gets implemented in PG
        typentry->typstorage = TYPSTORAGE_PLAIN; //typtup->typstorage;
        typentry->typtype = static_cast<char>(typeDesc.TypType);

        // TODO: fill for composite types only. Don't touch until YQL supports those
        Y_ASSERT(typeDesc.TypType != NYql::NPg::ETypType::Composite);
        // typentry->typrelid = 0; //typtup->typrelid;

        typentry->typsubscript = typeDesc.TypeSubscriptFuncId;
        typentry->typelem = typeDesc.ElementTypeId;
        typentry->typcollation = typeDesc.TypeCollation;
        typentry->flags |= TCFLAGS_HAVE_PG_TYPE_DATA;

        // We don't fill base domain type since domains aren't supported in YQL
    }
    // typentries don't change from callbacks, cause we have none

	/*
	 * Look up opclasses if we haven't already and any dependent info is
	 * requested.
	 */
    if ((flags & (TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR |
                  TYPECACHE_CMP_PROC |
                  TYPECACHE_EQ_OPR_FINFO | TYPECACHE_CMP_PROC_FINFO |
                  TYPECACHE_BTREE_OPFAMILY)) &&
        !(typentry->flags & TCFLAGS_CHECKED_BTREE_OPCLASS))
    {
        const auto opClassDescPtr = NYql::NPg::LookupDefaultOpClass(NYql::NPg::EOpClassMethod::Btree, type_id);

        if (opClassDescPtr != nullptr) {
            typentry->btree_opf = opClassDescPtr->FamilyId;
            typentry->btree_opintype = opClassDescPtr->TypeId;
        } else {
            typentry->btree_opf = typentry->btree_opintype = InvalidOid;
        }

        /*
         * Reset information derived from btree opclass.  Note in particular
         * that we'll redetermine the eq_opr even if we previously found one;
         * this matters in case a btree opclass has been added to a type that
         * previously had only a hash opclass.
         */
        typentry->flags &= ~(TCFLAGS_CHECKED_EQ_OPR |
                             TCFLAGS_CHECKED_LT_OPR |
                             TCFLAGS_CHECKED_GT_OPR |
                             TCFLAGS_CHECKED_CMP_PROC);
        typentry->flags |= TCFLAGS_CHECKED_BTREE_OPCLASS;
    }

    /*
     * If we need to look up equality operator, and there's no btree opclass,
     * force lookup of hash opclass.
     */
    if ((flags & (TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO)) &&
        !(typentry->flags & TCFLAGS_CHECKED_EQ_OPR) &&
        typentry->btree_opf == InvalidOid)
        flags |= TYPECACHE_HASH_OPFAMILY;

    if ((flags & (TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO |
                  TYPECACHE_HASH_EXTENDED_PROC |
                  TYPECACHE_HASH_EXTENDED_PROC_FINFO |
                  TYPECACHE_HASH_OPFAMILY)) &&
        !(typentry->flags & TCFLAGS_CHECKED_HASH_OPCLASS))
    {
        const auto opClassDescPtr = NYql::NPg::LookupDefaultOpClass(NYql::NPg::EOpClassMethod::Hash, type_id);

        if (opClassDescPtr != nullptr) {
            typentry->hash_opf = opClassDescPtr->FamilyId;
            typentry->hash_opintype = opClassDescPtr->TypeId;
        } else {
            typentry->hash_opf = typentry->hash_opintype = InvalidOid;
        }

        /*
         * Reset information derived from hash opclass.  We do *not* reset the
         * eq_opr; if we already found one from the btree opclass, that
         * decision is still good.
         */
        typentry->flags &= ~(TCFLAGS_CHECKED_HASH_PROC |
                             TCFLAGS_CHECKED_HASH_EXTENDED_PROC);
        typentry->flags |= TCFLAGS_CHECKED_HASH_OPCLASS;
    }

    /*
     * Look for requested operators and functions, if we haven't already.
     */
    if ((flags & (TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO)) &&
        !(typentry->flags & TCFLAGS_CHECKED_EQ_OPR))
    {
        ui32 eq_opr = InvalidOid;

        if (typentry->btree_opf != InvalidOid) {
            eq_opr = get_opfamily_member(typentry->btree_opf,
                                         typentry->btree_opintype,
                                         typentry->btree_opintype,
                                         BTEqualStrategyNumber);
        }

        if (eq_opr == InvalidOid &&
            typentry->hash_opf != InvalidOid) {
            eq_opr = get_opfamily_member(typentry->hash_opf,
                                         typentry->hash_opintype,
                                         typentry->hash_opintype,
                                         HTEqualStrategyNumber);
        }

        /*
         * If the proposed equality operator is array_eq or record_eq, check
         * to see if the element type or column types support equality.  If
         * not, array_eq or record_eq would fail at runtime, so we don't want
         * to report that the type has equality.  (We can omit similar
         * checking for ranges and multiranges because ranges can't be created
         * in the first place unless their subtypes support equality.)
         */
        if (eq_opr == ARRAY_EQ_OP &&
            !array_element_has_equality(typentry))
            eq_opr = InvalidOid;
        // TODO: uncomment when YQL supports PG records
        else if (eq_opr == RECORD_EQ_OP /* &&
                 !record_fields_have_equality(typentry) */)
            eq_opr = InvalidOid;

        /* Force update of eq_opr_finfo only if we're changing state */
        if (typentry->eq_opr != eq_opr)
            typentry->eq_opr_finfo.fn_oid = InvalidOid;

        typentry->eq_opr = eq_opr;

        /*
         * Reset info about hash functions whenever we pick up new info about
         * equality operator.  This is so we can ensure that the hash
         * functions match the operator.
         */
        typentry->flags &= ~(TCFLAGS_CHECKED_HASH_PROC |
                             TCFLAGS_CHECKED_HASH_EXTENDED_PROC);
        typentry->flags |= TCFLAGS_CHECKED_EQ_OPR;
    }

    if ((flags & TYPECACHE_LT_OPR) &&
        !(typentry->flags & TCFLAGS_CHECKED_LT_OPR))
    {
        ui32 lt_opr = InvalidOid;

        if (typentry->btree_opf != InvalidOid) {
            lt_opr = get_opfamily_member(typentry->btree_opf,
                                         typentry->btree_opintype,
                                         typentry->btree_opintype,
                                         BTLessStrategyNumber);
        }

        /*
         * As above, make sure array_cmp or record_cmp will succeed; but again
         * we need no special check for ranges or multiranges.
         */
        if (lt_opr == ARRAY_LT_OP &&
            !array_element_has_compare(typentry))
            lt_opr = InvalidOid;
        // TODO: uncomment when YQL supports PG records
        else if (lt_opr == RECORD_LT_OP /* &&
                 !record_fields_have_compare(typentry) */)
            lt_opr = InvalidOid;

        typentry->lt_opr = lt_opr;
        typentry->flags |= TCFLAGS_CHECKED_LT_OPR;
    }

    if ((flags & TYPECACHE_GT_OPR) &&
        !(typentry->flags & TCFLAGS_CHECKED_GT_OPR))
    {
        ui32 gt_opr = InvalidOid;

        if (typentry->btree_opf != InvalidOid) {
            gt_opr = get_opfamily_member(typentry->btree_opf,
                                         typentry->btree_opintype,
                                         typentry->btree_opintype,
                                         BTGreaterStrategyNumber);
        }

        /*
         * As above, make sure array_cmp or record_cmp will succeed; but again
         * we need no special check for ranges or multiranges.
         */
        if (gt_opr == ARRAY_GT_OP &&
            !array_element_has_compare(typentry))
            gt_opr = InvalidOid;
        // TODO: uncomment when YQL supports PG records
        else if (gt_opr == RECORD_GT_OP /* &&
                 !record_fields_have_compare(typentry) */)
            gt_opr = InvalidOid;

        typentry->gt_opr = gt_opr;
        typentry->flags |= TCFLAGS_CHECKED_GT_OPR;
    }

    if ((flags & (TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO)) &&
        !(typentry->flags & TCFLAGS_CHECKED_CMP_PROC))
    {
        Oid cmp_proc = InvalidOid;

        if (typentry->btree_opf != InvalidOid)
            cmp_proc = get_opfamily_proc(typentry->btree_opf,
                                         typentry->btree_opintype,
                                         typentry->btree_opintype,
                                         BTORDER_PROC);

        /*
         * As above, make sure array_cmp or record_cmp will succeed; but again
         * we need no special check for ranges or multiranges.
         */
        if (cmp_proc == F_BTARRAYCMP &&
            !array_element_has_compare(typentry))
            cmp_proc = InvalidOid;
        // TODO: uncomment when YQL supports PG records
        else if (cmp_proc == F_BTRECORDCMP /* &&
                 !record_fields_have_compare(typentry) */)
            cmp_proc = InvalidOid;

        /* Force update of cmp_proc_finfo only if we're changing state */
        if (typentry->cmp_proc != cmp_proc)
            typentry->cmp_proc_finfo.fn_oid = InvalidOid;

        typentry->cmp_proc = cmp_proc;
        typentry->flags |= TCFLAGS_CHECKED_CMP_PROC;
    }

    if ((flags & (TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO)) &&
        !(typentry->flags & TCFLAGS_CHECKED_HASH_PROC))
    {
        Oid hash_proc = InvalidOid;

        /*
         * We insist that the eq_opr, if one has been determined, match the
         * hash opclass; else report there is no hash function.
         */
        if (typentry->hash_opf != InvalidOid &&
            (!OidIsValid(typentry->eq_opr) ||
             typentry->eq_opr == get_opfamily_member(typentry->hash_opf,
                                                     typentry->hash_opintype,
                                                     typentry->hash_opintype,
                                                     HTEqualStrategyNumber))) {
            hash_proc = get_opfamily_proc(typentry->hash_opf,
                                          typentry->hash_opintype,
                                          typentry->hash_opintype,
                                          HASHSTANDARD_PROC);
        }

        /*
         * As above, make sure hash_array, hash_record, or hash_range will
         * succeed.
         */
        if (hash_proc == F_HASH_ARRAY &&
            !array_element_has_hashing(typentry))
            hash_proc = InvalidOid;
        // TODO: uncomment when YQL supports PG records
        else if (hash_proc == F_HASH_RECORD /* &&
                 !record_fields_have_hashing(typentry) */)
            hash_proc = InvalidOid;
        // TODO: uncomment when YQL supports PG ranges
        else if (hash_proc == F_HASH_RANGE /* &&
                 !range_element_has_hashing(typentry) */)
            hash_proc = InvalidOid;

        /*
         * Likewise for hash_multirange.
         */
        // TODO: uncomment when YQL supports PG multiranges
        if (hash_proc == F_HASH_MULTIRANGE /* &&
            !multirange_element_has_hashing(typentry) */)
            hash_proc = InvalidOid;

        /* Force update of hash_proc_finfo only if we're changing state */
        if (typentry->hash_proc != hash_proc)
            typentry->hash_proc_finfo.fn_oid = InvalidOid;

        typentry->hash_proc = hash_proc;
        typentry->flags |= TCFLAGS_CHECKED_HASH_PROC;
    }

    if ((flags & (TYPECACHE_HASH_EXTENDED_PROC |
                  TYPECACHE_HASH_EXTENDED_PROC_FINFO)) &&
        !(typentry->flags & TCFLAGS_CHECKED_HASH_EXTENDED_PROC))
    {
        Oid hash_extended_proc = InvalidOid;

        /*
         * We insist that the eq_opr, if one has been determined, match the
         * hash opclass; else report there is no hash function.
         */
        if (typentry->hash_opf != InvalidOid &&
            (!OidIsValid(typentry->eq_opr) ||
             typentry->eq_opr == get_opfamily_member(typentry->hash_opf,
                                                     typentry->hash_opintype,
                                                     typentry->hash_opintype,
                                                     HTEqualStrategyNumber)))
        {
            hash_extended_proc = get_opfamily_proc(typentry->hash_opf,
                                                   typentry->hash_opintype,
                                                   typentry->hash_opintype,
                                                   HASHEXTENDED_PROC);
        }

        /*
         * As above, make sure hash_array_extended, hash_record_extended, or
         * hash_range_extended will succeed.
         */
        if (hash_extended_proc == F_HASH_ARRAY_EXTENDED &&
            !array_element_has_extended_hashing(typentry))
            hash_extended_proc = InvalidOid;
        // TODO: uncomment when YQL supports PG records
        else if (hash_extended_proc == F_HASH_RECORD_EXTENDED /* &&
                 !record_fields_have_extended_hashing(typentry) */)
            hash_extended_proc = InvalidOid;
        // TODO: uncomment when YQL supports PG ranges
        else if (hash_extended_proc == F_HASH_RANGE_EXTENDED /* &&
                 !range_element_has_extended_hashing(typentry) */)
            hash_extended_proc = InvalidOid;

        /*
         * Likewise for hash_multirange_extended.
         */
        // TODO: uncomment when YQL supports PG multiranges
        if (hash_extended_proc == F_HASH_MULTIRANGE_EXTENDED /* &&
            !multirange_element_has_extended_hashing(typentry) */)
            hash_extended_proc = InvalidOid;

        /* Force update of proc finfo only if we're changing state */
        if (typentry->hash_extended_proc != hash_extended_proc)
            typentry->hash_extended_proc_finfo.fn_oid = InvalidOid;

        typentry->hash_extended_proc = hash_extended_proc;
        typentry->flags |= TCFLAGS_CHECKED_HASH_EXTENDED_PROC;
    }

    /*
     * Set up fmgr lookup info as requested
     *
     * Note: we tell fmgr the finfo structures live in CacheMemoryContext,
     * which is not quite right (they're really in the hash table's private
     * memory context) but this will do for our purposes.
     */
    if ((flags & TYPECACHE_EQ_OPR_FINFO) &&
        typentry->eq_opr_finfo.fn_oid == InvalidOid &&
        typentry->eq_opr != InvalidOid)
    {
        ui32 eq_opr_func = get_opcode(typentry->eq_opr);
        if (eq_opr_func != InvalidOid)
            GetPgFuncAddr(eq_opr_func, typentry->eq_opr_finfo);
    }

    if ((flags & TYPECACHE_CMP_PROC_FINFO) &&
        typentry->cmp_proc_finfo.fn_oid == InvalidOid &&
        typentry->cmp_proc != InvalidOid)
    {
        GetPgFuncAddr(typentry->cmp_proc, typentry->cmp_proc_finfo);
    }

    if ((flags & TYPECACHE_HASH_PROC_FINFO) &&
        typentry->hash_proc_finfo.fn_oid == InvalidOid &&
        typentry->hash_proc != InvalidOid)
    {
        GetPgFuncAddr(typentry->hash_proc, typentry->hash_proc_finfo);
    }

    if ((flags & TYPECACHE_HASH_EXTENDED_PROC_FINFO) &&
        typentry->hash_extended_proc_finfo.fn_oid == InvalidOid &&
        typentry->hash_extended_proc != InvalidOid)
    {
        GetPgFuncAddr(typentry->hash_extended_proc,
                      typentry->hash_extended_proc_finfo);
    }

    return typentry;
}

Oid
get_base_element_type(Oid type_id) {
    const auto& typeDesc = NYql::NPg::LookupType(type_id);

    return (typeDesc.TypeSubscriptFuncId == F_ARRAY_SUBSCRIPT_HANDLER)
           ? typeDesc.ElementTypeId
           : InvalidOid;
}

Oid
get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype, int16 strategy) {
    if (NYql::NPg::HasAmOp(opfamily, strategy, lefttype, righttype)) {
        const auto &opDesc = NYql::NPg::LookupAmOp(opfamily, strategy, lefttype, righttype);
        return opDesc.OperId;
    }
    return InvalidOid;
}

Oid
get_opfamily_proc(Oid opfamily, Oid lefttype, Oid righttype, int16 procnum) {
    if (NYql::NPg::HasAmProc(opfamily, procnum, lefttype, righttype)) {
        const auto &procDesc = NYql::NPg::LookupAmProc(opfamily, procnum, lefttype, righttype);
        return procDesc.ProcId;
    }
    return InvalidOid;
}

RegProcedure
get_opcode(Oid opno)
{
    const auto& oprDesc = NYql::NPg::LookupOper(opno);
    return oprDesc.ProcId;
}

void destroy_typecache_hashtable() {
    hash_destroy(YQL_TypeCacheHash);
    YQL_TypeCacheHash = nullptr;
}

void
TupleDescInitEntry(TupleDesc desc,
                   AttrNumber attributeNumber,
                   const char *attributeName,
                   Oid oidtypeid,
                   int32 typmod,
                   int attdim)
{
    HeapTuple tuple;
    Form_pg_attribute att;

    /*
     * sanity checks
     */
    //AssertArg(PointerIsValid(desc));
    //AssertArg(attributeNumber >= 1);
    //AssertArg(attributeNumber <= desc->natts);

    /*
     * initialize the attribute fields
     */
    att = TupleDescAttr(desc, attributeNumber - 1);

    att->attrelid = 0;			/* dummy value */

    /*
     * Note: attributeName can be NULL, because the planner doesn't always
     * fill in valid resname values in targetlists, particularly for resjunk
     * attributes. Also, do nothing if caller wants to re-use the old attname.
     */
    if (attributeName == NULL)
        MemSet(NameStr(att->attname), 0, NAMEDATALEN);
    else if (attributeName != NameStr(att->attname))
        namestrcpy(&(att->attname), attributeName);

    att->attstattarget = -1;
    att->attcacheoff = -1;
    att->atttypmod = typmod;

    att->attnum = attributeNumber;
    att->attndims = attdim;

    att->attnotnull = false;
    att->atthasdef = false;
    att->atthasmissing = false;
    att->attidentity = '\0';
    att->attgenerated = '\0';
    att->attisdropped = false;
    att->attislocal = true;
    att->attinhcount = 0;
    /* attacl, attoptions and attfdwoptions are not present in tupledescs */

    const auto& typeDesc = NYql::NPg::LookupType(oidtypeid);
    att->atttypid = oidtypeid;
    att->attlen = typeDesc.TypeLen;
    att->attbyval = typeDesc.PassByValue;
    att->attalign = typeDesc.TypeAlign;
    att->attstorage = TYPSTORAGE_PLAIN;
    att->attcompression = InvalidCompressionMethod;
    att->attcollation = typeDesc.TypeCollation;
}


}
}
