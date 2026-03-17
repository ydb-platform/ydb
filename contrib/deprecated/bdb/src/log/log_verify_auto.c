/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/btree.h"
#include "dbinc/txn.h"
#include "dbinc/hash.h"
#include "dbinc/heap.h"
#include "dbinc/qam.h"
#include "dbinc/fop.h"

/*
 * PUBLIC: int __crdel_init_verify __P((ENV *, DB_DISTAB *));
 */
int
__crdel_init_verify(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_metasub_verify, DB___crdel_metasub)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_inmem_create_verify, DB___crdel_inmem_create)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_inmem_rename_verify, DB___crdel_inmem_rename)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_inmem_remove_verify, DB___crdel_inmem_remove)) != 0)
		return (ret);
	return (0);
}

/*
 * PUBLIC: int __db_init_verify __P((ENV *, DB_DISTAB *));
 */
int
__db_init_verify(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_addrem_verify, DB___db_addrem)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_big_verify, DB___db_big)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_ovref_verify, DB___db_ovref)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_debug_verify, DB___db_debug)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_noop_verify, DB___db_noop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_alloc_verify, DB___db_pg_alloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_free_verify, DB___db_pg_free)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_cksum_verify, DB___db_cksum)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_freedata_verify, DB___db_pg_freedata)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_init_verify, DB___db_pg_init)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_trunc_verify, DB___db_pg_trunc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_realloc_verify, DB___db_realloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_relink_verify, DB___db_relink)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_merge_verify, DB___db_merge)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pgno_verify, DB___db_pgno)) != 0)
		return (ret);
	return (0);
}

/*
 * PUBLIC: int __dbreg_init_verify __P((ENV *, DB_DISTAB *));
 */
int
__dbreg_init_verify(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __dbreg_register_verify, DB___dbreg_register)) != 0)
		return (ret);
	return (0);
}

/*
 * PUBLIC: int __bam_init_verify __P((ENV *, DB_DISTAB *));
 */
int
__bam_init_verify(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_split_verify, DB___bam_split)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_rsplit_verify, DB___bam_rsplit)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_adj_verify, DB___bam_adj)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_cadjust_verify, DB___bam_cadjust)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_cdel_verify, DB___bam_cdel)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_repl_verify, DB___bam_repl)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_root_verify, DB___bam_root)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_curadj_verify, DB___bam_curadj)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_rcuradj_verify, DB___bam_rcuradj)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_irep_verify, DB___bam_irep)) != 0)
		return (ret);
	return (0);
}

/*
 * PUBLIC: int __fop_init_verify __P((ENV *, DB_DISTAB *));
 */
int
__fop_init_verify(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_create_verify, DB___fop_create)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_remove_verify, DB___fop_remove)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_write_verify, DB___fop_write)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_rename_verify, DB___fop_rename)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_rename_verify, DB___fop_rename_noundo)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_file_remove_verify, DB___fop_file_remove)) != 0)
		return (ret);
	return (0);
}

#ifdef HAVE_HASH
/*
 * PUBLIC: int __ham_init_verify __P((ENV *, DB_DISTAB *));
 */
int
__ham_init_verify(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_insdel_verify, DB___ham_insdel)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_newpage_verify, DB___ham_newpage)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_splitdata_verify, DB___ham_splitdata)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_replace_verify, DB___ham_replace)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_copypage_verify, DB___ham_copypage)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_metagroup_verify, DB___ham_metagroup)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_groupalloc_verify, DB___ham_groupalloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_changeslot_verify, DB___ham_changeslot)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_contract_verify, DB___ham_contract)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_curadj_verify, DB___ham_curadj)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_chgpg_verify, DB___ham_chgpg)) != 0)
		return (ret);
	return (0);
}

#endif /* HAVE_HASH */
#ifdef HAVE_HEAP
/*
 * PUBLIC: int __heap_init_verify __P((ENV *, DB_DISTAB *));
 */
int
__heap_init_verify(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_addrem_verify, DB___heap_addrem)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_pg_alloc_verify, DB___heap_pg_alloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_trunc_meta_verify, DB___heap_trunc_meta)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_trunc_page_verify, DB___heap_trunc_page)) != 0)
		return (ret);
	return (0);
}
#endif /* HAVE_HEAP */
#ifdef HAVE_QUEUE
/*
 * PUBLIC: int __qam_init_verify __P((ENV *, DB_DISTAB *));
 */
int
__qam_init_verify(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_incfirst_verify, DB___qam_incfirst)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_mvptr_verify, DB___qam_mvptr)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_del_verify, DB___qam_del)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_add_verify, DB___qam_add)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_delext_verify, DB___qam_delext)) != 0)
		return (ret);
	return (0);
}

#endif /* HAVE_QUEUE */
/*
 * PUBLIC: int __txn_init_verify __P((ENV *, DB_DISTAB *));
 */
int
__txn_init_verify(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_regop_verify, DB___txn_regop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_ckp_verify, DB___txn_ckp)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_child_verify, DB___txn_child)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_prepare_verify, DB___txn_prepare)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_recycle_verify, DB___txn_recycle)) != 0)
		return (ret);
	return (0);
}
