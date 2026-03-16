/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/txn.h"

/*
 * PUBLIC: int __db_addrem_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_addrem_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_addrem", __db_addrem_desc, info));
}

/*
 * PUBLIC: int __db_addrem_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_addrem_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_addrem_42", __db_addrem_42_desc, info));
}

/*
 * PUBLIC: int __db_big_print __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__db_big_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_big", __db_big_desc, info));
}

/*
 * PUBLIC: int __db_big_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_big_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_big_42", __db_big_42_desc, info));
}

/*
 * PUBLIC: int __db_ovref_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_ovref_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_ovref", __db_ovref_desc, info));
}

/*
 * PUBLIC: int __db_relink_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_relink_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_relink_42", __db_relink_42_desc, info));
}

/*
 * PUBLIC: int __db_debug_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_debug_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_debug", __db_debug_desc, info));
}

/*
 * PUBLIC: int __db_noop_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_noop_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_noop", __db_noop_desc, info));
}

/*
 * PUBLIC: int __db_pg_alloc_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_alloc_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_pg_alloc_42", __db_pg_alloc_42_desc, info));
}

/*
 * PUBLIC: int __db_pg_alloc_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_alloc_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_pg_alloc", __db_pg_alloc_desc, info));
}

/*
 * PUBLIC: int __db_pg_free_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_free_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_pg_free_42", __db_pg_free_42_desc, info));
}

/*
 * PUBLIC: int __db_pg_free_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_free_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_pg_free", __db_pg_free_desc, info));
}

/*
 * PUBLIC: int __db_cksum_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_cksum_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_cksum", __db_cksum_desc, info));
}

/*
 * PUBLIC: int __db_pg_freedata_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_freedata_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_pg_freedata_42", __db_pg_freedata_42_desc, info));
}

/*
 * PUBLIC: int __db_pg_freedata_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_freedata_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_pg_freedata", __db_pg_freedata_desc, info));
}

/*
 * PUBLIC: int __db_pg_init_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_init_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_pg_init", __db_pg_init_desc, info));
}

/*
 * PUBLIC: int __db_pg_sort_44_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_sort_44_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_pg_sort_44", __db_pg_sort_44_desc, info));
}

/*
 * PUBLIC: int __db_pg_trunc_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_trunc_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_pg_trunc", __db_pg_trunc_desc, info));
}

/*
 * PUBLIC: int __db_realloc_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_realloc_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_realloc", __db_realloc_desc, info));
}

/*
 * PUBLIC: int __db_relink_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_relink_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_relink", __db_relink_desc, info));
}

/*
 * PUBLIC: int __db_merge_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_merge_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_merge", __db_merge_desc, info));
}

/*
 * PUBLIC: int __db_pgno_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pgno_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__db_pgno", __db_pgno_desc, info));
}

/*
 * PUBLIC: int __db_init_print __P((ENV *, DB_DISTAB *));
 */
int
__db_init_print(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_addrem_print, DB___db_addrem)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_big_print, DB___db_big)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_ovref_print, DB___db_ovref)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_debug_print, DB___db_debug)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_noop_print, DB___db_noop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_alloc_print, DB___db_pg_alloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_free_print, DB___db_pg_free)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_cksum_print, DB___db_cksum)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_freedata_print, DB___db_pg_freedata)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_init_print, DB___db_pg_init)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_trunc_print, DB___db_pg_trunc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_realloc_print, DB___db_realloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_relink_print, DB___db_relink)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_merge_print, DB___db_merge)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pgno_print, DB___db_pgno)) != 0)
		return (ret);
	return (0);
}
