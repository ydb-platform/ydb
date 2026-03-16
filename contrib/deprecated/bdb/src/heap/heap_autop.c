/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"

#ifdef HAVE_HEAP
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/heap.h"
#include "dbinc/txn.h"

/*
 * PUBLIC: int __heap_addrem_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__heap_addrem_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__heap_addrem", __heap_addrem_desc, info));
}

/*
 * PUBLIC: int __heap_pg_alloc_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__heap_pg_alloc_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__heap_pg_alloc", __heap_pg_alloc_desc, info));
}

/*
 * PUBLIC: int __heap_trunc_meta_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__heap_trunc_meta_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__heap_trunc_meta", __heap_trunc_meta_desc, info));
}

/*
 * PUBLIC: int __heap_trunc_page_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__heap_trunc_page_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__heap_trunc_page", __heap_trunc_page_desc, info));
}

/*
 * PUBLIC: int __heap_init_print __P((ENV *, DB_DISTAB *));
 */
int
__heap_init_print(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_addrem_print, DB___heap_addrem)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_pg_alloc_print, DB___heap_pg_alloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_trunc_meta_print, DB___heap_trunc_meta)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_trunc_page_print, DB___heap_trunc_page)) != 0)
		return (ret);
	return (0);
}
#endif /* HAVE_HEAP */
