/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/txn.h"

/*
 * PUBLIC: int __crdel_metasub_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__crdel_metasub_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__crdel_metasub", __crdel_metasub_desc, info));
}

/*
 * PUBLIC: int __crdel_inmem_create_print __P((ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__crdel_inmem_create_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__crdel_inmem_create", __crdel_inmem_create_desc, info));
}

/*
 * PUBLIC: int __crdel_inmem_rename_print __P((ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__crdel_inmem_rename_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__crdel_inmem_rename", __crdel_inmem_rename_desc, info));
}

/*
 * PUBLIC: int __crdel_inmem_remove_print __P((ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__crdel_inmem_remove_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__crdel_inmem_remove", __crdel_inmem_remove_desc, info));
}

/*
 * PUBLIC: int __crdel_init_print __P((ENV *, DB_DISTAB *));
 */
int
__crdel_init_print(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_metasub_print, DB___crdel_metasub)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_inmem_create_print, DB___crdel_inmem_create)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_inmem_rename_print, DB___crdel_inmem_rename)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_inmem_remove_print, DB___crdel_inmem_remove)) != 0)
		return (ret);
	return (0);
}
