/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/lock.h"
#include "dbinc/txn.h"

/*
 * PUBLIC: int __txn_regop_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_regop_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__txn_regop_42", __txn_regop_42_desc, info));
}

/*
 * PUBLIC: int __txn_regop_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_regop_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__txn_regop", __txn_regop_desc, info));
}

/*
 * PUBLIC: int __txn_ckp_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_ckp_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__txn_ckp_42", __txn_ckp_42_desc, info));
}

/*
 * PUBLIC: int __txn_ckp_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_ckp_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__txn_ckp", __txn_ckp_desc, info));
}

/*
 * PUBLIC: int __txn_child_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_child_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__txn_child", __txn_child_desc, info));
}

/*
 * PUBLIC: int __txn_xa_regop_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_xa_regop_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__txn_xa_regop_42", __txn_xa_regop_42_desc, info));
}

/*
 * PUBLIC: int __txn_prepare_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_prepare_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__txn_prepare", __txn_prepare_desc, info));
}

/*
 * PUBLIC: int __txn_recycle_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_recycle_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__txn_recycle", __txn_recycle_desc, info));
}

/*
 * PUBLIC: int __txn_init_print __P((ENV *, DB_DISTAB *));
 */
int
__txn_init_print(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_regop_print, DB___txn_regop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_ckp_print, DB___txn_ckp)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_child_print, DB___txn_child)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_prepare_print, DB___txn_prepare)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_recycle_print, DB___txn_recycle)) != 0)
		return (ret);
	return (0);
}
