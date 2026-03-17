/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"

#ifdef HAVE_QUEUE
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/qam.h"
#include "dbinc/txn.h"

/*
 * PUBLIC: int __qam_incfirst_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__qam_incfirst_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__qam_incfirst", __qam_incfirst_desc, info));
}

/*
 * PUBLIC: int __qam_mvptr_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__qam_mvptr_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__qam_mvptr", __qam_mvptr_desc, info));
}

/*
 * PUBLIC: int __qam_del_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__qam_del_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__qam_del", __qam_del_desc, info));
}

/*
 * PUBLIC: int __qam_add_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__qam_add_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__qam_add", __qam_add_desc, info));
}

/*
 * PUBLIC: int __qam_delext_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__qam_delext_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__qam_delext", __qam_delext_desc, info));
}

/*
 * PUBLIC: int __qam_init_print __P((ENV *, DB_DISTAB *));
 */
int
__qam_init_print(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_incfirst_print, DB___qam_incfirst)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_mvptr_print, DB___qam_mvptr)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_del_print, DB___qam_del)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_add_print, DB___qam_add)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_delext_print, DB___qam_delext)) != 0)
		return (ret);
	return (0);
}
#endif /* HAVE_QUEUE */
