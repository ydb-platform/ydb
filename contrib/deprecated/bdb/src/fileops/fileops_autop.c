/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/txn.h"
#include "dbinc/fop.h"

/*
 * PUBLIC: int __fop_create_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_create_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__fop_create_42", __fop_create_42_desc, info));
}

/*
 * PUBLIC: int __fop_create_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_create_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__fop_create", __fop_create_desc, info));
}

/*
 * PUBLIC: int __fop_remove_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_remove_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__fop_remove", __fop_remove_desc, info));
}

/*
 * PUBLIC: int __fop_write_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_write_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__fop_write_42", __fop_write_42_desc, info));
}

/*
 * PUBLIC: int __fop_write_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_write_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__fop_write", __fop_write_desc, info));
}

/*
 * PUBLIC: int __fop_rename_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_rename_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__fop_rename_42", __fop_rename_42_desc, info));
}

/*
 * PUBLIC: int __fop_rename_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_rename_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__fop_rename", __fop_rename_desc, info));
}

/*
 * PUBLIC: int __fop_file_remove_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_file_remove_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__fop_file_remove", __fop_file_remove_desc, info));
}

/*
 * PUBLIC: int __fop_init_print __P((ENV *, DB_DISTAB *));
 */
int
__fop_init_print(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_create_print, DB___fop_create)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_remove_print, DB___fop_remove)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_write_print, DB___fop_write)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_rename_print, DB___fop_rename)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_rename_print, DB___fop_rename_noundo)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_file_remove_print, DB___fop_file_remove)) != 0)
		return (ret);
	return (0);
}
