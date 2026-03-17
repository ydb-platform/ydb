/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/btree.h"
#include "dbinc/txn.h"

/*
 * PUBLIC: int __bam_split_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_split_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_split", __bam_split_desc, info));
}

/*
 * PUBLIC: int __bam_split_48_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_split_48_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_split_48", __bam_split_48_desc, info));
}

/*
 * PUBLIC: int __bam_split_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_split_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_split_42", __bam_split_42_desc, info));
}

/*
 * PUBLIC: int __bam_rsplit_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_rsplit_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_rsplit", __bam_rsplit_desc, info));
}

/*
 * PUBLIC: int __bam_adj_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_adj_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_adj", __bam_adj_desc, info));
}

/*
 * PUBLIC: int __bam_cadjust_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_cadjust_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_cadjust", __bam_cadjust_desc, info));
}

/*
 * PUBLIC: int __bam_cdel_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_cdel_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_cdel", __bam_cdel_desc, info));
}

/*
 * PUBLIC: int __bam_repl_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_repl_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_repl", __bam_repl_desc, info));
}

/*
 * PUBLIC: int __bam_irep_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_irep_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_irep", __bam_irep_desc, info));
}

/*
 * PUBLIC: int __bam_root_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_root_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_root", __bam_root_desc, info));
}

/*
 * PUBLIC: int __bam_curadj_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_curadj_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_curadj", __bam_curadj_desc, info));
}

/*
 * PUBLIC: int __bam_rcuradj_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_rcuradj_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_rcuradj", __bam_rcuradj_desc, info));
}

/*
 * PUBLIC: int __bam_relink_43_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_relink_43_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_relink_43", __bam_relink_43_desc, info));
}

/*
 * PUBLIC: int __bam_merge_44_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_merge_44_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__bam_merge_44", __bam_merge_44_desc, info));
}

/*
 * PUBLIC: int __bam_init_print __P((ENV *, DB_DISTAB *));
 */
int
__bam_init_print(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_split_print, DB___bam_split)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_rsplit_print, DB___bam_rsplit)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_adj_print, DB___bam_adj)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_cadjust_print, DB___bam_cadjust)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_cdel_print, DB___bam_cdel)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_repl_print, DB___bam_repl)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_irep_print, DB___bam_irep)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_root_print, DB___bam_root)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_curadj_print, DB___bam_curadj)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_rcuradj_print, DB___bam_rcuradj)) != 0)
		return (ret);
	return (0);
}
