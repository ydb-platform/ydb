/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/txn.h"
#include "dbinc/fop.h"

DB_LOG_RECSPEC __fop_create_42_desc[] = {
	{LOGREC_DBT, SSZ(__fop_create_42_args, name), "name", ""},
	{LOGREC_ARG, SSZ(__fop_create_42_args, appname), "appname", "%lu"},
	{LOGREC_ARG, SSZ(__fop_create_42_args, mode), "mode", "%o"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __fop_create_desc[] = {
	{LOGREC_DBT, SSZ(__fop_create_args, name), "name", ""},
	{LOGREC_DBT, SSZ(__fop_create_args, dirname), "dirname", ""},
	{LOGREC_ARG, SSZ(__fop_create_args, appname), "appname", "%lu"},
	{LOGREC_ARG, SSZ(__fop_create_args, mode), "mode", "%o"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __fop_remove_desc[] = {
	{LOGREC_DBT, SSZ(__fop_remove_args, name), "name", ""},
	{LOGREC_DBT, SSZ(__fop_remove_args, fid), "fid", ""},
	{LOGREC_ARG, SSZ(__fop_remove_args, appname), "appname", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __fop_write_42_desc[] = {
	{LOGREC_DBT, SSZ(__fop_write_42_args, name), "name", ""},
	{LOGREC_ARG, SSZ(__fop_write_42_args, appname), "appname", "%lu"},
	{LOGREC_ARG, SSZ(__fop_write_42_args, pgsize), "pgsize", "%lu"},
	{LOGREC_ARG, SSZ(__fop_write_42_args, pageno), "pageno", "%lu"},
	{LOGREC_ARG, SSZ(__fop_write_42_args, offset), "offset", "%lu"},
	{LOGREC_DBT, SSZ(__fop_write_42_args, page), "page", ""},
	{LOGREC_ARG, SSZ(__fop_write_42_args, flag), "flag", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __fop_write_desc[] = {
	{LOGREC_DBT, SSZ(__fop_write_args, name), "name", ""},
	{LOGREC_DBT, SSZ(__fop_write_args, dirname), "dirname", ""},
	{LOGREC_ARG, SSZ(__fop_write_args, appname), "appname", "%lu"},
	{LOGREC_ARG, SSZ(__fop_write_args, pgsize), "pgsize", "%lu"},
	{LOGREC_ARG, SSZ(__fop_write_args, pageno), "pageno", "%lu"},
	{LOGREC_ARG, SSZ(__fop_write_args, offset), "offset", "%lu"},
	{LOGREC_DBT, SSZ(__fop_write_args, page), "page", ""},
	{LOGREC_ARG, SSZ(__fop_write_args, flag), "flag", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __fop_rename_42_desc[] = {
	{LOGREC_DBT, SSZ(__fop_rename_42_args, oldname), "oldname", ""},
	{LOGREC_DBT, SSZ(__fop_rename_42_args, newname), "newname", ""},
	{LOGREC_DBT, SSZ(__fop_rename_42_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__fop_rename_42_args, appname), "appname", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __fop_rename_noundo_46_desc[] = {
	{LOGREC_DBT, SSZ(__fop_rename_42_args, oldname), "oldname", ""},
	{LOGREC_DBT, SSZ(__fop_rename_42_args, newname), "newname", ""},
	{LOGREC_DBT, SSZ(__fop_rename_42_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__fop_rename_42_args, appname), "appname", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __fop_rename_desc[] = {
	{LOGREC_DBT, SSZ(__fop_rename_args, oldname), "oldname", ""},
	{LOGREC_DBT, SSZ(__fop_rename_args, newname), "newname", ""},
	{LOGREC_DBT, SSZ(__fop_rename_args, dirname), "dirname", ""},
	{LOGREC_DBT, SSZ(__fop_rename_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__fop_rename_args, appname), "appname", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __fop_rename_noundo_desc[] = {
	{LOGREC_DBT, SSZ(__fop_rename_args, oldname), "oldname", ""},
	{LOGREC_DBT, SSZ(__fop_rename_args, newname), "newname", ""},
	{LOGREC_DBT, SSZ(__fop_rename_args, dirname), "dirname", ""},
	{LOGREC_DBT, SSZ(__fop_rename_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__fop_rename_args, appname), "appname", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __fop_file_remove_desc[] = {
	{LOGREC_DBT, SSZ(__fop_file_remove_args, real_fid), "real_fid", ""},
	{LOGREC_DBT, SSZ(__fop_file_remove_args, tmp_fid), "tmp_fid", ""},
	{LOGREC_DBT, SSZ(__fop_file_remove_args, name), "name", ""},
	{LOGREC_ARG, SSZ(__fop_file_remove_args, appname), "appname", "%lu"},
	{LOGREC_ARG, SSZ(__fop_file_remove_args, child), "child", "%lx"},
	{LOGREC_Done, 0, "", ""}
};
/*
 * PUBLIC: int __fop_init_recover __P((ENV *, DB_DISTAB *));
 */
int
__fop_init_recover(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_create_recover, DB___fop_create)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_remove_recover, DB___fop_remove)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_write_recover, DB___fop_write)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_rename_recover, DB___fop_rename)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_rename_noundo_recover, DB___fop_rename_noundo)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __fop_file_remove_recover, DB___fop_file_remove)) != 0)
		return (ret);
	return (0);
}
