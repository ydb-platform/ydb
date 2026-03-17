/* DO NOT EDIT: automatically built by dist/s_include. */
#ifndef	_heap_ext_h_
#define	_heap_ext_h_

#if defined(__cplusplus)
extern "C" {
#endif

int __heapc_init __P((DBC *));
int __heap_ditem __P((DBC *, PAGE *, u_int32_t, u_int32_t));
int __heap_append __P((DBC *, DBT *, DBT *));
int __heap_pitem __P((DBC *, PAGE *, u_int32_t, u_int32_t, DBT *, DBT *));
int __heapc_dup __P((DBC *, DBC *));
int __heapc_gsplit __P((DBC *, DBT *, void **, u_int32_t *));
int __heapc_refresh __P((DBC *));
int __heap_init_recover __P((ENV *, DB_DISTAB *));
int __heap_addrem_print __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
int __heap_pg_alloc_print __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
int __heap_trunc_meta_print __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
int __heap_trunc_page_print __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
int __heap_init_print __P((ENV *, DB_DISTAB *));
int __heap_backup __P((DB_ENV *, DB *, DB_THREAD_INFO *, DB_FH *, void *, u_int32_t));
int __heap_pgin __P((DB *, db_pgno_t, void *, DBT *));
int __heap_pgout __P((DB *, db_pgno_t, void *, DBT *));
int __heap_mswap __P((ENV *, PAGE *));
int __heap_db_create __P((DB *));
int __heap_db_close __P((DB *));
int __heap_get_heapsize __P((DB *, u_int32_t *, u_int32_t *));
int __heap_get_heap_regionsize __P((DB *, u_int32_t *));
int __heap_set_heapsize __P((DB *, u_int32_t, u_int32_t, u_int32_t));
int __heap_set_heap_regionsize __P((DB *, u_int32_t));
int __heap_exist __P((void));
int __heap_open __P((DB *, DB_THREAD_INFO *, DB_TXN *, const char *, db_pgno_t, u_int32_t));
int __heap_metachk __P((DB *, const char *, HEAPMETA *));
int __heap_read_meta __P((DB *, DB_THREAD_INFO *, DB_TXN *, db_pgno_t, u_int32_t));
int __heap_new_file __P((DB *, DB_THREAD_INFO *, DB_TXN *, DB_FH *, const char *));
int __heap_create_region __P((DBC *, db_pgno_t));
int __heap_addrem_recover __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
int __heap_pg_alloc_recover __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
int __heap_trunc_meta_recover __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
int __heap_trunc_page_recover __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
int __heap_truncate __P((DBC *, u_int32_t *));
int __heap_stat __P((DBC *, void *, u_int32_t));
int __heap_stat_print __P((DBC *, u_int32_t));
void __heap_print_cursor __P((DBC *));
int __heap_stat_callback __P((DBC *, PAGE *, void *, int *));
int __heap_traverse __P((DBC *, int (*)(DBC *, PAGE *, void *, int *), void *));
int __db_no_heap_am __P((ENV *));
int __heap_vrfy_meta __P((DB *, VRFY_DBINFO *, HEAPMETA *, db_pgno_t, u_int32_t));
int __heap_vrfy __P((DB *, VRFY_DBINFO *, PAGE *, db_pgno_t, u_int32_t));
int __heap_vrfy_structure __P((DB *, VRFY_DBINFO *, u_int32_t));
int __heap_salvage __P((DB *, VRFY_DBINFO *, db_pgno_t, PAGE *, void *, int (*)(void *, const void *), u_int32_t));
int __heap_meta2pgset __P((DB *, VRFY_DBINFO *, HEAPMETA *, DB *));

#if defined(__cplusplus)
}
#endif
#endif /* !_heap_ext_h_ */
