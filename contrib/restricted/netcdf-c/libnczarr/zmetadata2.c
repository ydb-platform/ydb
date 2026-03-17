/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"

#define  MINIMIM_CSL_REP_RAW "{\"metadata\":{},\"zarr_consolidated_format\":1}"

/// @brief Retrieve the group and variable names contained within a group specified by `key` on the storage. The order of the names may be arbitrary
/// @param zfile - The zarr file info structure
/// @param key - the key of the node - group
/// @param groups - NClist where names will be added
/// @param variables - NClist where names will be added
/// @return `NC_NOERR` if succeeding
int NCZMD_v2_list_nodes(NCZ_FILE_INFO_T *zfile, const char * key, NClist *groups, NClist *vars);

/// @brief Retrieve the group and variable names contained within a group specified by `key` on the consolidated representation. The order of the names may be arbitrary
/// @param zfile - The zarr file info structure
/// @param key - the key of the node - group
/// @param groups - NClist where names will be added
/// @param variables - NClist where names will be added
/// @return `NC_NOERR` if succeeding
int NCZMD_v2_csl_list_nodes(NCZ_FILE_INFO_T *zfile, const char * key, NClist *groups, NClist *vars);

/// @brief Retrieve the group names contained within a group specified by `key` on the storage. The order of the names may be arbitrary
/// @param zfile - The zarr file info structure
/// @param key - the key of the node - group
/// @param groups - NClist where names will be added
/// @return `NC_NOERR` if succeeding
int NCZMD_v2_list_groups(NCZ_FILE_INFO_T *zfile, const char * key, NClist *groups);

/// @brief Retrieve the group names contained within a group specified by `key` on the consolidated represention. The order of the names may be arbitrary
/// @param zfile - The zarr file info structure
/// @param key - the key of the node - group
/// @param groups - NClist where names will be added
/// @return `NC_NOERR` if succeeding
int NCZMD_v2_csl_list_groups(NCZ_FILE_INFO_T *zfile, const char * key, NClist *groups);

/// @brief Retrieve the variable names contained by a group specified by `key` on the storage. The order of the names may be arbitrary
/// @param zfile - The zarr file info structure
/// @param key - the key of the node - group
/// @param variables - NClist where names will be added
/// @return `NC_NOERR` if succeeding
int NCZMD_v2_list_variables(NCZ_FILE_INFO_T *zfile, const char * key, NClist * variables);

/// @brief Retrieve the variable names contained by a group specified by `key` on the consolidated representation. The order of the names may be arbitrary
/// @param zfile - The zarr file info structure
/// @param key - the key of the node - group
/// @param variables - NClist where names will be added
/// @return `NC_NOERR` if succeeding
int NCZMD_v2_csl_list_variables(NCZ_FILE_INFO_T *zfile, const char * key, NClist *groups);

/// @brief Retrieve JSON metadata of a given type for the specified `key` from the storage
/// @param zfile - The zarr file info structure
///	@param zobj - The type of metadata to set
/// @param key - the key of the node - group or array
/// @param jobj - JSON to be written
/// @return `NC_NOERR` if succeeding
int fetch_json_content_v2(NCZ_FILE_INFO_T *zfile, NCZMD_MetadataType zarr_obj_type, const char *key, NCjson **jobj);

/// @brief Retrieve JSON metadata of a given type for the specified `key` from the consolidate representation
/// @param zfile - The zarr file info structure
///	@param zobj - The type of metadata to set
/// @param key - the key of the node - group or array
/// @param jobj - JSON to be written
/// @return `NC_NOERR` if succeeding
int fetch_csl_json_content_v2(NCZ_FILE_INFO_T *zfile, NCZMD_MetadataType zarr_obj_type, const char *key, NCjson **jobj);

/// @brief Write JSON metadata of a given type for the specified `key` to the storage
/// @param zfile - The zarr file info structure
///	@param zobj - The type of metadata to set
/// @param key - the key of the node - group or array
/// @param jobj - JSON to be written
/// @return `NC_NOERR` if succeeding
int update_json_content_v2(NCZ_FILE_INFO_T *zfile, NCZMD_MetadataType zobj, const char *key, const NCjson *jobj);

/// @brief Updates the JSON metadata of a given type for the specified `key` to the consolidated representation
/// @param zfile - The zarr file info structure
///	@param zobj - The type of metadata to set
/// @param key - the key of the node - group or array
/// @param jobj - JSON to be written
/// @return `NC_NOERR` if succeeding
int update_csl_json_content_v2(NCZ_FILE_INFO_T *zfile, NCZMD_MetadataType zobj, const char *key, const NCjson *jobj);

/// @brief Place holder for non consolidated handler
/// @param json - Not used!
/// @return `NC_NOERR` always
int validate_consolidated_json_noop_v2(const NCjson *json);

///@brief Checks if `json` is a compliant to what .zmetadata expects
///		- non empty `{}` in "metadata"
/// 	- `zarr_consolidated_format` exists and is `1`
/// @param json corresponding to the full .zmetadata content
/// @return `NC_NOERR` if valid, `NC_EZARRMETA` otherwise
int validate_consolidated_json_v2(const NCjson *json);

static const NCZ_Metadata NCZ_md2_table = {
	ZARRFORMAT2,
	NCZ_METADATA_VERSION,
	ZARR_NOT_CONSOLIDATED,
	.jcsl = NULL,

	.list_nodes = NCZMD_v2_list_nodes,
	.list_groups = NCZMD_v2_list_groups,
	.list_variables = NCZMD_v2_list_variables,

	.fetch_json_content = fetch_json_content_v2,
	.update_json_content = update_json_content_v2,
    .validate_consolidated = validate_consolidated_json_noop_v2,
};

const NCZ_Metadata *NCZ_metadata_handler2 = &NCZ_md2_table;

static const NCZ_Metadata NCZ_csl_md2_table = {
	ZARRFORMAT2,
	NCZ_METADATA_VERSION,
	ZARR_CONSOLIDATED,
	.jcsl = NULL,

	.list_nodes = NCZMD_v2_csl_list_nodes,
	.list_groups = NCZMD_v2_csl_list_groups,
	.list_variables = NCZMD_v2_csl_list_variables,

	.fetch_json_content = fetch_csl_json_content_v2,
	.update_json_content = update_csl_json_content_v2,
    .validate_consolidated = validate_consolidated_json_v2,
};

const NCZ_Metadata *NCZ_csl_metadata_handler2 = &NCZ_csl_md2_table;

int NCZMD_v2_list_nodes(NCZ_FILE_INFO_T *zfile, const char * key, NClist *groups, NClist *variables)
{
	size_t i;
	int stat = NC_NOERR;
	char *subkey = NULL;
	char *zkey = NULL;
	NClist *matches = nclistnew();

	if ((stat = nczmap_search(zfile->map, key, matches)))
		goto done;
	for (i = 0; i < nclistlength(matches); i++)
	{
		const char *name = nclistget(matches, i);
		if (name[0] == NCZM_DOT)
			continue;
		if ((stat = nczm_concat(key, name, &subkey)))
			goto done;
		if ((stat = nczm_concat(subkey, Z2GROUP, &zkey)))
			goto done;
		if (NC_NOERR == nczmap_exists(zfile->map, zkey) && groups != NULL)
			nclistpush(groups, strdup(name));

		nullfree(zkey);
		zkey = NULL;
		if ((stat = nczm_concat(subkey, Z2ARRAY, &zkey)))
			goto done;
		if (NC_NOERR == nczmap_exists(zfile->map, zkey) && variables != NULL)
			nclistpush(variables, strdup(name));
		stat = NC_NOERR;

		nullfree(subkey);
		subkey = NULL;
		nullfree(zkey);
		zkey = NULL;
	}

done:
	nullfree(subkey);
	nullfree(zkey);
	nclistfreeall(matches);
	return stat;
}

int NCZMD_v2_csl_list_nodes(NCZ_FILE_INFO_T *zfile, const char * key, NClist *groups, NClist *variables)
{
	size_t i;
	int stat = NC_NOERR;
	char *subkey = NULL;
	char *zgroup = NULL;
	NClist *segments = nclistnew();

	const char *group = key + (key[0] == '/');
	size_t lgroup = strlen(group);

	const NCjson *jmetadata = NULL;
	NCJdictget(zfile->metadata.jcsl, "metadata", &jmetadata);
	for (i = 0; i < NCJdictlength(jmetadata); i++)
	{
		NCjson *jname = NCJdictkey(jmetadata, i);
		const char *fullname = NCJstring(jname);
		size_t lfullname = strlen(fullname);
		if (lfullname < lgroup ||
			strncmp(fullname, group, lgroup) ||
			(lgroup > 0 && fullname[lgroup] != NCZM_SEP[0]))
		{
			continue;
		}

		nclistclearall(segments);
		NC_split_delim(fullname + lgroup + (lgroup > 0), NCZM_SEP[0] ,segments);
		size_t slen = nclistlength(segments);
		if (slen != 2) {
			continue;
		}

		if (strncmp(Z2GROUP, nclistget(segments,1), sizeof(Z2GROUP)) == 0 && groups != NULL)
		{
			nclistpush(groups, strdup(nclistget(segments,0)));
		}
		else if (strncmp(Z2ARRAY, nclistget(segments,1), sizeof(Z2ARRAY)) == 0 && variables != NULL)
		{
			nclistpush(variables, strdup(nclistget(segments,0)));
		}
	}
	nullfree(subkey);
	nullfree(zgroup);
	nclistfreeall(segments);
	return stat;
}

int NCZMD_v2_list_groups(NCZ_FILE_INFO_T *zfile, const char * key, NClist *groups)
{
	return NCZMD_v2_list_nodes(zfile, key, groups, NULL);
}

int NCZMD_v2_csl_list_groups(NCZ_FILE_INFO_T *zfile, const char * key, NClist *groups)
{
	return NCZMD_v2_csl_list_nodes(zfile, key, groups, NULL);
}

int NCZMD_v2_list_variables(NCZ_FILE_INFO_T *zfile, const char * key, NClist *variables)
{
	return NCZMD_v2_list_nodes(zfile, key, NULL, variables);
}

int NCZMD_v2_csl_list_variables(NCZ_FILE_INFO_T *zfile, const char* key, NClist *variables)
{
	return NCZMD_v2_csl_list_nodes(zfile, key, NULL, variables);
}

static int zarr_obj_type2suffix(NCZMD_MetadataType zarr_obj_type, const char **suffix){
	switch (zarr_obj_type)
	{
		case NCZMD_GROUP:
			*suffix = Z2GROUP;
			break;
		case NCZMD_ATTRS:
			*suffix = Z2ATTRS;
			break;
		case NCZMD_ARRAY:
			*suffix = Z2ARRAY;
			break;
		default:
			return NC_EINVAL;
	}
	return NC_NOERR;
}

int fetch_json_content_v2(NCZ_FILE_INFO_T *zfile, NCZMD_MetadataType zobj, const char *prefix, NCjson **jobj)
{
	int stat = NC_NOERR;
	const char *suffix;
	char * key = NULL;
	if ((stat = zarr_obj_type2suffix(zobj, &suffix))
		|| (stat = nczm_concat(prefix, suffix, &key))){
		goto done;
	}

	stat = NCZ_downloadjson(zfile->map, key, jobj);
done:
	nullfree(key);
	return stat;
}

int fetch_csl_json_content_v2(NCZ_FILE_INFO_T *zfile, NCZMD_MetadataType zobj_t, const char *prefix, NCjson **jobj)
{
	int stat = NC_NOERR;
	const NCjson *jtmp = NULL;
	const char *suffix;
	char * key = NULL;
	if ( (stat = zarr_obj_type2suffix(zobj_t, &suffix))
	   ||(stat = nczm_concat(prefix, suffix, &key))){
		return stat;
	}

	if (NCJdictget(zfile->metadata.jcsl, "metadata", &jtmp) == 0
	&& jtmp && NCJsort(jtmp) == NCJ_DICT)
	{
		NCjson *tmp = NULL;
		if ((stat = NCJdictget(jtmp, key + (key[0] == '/'), (const NCjson**)&tmp)))
			goto done;
		if (tmp)
			NCJclone(tmp, jobj);
	}
done:
	nullfree(key);
	return stat;

}

int update_csl_json_content_v2(NCZ_FILE_INFO_T *zfile, NCZMD_MetadataType zobj_t, const char *prefix, const NCjson *jobj)
{
	int stat = NC_NOERR;

	if ((stat=update_json_content_v2(zfile,zobj_t,prefix,jobj))){
		goto done;
	}
	if (zfile->metadata.jcsl == NULL &&
		(stat = NCJparse(MINIMIM_CSL_REP_RAW,0,&zfile->metadata.jcsl))){
		goto done;
	}

	NCjson * jrep = NULL;
	if ((stat = NCJdictget(zfile->metadata.jcsl,"metadata", (const NCjson**)&jrep)) || jrep == NULL) {
		goto done;
	}

	const char *suffix;
	char * key = NULL;
	if ((stat = zarr_obj_type2suffix(zobj_t, &suffix))
		|| (stat = nczm_concat(prefix, suffix, &key))){
		goto done;
	}
	const char * mdkey= key[0] == '/'?key+1:key;
	NCjson * jval = NULL;
	NCJclone(jobj,&jval);
	NCJinsert(jrep, mdkey, jval);
done:
	free(key);
	return stat;

}
int update_json_content_v2(NCZ_FILE_INFO_T *zfile, NCZMD_MetadataType zobj, const char *prefix, const NCjson *jobj)
{
	int stat = NC_NOERR;
	const char *suffix;
	char * key = NULL;
	if ((stat = zarr_obj_type2suffix(zobj, &suffix))
		|| (stat = nczm_concat(prefix, suffix, &key))){
		goto done;
	}

	stat = NCZ_uploadjson(zfile->map, key, jobj);
done:
	nullfree(key);
	return stat;
}

int validate_consolidated_json_noop_v2(const NCjson *json){
    NC_UNUSED(json);
    return NC_NOERR;
}

int validate_consolidated_json_v2(const NCjson *json)
{
    if (json == NULL || NCJsort(json) != NCJ_DICT || NCJdictlength(json) == 0)
        return NC_EZARRMETA;

    const NCjson *jtmp = NULL;
    NCJdictget(json, "metadata", &jtmp);
    if (jtmp == NULL || NCJsort(jtmp) != NCJ_DICT || NCJdictlength(jtmp) == 0)
        return NC_EZARRMETA;

    jtmp = NULL;
    struct NCJconst format = {0,0,0,0};
    NCJdictget(json, "zarr_consolidated_format", &jtmp);
    if (jtmp == NULL || NCJsort(jtmp) != NCJ_INT || NCJcvt(jtmp,NCJ_INT, &format ) || format.ival != 1)
        return NC_EZARRMETA;

    return NC_NOERR;
}
