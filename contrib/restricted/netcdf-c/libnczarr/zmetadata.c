/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"

static int
cmpstrings(const void* a1, const void* a2)
{
    const char** s1 = (const char**)a1;
    const char** s2 = (const char**)a2;
    return strcmp(*s1,*s2);
}

int NCZMD_list_nodes(NCZ_FILE_INFO_T *zfile, const char * key, NClist *groups, NClist *vars)
{
    int stat = NC_NOERR;
    if((stat = zfile->metadata.list_nodes(zfile,key, groups, vars))){
        return stat;
    }
    qsort(groups->content, groups->length, sizeof(char*), cmpstrings);
    qsort(vars->content, vars->length, sizeof(char*), cmpstrings);
    return stat;
}

int NCZMD_list_groups(NCZ_FILE_INFO_T *zfile, const char * key, NClist *subgrpnames)
{
    int stat = NC_NOERR;
    if((stat = zfile->metadata.list_groups(zfile,key, subgrpnames))){
        return stat;
    }
    qsort(subgrpnames->content, subgrpnames->length, sizeof(char*), cmpstrings);
    return stat;
}

int NCZMD_list_variables(NCZ_FILE_INFO_T *zfile, const char * key, NClist *varnames)
{
	int stat = NC_NOERR;
    if((stat = zfile->metadata.list_variables(zfile, key, varnames))){
        return stat;
    }
    qsort(varnames->content, varnames->length, sizeof(char*), cmpstrings);
    return stat;
}

int NCZMD_fetch_json_group(NCZ_FILE_INFO_T *zfile, const char *key, NCjson **jgroup) {
	return zfile->metadata.fetch_json_content(zfile, NCZMD_GROUP, key, jgroup);
}

int NCZMD_fetch_json_attrs(NCZ_FILE_INFO_T *zfile, const char *key, NCjson **jattrs) {
	return  zfile->metadata.fetch_json_content(zfile, NCZMD_ATTRS, key, jattrs);
}

int NCZMD_fetch_json_array(NCZ_FILE_INFO_T *zfile, const char *key, NCjson **jarray) {
	return zfile->metadata.fetch_json_content(zfile, NCZMD_ARRAY, key, jarray);
}

int NCZMD_update_json_group(NCZ_FILE_INFO_T *zfile, const char *key, const NCjson *jgroup) {
	return zfile->metadata.update_json_content(zfile, NCZMD_GROUP, key, jgroup);
}

int NCZMD_update_json_attrs(NCZ_FILE_INFO_T *zfile, const char *key, const NCjson *jattrs) {
	return zfile->metadata.update_json_content(zfile, NCZMD_ATTRS, key , jattrs);
}

int NCZMD_update_json_array(NCZ_FILE_INFO_T *zfile, const char *key, const NCjson *jarray) {
	return zfile->metadata.update_json_content(zfile, NCZMD_ARRAY, key, jarray);
}

int NCZMD_get_metadata_format(NCZ_FILE_INFO_T *zfile, int *zarrformat)
{
    NCZ_Metadata *zmd = &(zfile->metadata);

	if (zmd->zarr_format >= ZARRFORMAT2)
	{
		*zarrformat = zmd->zarr_format;
		return NC_NOERR;
	}

	if (!nczmap_exists(zfile->map, "/" Z2ATTRS) && !nczmap_exists(zfile->map, "/" Z2GROUP) && !nczmap_exists(zfile->map, "/" Z2ARRAY))
	{
		return NC_ENOTZARR;
	}

	*zarrformat = ZARRFORMAT2;
	return NC_NOERR;
}

int use_consolidated_metadata(NCZ_FILE_INFO_T *zfile) {
    int use_consolidated = NCZARR_CONSOLIDATED_DEFAULT || (zfile->controls.flags & FLAG_CONSOLIDATED);
    const char *e = getenv(NCZARR_CONSOLIDATED_ENV);

    int env_use_consolidated = (e != NULL)  && (
            (atoi(e) > 0) ||(strcasecmp(e, "true") == 0) || (strcasecmp(e, "yes") == 0)
        );

    return use_consolidated || env_use_consolidated;
}

int NCZMD_set_metadata_handler(NCZ_FILE_INFO_T *zfile) {
    NCjson *jcsl = NULL;

    int use_consolidated = use_consolidated_metadata(zfile);
    if (!use_consolidated){
        nclog(NCLOGNOTE, "Not using consolidated metadata! Doing so could improve reading performance");
    }

    if (use_consolidated && zfile->creating) {
        zfile->metadata = *NCZ_csl_metadata_handler2;
        return NC_NOERR;
    }

	zfile->metadata = *NCZ_metadata_handler2;
    if (!use_consolidated)
        return NC_NOERR;

    if (NCZ_downloadjson(zfile->map, Z2METADATA, &jcsl) || jcsl == NULL) {
        nclog(NCLOGNOTE, "Dataset not consolidated! Doing so will improve performance");
        return NC_NOERR;
    }

    if (NCZ_csl_metadata_handler2->validate_consolidated(jcsl) != NC_NOERR) {
        nclog(NCLOGWARN,"Consolidated metadata is invalid, ignoring it!");
        return NC_EZARRMETA;
    }

    zfile->metadata = *NCZ_csl_metadata_handler2;
    zfile->metadata.jcsl = jcsl;
    return NC_NOERR;
}

void NCZMD_free_metadata_handler(NCZ_Metadata * zmd){
	if (zmd == NULL) return;
	NCJreclaim(zmd->jcsl);
    zmd->jcsl = NULL;
}

int NCZMD_consolidate(NCZ_FILE_INFO_T *zfile)
{
	int stat = NC_NOERR;
	if (zfile->creating == 1 && zfile->metadata.jcsl !=NULL){
		stat = NCZ_uploadjson(zfile->map, Z2METADATA ,zfile->metadata.jcsl);
	}
	return stat;
}
