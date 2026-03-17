/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1997, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"

/*
 * Raw data reads must be done in multiples of the disk sector size. Currently
 * the sector size is either 512 bytes or 4096 bytes. So we set the
 * MAX_SECTOR_SIZE to 4096.
 */
#define	MAX_SECTOR_SIZE 4096

/*
 * Find the cluster size of the file system that would contain the given path.
 * If the value can't be determined, an error is returned.
 */
int
__os_get_cluster_size(path, psize)
	const char *path;
	u_int32_t *psize;
{

#if (WINVER < 0x500) || defined(DB_WINCE)
	/*
	 * WinCE and versions of Windows earlier than Windows NT don't have
	 * the APIs required to retrieve the cluster size.
	 */
	*psize = DB_DEF_IOSIZE;
	return (0);
#else
	BYTE clustershift, sectorshift, *pcluster;
	char buffer[MAX_SECTOR_SIZE];
	DWORD flags, infolen, length, mcl, name_size;
	HANDLE vhandle;
	int ret;
	NTFS_VOLUME_DATA_BUFFER ntfsinfo;
	size_t name_len;
	TCHAR *env_path, name_buffer[MAX_PATH + 1], root_path[MAX_PATH + 1];
	WORD *psector;

	if (path == NULL || psize == NULL) {
		return (EINVAL);
	}

	name_size = MAX_PATH + 1;
	*psize = 0;

	TO_TSTRING(NULL, path, env_path, ret);
	if (ret != 0)
		return (ret);
	/* Retrieve the volume root path where the input path resides. */
	if (!GetVolumePathName(env_path, root_path, name_size)) {
		FREE_STRING(NULL, env_path);
		return (__os_posix_err(__os_get_syserr()));
	}
	FREE_STRING(NULL, env_path);

	/* Get the volume GUID name from the root path. */
	if (!GetVolumeNameForVolumeMountPoint(
	    root_path, name_buffer, name_size))
		return (__os_posix_err(__os_get_syserr()));

	/* Delete the last trail "\" in the GUID name. */
	name_len = _tcsclen(name_buffer);
	if (name_len > 0)
		name_buffer[name_len - 1] = _T('\0');

	/* Create a handle to the volume. */
	vhandle = CreateFile(name_buffer, FILE_READ_ATTRIBUTES | FILE_READ_DATA,
	    FILE_SHARE_READ | FILE_SHARE_WRITE,	NULL, OPEN_EXISTING,
	    FILE_ATTRIBUTE_NORMAL, NULL);

	/* If open failed, return error */
	if (vhandle == INVALID_HANDLE_VALUE)
		return (__os_posix_err(__os_get_syserr()));

	/* Get the volume information through the root path. */
	if (!GetVolumeInformation(root_path, NULL, name_size, NULL, &mcl,
	    &flags, name_buffer, name_size)) {
		ret = __os_posix_err(__os_get_syserr());
		CloseHandle(vhandle);
		return (ret);
	}

	ret = 0;
	if (_tcscmp(name_buffer, _T("NTFS")) == 0) {
		/*
		 * If this is NTFS file system, use FSCTL_GET_NTFS_VOLUME_DATA
		 * to get the cluster size.
		 */
		if (DeviceIoControl(
		    vhandle,			/* volume handle */
		    FSCTL_GET_NTFS_VOLUME_DATA,	/* Control Code */
		    NULL,			/* not use */
		    0,				/* not use */
		    &ntfsinfo,			/* output buffer */
		    sizeof(NTFS_VOLUME_DATA_BUFFER),/* output buffer length */
		    &infolen,			/* number of returned bytes */
		    NULL))			/* ignore here */
			*psize = ntfsinfo.BytesPerCluster;
		else
			ret = __os_posix_err(__os_get_syserr());
	} else if (_tcscmp(name_buffer, _T("exFAT")) == 0) {
		/*
		 * If this is exFAT file system, read the information of sector
		 * and cluster from the BPB on sector 0
		 * +6C H: BYTE SectorSizeShift
		 * +6D H: BYTE ClusterShift
		 */
		if (ReadFile(vhandle, buffer, MAX_SECTOR_SIZE, &length, NULL)) {
			sectorshift = *(BYTE *)(&buffer[0x6C]);
			clustershift = *(BYTE *)(&buffer[0x6D]);
			*psize = 1 << sectorshift;
			*psize = (*psize) << clustershift;
		}
		else
			ret = __os_posix_err(__os_get_syserr());
	} else if (_tcscmp(name_buffer, _T("FAT")) == 0 ||
	    _tcscmp(name_buffer, _T("FAT32")) == 0) {
		/*
		 * If this is FAT or FAT32 file system, read the information of
		 * sector and cluster from the BPB on sector 0.
		 * +0B H: WORD Bytes per Sector.
		 * +0D H: BYTE Sectors Per Cluster.
		 */
		if (ReadFile(vhandle, buffer, MAX_SECTOR_SIZE, &length, NULL)) {
			psector = (WORD *)(&buffer[0x0B]);
			pcluster = (BYTE *)(&buffer[0x0D]);
			*psize = (*psector) * (*pcluster);
		}
		else
			ret = __os_posix_err(__os_get_syserr());
	}

	CloseHandle(vhandle);
	return (ret);
#endif
}

/*
 * __os_exists --
 *	Return if the file exists.
 */
int
__os_exists(env, path, isdirp)
	ENV *env;
	const char *path;
	int *isdirp;
{
	DB_ENV *dbenv;
	DWORD attrs;
	_TCHAR *tpath;
	int ret;

	dbenv = env == NULL ? NULL : env->dbenv;

	TO_TSTRING(env, path, tpath, ret);
	if (ret != 0)
		return (ret);

	if (dbenv != NULL &&
	    FLD_ISSET(dbenv->verbose, DB_VERB_FILEOPS | DB_VERB_FILEOPS_ALL))
		__db_msg(env, DB_STR_A("0033", "fileops: stat %s",
		    "%s"), path);

	RETRY_CHK(
	    ((attrs = GetFileAttributes(tpath)) == (DWORD)-1 ? 1 : 0), ret);
	if (ret == 0) {
		if (isdirp != NULL)
			*isdirp = (attrs & FILE_ATTRIBUTE_DIRECTORY);
	} else
		ret = __os_posix_err(ret);

	FREE_STRING(env, tpath);
	return (ret);
}

/*
 * __os_ioinfo --
 *	Return file size and I/O size; abstracted to make it easier
 *	to replace.
 */
int
__os_ioinfo(env, path, fhp, mbytesp, bytesp, iosizep)
	ENV *env;
	const char *path;
	DB_FH *fhp;
	u_int32_t *mbytesp, *bytesp, *iosizep;
{
	int ret;
	BY_HANDLE_FILE_INFORMATION bhfi;
	unsigned __int64 filesize;
	u_int32_t io_sz;

	RETRY_CHK((!GetFileInformationByHandle(fhp->handle, &bhfi)), ret);
	if (ret != 0) {
		__db_syserr(env, ret, DB_STR("0034",
		    "GetFileInformationByHandle"));
		return (__os_posix_err(ret));
	}

	filesize = ((unsigned __int64)bhfi.nFileSizeHigh << 32) +
	    bhfi.nFileSizeLow;

	/* Return the size of the file. */
	if (mbytesp != NULL)
		*mbytesp = (u_int32_t)(filesize / MEGABYTE);
	if (bytesp != NULL)
		*bytesp = (u_int32_t)(filesize % MEGABYTE);

	if (iosizep != NULL) {
		/*
		 * Attempt to retrieve a file system cluster size, if the
		 * call succeeds, and the value returned is reasonable,
		 * use it as the default page size. Otherwise use a
		 * reasonable default value.
		 */
		if (__os_get_cluster_size(path, &io_sz) != 0 || io_sz < 1025)
			*iosizep = DB_DEF_IOSIZE;
		else
			*iosizep = io_sz;
	}
	return (0);
}
