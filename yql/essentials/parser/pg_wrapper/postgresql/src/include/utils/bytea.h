/*-------------------------------------------------------------------------
 *
 * bytea.h
 *	  Declarations for BYTEA data type support.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/bytea.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BYTEA_H
#define BYTEA_H



typedef enum
{
	BYTEA_OUTPUT_ESCAPE,
	BYTEA_OUTPUT_HEX
}			ByteaOutputType;

extern __thread PGDLLIMPORT int bytea_output;	/* ByteaOutputType, but int for GUC
										 * enum */

#endif							/* BYTEA_H */
