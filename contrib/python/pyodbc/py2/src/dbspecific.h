#ifndef DBSPECIFIC_H
#define DBSPECIFIC_H

// Items specific to databases.
//
// Obviously we'd like to minimize this, but if they are needed this file isolates them.  I'd like for there to be a
// single build of pyodbc on each platform and not have a bunch of defines for supporting different databases.


// ---------------------------------------------------------------------------------------------------------------------
// SQL Server


#define SQL_SS_XML -152         // SQL Server 2005 XML type
#define SQL_DB2_DECFLOAT -360   // IBM DB/2 DECFLOAT type
#define SQL_DB2_XML -370        // IBM DB/2 XML type
#define SQL_SS_TIME2 -154       // SQL Server 2008 time type

struct SQL_SS_TIME2_STRUCT
{
   SQLUSMALLINT hour;
   SQLUSMALLINT minute;
   SQLUSMALLINT second;
   SQLUINTEGER  fraction;
};

// The SQLGUID type isn't always available when compiling, so we'll make our own with a
// different name.

struct PYSQLGUID
{
    // I was hoping to use uint32_t, etc., but they aren't included in a Python build.  I'm not
    // going to require that the compilers supply anything beyond that.  There is PY_UINT32_T,
    // but there is no 16-bit version.  We'll stick with Microsoft's WORD and DWORD which I
    // believe the ODBC headers will have to supply.
    DWORD Data1;
    WORD Data2;
    WORD Data3;
    byte Data4[8];
};

#endif // DBSPECIFIC_H
