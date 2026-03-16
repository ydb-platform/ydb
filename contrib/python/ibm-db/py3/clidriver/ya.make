RESOURCES_LIBRARY()

LICENSE(Apache-2.0)

VERSION(Linux-2025-10-27-Darwin-2025-10-27-Windows-2025-10-27)

# https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/
# ya upload --attr url=? --attr version=? --attr platform=? --ttl=inf ?
# version = Last modified date, e.g. 2019-01-01
# platform = linux | darwin | win32

IF (OS_LINUX)
    DECLARE_EXTERNAL_RESOURCE(DB2_ODBC_CLI sbr:11037289640) # linuxx64_odbc_cli.tar.gz
    LDFLAGS($DB2_ODBC_CLI_RESOURCE_GLOBAL/clidriver/lib/libdb2.so)
ELSEIF (OS_DARWIN)
    DECLARE_EXTERNAL_RESOURCE(DB2_ODBC_CLI sbr:11037290480) # macos64_odbc_cli.tar.gz
    LDFLAGS($DB2_ODBC_CLI_RESOURCE_GLOBAL/clidriver/lib/libdb2.dylib)
ELSEIF (OS_WINDOWS)
    DECLARE_EXTERNAL_RESOURCE(DB2_ODBC_CLI sbr:11037293129) # ntx64_odbc_cli.zip
    LDFLAGS($DB2_ODBC_CLI_RESOURCE_GLOBAL/clidriver/lib/db2cli64.lib)
ENDIF()

CFLAGS(
    GLOBAL -I$DB2_ODBC_CLI_RESOURCE_GLOBAL/clidriver/include
)

END()
