RESOURCES_LIBRARY()

LICENSE(Apache-2.0)

VERSION(Linux-2019-01-01-Darwin-2017-02-23-Windows-2018-09-24)

# https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/
# ya upload --attr url=? --attr version=? --attr platform=? --ttl=inf ?
# version = Last modified date, e.g. 2019-01-01
# platform = linux | darwin | win32

IF (OS_LINUX)
    DECLARE_EXTERNAL_RESOURCE(DB2_ODBC_CLI sbr:1073855936) # linuxx64_odbc_cli.tar.gz
    LDFLAGS($DB2_ODBC_CLI_RESOURCE_GLOBAL/clidriver/lib/libdb2.so)
ELSEIF (OS_DARWIN)
    DECLARE_EXTERNAL_RESOURCE(DB2_ODBC_CLI sbr:1073929736) # macos64_odbc_cli.tar.gz
    LDFLAGS($DB2_ODBC_CLI_RESOURCE_GLOBAL/clidriver/lib/libdb2.dylib)
ELSEIF (OS_WINDOWS)
    DECLARE_EXTERNAL_RESOURCE(DB2_ODBC_CLI sbr:1073972095) # ntx64_odbc_cli.zip
    LDFLAGS($DB2_ODBC_CLI_RESOURCE_GLOBAL/clidriver/lib/db2cli64.lib)
ENDIF()

CFLAGS(
    GLOBAL -I$DB2_ODBC_CLI_RESOURCE_GLOBAL/clidriver/include
)

END()
