# Python support for IBM Db2 for LUW and IBM Db2 for z/OS

## Python, DB-API components for IBM Db2 for LUW and IBM Db2 for z/OS

Provides Python interface for connecting to IBM Db2 for LUW and IBM Db2 for z/OS

<a name='components'></a>

## Components

1. The **ibm_db** contains:
   - **ibm_db** driver: Python driver for IBM Db2 for LUW and IBM Db2 for z/OS databases. Uses the IBM Data Server Driver for ODBC and CLI APIs to connect to IBM Db2 for LUW.
   - **ibm_db_dbi**: Python driver for IBM Db2 for LUW that complies to the DB-API 2.0 specification.

## <a name="api"></a> API Documentation

For more information on the APIs supported by ibm_db, please refer to below link:

https://github.com/ibmdb/python-ibmdb/wiki/APIs

<a name="prereq"></a>

## Pre-requisites

Install Python 3.9 <= 3.14. The minimum python version supported by driver is python 3.9 and the latest version supported is python 3.14.
MacOS arm64 is supported Python 3.9 onwards.

### To install ibm_db on z/OS system

Please follow detailed installation instructions as documented here: [ibm_db Installation on z/OS](INSTALL.md#inszos)

- **SQL1598N Error** - It is expected in absence of valid db2connect license. Please click [here](#Licenserequirements) and read instructions.
- **SQL0805N Error** - You need to rebind CLI packages to fix this error. Please read detailed instructions [here](#SQL0805NError).

### For MacOS M1/M2/ Apple Silicon chip system

**MacOS with Silicon Chip** - Supported from v3.2.5 onwards using v12.x clidriver.
**MacOS with Intel Chip** - Supported using v11.x clidriver only. By default v11.5.9 clidriver will get downloaded.

### Linux/Unix:

If you face problems due to missing python header files while installing the driver, you would need to install python developer package and retry install. e.g:

```
    zypper install python-devel
     or
    yum install python-devel
```
If you have installed `DB2_RTC*` i.e. **DB2 Runtime Client** and want `ibm_db` to use it instead of clidriver, please read [this comment](https://github.com/ibmdb/python-ibmdb/issues/1023#issuecomment-3062805368) and take action.

- clidriver v12.1 on Linux uses x86_64_v2 instruction set which is not supported by cibuildwheel and wheel image creation [fails](https://github.com/pypa/manylinux/issues/1725) using v12.1 clidriver. So, default version of clidriver on Linux platform is v11.5.9 only.

- You can force installation of ibm_db on Linux with clidriver v12.1 using below commands:
```
export CLIDRIVER_VERSION=v12.1.0
pip install ibm_db --no-binary :all: --no-cache-dir
```

### Windows:

- If a db2 client or server or dsdriver or clidriver is already installed in the system and user has already set installed path to `PATH` environment variable, then user needs to set [environment variable](#envvar) `IBM_DB_HOME` manually to the installed path before installing `ibm_db`.

- To verify it, just execute `db2level` command before installation of `ibm_db`. If it works, note down the install directory path and set system level environment variable `IBM_DB_HOME` as install path shown in output of `db2level` command.

- If user has installed clidriver in `F:\DSDRIVER` and if the "PATH" environment variable has `F:\DSDRIVER\bin`, then user should also set `IBM_DB_HOME` to `F:\DSDRIVER`.

### Docker Linux containers:

- You may need to install **gcc, python, pip, python-devel, libxml2 and pam** if not already installed. Refer to [Installation](#docker) for more details.

### Manual Installation

To install ibm_db from source code after clone from git, or to know the detialed installation steps for various platforms, please check [INSTALL document](https://github.com/ibmdb/python-ibmdb/blob/master/INSTALL.md#inslnx).

<a name="installation"></a>

## Installation

- Python **Wheels** are built for Linux, MacOS and Windows operating systems for multiple python versions (from python version 3.7 to 3.13). For other platforms, package gets installed from source distribution. For MaxOS arm64, python wheels are available from version 3.9 onwards.

You can install the driver using pip as:

```
pip install ibm_db
```

This will install ibm_db and ibm_db_dbi module.

- When we install ibm_db package on Linux, MacOS and Windows, `pip install ibm_db` command install
  prebuilt Wheel package that includes clidriver too and ignores `IBM_DB_HOME` or `IBM_DB_INSTALLER_URL`
  or `CLIDRIVER_VERSION` environment variables if set. Also, auto downloading of clidriver does not happen
  as clidriver is already present inside wheel package.

- For platforms not supported by python-wheel, ibm_db will get installed from souce distribution.
  GCC compiler is required on non-windows platform and VC++ compiler on Windows platform to
  install `ibm_db` from souce distribution.

- If `db2cli validate` command works in your system and installed db2 client/server
  has `include` directory, ibm_db installation from souce distribution will not download clidriver, but
  it will use the existing client/server from the system.

- To inforce auto downloading of clidriver _OR_ to make setting of environment variable `IBM_DB_HOME` or
  `IBM_DB_INSTALLER_URL` or `CLIDRIVER_VERSION` effective; install ibm_db from source distribution
  using below command:

```
pip install ibm_db --no-binary :all: --no-cache-dir
```

- If you want to use your own URL for clidriver.tar.gz/.zip please set below environment variable:

```
export IBM_DB_INSTALLER_URL=full_path_of_clidriver.tar.gz/.zip
pip install ibm_db --no-binary :all: --no-cache-dir
```

- To install using specific version of clidriver from https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/:

```
export CLIDRIVER_VERSION=v11.5.9
pip install ibm_db --no-binary :all: --no-cache-dir
```

- ibm_db will override value of CLIDRIVER_VERSION to v12.1.0 for MacARM64 and to v11.5.9 for Macx64 platform.

- When ibm_db get installed from wheel package, you can find clidriver under site_packages directory
  of Python. You need to copy license file under `site_packages/clidriver/license` to be effective, if any.

**Note:** For windows after installing ibm_db, recieves the below error when we try to import ibm_db :

```>python
Python 3.11.4 (tags/v3.11.4:d2340ef, Jun  7 2023, 05:45:37) [MSC v.1934 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license" for more information.
>>> import ibm_db
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ImportError: DLL load failed while importing ibm_db: The specified module could not be found.
>>>
```

We need to make sure to set dll path of dependent library of clidriver before importing the module as:

```
import os
os.add_dll_directory('path to clidriver installation until bin')
import ibm_db

e.g:
os.add_dll_directory('C:\\Program Files\\IBM\\CLIDRIVER\\bin')
import ibm_db
```

Refer https://bugs.python.org/issue36085 for more details.

- <a name="docker"></a>For installing ibm_db on docker Linux container, you can refer as below:

```
yum install python gcc pam wget python-devel.x86_64 libxml2
use, `yum install python3` to install python 3.x

if pip or pip3 does not exist, install it as:
wget https://bootstrap.pypa.io/get-pip.py
docker cp get-pip.py /root:<containerid>
cd root
python3 get-pip.py

Install python ibm_db as:
pip install ibm_db
or
pip3 install ibm_db

```

- Uninstalling the ibm_db driver :

```python
pip uninstall ibm_db
```

> The ODBC and CLI Driver(clidriver) is automatically downloaded at the time of installation and it is recommended to use this driver. However, if you wish to use an existing installation of clidriver or install the clidriver manually and use it, you can set IBM_DB_HOME environment variable as documented below:

- <a name="envvar"></a>Environment Variables:
  `IBM_DB_HOME :`

  Set this environment variable to avoid automatic downloading of the clidriver during installation. You could set this to the installation path of ODBC and CLI driver in your environment.<br>
  e.g:

  ```
  Windows :
  set IBM_DB_HOME=C:/Users/userid/clidriver

  Other platforms:
  export IBM_DB_HOME=/home/userid/clidriver
  ```

  **Note:** You must need to install ibm_db using command `pip install ibm_db --no-binary :all: --no-cache-dir`
  on Linux, Windows and MacOS to make setting of `IBM_DB_HOME` effective. If you want to use Runtime Client or clidriver downloaded from fix central which do not have `include` file, please read [this comment](https://github.com/ibmdb/python-ibmdb/issues/1023#issuecomment-3062805368).

  You are required to set the library path to the clidriver under IBM_DB_HOME to pick this version of the ODBC and CLI Driver.<br>
  e.g:

  ```
  Windows:
  set LIB=%IBM_DB_HOME%/lib;%LIB%

  AIX:
  export LIBPATH=$IBM_DB_HOME/lib:$LIBPATH

  MAC:
  export DYLD_LIBRARY_PATH=$IBM_DB_HOME/lib:$DYLD_LIBRARY_PATH

  Other platforms:
  export LD_LIBRARY_PATH=$IBM_DB_HOME/lib:$LD_LIBRARY_PATH
  ```

  The ODBC and CLI driver is available for download at [Db2 LUW ODBC and CLI Driver](https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/).
  Refer to ([License requirements](#Licenserequirements)) for more details on the CLI driver for manual download and installation.

- Installing using Anaconda distribution of python

```
conda install -c conda-forge ibm_db
```

- Supported Platform for Anaconda Installation

| Platform |  Architecture  | Supported | Version |
| :------: | :------------: | :-------: | :-----: |
|  Linux   | amd64 (x86_64) |    Yes    | Latest  |
|  Linux   |    ppc64le     |    Yes    | Latest  |
|  Darwin  |   Mac OS x64   |    Yes    | Latest  |
|  Darwin  |  Mac OS arm64  |    Yes    | Latest  |
| Windows  |      x64       |    Yes    | Latest  |
| Windows  |      x32       |    Yes    | Latest  |

## <a name="quick example"></a> Quick Example

```python
$ python
Python 3.13.0 (main, Oct  7 2024, 05:02:14) [Clang 16.0.0 (clang-1600.0.26.4)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> import ibm_db
>>> #For connecting to local database named pydev for user db2inst1 and password secret, use below example
>>> #ibm_db_conn = ibm_db.connect('pydev', 'db2inst1', 'secret')
>>> #For connecting to remote database named pydev for uid db2inst and pwd secret on host host.test.com, use below example
>>> # Connect using ibm_db
>>> conn_str='database=pydev;hostname=host.test.com;port=portno;protocol=tcpip;uid=db2inst1;pwd=secret'
>>> ibm_db_conn = ibm_db.connect(conn_str,'','')
>>>
>>> # Connect using ibm_db_dbi
>>> import ibm_db_dbi
>>> conn = ibm_db_dbi.Connection(ibm_db_conn)
>>> # create table using ibm_db
>>> create="create table mytable(id int, name varchar(50))"
>>> ibm_db.exec_immediate(ibm_db_conn, create)
<ibm_db.IBM_DBStatement object at 0x7fcc5f44f650>
>>>
>>> # Execute tables API
>>> conn.tables('DB2INST1', '%')
[{'TABLE_CAT': None, 'TABLE_SCHEM': 'DB2INST1', 'TABLE_NAME': 'MYTABLE', 'TABLE_TYPE': 'TABLE', 'REMARKS': None}]
>>>
>>> # Insert 3 rows into the table
>>> insert = "insert into mytable values(?,?)"
>>> params=((1,'Sanders'),(2,'Pernal'),(3,'OBrien'))
>>> stmt_insert = ibm_db.prepare(ibm_db_conn, insert)
>>> ibm_db.execute_many(stmt_insert,params)
3
>>> # Fetch data using ibm_db_dbi
>>> select="select id, name from mytable"
>>> cur = conn.cursor()
>>> cur.execute(select)
True
>>> row=cur.fetchall()
>>> print("{} \t {} \t {}".format(row[0],row[1],row[2]),end="\n")
(1, 'Sanders')   (2, 'Pernal')   (3, 'OBrien')
>>> row=cur.fetchall()
>>> print(row)
[]
>>>
>>> # Fetch data using ibm_db
>>> stmt_select = ibm_db.exec_immediate(ibm_db_conn, select)
>>> cols = ibm_db.fetch_tuple( stmt_select )
>>> print("%s, %s" % (cols[0], cols[1]))
1, Sanders
>>> cols = ibm_db.fetch_tuple( stmt_select )
>>> print("%s, %s" % (cols[0], cols[1]))
2, Pernal
>>> cols = ibm_db.fetch_tuple( stmt_select )
>>> print("%s, %s" % (cols[0], cols[1]))
3, OBrien
>>> cols = ibm_db.fetch_tuple( stmt_select )
>>> print(cols)
False
>>>
>>> # Close connections
>>> cur.close()
True
>>> # Dropping the table created
>>> drop = "drop table mytable"
>>> stmt_delete = ibm_db.exec_immediate(ibm_db_conn,drop)
>>> conn1.tables('DB2INST1','MY%')
[]
>>>
>>> ibm_db.close(ibm_db_conn)
True
```

## Logging

### Logging in ibm_db Module

You can enable logging in the ibm_db module to debug and trace activities.
Logging can be directed to the console or a specified file.

To enable logging:

```
import ibm_db

# Log to console
ibm_db.debug(True)

# Log to a file (e.g., log.txt)
ibm_db.debug("log.txt")

# stop logging
ibm_db.debug(False)
```

Calling ibm_db.debug(True) with boolean True argument will output logs to the console.

Calling ibm_db.debug("log.txt") will log messages to the specified file (log.txt in this example).

Calling ibm_db.debug(False) with boolean False argument will stop logging.

### Logging in ibm_db_dbi Module

You can enable logging in the ibm_db_dbi module to debug and trace activities.
Logging can be directed to the console or a specified file.

To enable logging:

```
import ibm_db_dbi as dbi

# Log to console
dbi.debug(True)

# Log to a file (e.g., log.txt)
dbi.debug("log.txt")

# stop logging
dbi.debug(False)
```

Calling dbi.debug(True) with boolean True argument will output logs to the console.

Calling dbi.debug("log.txt") will log messages to the specified file (log.txt in this example).

Calling dbi.debug(False) with boolean False argument will stop logging.

## Example of SSL Connection String

- **Secure Database Connection using SSL/TSL** - ibm_db supports secure connection to Database Server over SSL same as ODBC/CLI driver. If you have SSL Certificate from server or an CA signed certificate, just use it in connection string as below:

```
Using SSLServerCertificate keyword

connStr = "DATABASE=<DATABASE_NAME>;HOSTNAME=<HOSTNAME>;PORT=<SSL_PORT>;UID=<USER_ID>;PWD=<PASSWORD>;" +
          "SECURITY=SSL;SSLServerCertificate=<FULL_PATH_TO_SERVER_CERTIFICATE>;"
conn = ibm_db.connect(connStr,'','')
```

> Note the two extra keywords **Security** and **SSLServerCertificate** used in connection string. `SSLServerCertificate` should point to the SSL Certificate from server or an CA signed certificate. Also, `PORT` must be `SSL` port and not the TCPI/IP port. Make sure Db2 server is configured to accept connection on SSL port else `ibm_db` will throw SQL30081N error.

> Value of `SSLServerCertificate` keyword must be full path of a certificate file generated for client authentication.
> It normally has `*.arm` or `*.cert` or `*.pem` extension. `ibm_db` do not support `*.jks` format file as it is not a
> certificate file but a Java KeyStore file, extract certificate from it using keytool and then use the \*.cert file.

> `ibm_db` uses IBM ODBC/CLI Driver for connectivity and it do not support a `*.jks` file as keystoredb as `keystore.jks` is meant for Java applications.
> Note that `*.jks` file is a `Java Key Store` file and it is not an SSL Certificate file. You can extract SSL certificate from JKS file using below `keytool` command:

```
keytool -exportcert -alias your_certificate_alias -file client_cert.cert -keystore  keystore.jks
```

Now, you can use the generated `client_cert.cert` as the value of `SSLServerCertificate` in connection string.

> Do not use keyworkds like `sslConnection=true` in connection string as it is a JDBC connection keyword and ibm_db
> ignores it. Corresponding ibm_db connection keyword for `sslConnection` is `Security` hence, use `Security=SSL;` in
> connection string instead.

> `ibm_db` supports only ODBC/CLI Driver keywords in connection string: https://www.ibm.com/docs/en/db2/12.1?topic=odbc-cliodbc-configuration-keywords

- To connect to dashDB in IBM Cloud, use below connection string:

```
connStr = "DATABASE=database;HOSTNAME=hostname;PORT=port;PROTOCOL=TCPIP;UID=username;PWD=passwd;Security=SSL";
```

> We just need to add **Security=SSL** in connection string to have a secure connection against Db2 server in IBM Cloud.

**Note:** You can also create a KeyStore DB using GSKit command line tool and use it in connection string along with other keywords as documented in [DB2 Documentation](https://www.ibm.com/docs/en/db2/12.1.0?topic=ctsidc-configuring-tls-support-in-non-java-db2-clients).

- If you have created a KeyStore DB using GSKit using password or you have got _.kdb file with _.sth file:

```
Using SSLClientKeyStoreDB and SSLClientKeyStoreDBPassword keyword

connStr = "DATABASE=<DATABASE_NAME>;HOSTNAME=<HOSTNAME>;PORT=<SSL_PORT>;UID=<USER_ID>;PWD=<PASSWORD>;" +
          "SECURITY=SSL;SSLClientKeyStoreDB=<FULL_PATH_TO_KEY_STORE_DB>;SSLClientKeyStoreDBPassword=<KEYSTORE_PASSWD>;"
conn = ibm_db.connect(connStr,'','')
```

```
Using SSLClientKeyStoreDB and SSLClientKeyStash keyword

connStr = "DATABASE=<DATABASE_NAME>;HOSTNAME=<HOSTNAME>;PORT=<SSL_PORT>;UID=<USER_ID>;PWD=<PASSWORD>;" +
          "SECURITY=SSL;SSLClientKeyStoreDB=<FULL_PATH_TO_KEY_STORE_DB>;" +
          "SSLClientKeyStash=<FULL_PATH_TO_CLIENT_KEY_STASH>;"
conn = ibm_db.connect(connStr,'','')
```

> If you have downloaded `IBMCertTrustStore` from IBM site, ibm*db will not work with it; you need to
> download `Secure Connection Certificates.zip` file that comes for IBM DB2 Command line tool(CLP).
> `Secure Connection Certificates.zip` has *.kdb and \_.sth files that should be used as the value of
> `SSLClientKeystoreDB` and `SSLClientKeystash` in connection string.

- More examples can be found under ['ibm_db_tests'](https://github.com/ibmdb/python-ibmdb/tree/master/IBM_DB/ibm_db/ibm_db_tests) folder.

- [API Documentation](https://github.com/ibmdb/python-ibmdb/wiki/APIs) has examples for each API.

- **Jupyter Notebook** examples can be found here -> [Other Examples](https://github.com/IBM/db2-python/tree/master/Jupyter_Notebooks)

## <a name="SQL0805NError"></a>SQL0805N Error - Rebind packages

- Connecting first time to `Db2 for z/OS` from distributed platforms will result in error **SQL0805N** Package "_location_.NULLID.SYSSH200.\_\_\_" or variation of package name.
- To solve it, the bind packages are available within the `ibm_db` python install. Do the following:

  ```
   # Change directory to where your pip package was installed locally, e.g.:
    cd .local/lib/python3.10/site-packages/clidriver/bin
   # Run the bind command using db2cli utility from there, e.g:
   ./db2cli bind ../bnd/@db2cli.lst -database _dbname_ : _hostname_ : _port_ -user _userid_  -passwd _password_ -options "grant public action replace blocking no"
  ```

## <a name="Licenserequirements"></a>For z/OS and iSeries Connectivity and SQL1598N error

- Connection to `Db2 for z/OS` or `Db2 for i`(AS400) Server using `ibm_db` driver from distributed platforms (Linux, Unix, Windows and MacOS) is not free. It requires either client side or server side license. See note below about license activation.

- Connection to `Db2 for LUW` Server using `ibm_db` driver is free.

- `ibm_db` returns SQL1598N error in absence of a valid db2connect license. SQL1598N error is returned by the Db2 Server to client.
  To suppress this error, Db2 server must be activated with db2connectactivate utility OR a client side db2connect license file must exist.

- Db2connect license can be applied on database server or client side. A **db2connect license of version 12.1** is required for ibm_db.

- For MacOSx64(Intel processor) and Linuxx64, **db2connect license of version 11.5** is required.

- clidriver v12.1 on Linux uses x86_64_v2 instruction set which is not supported by `cibuildwheel` and wheel image creation [fails](https://github.com/pypa/manylinux/issues/1725) using v12.1 clidriver. So, default version of clidriver on Linux platform is v11.5.9 only.

- For activating server side license, you can purchase either `Db2 Connect Unlimited Edition for System z®` or `Db2 Connect Unlimited Edition for System i®` license from IBM.

- Ask your DBA to run db2connectactivate utility on Server to activate db2connect license.

- If database Server is enabled for db2connect, no need to apply client side db2connect license.

- If Db2 Server is not db2connectactivated to accept unlimited number of client connection, you must need to apply client side db2connect license.

- db2connectactivate utility and client side db2connect license both comes together from IBM in a single zip file.

- Client side db2connect license is a `db2con*.lic` file that must be copied under `clidriver\license` directory and it must not be a **symlink** [file](https://github.com/ibmdb/python-ibmdb/issues/1019#issuecomment-3072461389).

- If you have a `db2jcc_license_cisuz.jar` file, it will not work for ibm_db. `db2jcc_license_cisuz.jar` is a db2connect license file for Java Driver. For non-Java Driver, client side db2connect license comes as a file name `db2con*.lic`.

- If environment variable `IBM_DB_HOME` or `IBM_DB_INSTALLER_URL` is not set, `ibm_db` automatically downloads open source driver specific clidriver from https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli and save as `site_packages\clidriver`. Ignores any other installation.

- If `IBM_DB_HOME` or `IBM_DB_INSTALLER_URL` is set, you need to have same version of db2connect license as installed db2 client. Check db2 client version using `db2level` command to know version of required db2connect license. The license file should get copied under `$IBM_DB_HOME\license` directory.

- If you do not have db2connect license, contact [IBM Customer Support](https://www.ibm.com/mysupport/s/?language=en_US) to buy db2connect license. Find the `db2con*.lic` file in the db2connect license shared by IBM and copy it under `.../site_packages/clidriver/license` directory or `$IBM_DB_HOME\license` directory (if IBM_DB_HOME is set), to be effective.

- To know more about license and purchasing cost, please contact [IBM Customer Support](https://www.ibm.com/mysupport/s/?language=en_US).

- To know more about server based licensing viz db2connectactivate, follow below links:

* [Activating the license certificate file for Db2 Connect Unlimited Edition](https://www.ibm.com/docs/en/db2/12.1.0?topic=dswnls-activating-license-key-db2-connect-unlimited-edition-system-z).
* [Unlimited licensing using db2connectactivate utility](https://www.ibm.com/docs/en/db2/12.1.0?topic=z-db2connectactivate-server-license-activation-utility).

#### Troubleshooting SQL1598N Error:

If you have copied db2con\*.lic file under clidriver/license directory, but still getting SQL1598N Error; try below options:

- `cd clidriver/bin` directory and run `./db2level` command. Make sure you have the db2connect license of same major and minor version as of clidriver.

- Make sure the user running python program has read permission for license file.

- Make sure the user running python program has read-write permission for `clidriver/license` and `clidriver/cfgcache` directories as license check utility need to create some cache files under `cfgcache` and `license` directories.

- Validate your license file and connectivity using below db2cli command:

```
  db2cli validate -connstring "connection string as used in python program" -displaylic

  OR

  db2cli validate -database "dbname:hostname:port" -user dbuser -passwd dbpasswd -connect -displaylic
```

- Installed license file under your `DB2_RTC*` client, but it is not working, please read [this comment](https://github.com/ibmdb/python-ibmdb/issues/1023#issuecomment-3062805368).

If you intend to install the clidriver manually, Following are the details of the client driver versions that you can download from [CLIDRIVER](https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/) to be able to connect to databases on non-LUW servers. You would need the client side license file as per Version for corresponding installation.:

#### <a name="LicenseDetails"></a> CLIDriver and Client license versions for Specific Platform and Architecture

| Platform | Architecture |        Cli Driver         | Supported |   Version    |
| :------: | :----------: | :-----------------------: | :-------: | :----------: |
|   AIX    |     ppc      |   aix32_odbc_cli.tar.gz   |    Yes    |   V12.1.0    |
|          |    others    |   aix64_odbc_cli.tar.gz   |    Yes    |   V12.1.0    |
|  Darwin  |     x64      |  macos64_odbc_cli.tar.gz  |    Yes    | Till V11.5.9 |
|          |    arm64     | macarm64_odbc_cli.tar.gz  |    Yes    | From V12.1.0 |
|  Linux   |     x64      | linuxx64_odbc_cli.tar.gz  |    Yes    |   V12.1.0    |
|          |    s390x     |  s390x64_odbc_cli.tar.gz  |    Yes    |   V12.1.0    |
|          |     s390     |   s390_odbc_cli.tar.gz    |    Yes    |    V11.1     |
|          |  ppc64 (LE)  |  ppc64le_odbc_cli.tar.gz  |    Yes    |   V12.1.0    |
|          |    ppc64     |   ppc64_odbc_cli.tar.gz   |    Yes    |    V10.5     |
|          |    ppc32     |   ppc32_odbc_cli.tar.gz   |    Yes    |    V10.5     |
|          |    others    | linuxia32_odbc_cli.tar.gz |    Yes    |   V12.1.0    |
| Windows  |     x64      |    ntx64_odbc_cli.zip     |    Yes    |   V12.1.0    |
|          |     x32      |     nt32_odbc_cli.zip     |    Yes    |   V12.1.0    |
|   Sun    |    i86pc     | sunamd64_odbc_cli.tar.gz  |    Yes    |    V10.5     |
|          |              | sunamd32_odbc_cli.tar.gz  |    Yes    |    V10.5     |
|          |    sparc     |   sun64_odbc_cli.tar.gz   |    Yes    |    V11.1     |
|          |    sparc     |   sun32_odbc_cli.tar.gz   |    Yes    |    V11.1     |


<a name='downloads'></a>

## Downloads

Use following pypi web location for downloading source code and binaries
**ibm_db**: https://pypi.python.org/pypi/ibm_db .
You can also get the source code by cloning the ibm_db github repository as :

```
git clone git@github.com:ibmdb/python-ibmdb.git
```

<a name='support'></a>

## Support & feedback

**Your feedback is very much appreciated and expected through project ibm-db:**

- ibm-db issues reports: **https://github.com/ibmdb/python-ibmdb/issues**
- ibm_db discuss: **http://groups.google.com/group/ibm_db**

<a name='contributing-to-the-ibm_db-python-project'></a>

## Contributing to the ibm_db python project

See [CONTRIBUTING](https://github.com/ibmdb/python-ibmdb/blob/master/contributing/CONTRIBUTING.md)

```
The developer sign-off should include the reference to the DCO in remarks(example below):
DCO 1.1 Signed-off-by: Random J Developer <random@developer.org>
```

<a name='KnownIssues'></a>

## Some common issues

## 1. Installation Issues for missing python.h file

### Always use the latest pip

`python3 -m pip install --upgrade pip`

### Install the package python3-devel that delivers the python.h header file

```
For RHEL use
sudo yum install python3-devel
```

```
For Ubuntu use
sudo apt-get install python3-devel
```

- Once the above steps works fine, try re-installing ibm_db.

## 2. SQL30081N Error

If connection fails with SQL30081N error - means `ibm_db` installation is correct and there is some issue with connection string. Please check database connection info and use correct connection string. If you are using SSL connection, port must be SSL port and connection string must have `Security=SSL;` and `SSLServerCertificate=<full path of cert.arm file>;`.

## 3. Issues with MAC OS X

- If `import ibm_db` fails with `Symbol not found: ___cxa_throw_bad_array_new_length` error or `malloc` error: Please follow instructions as documented [here](INSTALL.md#symbolerror).

- If you run into errors for libdb2.dylib as below:

```python
>>> import ibm_db
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ImportError: dlopen(/usr/local/lib/python3.5/site-packages/ibm_db.cpython-35m-darwin.so, 2): Library not loaded: libdb2.dylib
  Referenced from: /usr/local/lib/python3.5/site-packages/ibm_db.cpython-35m-darwin.so
  Reason: image not found

```

You would need to set DYLD_LIBRARY_PATH to point to lib folder as per the installation location of clidriver in your environment. Assuming the driver is installed at /usr/local/lib/python3.5/site-packages/clidriver, you can set the path as:

```
export DYLD_LIBRARY_PATH=/usr/local/lib/python3.5/site-packages/clidriver/lib:$DYLD_LIBRARY_PATH

```

If the issue is not resolved even after setting DYLD_LIBRARY_PATH, you could refer:
[MAC OS Hints and Tips](https://medium.com/@sudhanvalp/overcome-ibm-db-import-error-image-not-found-on-macos-578f07b70762)

### Resolving SQL1042C error

If you hit following error while attempting to connect to a database:

```python
>>> import ibm_db
>>> ibm_db.connect("my_connection_string", "", "")
 Traceback (most recent call last):
   File "<stdin>", line 1, in <module>
 Exception: [IBM][CLI Driver] SQL1042C An unexpected system error occurred. SQLSTATE=58004 SQLCODE=-1042
```

Set DYLD_LIBRARY_PATH to point to icc folder as per the installation location of clidriver in your environment.

```
export DYLD_LIBRARY_PATH=/usr/local/lib/python3.5/site-packages/clidriver/lib/icc:$DYLD_LIBRARY_PATH
```

In case of similar issue in windows platform

```
set PATH=<clidriver_folder_path>\bin\amd64.VC12.CRT;%PATH%
```

## 4. ERROR: Failed building wheel for ibm_db

In case of the error seen while building wheel use the following flag along with ibm_db for installation

```
To install the package ibm_db it is necessary at first to install the build dependency package - wheel:
pip3 install wheel
Install ibm_db with the pip flag --no-build-isolation:
pip3 install ibm_db --no-build-isolation
```

## 5. For Issues on IBM iSeries System (AS400)

- If you have installed `ibm_db` on IBM i and need help, please open an issue [here](https://github.com/kadler/python-ibmdb/issues).

- If you have installed `ibm_db` on distributed platform and want to connect to AS400 server, you must have to use db2connect license. `ibm_db` do not work with `IBM i Access` driver.

<a name='testing'></a>

# Testing

Tests displaying Python ibm_db driver code examples are located in the ibm_db_tests
directory. A valid config.py will need to be created to configure your Db2
settings. A config.py.sample exists that can be copied and modified for your
environment.

- Set Environment Variables DB2_USER, DB2_PASSWD accordingly.

```
For example, by sourcing the following ENV variables:
For Linux
export DB2_USER=<Username>
export DB2_PASSWD=<Password>

For windows
set DB2_USER=<Username>
set DB2_PASSWD=<Password>

```

- OR

```
If not using environment variables, update user and password information in
config.json file.

```

The config.py should look like this:

```python
test_dir =      'ibm_db_tests'         # Location of testsuite file (relative to current directory)
file_path = 'config.json'

with open(file_path, 'r') as file:
    data = json.load(file)

database = data['database']               # Database to connect to
hostname = data['hostname']               # Hostname
port = data['port']                       # Port Number

env_not_set = False
if 'DB2_USER' in os.environ:
     user = os.getenv('DB2_USER')         # User ID to connect with
else:
    user = data['user']
    env_not_set = True
if 'DB2_PASSWD' in os.environ:
    password = os.getenv('DB2_PASSWD')    # Password for given User ID
else:
    password = data['password']
    env_not_set = True

if env_not_set == True:
    warnings.warn("Warning: Environment variable DB2_USER or DB2_PASSWD is not set.")
    print("Please set it before running test file and avoid")
    print("hardcoded password in config.json file." )
```

Point the database to mydatabase as created by the following command.

The tests that ibm_db driver uses depends on a UTF-8 database. This can be
created by running:

```
  CREATE DATABASE mydatabase USING CODESET UTF-8 TERRITORY US
```

Some of the tests utilize XML functionality only available in version 9 or
later of Db2. While Db2 v8.x is fully supported, two of the tests
(test_195.py and test_52949.py) utilize XML functionality. These tests will
fail on version 8.x of Db2.

## Running the driver testsuite on Linux

In order to run the entire python driver testsuite on Linux, run this
command at the command prompt:

```
  python ibmdb_tests.py
```

To run a single test, set the environment variable, **SINGLE_PYTHON_TEST**, to
the test filename you would like to run, followed by the previous command.

## Running the driver testsuite on Windows

In order to run the entire python driver testsuite on Windows, run this
command at the command prompt:

```
  python ibmdb_tests.py
```

To run a single test, set the environment variable, **SINGLE_PYTHON_TEST**, to
the test filename you would like to run, followed by the previous command.

## Known Limitations for the Python driver

If trusted context is not set up, there will be two failures related to trusted context. When thick client has been used then additional three failures related to create, recreate DB.

## Known Limitations for the Python wrapper

1. The rowcount for select statements can not be generated.
2. Some warnings from the drivers are not caught by the wrapper.
   As such these might go unnoticed.

Happy coding!
