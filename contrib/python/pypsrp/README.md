# pypsrp - Python PowerShell Remoting Protocol Client library

[![Test workflow](https://github.com/jborean93/pypsrp/actions/workflows/ci.yml/badge.svg)](https://github.com/jborean93/pypsrp/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/jborean93/pypsrp/branch/master/graph/badge.svg)](https://codecov.io/gh/jborean93/pypsrp)
[![PyPI version](https://badge.fury.io/py/pypsrp.svg)](https://badge.fury.io/py/pypsrp)

pypsrp is a Python client for the PowerShell Remoting Protocol (PSRP) and
Windows Remote Management (WinRM) service. It allows your to execute commands
on a remote Windows host from any machine that can run Python.

This library exposes 4 different types of APIs;

* A simple client API that can copy files to and from the remote Windows host as well as execute processes and PowerShell scripts
* A WSMan interface to execute various WSMan calls like `Send`, `Create`, `Connect`, `Disconnect`, etc
* A Windows Remote Shell (WinRS) layer that executes cmd commands and executables using the base WinRM protocol
* A PowerShell Remoting Protocol (PSRP) layer allows you to create remote Runspace Pools and PowerShell pipelines

At a basic level, you can use this library to;

* Execute a cmd command
* Run another executable
* Execute PowerShell scripts
* Copy a file from the localhost to the remote Windows host
* Fetch a file from the remote Windows host to the localhost
* Create a Runspace Pool that contains one or multiple PowerShell pipelines and execute them asynchronously
* Support for a reference host base implementation of PSRP for interactive scripts

Currently this library only supports the WSMan transport method but is designed
to support SSH at some point in the future (PR's are welcome). By default it
supports the following authentication methods with WSMan;

* Basic
* Certificate
* NTLM

It also supports `Negotiate/Kerberos`, and `CredSSP` but require extra
libraries to be installed.


## Requirements

See `How to Install` for more details

* CPython 3.10+
* [cryptography](https://github.com/pyca/cryptography)
* [pyspnego](https://github.com/jborean93/pyspnego)
* [requests](https://github.com/requests/requests)

### Optional Requirements

The following Python libraries can be installed to add extra features that do
not come with the base package:

* [python-gssapi](https://github.com/pythongssapi/python-gssapi) for Kerberos authentication on Linux
* [pykrb5](https://github.com/jborean93/pykrb5) for Kerberos authentication on Linux
* [requests-credssp](https://github.com/jborean93/requests-credssp) for CredSSP authentication


## How to Install

To install pypsrp with all the basic features, run:

```bash
pip install pypsrp
```

### Kerberos Authentication

While pypsrp supports Kerberos authentication, it isn't included by default for
Linux hosts due to it's reliance on system packages to be present.

To install these packages, depending on your distribution, run one of the following script blocks.

For Debian/Ubuntu

```bash
# For Python 2
apt-get install gcc python-dev libkrb5-dev

# For Python 3
apt-get install gcc python3-dev libkrb5-dev

# To add NTLM to the GSSAPI SPNEGO auth run
apt-get install gss-ntlmssp
```

For RHEL/Centos

```bash
yum install gcc python-devel krb5-devel

# To add NTLM to the GSSAPI SPNEGO auth run
yum install gssntlmssp
```

For Fedora

```bash
dnf install gcc python-devel krb5-devel

# To add NTLM to the GSSAPI SPNEGO auth run
dnf install gssntlmssp
```

For Arch Linux

```bash
pacman -S gcc krb5
```

Once installed you can install the Python packages with

```bash
pip install pypsrp[kerberos]
```

Kerberos also needs to be configured to talk to the domain but that is outside
the scope of this page.

### CredSSP Authentication

Like Kerberos auth, CredSSP is supported but isn't included by default. To add
support for CredSSP auth try to run the following

```bash
pip install pypsrp[credssp]
```

If that fails you may need to update pip and setuptools to a newer version
`pip install -U pip setuptools`, otherwise the following system package may be
required;

```bash
# For Debian/Ubuntu
apt-get install gcc python-dev

# For RHEL/Centos
yum install gcc python-devel

# For Fedora
dnf install gcc python-devel
```


## How to Use

There are 3 main components that are in use within this library;

* `Transport`: Handles the raw transport of messages to and from the server
* `Shell`: Handles the WSMV or PSRP protocol details used to create the remote shell that processes are run on, uses `Connection` to send the details
* `Process`: Runs the process or script within a shell

### Connection

Currently only the connection that is supported is the WSMan protocol over HTTP
through `pypsrp.wsman.WSMan` and offers mostly all the same features in the
WSMV spec including;

* Basic, Certificate, Negotiate, Kerberos, and CredSSP authentication
* TLS encryption
* Message encryption with Negotiate, Kerberos, and CredSSP authentication
* Definable proxy

These are the options that can be used to setup `WSMan`;

* `server`: The hostname or IP address of the host to connect to
* `max_envelope_size`: The maximum envelope size, in bytes, that can be sent to the server, default is `153600`
* `operation_timeout`: The operation timeout, in seconds, of each WSMan operation, default is `20`. This should always be lower than `read_timeout`.
* `port`: The port to connect to, default is `5986` if `ssl=True` else `5985`
* `username`: The username to connect with, required for all auths except `certificate` and optionally required for `negotiate/kerberos`
* `password`: The password for `username`. Due to a bug on MacOS/Heimdal GSSAPI implementations, this will persist in the user's ccache when using Negotiate or Kerberos authentication, run `kdestroy` manually to remove this
* `ssl`: Whether to connect over `https` or `https`, default is `True`
* `path`: The WinRM path to connect to, default is `wsman`
* `auth`: The authentication protocol to use, default is `negotiate`, choices are `basic`, `certificate`, `negotiate`, `ntlm`, `kerberos`, `credssp`
* `cert_validation`: Whether to validate the server's SSL certificate, default is `True`. Can be `False` to not validate or a path to a PEM file of trusted certificates
* `connection_timeout`: The timeout for creating a HTTP connection, default is `30`
* `read_timeout`: The timeout for receiving a response from the server after a request has been made, default is `30`
* `encryption`: Controls the encryption settings, default is `auto`, choices are `auto`, `always`, `never`. Set to `always` to always run message encryption even over HTTPS, `never` to never use message encryption even over HTTP
* `proxy`: The proxy URL used to connect to the remote host
* `no_proxy`: Whether to ignore any environment proxy variable and connect directly to the host, default is `False`
* `locale`: The `wsmv:Locale` value to set on each WSMan request. This specifies the language in which the cleint wants response text to be translated, default is `en-US`
* `data_locale`: The `wsmv:DataLocale` value to set on each WSMan request. This specifies the format in which numerical data is presented in the response text, default is the value of `locale`
* `reconnection_retries`: Number of retries on a connection problem, default is `0`
* `reconnection_backoff`: Number of seconds to backoff in between reconnection attempts (first sleeps X, then sleeps 2*X, 4*X, 8*X, ...), default is `2.0`
* `certificate_key_pem`: The path to the certificate key used in `certificate` authentication. The key can be in either a `PKCS#1` or `PKCS#8` format
* `certificate_key_password`: The password for `certificate_key_pem` if it is encrypted
* `certificate_pem`: The path to the certificate used in `certificate` authentication
* `credssp_auth_mechanism`: The sub-auth mechanism used in CredSSP, default is `auto`, choices are `auto`, `ntlm`, or `kerberos`
* `credssp_disable_tlsv1_2`: Whether to used CredSSP auth over the insecure TLSv1.0, default is `False`
* `credssp_minimum_version`: The minimum CredSSP server version that the client will connect to, default is `2`
* `negotiate_delegate`: Whether to negotiate the credential to the host, default is `False`. This is only valid if `negotiate` auth negotiated Kerberos or `kerberos` was explicitly set
* `negotiate_hostname_override`: The hostname used to calculate the host SPN when authenticating the host with Kerberos auth. This is only valid if `negotiate` auth negotiated Kerberos or `kerberos` was explicitly set
* `negotiate_send_cbt`: Whether to binding the channel binding token (HTTPS only) to the auth or ignore, default is `True`
* `negotiate_service`: Override the service part of the calculated SPN used when authenticating the server, default is `WSMAN`. This is only valid if `negotiate` auth negotiated Kerberos or `kerberos` was explicitly set

When running over HTTP, this library will enforce encryption by default but if
that is not supported (Basic auth) or isn't available on the host then either
use HTTPS or disable encryption with `encryption="never"`.

There are plans to add support for SSH as a connection but this still needs to
be implemented. SSH will work on hosts that are running PowerShell Core but
not the standard PowerShell.

### Shell

There are two shells that can be used in this library, `pypsrp.shell.WinRS` and
`pypsrp.powershell.RunspacePool`.

`WinRS` is a cmd shell that can be used to issue cmd commands, including but
not limited to other executables. Here are the options that can be used to
configure a `WinRS` shell;

* `wsman`: WinRS only works over WSMan, so this is the `pypsrp.wsman.WSMan` object to run the commands over
* `resource_uri`: The resource uri of the shell, defaults to `http://schemas.microsoft.com/wbem/wsman/1/windows/shell/cmd`
* `id`: The ID if the shell, this should be kept as `None` as it is created dynamically by the server
* `input_streams`: The input stream(s) of the shell, default is `stdin`
* `output_streams`: The output stream(s) of the shell, default is `stdout, stderr`
* `codepage`: The codepage of the shell, default is the default of the host
* `environment`: A dictionary of environment key/values to set for the remote shell
* `idle_time_out`: THe idle timeout in seconds of the shell
* `lifetime`: The total lifetime of the shell
* `name`: The name (description only) of the shell
* `no_profile`: Whether to create the shell with the user profile loaded or not. This no longer works on Server 2012/Windows 8 or newer
* `working_directory`: The default working directory of the created shell

`RunspacePool` is a shell used by the PSRP protocol, it is designed to be a
close implementation of the .NET
[System.Management.Automation.Runspaces.RunspacePool](https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.runspaces.runspacepool?view=powershellsdk-1.1.0)
class. The methods and properties are similar and can mostly do the same thing.
Here are the options that can be used to configure a `RunspacePool` shell;

* `connection`: The connection object used by the RunspacePool to send commands to the remote server, currently only supports `WSMan`
* `apartment_state`: The int value of `pypsrp.complex_objects.ApartmentState` for the remote thread, default is `UNKNOWN`
* `thread_options`: The int value of `pypsrp.complex_objects.ThreadOptions` that specifies the type of thread to create, default is `DEFAULT`
* `host`: The local host info implementation, default is no host
* `configuration_name`: The configuration name to connect to, default is `Microsoft.PowerShell` and can be used to specify the Just Enough Administration (JEA) to connect to
* `min_runspaces`: The minimuum number of runspaces that a pool can hold, default is 1
* `max_runspaces`: The maximum number of runspaces that a pool can hold. Each PowerShell pipeline is run in a single Runspace, default is 1
* `session_key_timeout_ms`: The maximum time to wait for a session key transfer from the server
* `no_profile`: Do not load the user profile on the remote Runspace Pool

### Process

There are two process objects that can be used, `pypsrp.shell.Process` for the
`WinRS` shell and `pypsrp.powershell.PowerShell` for the `RunspacePool` shell.
These objects are ultimately used to execute commands, processes, or scripts on
the remote host.

`Process` is used with the `WinRS` shell to execute a cmd command or another
executable. The following options are used to configure the `Process` object;

* `shell`: The `WinRS` shell the process is run over
* `executable`: The executable or command to run
* `arguments`: A list of arguments to the executable or command, default is no arguments
* `id`: The ID of the created command, if not specified then this is dynamically created
* `no_shell`: Whether to create a command in the cmd shell or bypass it, default is `False`. If `True` then the executable must be the full path to the exe. This only works on older OS' before 2012 R2 (not including)

To execute the process, call `.invoke()`, the `stdout`, `stderr`, and `rc`
properties contain the output of the command once complete.

`PowerShell` is used by the PSRP protocol, it is designed to be a close
implementation of the
[System.Management.Automation.PowerShell](https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.powershell?view=powershellsdk-1.1.0)
class. The methods and properties are similar and can mostly do the same thing.
Here are the options that can be used to configure a `PowerShell` process;

* `runspace_pool`: The `RunspacePool` object to run the `PowerShell` process on

To execute the process, call `.invoke()`, the `output`, `had_erros`, and
`streams` contains the execution status and output information of the process.
Before invoke can be called, cmdlets or scripts must be added. These can be
done with the following methods;

* `add_script`: Add a raw PowerShell script to the pending commands
* `add_cmdlet`: Add a cmdlet to the pending commands
* `add_parameters`: Add a dictionary of key/value parameters to the last added command
* `add_argument`: Add a value argument to the last added command
* `add_statement`: Set the last command/script to be the end of that pipeline so the next command/script is like a newline

See the examples below for more details.

### Examples

How to use the high level client API

```python
from pypsrp.client import Client

# this takes in the same kwargs as the WSMan object
with Client("server", username="user", password="password") as client:

    # execute a cmd command
    stdout, stderr, rc = client.execute_cmd("dir")

    stdout, stderr, rc = client.execute_cmd("powershell.exe gci $pwd")
    sanitised_stderr = client.sanitise_clixml(stderr)

    # execute a PowerShell script
    output, streams, had_errors = client.execute_ps('''$path = "%s"
if (Test-Path -Path $path) {
    Remove-Item -Path $path -Force -Recurse
}
New-Item -Path $path -ItemType Directory''' % path)
    output, streams, had_errors = client.execute_ps("New-Item -Path C:\\temp\\folder -ItemType Directory")

    # copy a file from the local host to the remote host
    client.copy("~/file.txt", "C:\\temp\\file.txt")

    # fetch a file from the remote host to the local host
    client.fetch("C:\\temp\\file.txt", "~/file.txt")
```

How to use WinRS/Process to execute a command


```python
from pypsrp.shell import Process, SignalCode, WinRS
from pypsrp.wsman import WSMan

# creates a http connection with no encryption and basic auth
wsman = WSMan("server", ssl=False, auth="basic", encryption="never",
              username="vagrant", password="vagrant")

with wsman, WinRS(wsman) as shell:
    process = Process(shell, "dir")
    process.invoke()
    process.signal(SignalCode.CTRL_C)

    # execute a process with arguments in the background
    process = Process(shell, "powershell", ["gci", "$pwd"])
    process.begin_invoke()  # start the invocation and return immediately
    process.poll_invoke()  # update the output stream
    process.end_invoke()  # finally wait until the process is finished
    process.signal(SignalCode.CTRL_C)
```

How to use RunspacePool/PowerShell to execute a PowerShell script/command

```python
from pypsrp.powershell import PowerShell, RunspacePool
from pypsrp.wsman import WSMan

# creates a https connection with explicit kerberos auth and implicit credentials
wsman = WSMan("server", auth="kerberos", cert_validation=False))

with wsman, RunspacePool(wsman) as pool, PowerShell(pool) as ps:
    # execute 'Get-Process | Select-Object Name'
    ps.add_cmdlet("Get-Process").add_cmdlet("Select-Object").add_argument("Name")
    output = ps.invoke()

    # execute 'Get-Process | Select-Object -Property Name'
    ps.add_cmdlet("Get-Process").add_cmdlet("Select-Object")
    ps.add_parameter("Property", "Name")
    ps.begin_invoke()  # execute process in the background
    ps.poll_invoke()  # update the output streams
    ps.end_invoke()  # wait until the process is finished

    # execute 'Get-Process | Select-Object -Property Name; Get-Service audiosrv'
    ps.add_cmdlet("Get-Process").add_cmdlet("Select-Object").add_parameter("Property", "Name")
    ps.add_statement()
    ps.add_cmdlet("Get-Service").add_argument("audiosrc")
    ps.invoke()

    # execute a PowerShell script with input being sent
    script = '''begin {
    $DebugPreference = "Continue"
    Write-Debug -Message "begin"
} process {
    Write-Output -InputObject $input
} end {
    Write-Debug -Message "end"
}
'''
    ps.add_script(script)
    ps.invoke(["string", 1])
    print(ps.output)
    print(ps.streams.debug)

    # It is possible to run the PowerShell pipeline again with invoke() but it
    # needs to be explicitly closed first and the commands/streams optionally
    # cleared if desired.
    ps.close()

    # Clears out ps.output and ps.streams to a blank value. Not required but
    # nice if the output should be separate from a previous run
    ps.clear_streams()

    # Removes all existing commands. Not required but needed if re-using the
    # same pipeline with a different set of commands
    ps.clear_commands()
```


## Logging

This library takes advantage of the Python logging configuration and messages
are logged to the `pypsrp` named logger as well as `pypsrp.*` where `*` is each
Python script in the `pypsrp` directory.

An easy way to turn on logging for the entire library is to create the
following JSON file and run your script with
`PYPSRP_LOG_CFG=log.json python script.py` (this does not work with Python
2.6).

```json
{
    "version": 1,
    "disable_existing_loggers": false,
    "formatters": {
        "simple": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },

    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        }
    },

    "loggers": {
        "pypsrp": {
            "level": "DEBUG",
            "handlers": ["console"],
            "propagate": "no"
        }
    }
}
```

You can adjust the log level by changing the level value in `logger` to `INFO`.

_Note: `DEBUG` contains a lot of information and will output all the messages
sent to and from the client. This can have the side effect of leaking sensitive
information and should only be used for debugging purposes._


## Testing

Any changes are more than welcome in pull request form, you can run the current
test suite with:

```bash
pip install -e .[dev]

python -m pytest \
    tests/tests_pypsrp \
    --verbose \
    --junitxml junit/test-results.xml \
    --cov pypsrp \
    --cov-report xml \
    --cov-report term-missing
```

A lot of the tests either simulate a remote Windows host but you can also run a
lot of them against a real Windows host. To do this, set the following
environment variables before running the tests;

* `PYPSRP_SERVER`: The hostname or IP of the remote host
* `PYPSRP_USERNAME`: The username to connect with
* `PYPSRP_PASSWORD`: The password to connect with
* `PYPSRR_PORT`: The port to connect with (default: `5986`)
* `PYPSRP_AUTH`: The authentication protocol to auth with (default: `negotiate`)

There are further integration tests that require a specific host setup to run
correctly. You can use `Vagrant` to set this host up. This is done by running
the following commands;

```bash
# download the Vagrant box and start it up based on the Vagrantfile
vagrant up

# once the above script is complete run the following
vagrant ssh  # password is vagrant

powershell.exe
Register-PSSessionConfiguration -Path "C:\Users\vagrant\Documents\JEARoleSettings.pssc" -Name JEARole -Force

$sec_pass = ConvertTo-SecureString -String "vagrant" -AsPlainText -Force
$credential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "vagrant", $sec_pass
$thumbprint = (Get-ChildItem -Path Cert:\LocalMachine\TrustedPeople)[0].Thumbprint

New-Item -Path WSMan:\localhost\ClientCertificate `
    -Subject "vagrant@localhost" `
    -URI * `
    -Issuer $thumbprint `
    -Credential $credential `
    -Force


# exit the remote PowerShell session
exit

# exist the SSH session
exit
```

Once complete, set the following environment variables to run the integration
tests;

* `PYPSRP_RUN_INTEGRATION`: To any value
* `PYPSRP_SERVER`: Set to `127.0.0.1`
* `PYPSRP_USERNAME`: Set to `vagrant`
* `PYPSRP_PASSWORD`: Set to `vagrant`
* `PYPSRP_HTTP_PORT`: Set to `55985`
* `PYPSRP_HTTPS_PORT`: Set to `55986`
* `PYPSRP_CERT_DIR`: Set to the full path of the project directory

From here you can run the normal test suite and it will run all the integration
tests.


## Backlog

* Look at implementing the following transport options
    * Named pipes
    * SSH
* Update CI to use named pipes for integration tests
* Add Ansible playbook for better integration tests
* Improved serialization between Python and .NET objects
* Live interactive console for PSRP
