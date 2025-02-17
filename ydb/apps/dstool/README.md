# How to run ydb-dstool from package

## Install ydb-dstool package

```bash
user@host:~$ pip install ydb-dstool
```

## Set up environment and run

```bash
user@host:~$ export PATH=${PATH}:${HOME}/.local/bin
user@host:~$ ydb-dstool -e ydb.endpoint cluster list
```

# How to run ydb-dstool from source

## Clone ydb repository

```bash
user@host:~$ mkdir github
user@host:~$ cd github
user@host:~/github$ git clone https://github.com/ydb-platform/ydb.git
```

## Install grpc_tools python package

Follow the steps described at https://grpc.io/docs/languages/python/quickstart.

Typical command to install the `grpc_tools` package:

```bash
pip3 install grpcio-tools 'protobuf<5.0.0,>=3.13.0'
```

## Compile proto files for python

```bash
user@host:~$ cd ~/github/ydb
user@host:~/github/ydb$ ydb_root=$(pwd)
user@host:~/github/ydb$ ./ydb/apps/dstool/compile_protos.py --ydb-root ${ydb_root} 2>/dev/null
```

## Set up environment and run

```bash
user@host:~$ cd ~/github/ydb
user@host:~/github/ydb$ ydb_root=$(pwd)
user@host:~/github/ydb$ export PATH=${PATH}:${ydb_root}/ydb/apps/dstool
user@host:~/github/ydb$ export PYTHONPATH=${PYTHONPATH}:${ydb_root}
user@host:~/github/ydb$ alias ydb-dstool=${PWD}/main.py
user@host:~/github/ydb$ ydb-dstool -e ydb.endpoint cluster list
```

# How to build and upload ydb-dstool package

```bash
user@host:~$ mkdir github
user@host:~$ cd github
user@host:~/github$ git clone https://github.com/ydb-platform/ydb.git
user@host:~/github$ cd ydb
user@host:~/github/ydb$ ydb_root=$(pwd)
user@host:~/github/ydb$ ./ydb/apps/dstool/compile_protos.py --ydb-root ${ydb_root} 2>/dev/null
user@host:~/github/ydb$ mv ydb/apps/dstool/setup.py .
user@host:~/github/ydb$ python3 -m pip install --upgrade build
user@host:~/github/ydb$ python3 -m build
user@host:~/github/ydb$ python3 -m pip install --upgrade twine
user@host:~/github/ydb$ python3 -m twine upload dist/*
```

# How to do things with ydb-dstool

### Get available commands

In order to list all available commands along with their descriptions in a nicely printed tree run

```bash
user@host:~$ ydb-dstool --help
```

### Get help for a particular subset of commands or a command

```bash
user@host:~$ ydb-dstool pdisk --help
```

The above command prints help for the ```pdisk``` commands.

```bash
user@host:~$ ydb-dstool pdisk list --help
```

The above command prints help for the ```pdisk list``` command.

### Make command operation verbose

To make operation of a command verbose add ```--verbose``` to global options:

```bash
user@host:~$ ydb-dstool --verbose -e ydbd.endpoint vdisk evict --vdisk-ids ${vdisk_id}
```

### Don't show non-vital messages

To dismiss non-vital messages of a command add ```--quiet``` to global options:

```bash
user@host:~$ ydb-dstool --quiet -e ydbd.endpoint pool balance
```

### Run command without side effects

To run command without side effect add ```--dry-run``` to global options:

```bash
user@host:~$ ydb-dstool --dry-run -e ydbd.endpoint vdisk evict --vdisk-ids ${vdisk_id}
```

### Handle errors

By convention ```ydb-dstool``` returns 0 on success, and non-zero on failure. You can check exit status
as follows:

```bash
~$ user@host:~$ ydb-dstool -e ydbd.endpoint vdisk evict --vdisk-ids ${vdisk_id}
~$ if [ $? -eq 0 ]; then echo "success"; else echo "failure"; fi
```

Since ```ydb-dstool``` outputs errors to ```stderr```, to redirect errors to ```errors.txt``` one could run:

```bash
~$ user@host:~$ ydb-dstool -e ydbd.endpoint vdisk evict --vdisk-ids ${vdisk_id} 2> ~/errors.txt
```

### Set endpoint

Еndpoint is a connection point used to perform operations on cluster. It is set by a triplet
```[PROTOCOL://]HOST[:PORT]```. To set endpoint use ```--endpoint``` global option:

```bash
user@host:~$ ydb-dstool --endpoint https://ydbd.endpoint:8765 pdisk list
```

The endpoint's protocol from the above command is ```https```, host is ```ydbd.endpoint```, port is ```8765```.
The default protocol is ```http```, the default port is ```8765```.

### Set authentication token

There is support for authentication with access token. When authentication is required, user can set authentication
token with ```--token-file``` global option:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint --token-file ~/access_token
```

The above command reads ```~/access_token``` and uses it's contents as an access token for authentication.

### Set output format

Output format can be set by the ```--format``` command option. The following formats
are available:

1. ```pretty``` (default)
2. ```tsv``` (available mainly for list commands)
3. ```csv``` (available mainly for list commands)
4. ```json```

To set output format to ```tsv``` add ```--format tsv``` to command options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk list --format tsv
```

### Exclude header from the output

To exclude header with the column names from the output add ```--no-header``` to command options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk list --format tsv --no-header
```

### Output all available columns

By default a listing like command outputs only certain columns. The default columns vary from command to command.
To output all available columns add ```--all-columns``` to command options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk list --all-columns
```

### Output only certain columns

To output only certain columns add ```--columns``` along with a space separated list of columns names to command
options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk list --columns NodeId:PDiskId Path
```

The above command lists only the ```NodeId:PDiskId```, ```Path``` columns while listing pdisks.

### Sort output by certain columns

To sort output by certain columns add ```--sort-by``` along with a space separated list of columns names to command
options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk list --sort-by FQDN
```

The above command lists pdisks sorted by the ```FQDN``` column.

### Output in a human-readable way

To output sizes in terms of kilobytes, megabytes, etc. and fractions in terms of percents add ```--human-readable```
or ```-H``` to command options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk list --show-pdisk-usage -H
```

## Do things with storage devices

A storage device is a hardware data storage device installed on a cluster's machine and prepared for cluster's use.
Currently the following types of storage devices are supported:

* HDD
* SSD
* NVME

A storage device available on a cluster and preperated for use, may or may not be used by the cluster.

### List devices

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint device list
```

The above command lists all storage devices of a cluster available for use.

## Do things with pdisks

Physical disk or pdisk is an abstraction of a storage device which is used by the cluster. As a result every pdisk
has an associated storage device, but not every storage device has an associated pdisk.

### List pdisks

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk list
```

The above command lists all pdisks of a cluster along with their state.

### Show space usage of every pdisk

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk list --show-pdisk-usage --human-readable
```

The above command lists usage of all pdisks of a cluster in a human-readable way.

### Prevent new groups from using certain pdisks

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk set --decommit-status DECOMMIT_PENDING --pdisk-ids "[NODE_ID:PDISK_ID]"
```

The above command prevents new groups from using pdisk ```"[NODE_ID:PDISK_ID]"```.

### Move data out from certain pdisks

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk set --decommit-status DECOMMIT_IMMINENT --pdisk-ids "[NODE_ID:PDISK_ID]"
```

The above command initiates a background process that is going to move all of the data from pdisk ```"[NODE_ID:PDISK_ID]"```
to some ```DECOMMIT_NONE``` pdisks. This command is useful prior to unplugging either certain disks or complete hosts from
a cluster.

### Move data out from broken pdisks

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk set --status BROKEN --unavail-as-offline --pdisk-ids "[NODE_ID:PDISK_ID]"
```

The above command moves all of the data from pdisk ```"[NODE_ID:PDISK_ID]"``` to some ```DECOMMIT_NONE``` pdisks.
The operation is synchronous and happens in foreground. This command is useful when data needs to be moved from
pdisk ASAP. The ```--unavail-as-offline``` command option treats pdisk unavailable in whiteboard as not working.

### Activate broken pdisks after recovery

Broken pdisks need to be enabled after recovery. The following command

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pdisk set --status ACTIVE --allow-working-disks --pdisk-ids "[NODE_ID:PDISK_ID]"
```

enables pdisk ```"[NODE_ID:PDISK_ID]"```. The ```--allow-working-disks``` command option allows to set state for working pdisks.

## Do things with vdisks

### List vdisks

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint vdisk list
```

The above command lists all vdisks of a cluster along with the corresponding pdisks.

### Show status of pdisks were vdisks reside

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint vdisk list --show-pdisk-status
```

The above command lists all vdisks of a cluster along with the corresponding pdisks. On top of that, for every
vdisk it lists the status of the corresponding pdisk where vdisk resides.

### Show space usage every vdisk

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint vdisk list --show-vdisk-usage --human-readable
```

The above command lists usage of all vdisks of a cluster in a human-readable way.

### Unload certain pdisks by moving some vdisks from them

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint vdisk evict --vdisk-ids "[8200001b:3:0:7:0] [8200001c:1:0:1:0]"
```

The above command evicts vdisks ```[8200001b:3:0:7:0]```, ```[8200001c:1:0:1:0]``` from their current pdisks to
some other pdisks in the cluster. This command is useful when certain pdisks are unable to cope with the load or
are running out of space. This might happen because of usage sckew of certain groups.

### Wipe certain vdisks

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint vdisk wipe --vdisk-ids "[8200001b:3:0:7:0] [8200001c:1:0:1:0]" --run
```

The above command wipes out vdisks ```[8200001b:3:0:7:0]```, ```[8200001c:1:0:1:0]```. This command is useful when
vdisk becomes unhealable.

### Remove no longer needed donor vdisks

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint vdisk remove-donor --vdisk-ids "[8200001b:3:0:7:0] [8200001c:1:0:1:0]"
```

The above command removes donor vdisks ```[8200001b:3:0:7:0]```, ```[8200001c:1:0:1:0]```. The provided vdisks
have to be in donor state.

## Do things with groups

Group is a collection of vdisks that constitute basic storage unit in YDB. Every read/write operation in distributed storage
is actually a read/write opeartion within a certain group. Group by design provides the following:

* redundancy
* persistence
* availability
* failover
* recovery

Group can be thought of as a RAID of vdisks. Any vdisk belongs to a single group.

### List groups

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint group list
```

The above command lists all groups of a cluster.

### Show aggregated statuses of vdisks within group

To show aggregated statuses of vdisks within a group (i.e. how many vdisks within a group are in a certain state),
add ```--show-vdisk-status``` to command options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint group list --show-vdisk-status
```

### Show space usage of every group

To show space usage of groups, add ```--show-vdisk-usage``` to command options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint group list --show-vdisk-usage -H
```

The above command lists all groups of a cluster along with their space usage in a human-readable way.

### Check certain groups for compliance with failure model

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint group check --group-ids 2181038097 2181038105 --failure-model
```

The above command checks groups ```2181038097```, ```2181038105``` for compliance with their failure model.

### Show space usage of groups by tablets

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint group show usage-by-tablets
```

The above command shows which tablets are using which groups and what the space usage is.

### Show info about certain blob from a certain group

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint group show blob-info --group-id 2181038081 --blob-id "[72075186224037892:1:2:1:8192:410:0]"
```

The above command shows information about blob ```[72075186224037892:1:2:1:8192:410:0]``` that is stored in
group ```2181038081```. This command might be useful in certain debug scenarios.

### Add new groups to certain pool

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint group add --pool-name /Root:nvme --groups 1
```

The above command adds one group to the pool ```/Root:nvme```

### Figure out whether certain number of groups can be added

```bash
user@host:~$ ydb-dstool --dry-run -e ydbd.endpoint group add --pool-name /Root:nvme --groups 10
```

The above command adds ten groups to the pool ```/Root:nvme``` without actually adding them. It might be useful
in capacity assesment scenarios.

## Do things with pools

Pool is a collection of groups.

### List pools

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pool list
```

The above command lists all pools of a cluster.

### Show aggregated statuses of groups within pool

To show aggregated statuses of groups within a pool (i.e. how many groups within a pool are in a certain state),
add ```--show-group-status``` to command options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pool list --show-group-status
```

### Show aggregated statuses of vdisks within pool

To show aggregated statuses of vdisks within a pool (i.e. how many vdisks within a pool are in a certain state),
add ```--show-vdisk-status``` to command options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pool list --show-vdisk-status
```

### Show space usage of pools

To show space usage of pools, add ```--show-vdisk-usage``` to command options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pool list --show-vdisk-usage -H
```

The above command lists all pools of a cluster along with their space usage in a human-readable way.

### Show estimated space usage of pools

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint pool list --show-vdisk-estimated-usage
```

The above command shows:

* ```GroupsForEstimatedUsage@85``` - how many groups are neccessary to make disk usage at about 85 percent.
* ```EstimatedUsage``` -  TODO

## Do things with boxes

Box is a collection of pdisks.

###  List boxes

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint box list
```

The above command lists all boxes of a cluster.

### Show aggregated statuses of pdisks within box

Tow show aggregated statuses of pdisks within a box (i.e. how many pdisks within a box are in a certain state),
add ```--show-pdisk-status``` to command options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint box list --show-pdisk-status
```

### Show space usage of boxes

To show space usage of boxes, add ```--show-pdisk-usage``` to command options:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint box list --show-pdisk-usage -H
```

The above command lists all boxes of a cluster along with their space usage in a human-readable way.

## Do things with nodes

A node is a basic working unit in a YDB cluster. The basic building blocks like pdisk and vdisk are run on nodes.
In terms of implementation, a node is a a YDB process running on one of cluster's machines.

### List nodes

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint node list
```

The above command lists all nodes of a cluster.

## Do things with a cluster as a whole

### Show how many cluster entities there are

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint cluster list
```

The above command shows how many

* hosts
* nodes
* pools
* groups
* vdisks
* boxes
* pdisks

are in the cluster.

### Move vdisks out from overpopulated pdisks

In rare cases some pdisks can become overpopulated (i.e. they host too many vdisks) and the cluster would benefit from
balancing of vdisks over pdisks. To accomplish this, run the following command:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint cluster balance
```

The above command moves out vdisks from overpopulated pdisks. A single vdisk is moved at a time so that the failure model of
the respective group doesn't brake.

### Enable/Disable self-healing

Sometimes disks or even nodes fail which impacts vdisks that reside on them. As a result failure model of impacted groups
acquires one of the following statuses:

* PARTIAL (some vdisks within the group don't function, but failure model allows some more failures within the group)
* DEGRADED (loss of one more vdisk within the group will make the group DISINTEGRATED)
* DISINTEGRATED (group can't process read/write requests)

Self-healing enables automatic eviction of vdisks along with the neccessary data recovery for groups where there is a single
failed vdisk within a group.

To enable self-healing on a cluster, run the following command:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint cluster set --enable-self-heal
```

To disable self-healing on a cluster, run the following command:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint cluster set --disable-self-heal
```

### Enable/Disable donors

When vdisk in a group is substituted, the new vdisk needs to recover all of the data, located on the old vdisk, from the remaining
vdisks of the group. The bigger the old vdisk, the more time and resources recovery takes. In order to alleviate this process, the
old vdisk could be used as a donor, so that the new vdisk would copy all of the data from the old vdisk.

To enable support for donor vdisk mode on a cluster, run the following command:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint cluster set --enable-donor-mode
```

To disable support for donor vdisk mode on a cluster, run the following command:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint cluster set --disable-donor-mode
```

### Adjust scrubbing intervals

Scrubbing is a background process that checks data integrity and performs data recovery if necessary. To disable data scrubbing on
a cluster enter the following command:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint cluster set --scrub-periodicity disable
```

To set scrubbing interval to two days run the following command:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint cluster set --scrub-periodicity 2d
```

### Set maximum number of simultaneously scrubbed pdisks

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint cluster set --max-scrubbed-disks-at-once 2
```

The above command sets maximum number of simultaneously scrubbed pdisk to two.

### Stress test failure model

To run workload that allows to stress test failure model of groups, run the following command:

```bash
user@host:~$ ydb-dstool -e ydbd.endpoint cluster workload run
```

The above command performs various

* vdisk wipe
* vdisk evict
* node restart

operations until user terminates the process (e.g. by entering ```Ctrl + c```). The operations are created so that they don't
break failure model of any groups.
