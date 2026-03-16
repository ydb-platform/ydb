# PySyncObj

[![Build Status][tests-image]][tests] [![Windows Build Status][appveyor-image]][appveyor] [![Coverage Status][coverage-image]][coverage] [![Release][release-image]][releases] [![License][license-image]][license] [![gitter][gitter-image]][gitter] [![docs][docs-image]][docs]

[tests-image]: https://github.com/bakwc/PySyncObj/actions/workflows/tests.yaml/badge.svg
[tests]: https://github.com/bakwc/PySyncObj/actions/workflows/tests.yaml

[appveyor-image]: https://ci.appveyor.com/api/projects/status/github/bakwc/pysyncobj?branch=master&svg=true
[appveyor]: https://ci.appveyor.com/project/bakwc/pysyncobj

[coverage-image]: https://coveralls.io/repos/github/bakwc/PySyncObj/badge.svg?branch=master
[coverage]: https://coveralls.io/github/bakwc/PySyncObj?branch=master

[release-image]: https://img.shields.io/badge/release-0.3.14-blue.svg?style=flat
[releases]: https://github.com/bakwc/PySyncObj/releases

[license-image]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat
[license]: LICENSE.txt

[gitter-image]: https://badges.gitter.im/bakwc/PySyncObj.svg
[gitter]: https://gitter.im/bakwc/PySyncObj?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

[docs-image]: https://readthedocs.org/projects/pysyncobj/badge/?version=latest
[docs]: http://pysyncobj.readthedocs.io/en/latest/

PySyncObj is a python library for building fault-tolerant distributed systems. It provides the ability to replicate your application data between multiple servers. It has following features:

- [raft protocol](http://raft.github.io/) for leader election and log replication
- Log compaction - it use fork for copy-on-write while serializing data on disk
- Dynamic membership changes - you can do it with [syncobj_admin](https://github.com/bakwc/PySyncObj/wiki/syncobj_admin) utility or [directly from your code](https://github.com/bakwc/PySyncObj/wiki/Dynamic-membership-change)
- [Zero downtime deploy](https://github.com/bakwc/PySyncObj/wiki/Zero-downtime-deploy) - no need to stop cluster to update nodes
- In-memory and on-disk serialization - you can use in-memory mode for small data and on-disk for big one
- Encryption - you can set password and use it in external network
- Python2 and Python3 on linux, macos and windows - no dependencies required (only optional one, eg. cryptography)
- Configurable event loop - it can works in separate thread with it's own event loop - or you can call onTick function inside your own one
- Convenient interface - you can easily transform arbitrary class into a replicated one (see example below).

## Content
 * [Install](#install)
 * [Basic Usage](#usage)
 * ["Batteries"](#batteries)
 * [API Documentation](http://pysyncobj.readthedocs.io)
 * [Performance](#performance)
 * [Publications](#publications)

## Install
PySyncObj itself:
```bash
pip install pysyncobj
```
Cryptography for encryption (optional):
```bash
pip install cryptography
```

## Usage
Consider you have a class that implements counter:
```python
class MyCounter(object):
	def __init__(self):
		self.__counter = 0

	def incCounter(self):
		self.__counter += 1

	def getCounter(self):
		return self.__counter
```
So, to transform your class into a replicated one:
 - Inherit it from SyncObj
 - Initialize SyncObj with a self address and a list of partner addresses. Eg. if you have `serverA`, `serverB` and `serverC` and want to use 4321 port, you should use self address `serverA:4321` with partners `[serverB:4321, serverC:4321]` for your application, running at `serverA`; self address `serverB:4321` with partners `[serverA:4321, serverC:4321]` for your application at `serverB`; self address `serverC:4321` with partners `[serverA:4321, serverB:4321]` for app at `serverC`.
 - Mark all your methods that modifies your class fields with `@replicated` decorator.
So your final class will looks like:
```python
class MyCounter(SyncObj):
	def __init__(self):
		super(MyCounter, self).__init__('serverA:4321', ['serverB:4321', 'serverC:4321'])
		self.__counter = 0

	@replicated
	def incCounter(self):
		self.__counter += 1

	def getCounter(self):
		return self.__counter
```
And thats all! Now you can call `incCounter` on `serverA`, and check counter value on `serverB` - they will be synchronized.

## Batteries
If you just need some distributed data structures - try built-in "batteries". Few examples:
### Counter & Dict
```python
from pysyncobj import SyncObj
from pysyncobj.batteries import ReplCounter, ReplDict

counter1 = ReplCounter()
counter2 = ReplCounter()
dict1 = ReplDict()
syncObj = SyncObj('serverA:4321', ['serverB:4321', 'serverC:4321'], consumers=[counter1, counter2, dict1])

counter1.set(42, sync=True) # set initial value to 42, 'sync' means that operation is blocking
counter1.add(10, sync=True) # add 10 to counter value
counter2.inc(sync=True) # increment counter value by one
dict1.set('testKey1', 'testValue1', sync=True)
dict1['testKey2'] = 'testValue2' # this is basically the same as previous, but asynchronous (non-blocking)
print(counter1, counter2, dict1['testKey1'], dict1.get('testKey2'))
```
### Lock
```python
from pysyncobj import SyncObj
from pysyncobj.batteries import ReplLockManager

lockManager = ReplLockManager(autoUnlockTime=75) # Lock will be released if connection dropped for more than 75 seconds
syncObj = SyncObj('serverA:4321', ['serverB:4321', 'serverC:4321'], consumers=[lockManager])
if lockManager.tryAcquire('testLockName', sync=True):
  # do some actions
  lockManager.release('testLockName')
```
You can look at [batteries implementation](https://github.com/bakwc/PySyncObj/blob/master/pysyncobj/batteries.py), [examples](https://github.com/bakwc/PySyncObj/tree/master/examples) and [unit-tests](https://github.com/bakwc/PySyncObj/blob/master/test_syncobj.py) for more use-cases. Also there is an [API documentation](http://pysyncobj.readthedocs.io). Feel free to create proposals and/or pull requests with new batteries, features, etc. Join our [gitter chat](https://gitter.im/bakwc/PySyncObj) if you have any questions.


## Performance
![15K rps on 3 nodes; 14K rps on 7 nodes;](http://pastexen.com/i/Ge3lnrM1OY.png "RPS vs Cluster Size")
![22K rps on 10 byte requests; 5K rps on 20Kb requests;](http://pastexen.com/i/0RIsrKxJsV.png "RPS vs Request Size")

## Publications
- [Adventures in fault tolerant alerting with Python](https://blog.hostedgraphite.com/2017/05/05/adventures-in-fault-tolerant-alerting-with-python/)
- [Строим распределенную систему c PySyncObj](https://habrahabr.ru/company/wargaming/blog/301398/)
