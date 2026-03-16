<h1 align="center">
  <div>
    <img src="logo.svg" width="300px" height="300px" alt="orderedmultidict">
  </div>
  orderedmultidict
</h1>

<p align="center">
  <a href="https://pypi.python.org/pypi/orderedmultidict"><img src="https://badge.fury.io/py/orderedmultidict.svg"></a>
  <a href="https://travis-ci.org/gruns/orderedmultidict"><img src="https://api.travis-ci.org/gruns/orderedmultidict.svg"></a>
  <a href="http://unlicense.org/"><img src="https://img.shields.io/pypi/l/orderedmultidict.svg"></a>
  <a href="https://pypi.python.org/pypi/orderedmultidict"><img src="https://img.shields.io/pypi/pyversions/orderedmultidict.svg"></a>
</p>

### omdict is an ordered multivalue dictionary that retains method parity with Python's [dict](http://docs.python.org/library/stdtypes.html#dict) and helps power [furl](https://github.com/gruns/furl).

A multivalue dictionary is a dictionary that can store multiple values
per key. An ordered multivalue dictionary is a multivalue dictionary
that retains the order of insertions and deletions.

orderedmultidict is well tested, [Unlicensed](http://unlicense.org/) in
the public domain, supports Python 2, Python 3, PyPy2, and PyPy3.

Code time: omdict can store multiple values per key.

```python
>>> from orderedmultidict import omdict
>>> omd = omdict()
>>> omd[1] = 1
>>> omd[1]
1
>>> omd.add(1, 11)
>>> omd.getlist(1)
[1, 11]
>>> omd.addlist(1, [111, 1111])
>>> omd.getlist(1)
[1, 11, 111, 1111]
>>> omd.allitems()
[(1, 1), (1, 11), (1, 111), (1, 1111)]
```

omdict retains insertion and deletion order.

```python
>>> omd = omdict()
>>> omd[2] = 2
>>> omd[1] = 1
>>> omd.items()
[(2, 2), (1, 1)]
>>> omd[2] = 'sup'
>>> omd.items()
[(2, 'sup'), (1, 1)]
```

Method parity with dict is retained; omdict can be a drop-in replacement.

```python
>>> d, omd = dict(), omdict()
>>> d.update([(1,1), (1,11), (2,2), (2,22)])
>>> omd.update([(1,1), (1,11), (2,2), (2,22)])
>>> d[1], omd[1]
(11, 11)
>>> d[3] = 3
>>> omd[3] = 3
>>> d.get(3), omd.get(3)
(3, 3)
>>> d.items() == omd.items()
True
```


### API

See all of omdict's methods, with examples, in omdict's API document,
[API.md](https://github.com/gruns/orderedmultidict/blob/master/API.md).


### Installation

Installing orderedmultidict with pip is easy.

```
$ pip install orderedmultidict
```
