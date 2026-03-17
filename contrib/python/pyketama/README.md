pyketama
========
![build](https://img.shields.io/travis/maralla/pyketama.svg)

pyketama is a cython implementation of ketama consistent hashing algorithm.


Install
=======

```bash
pip install pyketama
```


Usage
=====

```python
import ketama

data = ["127.0.0.1:12211", "127.0.0.1:12212"]
continuum = ketama.Continuum(data)
print(continuum['test'])


# with weight, (key, weith)
data = [("127.0.0.1:12211", 400), ("127.0.0.1:12212", 500)]
continuum = ketama.Continuum(data)
print(continuum['test'])


# with value, (key, value, weight)
data = [("node1", "127.0.0.1:12211", 400), ("node2", "127.0.0.1:12212", 500)]
continuum = ketama.Continuum(data)
print(continuum['test'])


# set item like dict
continuum = ketama.Continuum()
continuum['tom'] = 123
continuum['jerry'] = "hello world"
print(continuum['test'])

# with weight (key, weight)
continuum = ketama.Continuum()
continuum[('tom', 100)] = 123
continuum[('jerry', 300)] = "hello world"
print(continuum['test'])
```
