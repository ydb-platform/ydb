# pyfst

Python interface to [OpenFst](http://openfst.org)

Documentation: http://pyfst.github.io

## Installation

1. Install OpenFst 1.3
2. `pip install pyfst` (do not try installing directly from the git repository)

## Basic Usage

```python
import fst

t = fst.Transducer()

t.add_arc(0, 1, 'a', 'A', 0.5)
t.add_arc(0, 1, 'b', 'B', 1.5)
t.add_arc(1, 2, 'c', 'C', 2.5)

t[2].final = 3.5

t.shortest_path() # 2 -(a:A/0.5)-> 1 -(c:C/2.5)-> 0/3.5 
```

The pyfst API is [IPython notebook](http://ipython.org/ipython-doc/dev/interactive/htmlnotebook.html)-friendly: the transducers objects are [automatically drawn](http://nbviewer.ipython.org/3835477/) using [Graphviz](http://www.graphviz.org).

## Development

See [the wiki](https://github.com/vchahun/pyfst/wiki/Contributing) to learn about how to install pyfst from the Cython source.
