The <b>Stan Math Library</b> is a C++, reverse-mode automatic differentiation library designed to be usable, extensive and extensible, efficient, scalable, stable, portable, and redistributable in order to facilitate the construction and utilization of algorithms that utilize derivatives.

[![DOI](https://zenodo.org/badge/38388440.svg)](https://zenodo.org/badge/latestdoi/38388440)

Licensing
---------
The Stan Math Library is licensed under the [new BSD license](LICENSE.md).

Required Libraries
------------------
Stan Math depends on three libraries:

- Boost (version 1.69.0): [Boost Home Page](http://www.boost.org)
- Eigen (version 3.3.3): [Eigen Home Page](http://eigen.tuxfamily.org/index.php?title=Main_Page)
- SUNDIALS (version 4.1.0): [Sundials Home Page](http://computation.llnl.gov/projects/sundials/sundials-software)

These are distributed under the `lib/` subdirectory. Only these versions of the dependent libraries have been tested with Stan Math.

Installation
------------
The Stan Math Library is largely a header-only C++ library, with
exceptions for the Sundials code.

A simple hello world program using Stan Math is as follows:

```
#include <stan/math.hpp>
#include <iostream>

int main() {
  std::cout << "log normal(1 | 2, 3)="
            << stan::math::normal_log(1, 2, 3)
            << std::endl;
}
```

If this is in the file `/path/to/foo/foo.cpp`, then you can compile and run this with something like this, with the `path/to` business replaced with actual paths:

```
> cd /path/to/foo
> clang++ -std=c++1y -I /path/to/stan-math -I /path/to/Eigen -I /path/to/boost -I /path/to/sundials foo.cpp
> ./a.out
log normal(1 | 2, 3)=-2.07311
```

The `-I` includes provide paths pointing to the four necessary includes:

* Stan Math Library:  path to source directory that contains `stan` as a subdirectory
* Eigen C++ Matrix Library:  path to source directory that contains `Eigen` as a subdirectory
* Boost C++ Library:  path to source directory that contains `boost` as a subdirectory
* SUNDIALS: path to source directory that contains `cvodes` and `idas` as a subdirectory

Note that the paths should *not* include the final directories `stan`, `Eigen`, or `boost` on the paths.  An example of a real instantiation:

```
clang++ -std=c++1y -I ~/stan-dev/math -I ~/stan-dev/math/lib/eigen_3.3.3/ -I ~/stan-dev/math/lib/boost_1.69.0/ -I ~/stan-dev/math/lib/sundials_4.1.0/include foo.cpp
```

The following directories all exist below the links given to `-I`: `~/stan-dev/math/stan` and `~/stan-dev/math/lib/eigen_3.3.3/Eigen` and `~stan-dev/math/lib/boost_1.69.0/boost` and `~stan-dev/math/lib/sundials_4.1.0/include`.

Other Compilers
---------------
There's nothing special about `clang++` --- the `g++` compiler behaves the same way.  You'll need to modify the commands for other compilers, which will need to be up-to-date enough to compile the Stan Math Library.
