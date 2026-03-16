Implicit
=======

[![Build Status](https://travis-ci.org/benfred/implicit.svg?branch=master)](https://travis-ci.org/benfred/implicit)
[![Windows Build Status](https://ci.appveyor.com/api/projects/status/9kfbvx5i6dc48yr0?svg=true)](https://ci.appveyor.com/project/benfred/implicit)

Fast Python Collaborative Filtering for Implicit Datasets.

This project provides fast Python implementations of several different popular recommendation algorithms for
implicit feedback datasets:

 * Alternating Least Squares as described in the papers [Collaborative Filtering for Implicit Feedback Datasets](http://yifanhu.net/PUB/cf.pdf) and [Applications of the Conjugate Gradient Method for Implicit
Feedback Collaborative Filtering](https://pdfs.semanticscholar.org/bfdf/7af6cf7fd7bb5e6b6db5bbd91be11597eaf0.pdf).

 * [Bayesian Personalized Ranking](https://arxiv.org/pdf/1205.2618.pdf).

 * [Logistic Matrix Factorization](https://web.stanford.edu/~rezab/nips2014workshop/submits/logmat.pdf)

 * Item-Item Nearest Neighbour models using Cosine, TFIDF or BM25 as a distance metric.

All models have multi-threaded training routines, using Cython and OpenMP to fit the models in
parallel among all available CPU cores.  In addition, the ALS and BPR models both have custom CUDA
kernels - enabling fitting on compatible GPU's. Approximate nearest neighbours libraries such as [Annoy](https://github.com/spotify/annoy), [NMSLIB](https://github.com/searchivarius/nmslib)
and [Faiss](https://github.com/facebookresearch/faiss) can also be used by Implicit to [speed up
making recommendations](https://www.benfrederickson.com/approximate-nearest-neighbours-for-recommender-systems/).

To install:

```
pip install implicit
```

Basic usage:

```python
import implicit

# initialize a model
model = implicit.als.AlternatingLeastSquares(factors=50)

# train the model on a sparse matrix of item/user/confidence weights
model.fit(item_user_data)

# recommend items for a user
user_items = item_user_data.T.tocsr()
recommendations = model.recommend(userid, user_items)

# find related items
related = model.similar_items(itemid)
```

The examples folder has a program showing how to use this to [compute similar artists on the
last.fm dataset](https://github.com/benfred/implicit/blob/master/examples/lastfm.py).

For more information see the [documentation](http://implicit.readthedocs.io/).

#### Articles about Implicit

These blog posts describe the algorithms that power this library:

 * [Finding Similar Music with Matrix Factorization](https://www.benfrederickson.com/matrix-factorization/)
 * [Faster Implicit Matrix Factorization](https://www.benfrederickson.com/fast-implicit-matrix-factorization/)
 * [Implicit Matrix Factorization on the GPU](https://www.benfrederickson.com/implicit-matrix-factorization-on-the-gpu/)
 * [Approximate Nearest Neighbours for Recommender Systems](https://www.benfrederickson.com/approximate-nearest-neighbours-for-recommender-systems/)
 * [Distance Metrics for Fun and Profit](https://www.benfrederickson.com/distance-metrics/)

There are also several other blog posts about using Implicit to build recommendation systems:

 * [Recommending GitHub Repositories with Google BigQuery and the implicit library](https://medium.com/@jbochi/recommending-github-repositories-with-google-bigquery-and-the-implicit-library-e6cce666c77)
 * [Intro to Implicit Matrix Factorization: Classic ALS with Sketchfab Models](http://blog.ethanrosenthal.com/2016/10/19/implicit-mf-part-1/)
 * [A Gentle Introduction to Recommender Systems with Implicit Feedback](https://jessesw.com/Rec-System/).


#### Requirements

This library requires SciPy version 0.16 or later. Running on OSX requires an OpenMP compiler,
which can be installed with homebrew: ```brew install gcc```. Running on Windows requires Python
3.5+.

GPU Support requires at least version 9 of the [NVidia CUDA Toolkit](https://developer.nvidia.com/cuda-downloads). The build will use the ```nvcc``` compiler
that is found on the path, but this can be overriden by setting the CUDAHOME enviroment variable
to point to your cuda installation.

This library has been tested with Python 2.7, 3.5, 3.6 and 3.7 on Ubuntu and OSX, and tested with
Python 3.5 and 3.6 on Windows.

#### Benchmarks

Simple benchmarks comparing the ALS fitting time versus [Spark and QMF can be found here](https://github.com/benfred/implicit/tree/master/benchmarks).

#### Optimal Configuration

I'd recommend configuring SciPy to use Intel's MKL matrix libraries. One easy way of doing this is by installing the Anaconda Python distribution.

For systems using OpenBLAS, I highly recommend setting 'export OPENBLAS_NUM_THREADS=1'. This
disables its internal multithreading ability, which leads to substantial speedups for this
package. Likewise for Intel MKL, setting 'export MKL_NUM_THREADS=1' should also be set.

Released under the MIT License
