[![PyPI version](https://badge.fury.io/py/motmetrics.svg)](https://badge.fury.io/py/motmetrics) [![Build Status](https://github.com/cheind/py-motmetrics/actions/workflows/python-package.yml/badge.svg)](https://github.com/cheind/py-motmetrics/actions/workflows/python-package.yml)

# py-motmetrics

The **py-motmetrics** library provides a Python implementation of metrics for benchmarking multiple object trackers (MOT).

While benchmarking single object trackers is rather straightforward, measuring the performance of multiple object trackers needs careful design as multiple correspondence constellations can arise (see image below). A variety of methods have been proposed in the past and while there is no general agreement on a single method, the methods of [[1,2,3,4]](#References) have received considerable attention in recent years. **py-motmetrics** implements these [metrics](#Metrics).

<div style="text-align:center;">

![](./motmetrics/etc/mot.png)<br/>

_Pictures courtesy of Bernardin, Keni, and Rainer Stiefelhagen [[1]](#References)_

</div>

In particular **py-motmetrics** supports `CLEAR-MOT`[[1,2]](#References) metrics and `ID`[[4]](#References) metrics. Both metrics attempt to find a minimum cost assignment between ground truth objects and predictions. However, while CLEAR-MOT solves the assignment problem on a local per-frame basis, `ID-MEASURE` solves the bipartite graph matching by finding the minimum cost of objects and predictions over all frames. This [blog-post](https://web.archive.org/web/20190413133409/http://vision.cs.duke.edu:80/DukeMTMC/IDmeasures.html) by Ergys illustrates the differences in more detail.

## Features at a glance

-   _Variety of metrics_ <br/>
    Provides MOTA, MOTP, track quality measures, global ID measures and more. The results are [comparable](#MOTChallengeCompatibility) with the popular [MOTChallenge][motchallenge] benchmarks [(\*1)](#asterixcompare).
-   _Distance agnostic_ <br/>
    Supports Euclidean, Intersection over Union and other distances measures.
-   _Complete event history_ <br/>
    Tracks all relevant per-frame events suchs as correspondences, misses, false alarms and switches.
-   _Flexible solver backend_ <br/>
    Support for switching minimum assignment cost solvers. Supports `scipy`, `ortools`, `munkres` out of the box. Auto-tunes solver selection based on [availability and problem size](#SolverBackends).
-   _Easy to extend_ <br/>
    Events and summaries are utilizing [pandas][pandas] for data structures and analysis. New metrics can reuse already computed values from depending metrics.

<a name="Metrics"></a>

## Metrics

**py-motmetrics** implements the following metrics. The metrics have been aligned with what is reported by [MOTChallenge][motchallenge] benchmarks.

```python
import motmetrics as mm
# List all default metrics
mh = mm.metrics.create()
print(mh.list_metrics_markdown())
```

| Name                 | Description                                                                        |
| :------------------- | :--------------------------------------------------------------------------------- |
| num_frames           | Total number of frames.                                                            |
| num_matches          | Total number matches.                                                              |
| num_switches         | Total number of track switches.                                                    |
| num_false_positives  | Total number of false positives (false-alarms).                                    |
| num_misses           | Total number of misses.                                                            |
| num_detections       | Total number of detected objects including matches and switches.                   |
| num_objects          | Total number of unique object appearances over all frames.                         |
| num_predictions      | Total number of unique prediction appearances over all frames.                     |
| num_unique_objects   | Total number of unique object ids encountered.                                     |
| mostly_tracked       | Number of objects tracked for at least 80 percent of lifespan.                     |
| partially_tracked    | Number of objects tracked between 20 and 80 percent of lifespan.                   |
| mostly_lost          | Number of objects tracked less than 20 percent of lifespan.                        |
| num_fragmentations   | Total number of switches from tracked to not tracked.                              |
| motp                 | Multiple object tracker precision.                                                 |
| mota                 | Multiple object tracker accuracy.                                                  |
| precision            | Number of detected objects over sum of detected and false positives.               |
| recall               | Number of detections over number of objects.                                       |
| idfp                 | ID measures: Number of false positive matches after global min-cost matching.      |
| idfn                 | ID measures: Number of false negatives matches after global min-cost matching.     |
| idtp                 | ID measures: Number of true positives matches after global min-cost matching.      |
| idp                  | ID measures: global min-cost precision.                                            |
| idr                  | ID measures: global min-cost recall.                                               |
| idf1                 | ID measures: global min-cost F1 score.                                             |
| obj_frequencies      | `pd.Series` Total number of occurrences of individual objects over all frames.     |
| pred_frequencies     | `pd.Series` Total number of occurrences of individual predictions over all frames. |
| track_ratios         | `pd.Series` Ratio of assigned to total appearance count per unique object id.      |
| id_global_assignment | `dict` ID measures: Global min-cost assignment for ID measures.                    |

<a name="MOTChallengeCompatibility"></a>

## MOTChallenge compatibility

**py-motmetrics** produces results compatible with popular [MOTChallenge][motchallenge] benchmarks [(\*1)](#asterixcompare). Below are two results taken from MOTChallenge [Matlab devkit][devkit] corresponding to the results of the CEM tracker on the training set of the 2015 MOT 2DMark.

```

TUD-Campus
 IDF1  IDP  IDR| Rcll  Prcn   FAR| GT  MT  PT  ML|   FP    FN  IDs   FM| MOTA  MOTP MOTAL
 55.8 73.0 45.1| 58.2  94.1  0.18|  8   1   6   1|   13   150    7    7| 52.6  72.3  54.3

TUD-Stadtmitte
 IDF1  IDP  IDR| Rcll  Prcn   FAR| GT  MT  PT  ML|   FP    FN  IDs   FM| MOTA  MOTP MOTAL
 64.5 82.0 53.1| 60.9  94.0  0.25| 10   5   4   1|   45   452    7    6| 56.4  65.4  56.9

```

In comparison to **py-motmetrics**

```
                IDF1   IDP   IDR  Rcll  Prcn GT MT PT ML FP  FN IDs  FM  MOTA  MOTP
TUD-Campus     55.8% 73.0% 45.1% 58.2% 94.1%  8  1  6  1 13 150   7   7 52.6% 0.277
TUD-Stadtmitte 64.5% 82.0% 53.1% 60.9% 94.0% 10  5  4  1 45 452   7   6 56.4% 0.346
```

<a name="asterixcompare"></a>(\*1) Besides naming conventions, the only obvious differences are

-   Metric `FAR` is missing. This metric is given implicitly and can be recovered by `FalsePos / Frames * 100`.
-   Metric `MOTP` seems to be off. To convert compute `(1 - MOTP) * 100`. [MOTChallenge][motchallenge] benchmarks compute `MOTP` as percentage, while **py-motmetrics** sticks to the original definition of average distance over number of assigned objects [[1]](#References).

You can compare tracker results to ground truth in MOTChallenge format by

```
python -m motmetrics.apps.eval_motchallenge --help
```

For MOT16/17, you can run

```
python -m motmetrics.apps.evaluateTracking --help
```

## Installation

To install latest development version of **py-motmetrics** (usually a bit more recent than PyPi below)

```
pip install git+https://github.com/cheind/py-motmetrics.git
```

### Install via PyPi

To install **py-motmetrics** use `pip`

```
pip install motmetrics
```

Python 3.5/3.6/3.9 and numpy, pandas and scipy is required. If no binary packages are available for your platform and building source packages fails, you might want to try a distribution like Conda (see below) to install dependencies.

Alternatively for developing, clone or fork this repository and install in editing mode.

```
pip install -e <path/to/setup.py>
```

### Install via Conda

In case you are using Conda, a simple way to run **py-motmetrics** is to create a virtual environment with all the necessary dependencies

```
conda env create -f environment.yml
> activate motmetrics-env
```

Then activate / source the `motmetrics-env` and install **py-motmetrics** and run the tests.

```
activate motmetrics-env
pip install .
pytest
```

In case you already have an environment you install the dependencies from within your environment by

```
conda install --file requirements.txt
pip install .
pytest
```

## Usage

### Populating the accumulator

```python
import motmetrics as mm
import numpy as np

# Create an accumulator that will be updated during each frame
acc = mm.MOTAccumulator(auto_id=True)

# Call update once for per frame. For now, assume distances between
# frame objects / hypotheses are given.
acc.update(
    [1, 2],                     # Ground truth objects in this frame
    [1, 2, 3],                  # Detector hypotheses in this frame
    [
        [0.1, np.nan, 0.3],     # Distances from object 1 to hypotheses 1, 2, 3
        [0.5,  0.2,   0.3]      # Distances from object 2 to hypotheses 1, 2, 3
    ]
)
```

The code above updates an event accumulator with data from a single frame. Here we assume that pairwise object / hypothesis distances have already been computed. Note `np.nan` inside the distance matrix. It signals that object `1` cannot be paired with hypothesis `2`. To inspect the current event history simple print the events associated with the accumulator.

```python
print(acc.events) # a pandas DataFrame containing all events

"""
                Type  OId HId    D
FrameId Event
0       0        RAW    1   1  0.1
        1        RAW    1   2  NaN
        2        RAW    1   3  0.3
        3        RAW    2   1  0.5
        4        RAW    2   2  0.2
        5        RAW    2   3  0.3
        6      MATCH    1   1  0.1
        7      MATCH    2   2  0.2
        8         FP  NaN   3  NaN
"""
```

The above data frame contains `RAW` and MOT events. To obtain just MOT events type

```python
print(acc.mot_events) # a pandas DataFrame containing MOT only events

"""
                Type  OId HId    D
FrameId Event
0       6      MATCH    1   1  0.1
        7      MATCH    2   2  0.2
        8         FP  NaN   3  NaN
"""
```

Meaning object `1` was matched to hypothesis `1` with distance 0.1. Similarily, object `2` was matched to hypothesis `2` with distance 0.2. Hypothesis `3` could not be matched to any remaining object and generated a false positive (FP). Possible assignments are computed by minimizing the total assignment distance (Kuhn-Munkres algorithm).

Continuing from above

```python
frameid = acc.update(
    [1, 2],
    [1],
    [
        [0.2],
        [0.4]
    ]
)
print(acc.mot_events.loc[frameid])

"""
        Type OId  HId    D
Event
2      MATCH   1    1  0.2
3       MISS   2  NaN  NaN
"""
```

While object `1` was matched, object `2` couldn't be matched because no hypotheses are left to pair with.

```python
frameid = acc.update(
    [1, 2],
    [1, 3],
    [
        [0.6, 0.2],
        [0.1, 0.6]
    ]
)
print(acc.mot_events.loc[frameid])

"""
         Type OId HId    D
Event
4       MATCH   1   1  0.6
5      SWITCH   2   3  0.6
"""
```

Object `2` is now tracked by hypothesis `3` leading to a track switch. Note, although a pairing `(1, 3)` with cost less than 0.6 is possible, the algorithm prefers prefers to continue track assignments from past frames which is a property of MOT metrics.

### Computing metrics

Once the accumulator has been populated you can compute and display metrics. Continuing the example from above

```python
mh = mm.metrics.create()
summary = mh.compute(acc, metrics=['num_frames', 'mota', 'motp'], name='acc')
print(summary)

"""
     num_frames  mota  motp
acc           3   0.5  0.34
"""
```

Computing metrics for multiple accumulators or accumulator views is also possible

```python
summary = mh.compute_many(
    [acc, acc.events.loc[0:1]],
    metrics=['num_frames', 'mota', 'motp'],
    names=['full', 'part'])
print(summary)

"""
      num_frames  mota      motp
full           3   0.5  0.340000
part           2   0.5  0.166667
"""
```

Finally, you may want to reformat column names and how column values are displayed.

```python
strsummary = mm.io.render_summary(
    summary,
    formatters={'mota' : '{:.2%}'.format},
    namemap={'mota': 'MOTA', 'motp' : 'MOTP'}
)
print(strsummary)

"""
      num_frames   MOTA      MOTP
full           3 50.00%  0.340000
part           2 50.00%  0.166667
"""
```

For MOTChallenge **py-motmetrics** provides predefined metric selectors, formatters and metric names, so that the result looks alike what is provided via their Matlab `devkit`.

```python
summary = mh.compute_many(
    [acc, acc.events.loc[0:1]],
    metrics=mm.metrics.motchallenge_metrics,
    names=['full', 'part'])

strsummary = mm.io.render_summary(
    summary,
    formatters=mh.formatters,
    namemap=mm.io.motchallenge_metric_names
)
print(strsummary)

"""
      IDF1   IDP   IDR  Rcll  Prcn GT MT PT ML FP FN IDs  FM  MOTA  MOTP
full 83.3% 83.3% 83.3% 83.3% 83.3%  2  1  1  0  1  1   1   1 50.0% 0.340
part 75.0% 75.0% 75.0% 75.0% 75.0%  2  1  1  0  1  1   0   0 50.0% 0.167
"""
```

In order to generate an overall summary that computes the metrics jointly over all accumulators add `generate_overall=True` as follows

```python
summary = mh.compute_many(
    [acc, acc.events.loc[0:1]],
    metrics=mm.metrics.motchallenge_metrics,
    names=['full', 'part'],
    generate_overall=True
    )

strsummary = mm.io.render_summary(
    summary,
    formatters=mh.formatters,
    namemap=mm.io.motchallenge_metric_names
)
print(strsummary)

"""
         IDF1   IDP   IDR  Rcll  Prcn GT MT PT ML FP FN IDs  FM  MOTA  MOTP
full    83.3% 83.3% 83.3% 83.3% 83.3%  2  1  1  0  1  1   1   1 50.0% 0.340
part    75.0% 75.0% 75.0% 75.0% 75.0%  2  1  1  0  1  1   0   0 50.0% 0.167
OVERALL 80.0% 80.0% 80.0% 80.0% 80.0%  4  2  2  0  2  2   1   1 50.0% 0.275
"""
```

### Computing distances

Up until this point we assumed the pairwise object/hypothesis distances to be known. Usually this is not the case. You are mostly given either rectangles or points (centroids) of related objects. To compute a distance matrix from them you can use `motmetrics.distance` module as shown below.

#### Euclidean norm squared on points

```python
# Object related points
o = np.array([
    [1., 2],
    [2., 2],
    [3., 2],
])

# Hypothesis related points
h = np.array([
    [0., 0],
    [1., 1],
])

C = mm.distances.norm2squared_matrix(o, h, max_d2=5.)

"""
[[  5.   1.]
 [ nan   2.]
 [ nan   5.]]
"""
```

#### Intersection over union norm for 2D rectangles

```python
a = np.array([
    [0, 0, 1, 2],    # Format X, Y, Width, Height
    [0, 0, 0.8, 1.5],
])

b = np.array([
    [0, 0, 1, 2],
    [0, 0, 1, 1],
    [0.1, 0.2, 2, 2],
])
mm.distances.iou_matrix(a, b, max_iou=0.5)

"""
[[ 0.          0.5                nan]
 [ 0.4         0.42857143         nan]]
"""
```

<a name="SolverBackends"></a>

### Solver backends

For large datasets solving the minimum cost assignment becomes the dominant runtime part. **py-motmetrics** therefore supports these solvers out of the box

-   `lapsolver` - https://github.com/cheind/py-lapsolver
-   `lapjv` - https://github.com/gatagat/lap
-   `scipy` - https://github.com/scipy/scipy/tree/master/scipy
-   `ortools<9.4` - https://github.com/google/or-tools
-   `munkres` - http://software.clapper.org/munkres/

A comparison for different sized matrices is shown below (taken from [here](https://github.com/cheind/py-lapsolver#benchmarks))

Please note that the x-axis is scaled logarithmically. Missing bars indicate excessive runtime or errors in returned result.
![](https://github.com/cheind/py-lapsolver/raw/master/lapsolver/etc/benchmark-dtype-numpy.float32.png)

By default **py-motmetrics** will try to find a LAP solver in the order of the list above. In order to temporarly replace the default solver use

```python
costs = ...
mysolver = lambda x: ... # solver code that returns pairings

with lap.set_default_solver(mysolver):
    ...
```

## Running tests

**py-motmetrics** uses the pytest framework. To run the tests, simply `cd` into the source directly and run `pytest`.

<a name="References"></a>

### References

1. Bernardin, Keni, and Rainer Stiefelhagen. "Evaluating multiple object tracking performance: the CLEAR MOT metrics."
   EURASIP Journal on Image and Video Processing 2008.1 (2008): 1-10.
2. Milan, Anton, et al. "Mot16: A benchmark for multi-object tracking." arXiv preprint arXiv:1603.00831 (2016).
3. Li, Yuan, Chang Huang, and Ram Nevatia. "Learning to associate: Hybridboosted multi-target tracker for crowded scene."
   Computer Vision and Pattern Recognition, 2009. CVPR 2009. IEEE Conference on. IEEE, 2009.
4. Performance Measures and a Data Set for Multi-Target, Multi-Camera Tracking. E. Ristani, F. Solera, R. S. Zou, R. Cucchiara and C. Tomasi. ECCV 2016 Workshop on Benchmarking Multi-Target Tracking.

## Docker

### Update ground truth and test data:

/data/train directory should contain MOT 2D 2015 Ground Truth files.
/data/test directory should contain your results.

You can check usage and directory listing at
https://github.com/cheind/py-motmetrics/blob/master/motmetrics/apps/eval_motchallenge.py

### Build Image

docker build -t desired-image-name -f Dockerfile .

### Run Image

docker run desired-image-name

(credits to [christosavg](https://github.com/christosavg))

## License

```
MIT License

Copyright (c) 2017-2022 Christoph Heindl
Copyright (c) 2018 Toka
Copyright (c) 2019-2022 Jack Valmadre

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

[pandas]: http://pandas.pydata.org/
[motchallenge]: https://motchallenge.net/
[devkit]: https://motchallenge.net/devkit/
