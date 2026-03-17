OpenSfM [![Build Status](https://travis-ci.org/mapillary/OpenSfM.svg?branch=master)](https://travis-ci.org/mapillary/OpenSfM)
=======

## Overview
OpenSfM is a Structure from Motion library written in Python. The library serves as a processing pipeline for reconstructing camera poses and 3D scenes from multiple images. It consists of basic modules for Structure from Motion (feature detection/matching, minimal solvers) with a focus on building a robust and scalable reconstruction pipeline. It also integrates external sensor (e.g. GPS, accelerometer) measurements for geographical alignment and robustness. A JavaScript viewer is provided to preview the models and debug the pipeline.

<p align="center">
  <img src="https://docs.opensfm.org/_images/berlin_viewer.jpg" />
</p>

Checkout this [blog post with more demos](http://blog.mapillary.com/update/2014/12/15/sfm-preview.html)


## Getting Started

* [Building the library][]
* [Running a reconstruction][]
* [Documentation][]


[Building the library]: https://docs.opensfm.org/building.html (OpenSfM building instructions)
[Running a reconstruction]: https://docs.opensfm.org/using.html (OpenSfM usage)
[Documentation]: https://docs.opensfm.org  (OpenSfM documentation)
