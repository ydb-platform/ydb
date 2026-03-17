PY3_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(0.3.0)

ORIGINAL_SOURCE(https://github.com/mapillary/OpenSfM/archive/v0.3.0.tar.gz)

PEERDIR(
    contrib/libs/ceres-solver
    contrib/libs/eigen
    contrib/libs/gflags
    contrib/libs/glog
    contrib/libs/opencv
    contrib/libs/opencv/modules/python/src2
    contrib/libs/pybind11
    contrib/libs/vlfeat
    contrib/libs/opengv/python
    contrib/python/ExifRead
    contrib/python/python-dateutil
    contrib/python/gpxpy
    contrib/python/networkx
    contrib/python/numpy
    contrib/python/pyproj
    contrib/python/Pillow
    contrib/python/PyYAML
    contrib/python/repoze.lru
    contrib/python/scikit-learn
    contrib/python/scipy
    contrib/python/six
    contrib/python/xmltodict
    library/python/resource
    library/cpp/vl_feat
)

ADDINCL(
    contrib/libs/ceres-solver/include
    contrib/libs/eigen
    contrib/libs/opencv/include
    contrib/libs/opencv/modules/python/src2
    contrib/libs/vlfeat
    contrib/python/opensfm/opensfm/src
    contrib/python/opensfm/opensfm/src/third_party/akaze/lib
)

NO_COMPILER_WARNINGS()

NO_LINT()

SRCS(
    opensfm/src/akaze_bind.cc
    opensfm/src/bundle/src/bundle_adjuster.cc
    opensfm/src/csfm.cc
    opensfm/src/depthmap.cc
    # opensfm/src/hahog.cc  # note: this two files is included in csfm.cc directly
    opensfm/src/matching.cc
    # opensfm/src/multiview.cc  # note: this two files is included in csfm.cc directly
    opensfm/src/third_party/akaze/lib/AKAZE.cpp
    opensfm/src/third_party/akaze/lib/fed.cpp
    opensfm/src/third_party/akaze/lib/nldiffusion_functions.cpp
    opensfm/src/third_party/akaze/lib/utils.cpp
    opensfm/src/types.cc
)

PY_REGISTER(
    opensfm.csfm
)

RESOURCE(
    contrib/python/opensfm/opensfm/data/bow/bow_hahog_root_uchar_10000.npz bow_hahog_10000
    contrib/python/opensfm/opensfm/data/bow/bow_hahog_root_uchar_64.npz bow_hahog_64
    contrib/python/opensfm/opensfm/data/sensor_data.json sensor_data
    contrib/python/opensfm/opensfm/data/sensor_data_detailed.json sensor_data_detailed
)

PY_SRCS(
    TOP_LEVEL
    opensfm/__init__.py
    opensfm/align.py
    opensfm/bow.py
    opensfm/commands/__init__.py
    opensfm/commands/align_submodels.py
    opensfm/commands/bundle.py
    opensfm/commands/compute_depthmaps.py
    opensfm/commands/create_submodels.py
    opensfm/commands/create_tracks.py
    opensfm/commands/detect_features.py
    opensfm/commands/export_geocoords.py
    opensfm/commands/export_openmvs.py
    opensfm/commands/export_ply.py
    opensfm/commands/export_visualsfm.py
    opensfm/commands/extract_metadata.py
    opensfm/commands/match_features.py
    opensfm/commands/mesh.py
    opensfm/commands/reconstruct.py
    opensfm/commands/undistort.py
    opensfm/config.py
    opensfm/context.py
    opensfm/dataset.py
    opensfm/dense.py
    opensfm/exif.py
    opensfm/feature_loader.py
    opensfm/feature_loading.py
    opensfm/features.py
    opensfm/geo.py
    opensfm/geometry.py
    opensfm/geotag_from_gpx.py
    opensfm/io.py
    opensfm/large/__init__.py
    opensfm/large/metadataset.py
    opensfm/large/tools.py
    opensfm/log.py
    opensfm/matching.py
    opensfm/mesh.py
    opensfm/multiview.py
    opensfm/pairs_selection.py
    opensfm/reconstruction.py
    opensfm/sensors.py
    opensfm/tracking.py
    opensfm/transformations.py
    opensfm/types.py
    opensfm/unionfind.py
    opensfm/upright.py
    opensfm/video.py
    opensfm/vlad.py
)

RESOURCE_FILES(
    PREFIX contrib/python/opensfm/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()

RECURSE(
    bin
)
