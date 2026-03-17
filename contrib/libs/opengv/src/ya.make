LIBRARY()

VERSION(2018-10-25-306a54e6)

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/libs/eigen
)

ADDINCL(
    contrib/libs/eigen
    contrib/libs/eigen/unsupported
    contrib/libs/opengv/include
)

NO_COMPILER_WARNINGS()

NO_SANITIZE()

NO_UTIL()

IF (BT_RELWITHDEBINFO)
    # This library builds painfully slow in relwithdebinfo mode, so promote to release
    CFLAGS(-DNDEBUG)
ENDIF()

SRCS(
    absolute_pose/CentralAbsoluteAdapter.cpp
    absolute_pose/MACentralAbsolute.cpp
    absolute_pose/MANoncentralAbsolute.cpp
    absolute_pose/NoncentralAbsoluteAdapter.cpp
    absolute_pose/NoncentralAbsoluteMultiAdapter.cpp
    absolute_pose/methods.cpp
    absolute_pose/modules/Epnp.cpp
    absolute_pose/modules/gp3p/code.cpp
    absolute_pose/modules/gp3p/init.cpp
    absolute_pose/modules/gp3p/reductors.cpp
    absolute_pose/modules/gp3p/spolynomials.cpp
    absolute_pose/modules/gpnp1/code.cpp
    absolute_pose/modules/gpnp1/init.cpp
    absolute_pose/modules/gpnp1/reductors.cpp
    absolute_pose/modules/gpnp1/spolynomials.cpp
    absolute_pose/modules/gpnp2/code.cpp
    absolute_pose/modules/gpnp2/init.cpp
    absolute_pose/modules/gpnp2/reductors.cpp
    absolute_pose/modules/gpnp2/spolynomials.cpp
    absolute_pose/modules/gpnp3/code.cpp
    absolute_pose/modules/gpnp3/init.cpp
    absolute_pose/modules/gpnp3/reductors.cpp
    absolute_pose/modules/gpnp3/spolynomials.cpp
    absolute_pose/modules/gpnp4/code.cpp
    absolute_pose/modules/gpnp4/init.cpp
    absolute_pose/modules/gpnp4/reductors.cpp
    absolute_pose/modules/gpnp4/spolynomials.cpp
    absolute_pose/modules/gpnp5/code.cpp
    absolute_pose/modules/gpnp5/init.cpp
    absolute_pose/modules/gpnp5/reductors.cpp
    absolute_pose/modules/gpnp5/spolynomials.cpp
    absolute_pose/modules/main.cpp
    absolute_pose/modules/upnp2.cpp
    absolute_pose/modules/upnp4.cpp
    math/Sturm.cpp
    math/arun.cpp
    math/cayley.cpp
    math/gauss_jordan.cpp
    math/quaternion.cpp
    math/roots.cpp
    point_cloud/MAPointCloud.cpp
    point_cloud/PointCloudAdapter.cpp
    point_cloud/methods.cpp
    relative_pose/CentralRelativeAdapter.cpp
    relative_pose/CentralRelativeMultiAdapter.cpp
    relative_pose/CentralRelativeWeightingAdapter.cpp
    relative_pose/MACentralRelative.cpp
    relative_pose/MANoncentralRelative.cpp
    relative_pose/MANoncentralRelativeMulti.cpp
    relative_pose/NoncentralRelativeAdapter.cpp
    relative_pose/NoncentralRelativeMultiAdapter.cpp
    relative_pose/methods.cpp
    relative_pose/modules/eigensolver/modules.cpp
    relative_pose/modules/fivept_kneip/code.cpp
    relative_pose/modules/fivept_kneip/init.cpp
    relative_pose/modules/fivept_kneip/reductors.cpp
    relative_pose/modules/fivept_kneip/spolynomials.cpp
    relative_pose/modules/fivept_nister/modules.cpp
    relative_pose/modules/fivept_stewenius/modules.cpp
    relative_pose/modules/ge/modules.cpp
    relative_pose/modules/main.cpp
    relative_pose/modules/sixpt/modules2.cpp
    sac_problems/absolute_pose/AbsolutePoseSacProblem.cpp
    sac_problems/absolute_pose/MultiNoncentralAbsolutePoseSacProblem.cpp
    sac_problems/point_cloud/PointCloudSacProblem.cpp
    sac_problems/relative_pose/CentralRelativePoseSacProblem.cpp
    sac_problems/relative_pose/EigensolverSacProblem.cpp
    sac_problems/relative_pose/MultiCentralRelativePoseSacProblem.cpp
    sac_problems/relative_pose/MultiNoncentralRelativePoseSacProblem.cpp
    sac_problems/relative_pose/NoncentralRelativePoseSacProblem.cpp
    sac_problems/relative_pose/RotationOnlySacProblem.cpp
    sac_problems/relative_pose/TranslationOnlySacProblem.cpp
    triangulation/methods.cpp
)

END()
