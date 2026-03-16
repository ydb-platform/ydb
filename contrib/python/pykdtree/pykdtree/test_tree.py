import numpy as np
import pytest

from pykdtree.kdtree import KDTree

data_pts_real = np.array([[  790535.062,  -369324.656,  6310963.5  ],
       [  790024.312,  -365155.688,  6311270.   ],
       [  789515.75 ,  -361009.469,  6311572.   ],
       [  789011.   ,  -356886.562,  6311869.5  ],
       [  788508.438,  -352785.969,  6312163.   ],
       [  788007.25 ,  -348707.219,  6312452.   ],
       [  787509.188,  -344650.875,  6312737.   ],
       [  787014.438,  -340616.906,  6313018.   ],
       [  786520.312,  -336604.156,  6313294.5  ],
       [  786030.312,  -332613.844,  6313567.   ],
       [  785541.562,  -328644.375,  6313835.5  ],
       [  785054.75 ,  -324696.031,  6314100.5  ],
       [  784571.188,  -320769.5  ,  6314361.5  ],
       [  784089.312,  -316863.562,  6314618.5  ],
       [  783610.562,  -312978.719,  6314871.5  ],
       [  783133.   ,  -309114.312,  6315121.   ],
       [  782658.25 ,  -305270.531,  6315367.   ],
       [  782184.312,  -301446.719,  6315609.   ],
       [  781715.062,  -297643.844,  6315847.5  ],
       [  781246.188,  -293860.281,  6316083.   ],
       [  780780.125,  -290096.938,  6316314.5  ],
       [  780316.312,  -286353.469,  6316542.5  ],
       [  779855.625,  -282629.75 ,  6316767.5  ],
       [  779394.75 ,  -278924.781,  6316988.5  ],
       [  778937.312,  -275239.625,  6317206.5  ],
       [  778489.812,  -271638.094,  6317418.   ],
       [  778044.688,  -268050.562,  6317626.   ],
       [  777599.688,  -264476.75 ,  6317831.5  ],
       [  777157.625,  -260916.859,  6318034.   ],
       [  776716.688,  -257371.125,  6318233.5  ],
       [  776276.812,  -253838.891,  6318430.5  ],
       [  775838.125,  -250320.266,  6318624.5  ],
       [  775400.75 ,  -246815.516,  6318816.5  ],
       [  774965.312,  -243324.953,  6319005.   ],
       [  774532.062,  -239848.25 ,  6319191.   ],
       [  774100.25 ,  -236385.516,  6319374.5  ],
       [  773667.875,  -232936.016,  6319555.5  ],
       [  773238.562,  -229500.812,  6319734.   ],
       [  772810.938,  -226079.562,  6319909.5  ],
       [  772385.25 ,  -222672.219,  6320082.5  ],
       [  771960.   ,  -219278.5  ,  6320253.   ],
       [  771535.938,  -215898.609,  6320421.   ],
       [  771114.   ,  -212532.625,  6320587.   ],
       [  770695.   ,  -209180.859,  6320749.5  ],
       [  770275.25 ,  -205842.562,  6320910.5  ],
       [  769857.188,  -202518.125,  6321068.5  ],
       [  769442.312,  -199207.844,  6321224.5  ],
       [  769027.812,  -195911.203,  6321378.   ],
       [  768615.938,  -192628.859,  6321529.   ],
       [  768204.688,  -189359.969,  6321677.5  ],
       [  767794.062,  -186104.844,  6321824.   ],
       [  767386.25 ,  -182864.016,  6321968.5  ],
       [  766980.062,  -179636.969,  6322110.   ],
       [  766575.625,  -176423.75 ,  6322249.5  ],
       [  766170.688,  -173224.172,  6322387.   ],
       [  765769.812,  -170038.984,  6322522.5  ],
       [  765369.5  ,  -166867.312,  6322655.   ],
       [  764970.562,  -163709.594,  6322786.   ],
       [  764573.   ,  -160565.781,  6322914.5  ],
       [  764177.75 ,  -157435.938,  6323041.   ],
       [  763784.188,  -154320.062,  6323165.5  ],
       [  763392.375,  -151218.047,  6323288.   ],
       [  763000.938,  -148129.734,  6323408.   ],
       [  762610.812,  -145055.344,  6323526.5  ],
       [  762224.188,  -141995.141,  6323642.5  ],
       [  761847.188,  -139025.734,  6323754.   ],
       [  761472.375,  -136066.312,  6323863.5  ],
       [  761098.125,  -133116.859,  6323971.5  ],
       [  760725.25 ,  -130177.484,  6324077.5  ],
       [  760354.   ,  -127247.984,  6324181.5  ],
       [  759982.812,  -124328.336,  6324284.5  ],
       [  759614.   ,  -121418.844,  6324385.   ],
       [  759244.688,  -118519.102,  6324484.5  ],
       [  758877.125,  -115629.305,  6324582.   ],
       [  758511.562,  -112749.648,  6324677.5  ],
       [  758145.625,  -109879.82 ,  6324772.5  ],
       [  757781.688,  -107019.953,  6324865.   ],
       [  757418.438,  -104170.047,  6324956.   ],
       [  757056.562,  -101330.125,  6325045.5  ],
       [  756697.   ,   -98500.266,  6325133.5  ],
       [  756337.375,   -95680.289,  6325219.5  ],
       [  755978.062,   -92870.148,  6325304.5  ],
       [  755621.188,   -90070.109,  6325387.5  ],
       [  755264.625,   -87280.008,  6325469.   ],
       [  754909.188,   -84499.828,  6325549.   ],
       [  754555.062,   -81729.609,  6325628.   ],
       [  754202.938,   -78969.43 ,  6325705.   ],
       [  753850.688,   -76219.133,  6325781.   ],
       [  753499.875,   -73478.836,  6325855.   ],
       [  753151.375,   -70748.578,  6325927.5  ],
       [  752802.312,   -68028.188,  6325999.   ],
       [  752455.75 ,   -65317.871,  6326068.5  ],
       [  752108.625,   -62617.344,  6326137.5  ],
       [  751764.125,   -59926.969,  6326204.5  ],
       [  751420.125,   -57246.434,  6326270.   ],
       [  751077.438,   -54575.902,  6326334.5  ],
       [  750735.312,   -51915.363,  6326397.5  ],
       [  750396.188,   -49264.852,  6326458.5  ],
       [  750056.375,   -46624.227,  6326519.   ],
       [  749718.875,   -43993.633,  6326578.   ]])

def test1d():

    data_pts = np.arange(1000)[..., None]
    kdtree = KDTree(data_pts, leafsize=15)
    query_pts = np.arange(400, 300, -10)[..., None]
    dist, idx = kdtree.query(query_pts)
    assert idx[0] == 400
    assert dist[0] == 0
    assert idx[1] == 390

def test3d():


    #7, 93, 45
    query_pts = np.array([[  787014.438,  -340616.906,  6313018.],
                          [751763.125, -59925.969, 6326205.5],
                          [769957.188, -202418.125, 6321069.5]])


    kdtree = KDTree(data_pts_real)
    dist, idx = kdtree.query(query_pts, sqr_dists=True)

    epsilon = 1e-5
    assert idx[0] == 7
    assert idx[1] == 93
    assert idx[2] == 45
    assert dist[0] == 0
    assert abs(dist[1] - 3.) < epsilon * dist[1]
    assert abs(dist[2] - 20001.) < epsilon * dist[2]

def test3d_float32():


    #7, 93, 45
    query_pts = np.array([[  787014.438,  -340616.906,  6313018.],
                          [751763.125, -59925.969, 6326205.5],
                          [769957.188, -202418.125, 6321069.5]], dtype=np.float32)


    kdtree = KDTree(data_pts_real.astype(np.float32))
    dist, idx = kdtree.query(query_pts, sqr_dists=True)
    epsilon = 1e-5
    assert idx[0] == 7
    assert idx[1] == 93
    assert idx[2] == 45
    assert dist[0] == 0
    assert abs(dist[1] - 3.) < epsilon * dist[1]
    assert abs(dist[2] - 20001.) < epsilon * dist[2]
    assert kdtree.data_pts.dtype == np.float32

def test3d_float32_mismatch():


    #7, 93, 45
    query_pts = np.array([[  787014.438,  -340616.906,  6313018.],
                          [751763.125, -59925.969, 6326205.5],
                          [769957.188, -202418.125, 6321069.5]], dtype=np.float32)

    kdtree = KDTree(data_pts_real)
    dist, idx = kdtree.query(query_pts, sqr_dists=True)

def test3d_float32_mismatch2():


    #7, 93, 45
    query_pts = np.array([[  787014.438,  -340616.906,  6313018.],
                          [751763.125, -59925.969, 6326205.5],
                          [769957.188, -202418.125, 6321069.5]])

    kdtree = KDTree(data_pts_real.astype(np.float32))
    try:
        dist, idx = kdtree.query(query_pts, sqr_dists=True)
        assert False
    except TypeError:
        assert True


def test3d_8n():
    query_pts = np.array([[  787014.438,  -340616.906,  6313018.],
                          [751763.125, -59925.969, 6326205.5],
                          [769957.188, -202418.125, 6321069.5]])

    kdtree = KDTree(data_pts_real)
    dist, idx = kdtree.query(query_pts, k=8)

    exp_dist = np.array([[  0.00000000e+00,   4.05250235e+03,   4.07389794e+03,   8.08201128e+03,
                            8.17063009e+03,   1.20904577e+04,   1.22902057e+04,   1.60775136e+04],
                        [  1.73205081e+00,   2.70216896e+03,   2.71431274e+03,   5.39537066e+03,
                            5.43793210e+03,   8.07855631e+03,   8.17119970e+03,   1.07513693e+04],
                        [  1.41424892e+02,   3.25500021e+03,   3.44284958e+03,   6.58019346e+03,
                            6.81038455e+03,   9.89140135e+03,   1.01918659e+04,   1.31892516e+04]])

    exp_idx = np.array([[ 7,  8,  6,  9,  5, 10,  4, 11],
                        [93, 94, 92, 95, 91, 96, 90, 97],
                        [45, 46, 44, 47, 43, 48, 42, 49]])

    assert np.array_equal(idx, exp_idx)
    assert np.allclose(dist, exp_dist)

def test3d_8n_ub():
    query_pts = np.array([[  787014.438,  -340616.906,  6313018.],
                          [751763.125, -59925.969, 6326205.5],
                          [769957.188, -202418.125, 6321069.5]])

    kdtree = KDTree(data_pts_real)
    dist, idx = kdtree.query(query_pts, k=8, distance_upper_bound=10e3, sqr_dists=False)

    exp_dist = np.array([[  0.00000000e+00,   4.05250235e+03,   4.07389794e+03,   8.08201128e+03,
                            8.17063009e+03,   np.inf,   np.inf,   np.inf],
                        [  1.73205081e+00,   2.70216896e+03,   2.71431274e+03,   5.39537066e+03,
                            5.43793210e+03,   8.07855631e+03,   8.17119970e+03,   np.inf],
                        [  1.41424892e+02,   3.25500021e+03,   3.44284958e+03,   6.58019346e+03,
                            6.81038455e+03,   9.89140135e+03,   np.inf,   np.inf]])
    n = 100
    exp_idx = np.array([[ 7,  8,  6,  9,  5, n,  n, n],
                        [93, 94, 92, 95, 91, 96, 90, n],
                        [45, 46, 44, 47, 43, 48, n, n]])

    assert np.array_equal(idx, exp_idx)
    assert np.allclose(dist, exp_dist)

def test3d_8n_ub_leaf20():
    query_pts = np.array([[  787014.438,  -340616.906,  6313018.],
                          [751763.125, -59925.969, 6326205.5],
                          [769957.188, -202418.125, 6321069.5]])

    kdtree = KDTree(data_pts_real, leafsize=20)
    dist, idx = kdtree.query(query_pts, k=8, distance_upper_bound=10e3, sqr_dists=False)

    exp_dist = np.array([[  0.00000000e+00,   4.05250235e+03,   4.07389794e+03,   8.08201128e+03,
                            8.17063009e+03,   np.inf,   np.inf,   np.inf],
                        [  1.73205081e+00,   2.70216896e+03,   2.71431274e+03,   5.39537066e+03,
                            5.43793210e+03,   8.07855631e+03,   8.17119970e+03,   np.inf],
                        [  1.41424892e+02,   3.25500021e+03,   3.44284958e+03,   6.58019346e+03,
                            6.81038455e+03,   9.89140135e+03,   np.inf,   np.inf]])
    n = 100
    exp_idx = np.array([[ 7,  8,  6,  9,  5, n,  n, n],
                        [93, 94, 92, 95, 91, 96, 90, n],
                        [45, 46, 44, 47, 43, 48, n, n]])

    assert np.array_equal(idx, exp_idx)
    assert np.allclose(dist, exp_dist)

def test3d_8n_ub_eps():
    query_pts = np.array([[  787014.438,  -340616.906,  6313018.],
                          [751763.125, -59925.969, 6326205.5],
                          [769957.188, -202418.125, 6321069.5]])

    kdtree = KDTree(data_pts_real)
    dist, idx = kdtree.query(query_pts, k=8, eps=0.1, distance_upper_bound=10e3, sqr_dists=False)

    exp_dist = np.array([[  0.00000000e+00,   4.05250235e+03,   4.07389794e+03,   8.08201128e+03,
                            8.17063009e+03,   np.inf,   np.inf,   np.inf],
                        [  1.73205081e+00,   2.70216896e+03,   2.71431274e+03,   5.39537066e+03,
                            5.43793210e+03,   8.07855631e+03,   8.17119970e+03,   np.inf],
                        [  1.41424892e+02,   3.25500021e+03,   3.44284958e+03,   6.58019346e+03,
                            6.81038455e+03,   9.89140135e+03,   np.inf,   np.inf]])
    n = 100
    exp_idx = np.array([[ 7,  8,  6,  9,  5, n,  n, n],
                        [93, 94, 92, 95, 91, 96, 90, n],
                        [45, 46, 44, 47, 43, 48, n, n]])

    assert np.array_equal(idx, exp_idx)
    assert np.allclose(dist, exp_dist)

def test3d_large_query():
    # Target idxs: 7, 93, 45
    query_pts = np.array([[  787014.438,  -340616.906,  6313018.],
                          [751763.125, -59925.969, 6326205.5],
                          [769957.188, -202418.125, 6321069.5]])

    # Repeat the same points multiple times to get 60000 query points
    n = 20000
    query_pts = np.repeat(query_pts, n, axis=0)

    kdtree = KDTree(data_pts_real)
    dist, idx = kdtree.query(query_pts, sqr_dists=True)

    epsilon = 1e-5
    assert np.all(idx[:n] == 7)
    assert np.all(idx[n:2*n] == 93)
    assert np.all(idx[2*n:] == 45)
    assert np.all(dist[:n] == 0)
    assert np.all(abs(dist[n:2*n] - 3.) < epsilon * dist[n:2*n])
    assert np.all(abs(dist[2*n:] - 20001.) < epsilon * dist[2*n:])

def test_scipy_comp():

    query_pts = np.array([[  787014.438,  -340616.906,  6313018.],
                          [751763.125, -59925.969, 6326205.5],
                          [769957.188, -202418.125, 6321069.5]])

    kdtree = KDTree(data_pts_real)
    assert id(kdtree.data) == id(kdtree.data_pts)


def test1d_mask():
    data_pts = np.arange(1000)[..., None]
    # put the input locations in random order
    np.random.shuffle(data_pts)
    bad_idx = np.nonzero(data_pts[..., 0] == 400)
    print(bad_idx)
    nearest_idx_1 = np.nonzero(data_pts[..., 0] == 399)
    nearest_idx_2 = np.nonzero(data_pts[..., 0] == 390)
    kdtree = KDTree(data_pts, leafsize=15)
    # shift the query points just a little bit for known neighbors
    # we want 399 as a result, not 401, when we query for ~400
    query_pts = np.arange(399.9, 299.9, -10)[..., None]
    query_mask = np.zeros(data_pts.shape[0]).astype(bool)
    query_mask[bad_idx] = True
    dist, idx = kdtree.query(query_pts, mask=query_mask)
    assert idx[0] == nearest_idx_1  # 399, would be 400 if no mask
    assert np.isclose(dist[0], 0.9)
    assert idx[1] == nearest_idx_2  # 390
    assert np.isclose(dist[1], 0.1)


def test1d_all_masked():
    data_pts = np.arange(1000)[..., None]
    np.random.shuffle(data_pts)
    kdtree = KDTree(data_pts, leafsize=15)
    query_pts = np.arange(400, 300, -10)[..., None]
    query_mask = np.ones(data_pts.shape[0]).astype(bool)
    dist, idx = kdtree.query(query_pts, mask=query_mask)
    # all invalid
    assert np.all(i >= 1000 for i in idx)
    assert np.all(d >= 1001 for d in dist)


def test3d_mask():
    #7, 93, 45
    query_pts = np.array([[  787014.438,  -340616.906,  6313018.],
                          [751763.125, -59925.969, 6326205.5],
                          [769957.188, -202418.125, 6321069.5]])

    kdtree = KDTree(data_pts_real)
    query_mask = np.zeros(data_pts_real.shape[0])
    query_mask[6:10] = True
    dist, idx = kdtree.query(query_pts, sqr_dists=True, mask=query_mask)

    epsilon = 1e-5
    assert idx[0] == 5  # would be 7 if no mask
    assert idx[1] == 93
    assert idx[2] == 45
    # would be 0 if no mask
    assert abs(dist[0] - 66759196.1053) < epsilon * dist[0]
    assert abs(dist[1] - 3.) < epsilon * dist[1]
    assert abs(dist[2] - 20001.) < epsilon * dist[2]

def test128d_fail():
    pts = 100
    dims = 128
    data_pts = np.arange(pts * dims).reshape(pts, dims)
    try:
        kdtree = KDTree(data_pts)
    except ValueError as exc:
        assert "Max 127 dimensions" in str(exc)
    else:
        raise Exception("Should not accept 129 dimensional data")

def test127d_ok():
    pts = 2
    dims = 127
    data_pts = np.arange(pts * dims).reshape(pts, dims)
    kdtree = KDTree(data_pts)
    dist, idx = kdtree.query(data_pts)
    assert np.all(dist == 0)


def test_empty_fail():
    data_pts = np.array([1, 2, 3])
    try:
        kdtree = KDTree(data_pts)
    except ValueError as e:
        assert 'exactly 2 dimensions' in str(e), str(e)
    data_pts = np.array([[]])
    try:
        kdtree = KDTree(data_pts)
    except ValueError as e:
        assert 'non-empty' in str(e), str(e)

@pytest.mark.skip(reason="Requires ~50G RAM")
def test_tree_n_lt_maxint32_n_query_k_gt_maxint32():
    # n_points < UINT32_MAX but n_query * k > UINT32_MAX -> still uses 32-bit index
    n_dim = 2
    n_points = 2**20
    n_query = 2**20 + 8
    k = 2**12
    data_pts = np.random.random((n_points, n_dim)).astype(np.float32)
    query_pts = np.random.random((n_query, n_dim)).astype(np.float32)
    data_pts[0] = query_pts[0]
    data_pts[1533] = query_pts[15633]
    data_pts[1048575] = query_pts[1048583]
    kdtree = KDTree(data_pts)
    dist, idx = kdtree.query(query_pts, k=k)
    assert idx.shape == (n_query, k)
    assert idx.dtype == np.uint32
    assert idx[0][0] == 0
    assert idx[15633][0] == 1533
    assert idx[1048583][0] == 1048575
    assert np.all(idx < data_pts.shape[0])
    assert dist.shape == (n_query, k)
    assert dist.dtype == np.float32

@pytest.mark.skip(reason="Requires ~50G RAM")
def test_tree_n_points_n_dim_gt_maxint32():
    # n_points < UINT32_MAX but n_points * n_dim > UINT32_MAX -> uses 64-bit index
    n_dim = 2**6
    n_points = 2**26 + 8
    data_pts = np.random.random((n_points, n_dim)).astype(np.float32)
    query_pts = np.random.random((3, n_dim)).astype(np.float32)
    data_pts[0] = query_pts[0]
    data_pts[874516] = query_pts[1]
    data_pts[67108871] = query_pts[2]
    kdtree = KDTree(data_pts)
    dist, idx = kdtree.query(query_pts, k=4)
    assert idx.shape == (3, 4)
    assert idx.dtype == np.uint64
    assert idx[0][0] == 0
    assert idx[1][0] == 874516
    assert idx[2][0] == 67108871
    assert np.all(idx < data_pts.shape[0])
    assert dist.shape == (3, 4)
    assert dist.dtype == np.float32
