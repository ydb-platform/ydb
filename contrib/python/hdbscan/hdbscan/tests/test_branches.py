import numpy as np
from scipy import stats
from scipy import sparse
from scipy.spatial import distance
from sklearn.utils.estimator_checks import check_estimator
from hdbscan import (
    HDBSCAN,
    BranchDetector,
    detect_branches_in_clusters,
    approximate_predict_branch,
)
from .test_hdbscan import (
    if_matplotlib,
    if_networkx,
    if_pandas,
)

from sklearn.utils import check_random_state, shuffle as util_shuffle
from sklearn.datasets import make_blobs
from sklearn.preprocessing import StandardScaler

from tempfile import mkdtemp
from functools import wraps
import numbers
import pytest

import warnings


def if_pygraphviz(func):
    """Test decorator that skips test if networkx or pygraphviz is not installed."""

    @wraps(func)
    def run_test(*args, **kwargs):
        try:
            import networkx
            import pygraphviz
        except ImportError:
            pytest.skip("NetworkX or pygraphviz not available.")
        else:
            return func(*args, **kwargs)

    return run_test


def make_branches(n_samples=100, shuffle=True, noise=None, random_state=None):
    if isinstance(n_samples, numbers.Integral):
        n_samples_out = n_samples // 3
        n_samples_in = n_samples - n_samples_out
    else:
        try:
            n_samples_out, n_samples_in = n_samples
        except ValueError as e:
            raise ValueError(
                "`n_samples` can be either an int or a two-element tuple."
            ) from e

    generator = check_random_state(random_state)

    outer_circ_x = np.cos(np.linspace(np.pi / 2, np.pi, n_samples_out))
    outer_circ_y = np.sin(np.linspace(np.pi / 2, np.pi, n_samples_out)) - 1
    inner_circ_x = np.cos(np.linspace(0, np.pi, n_samples_in))
    inner_circ_y = 1 - np.sin(np.linspace(0, np.pi, n_samples_in))

    X = np.vstack(
        [
            np.append(outer_circ_x, inner_circ_x),
            np.append(outer_circ_y, inner_circ_y),
        ]
    ).T
    y = np.hstack(
        [
            np.zeros(n_samples_out, dtype=np.intp),
            np.ones(n_samples_in, dtype=np.intp),
        ]
    )

    if shuffle:
        X, y = util_shuffle(X, y, random_state=generator)

    if noise is not None:
        X += generator.normal(scale=noise, size=X.shape)

    return X, y


def generate_noisy_data():
    blobs, yBlobs = make_blobs(
        n_samples=50,
        centers=[(-0.75, 2.25), (2.0, -0.5)],
        cluster_std=0.2,
        random_state=3,
    )
    moons, _ = make_branches(n_samples=150, noise=0.06, random_state=3)
    yMoons = np.full(moons.shape[0], 2)
    np.random.seed(5)
    noise = np.random.uniform(-1.0, 3.0, (50, 2))
    yNoise = np.full(50, -1)
    return (
        np.vstack((blobs, moons, noise)),
        np.concatenate((yBlobs, yMoons, yNoise)),
    )


X, y = generate_noisy_data()
X = StandardScaler().fit_transform(X)

X_missing_data = X.copy()
X_missing_data[0] = [np.nan, 1]
X_missing_data[5] = [np.nan, np.nan]

# --- Branch Detection Data


def test_branch_detection_data():
    """Check that the flag generates internal branch_detection_data."""
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    branch_data = c.branch_detection_data_
    assert c.minimum_spanning_tree_ is not None
    assert branch_data.all_finite == True
    assert branch_data.core_distances.shape[0] == X.shape[0]
    assert branch_data.neighbors.shape[0] == X.shape[0]
    assert branch_data.neighbors.shape[1] == c.min_samples or c.min_cluster_size
    assert branch_data.finite_index is None


def test_branch_detection_data_with_missing():
    """Check internal branch_detection_data recognizes missing data."""
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X_missing_data)
    branch_data = c.branch_detection_data_
    assert c.minimum_spanning_tree_ is not None
    assert branch_data.all_finite == False
    assert branch_data.core_distances.shape[0] == X.shape[0] - 2
    assert branch_data.neighbors.shape[0] == X.shape[0] - 2
    assert branch_data.neighbors.shape[1] == c.min_samples or c.min_cluster_size
    assert branch_data.finite_index is not None


@pytest.mark.skip(reason="Unreachable code-branch cannot be tested.")
def test_branch_detection_data_with_non_tree_metric():
    """Check warning on unsupported metric."""
    with warnings.catch_warnings(record=True) as w:
        # There are no fast metrics that are not supported by KDTree or BallTree!
        # Cosine and arccoss both crash HDBSCAN. They go down the BallTree path, but
        # the implementation does not support them.
        c = HDBSCAN(
            min_cluster_size=5, branch_detection_data=True, metric="cosine"
        ).fit(X)
        assert "Metric cosine not supported for branch detection!" in str(w[-1].message)
        assert c.minimum_spanning_tree_ is not None
        with pytest.raises(AttributeError):
            c.branch_detection_data


def test_branch_detection_data_with_unsupported_input():
    """Check warning on unsupported inputs."""
    # Distance matrix
    D = distance.squareform(distance.pdist(X))
    with warnings.catch_warnings(record=True) as w:
        c = HDBSCAN(
            min_cluster_size=5, metric="precomputed", branch_detection_data=True
        ).fit(D)
        assert (
            "Branch detection for non-vector space inputs is not (yet) implemented."
            in str(w[-1].message)
        )

    # Sparse matrix
    D /= np.max(D)
    threshold = stats.scoreatpercentile(D.flatten(), 50)
    D[D >= threshold] = 0.0
    D = sparse.csr_matrix(D)
    D.eliminate_zeros()
    with warnings.catch_warnings(record=True) as w:
        c = HDBSCAN(
            min_cluster_size=5, metric="precomputed", branch_detection_data=True
        ).fit(D)
        assert (
            "Branch detection for non-vector space inputs is not (yet) implemented."
            in str(w[-1].message)
        )


def test_generate_branch_detection_data():
    """Generate branch detection data function does not re-generate MST."""
    c = HDBSCAN(min_cluster_size=5).fit(X)
    c.generate_branch_detection_data()
    assert c.branch_detection_data_ is not None
    with pytest.raises(AttributeError):
        c.minimum_spanning_tree_


# --- Detecting Branches


def check_detected_groups(c, n_clusters=3, n_branches=6, overridden=False):
    """Checks branch_detector output for main invariants."""
    assert len(np.unique(c.labels_)) - int(-1 in c.labels_) == n_branches
    assert (
        len(np.unique(c.cluster_labels_)) - int(-1 in c.cluster_labels_) == n_clusters
    )
    noise_mask = c.labels_ == -1
    assert (c.branch_labels_[noise_mask] == 0).all()
    assert (c.branch_probabilities_[noise_mask] == 1.0).all()
    assert (c.probabilities_[noise_mask] == 0.0).all()
    assert (c.cluster_probabilities_[noise_mask] == 0.0).all()
    if not overridden:
        assert len(c.cluster_points_) == n_clusters
        assert len(c.branch_persistences_) == n_clusters
    assert sum(len(ps) for ps in c.branch_persistences_) >= (n_branches - n_clusters)


def test_branch_detector():
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector(
        branch_detection_method="core", cluster_selection_method="eom"
    ).fit(c)
    check_detected_groups(b, n_branches=7)

    b = BranchDetector(
        branch_detection_method="full", cluster_selection_method="eom"
    ).fit(c)
    check_detected_groups(b)

    b = BranchDetector(
        branch_detection_method="core", cluster_selection_method="leaf"
    ).fit(c)
    check_detected_groups(b, n_branches=9)

    b = BranchDetector(
        branch_detection_method="full", cluster_selection_method="leaf"
    ).fit(c)
    check_detected_groups(b)


def test_min_cluster_size():
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector(min_cluster_size=7).fit(c)
    labels, counts = np.unique(b.labels_, return_counts=True)
    assert (counts[labels >= 0] >= 7).all()
    check_detected_groups(b)


def test_label_sides_as_branches():
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector(label_sides_as_branches=True).fit(c)
    check_detected_groups(b, n_branches=8)


def test_max_cluster_size():
    """Suppresses one branch."""
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector(label_sides_as_branches=True, max_cluster_size=50).fit(c)
    check_detected_groups(b, n_branches=7)


def test_override_cluster_labels():
    split_y = np.full_like(y, -1)
    split_y[y == 0] = 0
    split_y[y == 1] = 0
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector(label_sides_as_branches=True).fit(c, split_y)
    check_detected_groups(b, n_clusters=1, n_branches=2, overridden=True)


def test_allow_single_cluster_with_filters():
    # Generate single-cluster data
    np.random.seed(0)
    no_structure = np.random.rand(150, 2)
    c = HDBSCAN(
        min_samples=5,
        min_cluster_size=150,
        allow_single_cluster=True,
        branch_detection_data=True,
    ).fit(no_structure)

    # Without persistence, find 6 branches
    b = BranchDetector(
        min_cluster_size=5,
        branch_detection_method="core",
        cluster_selection_method="leaf",
    ).fit(c)
    unique_labels = np.unique(b.labels_)
    assert len(unique_labels) == 6
    # Mac & Windows give 71, Linux gives 72. Probably different random values.
    num_noise = np.sum(b.branch_probabilities_ == 0)
    assert (num_noise == 71) | (num_noise == 72)

    # Adding persistence removes some branches
    b = BranchDetector(
        min_cluster_size=5,
        branch_detection_method="core",
        cluster_selection_method="leaf",
        cluster_selection_persistence=0.1,
    ).fit(c)
    unique_labels = np.unique(b.labels_)
    assert len(unique_labels) == 1
    assert np.sum(b.branch_probabilities_ == 0) == 0

    # Adding epsilon removes some branches
    b = BranchDetector(
        min_cluster_size=5,
        branch_detection_method="core",
        cluster_selection_epsilon=1 / 0.39,
        allow_single_cluster=True,
    ).fit(c)
    unique_labels = np.unique(b.labels_)
    assert len(unique_labels) == 1
    assert np.sum(b.branch_probabilities_ == 0) == 0


def test_badargs():
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    c_nofit = HDBSCAN(min_cluster_size=5, branch_detection_data=True)
    c_nobranch = HDBSCAN(min_cluster_size=5, gen_min_span_tree=True).fit(X)
    c_nomst = HDBSCAN(min_cluster_size=5).fit(X)
    c_nomst.generate_branch_detection_data()

    with pytest.raises(AttributeError):
        detect_branches_in_clusters("fail")
    with pytest.raises(AttributeError):
        detect_branches_in_clusters(None)
    with pytest.raises(AttributeError):
        detect_branches_in_clusters("fail")
    with pytest.raises(ValueError):
        detect_branches_in_clusters(c_nofit)
    with pytest.raises(AttributeError):
        detect_branches_in_clusters(c_nobranch)
    with pytest.raises(ValueError):
        detect_branches_in_clusters(c_nomst)
    with pytest.raises(ValueError):
        detect_branches_in_clusters(c, min_cluster_size=-1)
    with pytest.raises(ValueError):
        detect_branches_in_clusters(c, min_cluster_size=0)
    with pytest.raises(ValueError):
        detect_branches_in_clusters(c, min_cluster_size=1)
    with pytest.raises(ValueError):
        detect_branches_in_clusters(c, min_cluster_size=2.0)
    with pytest.raises(ValueError):
        detect_branches_in_clusters(c, min_cluster_size="fail")
    with pytest.raises(ValueError):
        detect_branches_in_clusters(c, cluster_selection_persistence=-0.1)
    with pytest.raises(ValueError):
        detect_branches_in_clusters(c, cluster_selection_epsilon=-0.1)
    with pytest.raises(ValueError):
        detect_branches_in_clusters(
            c,
            cluster_selection_method="something_else",
        )
    with pytest.raises(ValueError):
        detect_branches_in_clusters(
            c,
            branch_detection_method="something_else",
        )


# --- Branch Detector Functionality


def test_caching():
    cachedir = mkdtemp()
    c = HDBSCAN(memory=cachedir, min_samples=5, branch_detection_data=True).fit(X)
    b1 = BranchDetector().fit(c)
    b2 = BranchDetector(allow_single_cluster=True).fit(c)
    n_groups1 = len(set(b1.labels_)) - int(-1 in b1.labels_)
    n_groups2 = len(set(b2.labels_)) - int(-1 in b2.labels_)
    assert n_groups1 == n_groups2


def test_centroid_medoids():
    branch_centers = np.asarray(
        [[-0.9, -1.0], [-0.9, 0.1], [-0.8, 1.9], [-0.5, 0.0], [1.7, -0.9]]
    )

    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector().fit(c)

    centroids = np.asarray([b.weighted_centroid(i) for i in range(5)])
    rounded = np.around(np.asarray(centroids), decimals=1)
    corder = np.lexsort((rounded[:, 1], rounded[:, 0]))
    np.all(np.abs(centroids[corder, :] - branch_centers) < 0.1)

    medoids = np.asarray([b.weighted_medoid(i) for i in range(5)])
    rounded = np.around(np.asarray(medoids), decimals=1)
    corder = np.lexsort((rounded[:, 1], rounded[:, 0]))
    np.all(np.abs(medoids[corder, :] - branch_centers) < 0.1)


def test_exemplars():
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector().fit(c)

    branch_exemplars = b.exemplars_
    assert branch_exemplars[0] is None
    assert branch_exemplars[1] is None
    assert len(branch_exemplars[2]) == 3
    assert len(b.exemplars_) == 3


def test_approximate_predict():
    c = HDBSCAN(
        min_cluster_size=5, branch_detection_data=True, prediction_data=True
    ).fit(X)
    b = BranchDetector().fit(c)

    # A point on a branch (not noise) exact labels change per run
    l, p, cl, cp, bl, bp = approximate_predict_branch(b, np.array([[-0.8, 0.0]]))
    assert cl[0] > -1
    assert len(b.branch_persistences_[cl[0]]) > 2

    # A point in a cluster
    l, p, cl, cp, bl, bp = approximate_predict_branch(b, np.array([[-0.8, 2.0]]))
    assert l[0] == cl[0]
    assert bl[0] == 0
    assert bp[0] == 1.0

    # A noise point
    l, p, cl, cp, bl, bp = approximate_predict_branch(b, np.array([[1, 3.0]]))
    assert l[0] == -1
    assert cl[0] == -1
    assert cp[0] == 0
    assert p[0] == 0.0
    assert cp[0] == 0.0
    assert bp[0] == 1.0


# --- Attribute Output Formats


def test_trees_numpy_output_formats():
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector().fit(c)
    points, edges = b.approximation_graph_.to_numpy()
    assert points.shape[0] <= X.shape[0]  # Excludes noise points
    for t in b.condensed_trees_:
        t.to_numpy()
    for t in b.linkage_trees_:
        t.to_numpy()


def test_trees_pandas_output_formats():
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector().fit(c)
    if_pandas(b.approximation_graph_.to_pandas)()
    for t in b.condensed_trees_:
        if_pandas(t.to_pandas)()
    for t in b.linkage_trees_:
        if_pandas(t.to_pandas)()


def test_trees_networkx_output_formats():
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector().fit(c)
    if_networkx(b.approximation_graph_.to_networkx)()
    for t in b.condensed_trees_:
        if_networkx(t.to_networkx)()
    for t in b.linkage_trees_:
        if_networkx(t.to_networkx)()


# --- Attribute plots


def test_condensed_tree_plot():
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector().fit(c)
    for t in b.condensed_trees_:
        if_matplotlib(t.plot)(
            select_clusters=True,
            label_clusters=True,
            selection_palette=("r", "g", "b"),
            cmap="Reds",
        )
    if_matplotlib(t.plot)(log_size=True, colorbar=False, cmap="none")


def test_single_linkage_tree_plot():
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector().fit(c)
    for t in b.linkage_trees_:
        if_matplotlib(t.plot)(cmap="Reds")
        if_matplotlib(t.plot)(
            vary_line_width=False,
            truncate_mode="lastp",
            p=10,
            cmap="none",
            colorbar=False,
        )


def test_approximation_graph_plot():
    c = HDBSCAN(min_cluster_size=5, branch_detection_data=True).fit(X)
    b = BranchDetector().fit(c)
    g = b.approximation_graph_
    if_matplotlib(g.plot)(positions=X)
    if_pygraphviz(if_matplotlib(g.plot))(node_color="x", feature_names=["x", "y"])
    if_pygraphviz(if_matplotlib(g.plot))(node_color=X[:, 0])
    if_pygraphviz(if_matplotlib(g.plot))(edge_color="centrality", node_alpha=0)
    if_pygraphviz(if_matplotlib(g.plot))(
        edge_color=g._edges["centrality"], node_alpha=0
    )


@pytest.mark.skip(reason="need to refactor to meet newer standards")
def test_branch_detector_is_sklearn_estimator():
    check_estimator(BranchDetector)
