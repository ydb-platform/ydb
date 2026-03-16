# Support branch detection within clusters.
import numpy as np

from sklearn.base import BaseEstimator, ClusterMixin
from scipy.sparse import coo_array
from scipy.sparse.csgraph import minimum_spanning_tree, connected_components
from joblib import Memory
from joblib import Parallel, delayed
from joblib.parallel import cpu_count
from ._hdbscan_linkage import label
from .plots import CondensedTree, SingleLinkageTree, ApproximationGraph
from .prediction import approximate_predict
from ._hdbscan_tree import recurse_leaf_dfs
from .hdbscan_ import _tree_to_labels


def detect_branches_in_clusters(
    clusterer,
    cluster_labels=None,
    cluster_probabilities=None,
    branch_detection_method="full",
    label_sides_as_branches=False,
    min_cluster_size=None,
    max_cluster_size=None,
    allow_single_cluster=None,
    cluster_selection_method=None,
    cluster_selection_epsilon=0.0,
    cluster_selection_persistence=0.0,
):
    """
    Performs a flare-detection post-processing step to detect branches within
    clusters [1]_.

    For each cluster, a graph is constructed connecting the data points based on
    their mutual reachability distances. Each edge is given a centrality value
    based on how far it lies from the cluster's center. Then, the edges are
    clustered as if that centrality was a distance, progressively removing the
    'center' of each cluster and seeing how many branches remain.

    Parameters
    ----------

    clusterer : hdbscan.HDBSCAN
        The clusterer object that has been fit to the data with branch detection
        data generated.

    cluster_labels : np.ndarray, shape (n_samples, ), optional (default=None)
        The cluster labels for each point in the data set. If not provided, the
        clusterer's labels will be used.

    cluster_probabilities : np.ndarray, shape (n_samples, ), optional (default=None)
        The cluster probabilities for each point in the data set. If not provided,
        the clusterer's probabilities will be used, or all points will be given
        1.0 probability if labels are overridden.

    branch_detection_method : str, optional (default=``full``)
        Determines which graph is constructed to detect branches with. Valid
        values are, ordered by increasing computation cost and decreasing
        sensitivity to noise:
        - ``core``: Contains the edges that connect each point to all other
          points within a mutual reachability distance lower than or equal to
          the point's core distance. This is the cluster's subgraph of the
          k-NN graph over the entire data set (with k = ``min_samples``).
        - ``full``: Contains all edges between points in each cluster with a
          mutual reachability distance lower than or equal to the distance of
          the most-distance point in each cluster. These graphs represent the
          0-dimensional simplicial complex of each cluster at the first point in
          the filtration where they contain all their points.

    label_sides_as_branches : bool, optional (default=False),
        When this flag is False, branches are only labelled for clusters with at
        least three branches (i.e., at least y-shapes). Clusters with only two
        branches represent l-shapes. The two branches describe the cluster's
        outsides growing towards each other. Enabling this flag separates these
        branches from each other in the produced labelling.

    min_cluster_size : int, optional (default=None)
        The minimum number of samples in a group for that group to be
        considered a branch; groupings smaller than this size will seen as
        points falling out of a branch. Defaults to the clusterer's min_cluster_size.

    allow_single_cluster : bool, optional (default=None)
        Analogous to HDBSCAN's ``allow_single_cluster``.

    cluster_selection_method : str, optional (default=None)
        The method used to select branches from the cluster's condensed tree.
        The standard approach for FLASC is to use the ``eom`` approach.
        Options are:
          * ``eom``
          * ``leaf``

    cluster_selection_epsilon: float, optional (default=0.0)
        A lower epsilon threshold. Only branches with a death above this value
        will be considered. See [3]_ for more information. Note that this
        should not be used if we want to predict the cluster labels for new
        points in future (e.g. using approximate_predict), as the
        :func:`~hdbscan.branches.approximate_predict` function is not aware of
        this argument.

    cluster_selection_persistence: float, optional (default=0.0)
        An eccentricity persistence threshold. Branches with a persistence below
        this value will be merged. See [3]_ for more information. Note that this
        should not be used if we want to predict the cluster labels for new
        points in future (e.g. using approximate_predict), as the
        :func:`~hdbscan.branches.approximate_predict` function is not aware of
        this argument.

    max_cluster_size : int, optional (default=0)
        A limit to the size of clusters returned by the ``eom`` algorithm.
        Has no effect when using ``leaf`` clustering (where clusters are
        usually small regardless). Note that this should not be used if we
        want to predict the cluster labels for new points in future (e.g. using
        :func:`~hdbscan.branches.approximate_predict`), as that function is
        not aware of this argument.

    Returns
    -------
    labels : np.ndarray, shape (n_samples, )
        Labels that differentiate all subgroups (clusters and branches). Noisy
        samples are given the label -1.

    probabilities : np.ndarray, shape (n_samples, )
        Probabilities considering both cluster and branch membership. Noisy
        samples are assigned 0.

    cluster_labels : np.ndarray, shape (n_samples, )
        The cluster labels for each point in the data set. Noisy samples are
        given the label -1.

    cluster_probabilities : np.ndarray, shape (n_samples, )
        The cluster probabilities for each point in the data set. Noisy samples
        are assigned 1.0.

    branch_labels : np.ndarray, shape (n_samples, )
        Branch labels for each point. Noisy samples are given the label -1.

    branch_probabilities : np.ndarray, shape (n_samples, )
        Branch membership strengths for each point. Noisy samples are
        assigned 0.

    branch_persistences : tuple (n_clusters)
        A branch persistence (eccentricity range) for each detected branch.

    approximation_graphs : tuple (n_clusters)
        The graphs used to detect branches in each cluster stored as a numpy
        array with four columns: source, target, centrality, mutual reachability
        distance. Points are labelled by their row-index into the input data.
        The edges contained in the graphs depend on the ``branch_detection_method``:
        - ``core``: Contains the edges that connect each point to all other
          points in a cluster within a mutual reachability distance lower than
          or equal to the point's core distance. This is an extension of the
          minimum spanning tree introducing only edges with equal distances. The
          reachability distance introduces ``num_points`` * ``min_samples`` of
          such edges.
        - ``full``: Contains all edges between points in each cluster with a
          mutual reachability distance lower than or equal to the distance of
          the most-distance point in each cluster. These graphs represent the
          0-dimensional simplicial complex of each cluster at the first point in
          the filtration where they contain all their points.

    condensed_trees : tuple (n_clusters)
        A condensed branch hierarchy for each cluster produced during the
        branch detection step. Data points are numbered with in-cluster ids.

    linkage_trees : tuple (n_clusters)
        A single linkage tree for each cluster produced during the branch
        detection step, in the scipy hierarchical clustering format.
        (see http://docs.scipy.org/doc/scipy/reference/cluster.hierarchy.html).
        Data points are numbered with in-cluster ids.

    centralities : np.ndarray, shape (n_samples, )
        Centrality values for each point in a cluster. Overemphasizes points'
        eccentricity within the cluster as the values are based on minimum
        spanning trees that do not contain the equally distanced edges resulting
        from the mutual reachability distance.

    cluster_points : list (n_clusters)
        The data point row indices for each cluster.

    References
    ----------
    .. [1] Bot D.M., Peeters J., Liesenborgs J., Aerts J. 2025. FLASC: a
    flare-sensitive clustering algorithm. PeerJ Computer Science 11:e2792
    https://doi.org/10.7717/peerj-cs.2792.
    """
    # Check clusterer state
    if clusterer._min_spanning_tree is None:
        raise ValueError(
            "Clusterer does not have an explicit minimum spanning tree!"
            " Try fitting with branch_detection_data=True or"
            " gen_min_span_tree=True set."
        )
    if clusterer.branch_detection_data_ is None:
        raise ValueError(
            "Clusterer does not have branch detection data!"
            " Try fitting with branch_detection_data=True set,"
            " or run generate_branch_detection_data on the clusterer"
        )

    # Validate parameters
    if min_cluster_size is None:
        min_cluster_size = clusterer.min_cluster_size
    if max_cluster_size is None:
        max_cluster_size = clusterer.max_cluster_size
    if allow_single_cluster is None:
        allow_single_cluster = clusterer.allow_single_cluster
    if cluster_selection_method is None:
        cluster_selection_method = clusterer.cluster_selection_method
    cluster_selection_epsilon = float(cluster_selection_epsilon)
    cluster_selection_persistence = float(cluster_selection_persistence)
    if not (
        np.issubdtype(type(min_cluster_size), np.integer) and min_cluster_size >= 2
    ):
        raise ValueError(
            f"min_cluster_size must be an integer greater or equal "
            f"to 2,  {min_cluster_size} given."
        )
    if not (
        np.issubdtype(type(cluster_selection_persistence), np.floating)
        and cluster_selection_persistence >= 0.0
    ):
        raise ValueError(
            f"cluster_selection_persistence must be a float greater or equal to "
            f"0.0, {cluster_selection_persistence} given."
        )
    if not (
        np.issubdtype(type(cluster_selection_epsilon), np.floating)
        and cluster_selection_epsilon >= 0.0
    ):
        raise ValueError(
            f"cluster_selection_epsilon must be a float greater or equal to "
            f"0.0, {cluster_selection_epsilon} given."
        )
    if cluster_selection_method not in ("eom", "leaf"):
        raise ValueError(
            f"Invalid cluster_selection_method: {cluster_selection_method}\n"
            f'Should be one of: "eom", "leaf"\n'
        )
    if branch_detection_method not in ("core", "full"):
        raise ValueError(
            f"Invalid ``branch_detection_method``: {branch_detection_method}\n"
            'Should be one of: "core", "full"\n'
        )

    # Extract state
    memory = clusterer.memory
    if isinstance(memory, str):
        memory = Memory(memory, verbose=0)

    if cluster_labels is None:
        overridden_labels = False
        num_clusters = len(clusterer.cluster_persistence_)
        cluster_labels, _cluster_probabilities = update_single_cluster_labels(
            clusterer._condensed_tree,
            clusterer.labels_,
            clusterer.probabilities_,
            clusterer.cluster_persistence_,
            allow_single_cluster=clusterer.allow_single_cluster,
            cluster_selection_epsilon=clusterer.cluster_selection_epsilon,
        )
        if cluster_probabilities is None:
            cluster_probabilities = _cluster_probabilities
    else:
        overridden_labels = True
        num_clusters = cluster_labels.max() + 1
        if cluster_probabilities is None:
            cluster_probabilities = np.where(cluster_labels == -1, 0.0, 1.0)

    if not clusterer.branch_detection_data_.all_finite:
        finite_index = clusterer.branch_detection_data_.finite_index
        cluster_labels = cluster_labels[finite_index]
        cluster_probabilities = cluster_probabilities[finite_index]

    # Configure parallelization
    run_core = branch_detection_method == "core"
    num_jobs = clusterer.core_dist_n_jobs
    if num_jobs < 1:
        num_jobs = max(cpu_count() + 1 + num_jobs, 1)
    thread_pool = (
        SequentialPool() if run_core else Parallel(n_jobs=num_jobs, max_nbytes=None)
    )

    # Detect branches
    (
        points,
        centralities,
        linkage_trees,
        approximation_graphs,
    ) = memory.cache(compute_branch_linkage, ignore=["thread_pool"])(
        cluster_labels,
        cluster_probabilities,
        clusterer._min_spanning_tree,
        clusterer.branch_detection_data_.tree,
        clusterer.branch_detection_data_.neighbors,
        clusterer.branch_detection_data_.core_distances,
        clusterer.branch_detection_data_.dist_metric,
        num_clusters,
        thread_pool,
        run_core=run_core,
        overridden_labels=overridden_labels,
    )
    (
        branch_labels,
        branch_probabilities,
        branch_persistences,
        condensed_trees,
        linkage_trees,
    ) = memory.cache(compute_branch_segmentation, ignore=["thread_pool"])(
        linkage_trees,
        thread_pool,
        min_cluster_size=min_cluster_size,
        allow_single_cluster=allow_single_cluster,
        cluster_selection_method=cluster_selection_method,
        cluster_selection_epsilon=cluster_selection_epsilon,
        cluster_selection_persistence=cluster_selection_persistence,
        max_cluster_size=max_cluster_size,
    )
    (
        labels,
        probabilities,
        branch_labels,
        branch_probabilities,
        centralities,
    ) = memory.cache(update_labelling)(
        cluster_probabilities,
        linkage_trees,
        points,
        centralities,
        branch_labels,
        branch_probabilities,
        branch_persistences,
        label_sides_as_branches=label_sides_as_branches,
    )

    # Maintain data indices for non-finite data
    if not clusterer.branch_detection_data_.all_finite:
        internal_to_raw = clusterer.branch_detection_data_.internal_to_raw
        _remap_point_lists(points, internal_to_raw)
        _remap_edge_lists(approximation_graphs, internal_to_raw)

        num_points = len(clusterer.labels_)
        labels = _remap_labels(labels, finite_index, num_points)
        probabilities = _remap_probabilities(probabilities, finite_index, num_points)
        cluster_labels = _remap_labels(cluster_labels, finite_index, num_points)
        cluster_probabilities = _remap_probabilities(
            cluster_probabilities, finite_index, num_points
        )
        branch_labels = _remap_labels(branch_labels, finite_index, num_points, 0)
        branch_probabilities = _remap_probabilities(
            branch_probabilities, finite_index, num_points
        )
        centralities = _remap_probabilities(centralities, finite_index, num_points)

    return (
        labels,
        probabilities,
        cluster_labels,
        cluster_probabilities,
        branch_labels,
        branch_probabilities,
        branch_persistences,
        approximation_graphs,
        condensed_trees,
        linkage_trees,
        centralities,
        points,
    )


def update_single_cluster_labels(
    condensed_tree,
    labels,
    probabilities,
    persistences,
    allow_single_cluster=False,
    cluster_selection_epsilon=0.0,
):
    """Sets all points up to cluster_selection_epsilon to the zero-cluster if
    a single cluster is detected."""
    if allow_single_cluster and len(persistences) == 1:
        labels = np.zeros_like(labels)
        probabilities = np.ones_like(probabilities)
        if cluster_selection_epsilon > 0.0:
            size_mask = condensed_tree["child_size"] == 1
            lambda_mask = condensed_tree["lambda_val"] < (1 / cluster_selection_epsilon)
            noise_points = condensed_tree["child"][lambda_mask & size_mask]
            labels[noise_points] = -1
            probabilities[noise_points] = 0.0

    return labels, probabilities


def compute_branch_linkage(
    cluster_labels,
    cluster_probabilities,
    min_spanning_tree,
    space_tree,
    neighbors,
    core_distances,
    dist_metric,
    num_clusters,
    thread_pool,
    run_core=False,
    overridden_labels=False,
):
    result = thread_pool(
        delayed(_compute_branch_linkage_of_cluster)(
            cluster_labels,
            cluster_probabilities,
            min_spanning_tree,
            space_tree,
            neighbors,
            core_distances,
            dist_metric,
            run_core,
            overridden_labels,
            cluster_id,
        )
        for cluster_id in range(num_clusters)
    )
    if len(result):
        return tuple(zip(*result))
    return (), (), (), ()


def _compute_branch_linkage_of_cluster(
    cluster_labels,
    cluster_probabilities,
    min_spanning_tree,
    space_tree,
    neighbors,
    core_distances,
    dist_metric,
    run_core,
    overridden_labels,
    cluster_id,
):
    """Detect branches within one cluster."""
    # List points within cluster
    cluster_mask = cluster_labels == cluster_id
    cluster_points = np.where(cluster_mask)[0]
    in_cluster_ids = np.full(cluster_labels.shape[0], -1, dtype=np.double)
    in_cluster_ids[cluster_points] = np.arange(len(cluster_points), dtype=np.double)

    # Extract MST edges within cluster
    parent_mask = cluster_labels[min_spanning_tree[:, 0].astype(np.intp)] == cluster_id
    child_mask = cluster_labels[min_spanning_tree[:, 1].astype(np.intp)] == cluster_id
    cluster_mst = min_spanning_tree[parent_mask & child_mask]
    cluster_mst[:, 0] = in_cluster_ids[cluster_mst[:, 0].astype(np.intp)]
    cluster_mst[:, 1] = in_cluster_ids[cluster_mst[:, 1].astype(np.intp)]

    # Compute in cluster centrality
    points = space_tree.data.base[cluster_points]
    centroid = np.average(points, weights=cluster_probabilities[cluster_mask], axis=0)
    centralities = dist_metric.pairwise(centroid[None], points)[0, :]
    with np.errstate(divide="ignore"):
        centralities = 1 / centralities

    # Construct cluster approximation graph
    if run_core:
        edges = extract_core_cluster_graph(
            cluster_mst, core_distances, neighbors[cluster_points], in_cluster_ids
        )
    else:
        max_dist = cluster_mst.T[2].max()
        edges = extract_full_cluster_graph(
            space_tree, core_distances, cluster_points, in_cluster_ids, max_dist
        )

    # Compute linkage over the graph
    return compute_branch_linkage_from_graph(
        cluster_points, centralities, edges, overridden_labels
    )


def compute_branch_linkage_from_graph(
    cluster_points, centralities, edges, overridden_labels
):
    # Set max centrality as 'distance'
    np.maximum(
        centralities[edges[:, 0].astype(np.intp)],
        centralities[edges[:, 1].astype(np.intp)],
        edges[:, 2],
    )

    # Extract MST edges
    centrality_mst = minimum_spanning_tree(
        coo_array(
            (edges[:, 2], (edges[:, 0].astype(np.int32), edges[:, 1].astype(np.int32))),
            shape=(len(cluster_points), len(cluster_points)),
        ),
        overwrite=True,
    )

    # Re-label edges with data ids
    edges[:, 0] = cluster_points[edges[:, 0].astype(np.intp)]
    edges[:, 1] = cluster_points[edges[:, 1].astype(np.intp)]

    # Stop if the graph is disconnected, return component labels in
    # place of linkage tree, which is detected later on!
    if overridden_labels:
        num_components, labels = connected_components(centrality_mst, directed=False)
        if num_components > 1:
            return cluster_points, centralities, labels, edges

    # Compute linkage tree
    centrality_mst = centrality_mst.tocoo()
    centrality_mst = np.column_stack(
        (centrality_mst.row, centrality_mst.col, centrality_mst.data)
    )
    centrality_mst = centrality_mst[np.argsort(centrality_mst[:, 2]), :]
    linkage_tree = label(centrality_mst)

    # Return values
    return cluster_points, centralities, linkage_tree, edges


def extract_core_cluster_graph(
    cluster_spanning_tree,
    core_distances,
    neighbors,
    in_cluster_ids,
):
    """Create a graph connecting all points within each point's core distance."""
    # Allocate output (won't be filled completely)
    num_points = neighbors.shape[0]
    num_neighbors = neighbors.shape[1]
    count = cluster_spanning_tree.shape[0]
    edges = np.zeros((count + num_points * num_neighbors, 4), dtype=np.double)

    # Fill (undirected) MST edges with within-cluster-ids
    mst_parents = cluster_spanning_tree[:, 0].astype(np.intp)
    mst_children = cluster_spanning_tree[:, 1].astype(np.intp)
    np.minimum(mst_parents, mst_children, edges[:count, 0])
    np.maximum(mst_parents, mst_children, edges[:count, 1])

    # Fill neighbors with within-cluster-ids
    core_parent = np.repeat(np.arange(num_points, dtype=np.double), num_neighbors)
    core_children = in_cluster_ids[neighbors.flatten()]
    np.minimum(core_parent, core_children, edges[count:, 0])
    np.maximum(core_parent, core_children, edges[count:, 1])

    # Fill mutual reachabilities
    edges[:count, 3] = cluster_spanning_tree[:, 2]
    np.maximum(
        core_distances[edges[count:, 0].astype(np.intp)],
        core_distances[edges[count:, 1].astype(np.intp)],
        edges[count:, 3],
    )

    # Extract unique edges that stay within the cluster
    edges = np.unique(edges[edges[:, 0] > -1.0, :], axis=0)
    return edges


def extract_full_cluster_graph(
    space_tree, core_distances, cluster_points, in_cluster_ids, max_dist
):
    # Query KDTree/BallTree for neighors within the distance
    children_map, distances_map = space_tree.query_radius(
        space_tree.data.base[cluster_points], r=max_dist + 1e-8, return_distance=True
    )

    # Count number of returned edges per point
    num_children = np.zeros(len(cluster_points), dtype=np.intp)
    for i, children in enumerate(children_map):
        num_children[i] += len(children)

    # Create full edge list
    full_parents = np.repeat(
        np.arange(len(cluster_points), dtype=np.double), num_children
    )
    full_children = in_cluster_ids[np.concatenate(children_map)]
    full_distances = np.concatenate(distances_map)

    # Create output
    mask = (
        (full_children != -1.0)
        & (full_parents < full_children)
        & (full_distances <= max_dist)
    )
    edges = np.zeros((mask.sum(), 4), dtype=np.double)
    edges[:, 0] = full_parents[mask]
    edges[:, 1] = full_children[mask]
    np.maximum(
        np.maximum(
            core_distances[edges[:, 0].astype(np.intp)],
            core_distances[edges[:, 1].astype(np.intp)],
        ),
        full_distances[mask],
        edges[:, 3],
    )
    return edges


def compute_branch_segmentation(cluster_linkage_trees, thread_pool, **kwargs):
    """Extracts branches from the linkage hierarchies."""
    results = thread_pool(
        delayed(segment_branch_linkage_hierarchy)(cluster_linkage_tree, **kwargs)
        for cluster_linkage_tree in cluster_linkage_trees
    )
    if len(results):
        return tuple(zip(*results))
    return (), (), (), (), ()


def segment_branch_linkage_hierarchy(
    single_linkage_tree,
    allow_single_cluster=False,
    cluster_selection_epsilon=0.0,
    **kwargs,
):
    """Select branches within one cluster."""
    # Return component labels if the graph is disconnected
    if len(single_linkage_tree.shape) == 1:
        return (
            single_linkage_tree,
            np.ones(single_linkage_tree.shape[0], dtype=np.double),
            [0 for _ in range(single_linkage_tree.max() + 1)],
            None,
            None,
        )

    # Run normal branch detection
    (labels, probabilities, stabilities, condensed_tree, linkage_tree) = (
        _tree_to_labels(
            None,
            single_linkage_tree,
            allow_single_cluster=allow_single_cluster,
            cluster_selection_epsilon=cluster_selection_epsilon,
            **kwargs,
        )
    )
    labels, probabilities = update_single_cluster_labels(
        condensed_tree,
        labels,
        probabilities,
        stabilities,
        allow_single_cluster=allow_single_cluster,
        cluster_selection_epsilon=cluster_selection_epsilon,
    )
    return (labels, probabilities, stabilities, condensed_tree, linkage_tree)


def update_labelling(
    cluster_probabilities,
    tree_list,
    points_list,
    centrality_list,
    branch_label_list,
    branch_prob_list,
    branch_pers_list,
    label_sides_as_branches=False,
):
    """Updates the labelling with the detected branches."""
    # Allocate output
    num_points = len(cluster_probabilities)
    labels = -1 * np.ones(num_points, dtype=np.intp)
    probabilities = cluster_probabilities.copy()
    branch_labels = np.zeros(num_points, dtype=np.intp)
    branch_probabilities = np.ones(num_points, dtype=np.double)
    branch_centralities = np.zeros(num_points, dtype=np.double)

    # Compute the labels and probabilities
    running_id = 0
    for tree, _points, _labels, _probs, _centrs, _pers in zip(
        tree_list,
        points_list,
        branch_label_list,
        branch_prob_list,
        centrality_list,
        branch_pers_list,
    ):
        num_branches = len(_pers)
        branch_centralities[_points] = _centrs
        if num_branches <= (1 if label_sides_as_branches else 2) and tree is not None:
            labels[_points] = running_id
            running_id += 1
        else:
            has_noise = int(-1 in _labels)
            labels[_points] = _labels + running_id + has_noise
            branch_labels[_points] = _labels
            branch_probabilities[_points] = _probs
            probabilities[_points] += _probs
            probabilities[_points] /= 2
            running_id += num_branches + has_noise

    # Reorder other parts
    return (
        labels,
        probabilities,
        branch_labels,
        branch_probabilities,
        branch_centralities,
    )


def _remap_edge_lists(edge_lists, internal_to_raw):
    """
    Takes a list of edge lists and replaces the internal indices to raw indices.

    Parameters
    ----------
    edge_lists : list[np.ndarray]
        A list of numpy edgelists with the first two columns indicating
        datapoints.
    internal_to_raw: dict
        A mapping from internal integer index to the raw integer index.
    """
    for graph in edge_lists:
        for edge in graph:
            edge[0] = internal_to_raw[edge[0]]
            edge[1] = internal_to_raw[edge[1]]


def _remap_point_lists(point_lists, internal_to_raw):
    """
    Takes a list of points lists and replaces the internal indices to raw indices.

    Parameters
    ----------
    point_lists : list[np.ndarray]
        A list of numpy arrays with point indices.
    internal_to_raw: dict
        A mapping from internal integer index to the raw integer index.
    """
    for points in point_lists:
        for idx in range(len(points)):
            points[idx] = internal_to_raw[points[idx]]


def _remap_labels(old_labels, finite_index, num_points, fill_value=-1):
    """Creates new label array with infinite points set to -1."""
    new_labels = np.full(num_points, fill_value)
    new_labels[finite_index] = old_labels
    return new_labels


def _remap_probabilities(old_probs, finite_index, num_points):
    """Creates new probability array with infinite points set to 0."""
    new_probs = np.zeros(num_points)
    new_probs[finite_index] = old_probs
    return new_probs


class BranchDetector(BaseEstimator, ClusterMixin):
    """Performs a flare-detection post-processing step to detect branches within
    clusters [1]_.

    For each cluster, a graph is constructed connecting the data points based on
    their mutual reachability distances. Each edge is given a centrality value
    based on how far it lies from the cluster's center. Then, the edges are
    clustered as if that centrality was a distance, progressively removing the
    'center' of each cluster and seeing how many branches remain.

    Parameters
    ----------
    branch_detection_method : str, optional (default=``full``)
        Determines which graph is constructed to detect branches with. Valid
        values are, ordered by increasing computation cost and decreasing
        sensitivity to noise:
        - ``core``: Contains the edges that connect each point to all other
          points within a mutual reachability distance lower than or equal to
          the point's core distance. This is the cluster's subgraph of the
          k-NN graph over the entire data set (with k = ``min_samples``).
        - ``full``: Contains all edges between points in each cluster with a
          mutual reachability distance lower than or equal to the distance of
          the most-distance point in each cluster. These graphs represent the
          0-dimensional simplicial complex of each cluster at the first point in
          the filtration where they contain all their points.

    label_sides_as_branches : bool, optional (default=False),
        When this flag is False, branches are only labelled for clusters with at
        least three branches (i.e., at least y-shapes). Clusters with only two
        branches represent l-shapes. The two branches describe the cluster's
        outsides growing towards each other. Enabling this flag separates these
        branches from each other in the produced labelling.

    min_cluster_size : int, optional (default=None)
        The minimum number of samples in a group for that group to be
        considered a branch; groupings smaller than this size will seen as
        points falling out of a branch. Defaults to the clusterer's min_cluster_size.

    allow_single_cluster : bool, optional (default=None)
        Analogous to ``allow_single_cluster``.

    cluster_selection_method : str, optional (default=None)
        The method used to select branches from the cluster's condensed tree.
        The standard approach for FLASC is to use the ``eom`` approach.
        Options are:
          * ``eom``
          * ``leaf``

    cluster_selection_epsilon: float, optional (default=0.0)
        A lower epsilon threshold. Only branches with a death above this value
        will be considered.

    cluster_selection_persistence: float, optional (default=0.0)
        An eccentricity persistence threshold. Branches with a persistence below
        this value will be merged.

    max_cluster_size : int, optional (default=None)
        A limit to the size of clusters returned by the ``eom`` algorithm. Has
        no effect when using ``leaf`` clustering (where clusters are usually
        small regardless). Note that this should not be used if we want to
        predict the cluster labels for new points in future because
        `approximate_predict` is not aware of this argument.

    Attributes
    ----------
    labels_ : np.ndarray, shape (n_samples, )
        Labels that differentiate all subgroups (clusters and branches). Noisy
        samples are given the label -1.

    probabilities_ : np.ndarray, shape (n_samples, )
        Probabilities considering both cluster and branch membership. Noisy
        samples are assigned 0.

    cluster_labels_ : np.ndarray, shape (n_samples, )
        The cluster labels for each point in the data set. Noisy samples are
        given the label -1.

    cluster_probabilities_ : np.ndarray, shape (n_samples, )
        The cluster probabilities for each point in the data set. Noisy samples
        are assigned 1.0.

    branch_labels_ : np.ndarray, shape (n_samples, )
        Branch labels for each point. Noisy samples are given the label -1.

    branch_probabilities_ : np.ndarray, shape (n_samples, )
        Branch membership strengths for each point. Noisy samples are
        assigned 0.

    branch_persistences_ : tuple (n_clusters)
        A branch persistence (eccentricity range) for each detected branch.

    approximation_graph_ : ApproximationGraph
        The graphs used to detect branches in each cluster stored as a numpy
        array with four columns: source, target, centrality, mutual reachability
        distance. Points are labelled by their row-index into the input data.
        The edges contained in the graphs depend on the ``branch_detection_method``:
        - ``core``: Contains the edges that connect each point to all other
          points in a cluster within a mutual reachability distance lower than
          or equal to the point's core distance. This is an extension of the
          minimum spanning tree introducing only edges with equal distances. The
          reachability distance introduces ``num_points`` * ``min_samples`` of
          such edges.
        - ``full``: Contains all edges between points in each cluster with a
          mutual reachability distance lower than or equal to the distance of
          the most-distance point in each cluster. These graphs represent the
          0-dimensional simplicial complex of each cluster at the first point in
          the filtration where they contain all their points.

    condensed_trees_ : tuple (n_clusters)
        A condensed branch hierarchy for each cluster produced during the
        branch detection step. Data points are numbered with in-cluster ids.

    linkage_trees_ : tuple (n_clusters)
        A single linkage tree for each cluster produced during the branch
        detection step, in the scipy hierarchical clustering format.
        (see http://docs.scipy.org/doc/scipy/reference/cluster.hierarchy.html).
        Data points are numbered with in-cluster ids.

    centralities_ : np.ndarray, shape (n_samples, )
        Centrality values for each point in a cluster. Overemphasizes points'
        eccentricity within the cluster as the values are based on minimum
        spanning trees that do not contain the equally distanced edges resulting
        from the mutual reachability distance.

    cluster_points_ : list (n_clusters)
        The data point row indices for each cluster.

    References
    ----------
    .. [1] Bot D.M., Peeters J., Liesenborgs J., Aerts J. 2025. FLASC: a
    flare-sensitive clustering algorithm. PeerJ Computer Science 11:e2792
    https://doi.org/10.7717/peerj-cs.2792.
    """

    def __init__(
        self,
        branch_detection_method="full",
        label_sides_as_branches=False,
        min_cluster_size=None,
        max_cluster_size=None,
        allow_single_cluster=None,
        cluster_selection_method=None,
        cluster_selection_epsilon=0.0,
        cluster_selection_persistence=0.0,
    ):
        self.branch_detection_method = branch_detection_method
        self.label_sides_as_branches = label_sides_as_branches
        self.min_cluster_size = min_cluster_size
        self.max_cluster_size = max_cluster_size
        self.allow_single_cluster = allow_single_cluster
        self.cluster_selection_method = cluster_selection_method
        self.cluster_selection_epsilon = cluster_selection_epsilon
        self.cluster_selection_persistence = cluster_selection_persistence

        self._approximation_graphs = None
        self._condensed_trees = None
        self._cluster_linkage_trees = None
        self._branch_exemplars = None

    def fit(self, clusterer, labels=None, probabilities=None):
        """
        Perform a flare-detection post-processing step to detect branches within
        clusters.

        Parameters
        ----------
        clusterer : HDBSCAN
            A fitted HDBSCAN object with branch detection data generated.

        labels : np.ndarray, shape (n_samples, ), optional (default=None)
            The cluster labels for each point in the data set. If not provided, the
            clusterer's labels will be used.

        probabilities : np.ndarray, shape (n_samples, ), optional (default=None)
            The cluster probabilities for each point in the data set. If not provided,
            the clusterer's probabilities will be used, or all points will be given
            1.0 probability if labels are overridden.

        Returns
        -------
        self : object
            Returns self.
        """
        self._clusterer = clusterer
        kwargs = self.get_params()
        (
            self.labels_,
            self.probabilities_,
            self.cluster_labels_,
            self.cluster_probabilities_,
            self.branch_labels_,
            self.branch_probabilities_,
            self.branch_persistences_,
            self._approximation_graphs,
            self._condensed_trees,
            self._linkage_trees,
            self.centralities_,
            self.cluster_points_,
        ) = detect_branches_in_clusters(clusterer, labels, probabilities, **kwargs)

        return self

    def fit_predict(self, clusterer, labels=None, probabilities=None):
        """
        Perform a flare-detection post-processing step to detect branches within
        clusters [1]_.

        Parameters
        ----------
        clusterer : HDBSCAN
            A fitted HDBSCAN object with branch detection data generated.

        labels : np.ndarray, shape (n_samples, ), optional (default=None)
            The cluster labels for each point in the data set. If not provided, the
            clusterer's labels will be used.

        probabilities : np.ndarray, shape (n_samples, ), optional (default=None)
            The cluster probabilities for each point in the data set. If not provided,
            the clusterer's probabilities will be used, or all points will be given
            1.0 probability if labels are overridden.

        Returns
        -------
        labels : ndarray, shape (n_samples, )
            subgroup labels differentiated by cluster and branch.
        """
        self.fit(clusterer, labels, probabilities)
        return self.labels_

    def weighted_centroid(self, label_id, data=None):
        """Provides an approximate representative point for a given branch.
        Note that this technique assumes a euclidean metric for speed of
        computation. For more general metrics use the ``weighted_medoid`` method
        which is slower, but can work with the metric the model trained with.

        Parameters
        ----------
        label_id: int
            The id of the cluster to compute a centroid for.

        data : np.ndarray (n_samples, n_features), optional (default=None)
            A dataset to use instead of the raw data that was clustered on.

        Returns
        -------
        centroid: array of shape (n_features,)
            A representative centroid for cluster ``label_id``.
        """
        if self.labels_ is None:
            raise AttributeError("Model has not been fit to data")
        if self._clusterer._raw_data is None and data is None:
            raise AttributeError("Raw data not available")
        if label_id == -1:
            raise ValueError(
                "Cannot calculate weighted centroid for -1 cluster "
                "since it is a noise cluster"
            )
        if data is None:
            data = self._clusterer._raw_data
        mask = self.labels_ == label_id
        cluster_data = data[mask]
        cluster_membership_strengths = self.probabilities_[mask]

        return np.average(cluster_data, weights=cluster_membership_strengths, axis=0)

    def weighted_medoid(self, label_id, data=None):
        """Provides an approximate representative point for a given branch.

        Note that this technique can be very slow and memory intensive for large
        clusters. For faster results use the ``weighted_centroid`` method which
        is faster, but assumes a euclidean metric.

        Parameters
        ----------
        label_id: int
            The id of the cluster to compute a medoid for.

        data : np.ndarray (n_samples, n_features), optional (default=None)
            A dataset to use instead of the raw data that was clustered on.

        Returns
        -------
        centroid: array of shape (n_features,)
            A representative medoid for cluster ``label_id``.
        """
        if self.labels_ is None:
            raise AttributeError("Model has not been fit to data")
        if self._clusterer._raw_data is None and data is None:
            raise AttributeError("Raw data not available")
        if label_id == -1:
            raise ValueError(
                "Cannot calculate weighted centroid for -1 cluster "
                "since it is a noise cluster"
            )
        if data is None:
            data = self._clusterer._raw_data
        mask = self.labels_ == label_id
        cluster_data = data[mask]
        cluster_membership_strengths = self.probabilities_[mask]

        dist_metric = self._clusterer.branch_detection_data_.dist_metric
        dist_mat = dist_metric.pairwise(cluster_data) * cluster_membership_strengths
        medoid_index = np.argmin(dist_mat.sum(axis=1))
        return cluster_data[medoid_index]

    @property
    def approximation_graph_(self):
        """See :class:`~hdbscan.branches.BranchDetector` for documentation."""
        if self._approximation_graphs is None:
            raise AttributeError(
                "No approximation graph was generated; try running fit first."
            )
        return ApproximationGraph(
            self._approximation_graphs,
            self.labels_,
            self.probabilities_,
            self.centralities_,
            self.cluster_labels_,
            self.cluster_probabilities_,
            self.branch_labels_,
            self.branch_probabilities_,
            lens_name="centrality",
            sub_cluster_name="branch",
            raw_data=self._clusterer._raw_data,
        )

    @property
    def condensed_trees_(self):
        """See :class:`~hdbscan.branches.BranchDetector` for documentation."""
        if self._condensed_trees is None:
            raise AttributeError(
                "No condensed trees were generated; try running fit first."
            )

        return [
            CondensedTree(tree, self.branch_labels_[points])
            for tree, points in zip(
                self._condensed_trees, 
                self.cluster_points_,
            )
        ]

    @property
    def linkage_trees_(self):
        """See :class:`~hdbscan.branches.BranchDetector` for documentation."""
        if self._linkage_trees is None:
            raise AttributeError(
                "No linkage trees were generated; try running fit first."
            )
        return [
            SingleLinkageTree(tree) if tree is not None else None
            for tree in self._linkage_trees
        ]

    @property
    def exemplars_(self):
        """See :class:`~hdbscan.branches.BranchDetector` for documentation."""
        if self._branch_exemplars is not None:
            return self._branch_exemplars
        if self._clusterer._raw_data is None:
            raise AttributeError(
                "Branch exemplars not available with precomputed " "distances."
            )
        if self._condensed_trees is None:
            raise AttributeError("No branches detected; try running fit first.")

        num_clusters = len(self._condensed_trees)
        branch_cluster_trees = [
            branch_tree[branch_tree["child_size"] > 1]
            for branch_tree in self._condensed_trees
        ]
        selected_branch_ids = [
            sorted(branch_tree._select_clusters())
            for branch_tree in self.condensed_trees_
        ]

        self._branch_exemplars = [None] * num_clusters

        for i, points in enumerate(self.cluster_points_):
            selected_branches = selected_branch_ids[i]
            if len(selected_branches) <= (1 if self.label_sides_as_branches else 2):
                continue

            self._branch_exemplars[i] = []
            raw_condensed_tree = self._condensed_trees[i]

            for branch in selected_branches:
                _branch_exemplars = np.array([], dtype=np.intp)
                for leaf in recurse_leaf_dfs(branch_cluster_trees[i], np.intp(branch)):
                    leaf_max_lambda = raw_condensed_tree["lambda_val"][
                        raw_condensed_tree["parent"] == leaf
                    ].max()
                    candidates = raw_condensed_tree["child"][
                        (raw_condensed_tree["parent"] == leaf)
                        & (raw_condensed_tree["lambda_val"] == leaf_max_lambda)
                    ]
                    _branch_exemplars = np.hstack([_branch_exemplars, candidates])
                ids = points[_branch_exemplars]
                self._branch_exemplars[i].append(self._clusterer._raw_data[ids, :])

        return self._branch_exemplars


def approximate_predict_branch(branch_detector, points_to_predict):
    """Predict the cluster and branch label of new points.

    Extends ``approximate_predict`` to also predict in which branch
    new points lie (if the cluster they are part of has branches).

    Parameters
    ----------
    branch_detector : BranchDetector
        A clustering object that has been fit to vector input data.

    points_to_predict : array, or array-like (n_samples, n_features)
        The new data points to predict cluster labels for. They should
        have the same dimensionality as the original dataset over which
        clusterer was fit.

    Returns
    -------
    labels : array (n_samples,)
        The predicted cluster and branch labels.

    probabilities : array (n_samples,)
        The soft cluster scores for each.

    cluster_labels : array (n_samples,)
        The predicted cluster labels.

    cluster_probabilities : array (n_samples,)
        The soft cluster scores for each.

    branch_labels : array (n_samples,)
        The predicted cluster labels.

    branch_probabilities : array (n_samples,)
        The soft cluster scores for each.
    """

    cluster_labels, cluster_probabilities, connecting_points = approximate_predict(
        branch_detector._clusterer, points_to_predict, return_connecting_points=True
    )

    num_predict = len(points_to_predict)
    labels = np.empty(num_predict, dtype=np.intp)
    probabilities = np.zeros(num_predict, dtype=np.double)
    branch_labels = np.zeros(num_predict, dtype=np.intp)
    branch_probabilities = np.ones(num_predict, dtype=np.double)

    min_num_branches = 2 if not branch_detector.label_sides_as_branches else 1
    for i, (label, prob, connecting_point) in enumerate(
        zip(cluster_labels, cluster_probabilities, connecting_points)
    ):
        if label < 0:
            labels[i] = -1
        elif len(branch_detector.branch_persistences_[label]) <= min_num_branches:
            labels[i] = label
            probabilities[i] = prob
        else:
            labels[i] = branch_detector.labels_[connecting_point]
            branch_labels[i] = branch_detector.branch_labels_[connecting_point]
            branch_probabilities[i] = branch_detector.branch_probabilities_[
                connecting_point
            ]
            probabilities[i] = (prob + branch_probabilities[i]) / 2
    return (
        labels,
        probabilities,
        cluster_labels,
        cluster_probabilities,
        branch_labels,
        branch_probabilities,
    )


class SequentialPool:
    """API of a Joblib Parallel pool but sequential execution"""

    def __init__(self):
        self.n_jobs = 1

    def __call__(self, jobs):
        return [fun(*args, **kwargs) for (fun, args, kwargs) in jobs]
