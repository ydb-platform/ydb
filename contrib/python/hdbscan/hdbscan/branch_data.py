import numpy as np
from sklearn.neighbors import KDTree, BallTree
from .dist_metrics import DistanceMetric


class BranchDetectionData(object):
    """Input data for branch detection functionality.

    Recreates and caches internal data structures from the clustering stage.

    Parameters
    ----------

    data : array (n_samples, n_features)
        The original data set that was clustered.

    labels : array (n_samples)
        The cluster labels for every point in the data set.

    condensed_tree : array (n_points + n_merges, 4)
        The condensed tree produced during clustering, used to extract outliers.

    min_samples : int
        The min_samples value used in clustering.

    tree_type : string, optional
        Which type of space tree to use for core distance computation.
        One of:
            * ``kdtree``
            * ``balltree``

    metric : string, optional
        The metric used to determine distance for the clustering.
        This is the metric that will be used for the space tree to determine
        core distances etc.

    **kwargs :
        Any further arguments to the metric.

    Attributes
    ----------

    all_finite : bool
        Whether the data set contains any infinite or NaN values.

    finite_index : array (n_samples)
        The indices of the finite data points in the original data set.

    internal_to_raw : dict
        A mapping from the finite data set indices to the original data set.

    tree : KDTree or BallTree
        A space partitioning tree that can be queried for nearest neighbors if
        the metric is supported by a KDTree or BallTree.

    neighbors : array (n_samples, min_samples)
        The nearest neighbor for every non-noise point in the original data set.

    core_distances : array (n_samples)
        The core distance for every non-noise point in the original data set.

    dist_metric : callable
        Accelerated distance metric function.
    """

    _tree_type_map = {"kdtree": KDTree, "balltree": BallTree}

    def __init__(
        self,
        data,
        labels,
        condensed_tree,
        min_samples,
        tree_type="kdtree",
        metric="euclidean",
        **kwargs,
    ):
        clean_data = data.astype(np.float64)
        last_outlier = np.searchsorted(condensed_tree["lambda_val"], 0.0, side="right")
        if last_outlier == 0:
            self.all_finite = True
            self.internal_to_raw = None
            self.finite_index = None
        else:
            self.all_finite = False
            self.finite_index = np.setdiff1d(
                np.arange(data.shape[0]),
                condensed_tree["child"][:last_outlier]
            )
            labels = labels[self.finite_index]
            clean_data = clean_data[self.finite_index]
            self.internal_to_raw = {
                x: y for x, y in enumerate(self.finite_index)
            }

        # Construct tree
        self.tree = self._tree_type_map[tree_type](clean_data, metric=metric, **kwargs)
        self.dist_metric = DistanceMetric.get_metric(metric, **kwargs)

        # Allocate to maintain data point indices
        self.core_distances = np.full(clean_data.shape[0], np.nan)
        self.neighbors = np.full((clean_data.shape[0], min_samples), -1, dtype=np.int64)

        # Find neighbors for non-noise points
        noise_mask = labels != -1
        if noise_mask.any():
            distances, self.neighbors[noise_mask, :] = self.tree.query(
                clean_data[noise_mask], k=min_samples
            )
            self.core_distances[noise_mask] = distances[:, -1]

