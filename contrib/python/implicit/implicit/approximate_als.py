""" Models that use various Approximate Nearest Neighbours libraries in order to quickly
generate recommendations and lists of similar items.

See http://www.benfrederickson.com/approximate-nearest-neighbours-for-recommender-systems/
"""
import itertools
import logging

import numpy

import implicit.cuda
from implicit.als import AlternatingLeastSquares

log = logging.getLogger("implicit")


def augment_inner_product_matrix(factors):
    """ This function transforms a factor matrix such that an angular nearest neighbours search
    will return top related items of the inner product.

    This involves transforming each row by adding one extra dimension as suggested in the paper:
    "Speeding Up the Xbox Recommender System Using a Euclidean Transformation for Inner-Product
    Spaces" https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/XboxInnerProduct.pdf

    Basically this involves transforming each feature vector so that they have the same norm, which
    means the cosine of this transformed vector is proportional to the dot product (if the other
    vector in the cosine has a 0 in the extra dimension). """
    norms = numpy.linalg.norm(factors, axis=1)
    max_norm = norms.max()

    # add an extra dimension so that the norm of each row is the same
    # (max_norm)
    extra_dimension = numpy.sqrt(max_norm ** 2 - norms ** 2)
    return max_norm, numpy.append(factors, extra_dimension.reshape(norms.shape[0], 1), axis=1)


class NMSLibAlternatingLeastSquares(AlternatingLeastSquares):

    """ Speeds up the base :class:`~implicit.als.AlternatingLeastSquares` model by using
    `NMSLib <https://github.com/searchivarius/nmslib>`_ to create approximate nearest neighbours
    indices of the latent factors.

    Parameters
    ----------
    method : str, optional
        The NMSLib method to use
    index_params: dict, optional
        Optional params to send to the createIndex call in NMSLib
    query_params: dict, optional
        Optional query time params for the NMSLib 'setQueryTimeParams' call
    approximate_similar_items : bool, optional
        whether or not to build an NMSLIB index for computing similar_items
    approximate_recommend : bool, optional
        whether or not to build an NMSLIB index for the recommend call
    random_state : int, RandomState or None, optional
        The random state for seeding the initial item and user factors.
        Default is None.

    Attributes
    ----------
    similar_items_index : nmslib.FloatIndex
        NMSLib index for looking up similar items in the cosine space formed by the latent
        item_factors

    recommend_index : nmslib.FloatIndex
        NMSLib index for looking up similar items in the inner product space formed by the latent
        item_factors
    """

    def __init__(self,
                 approximate_similar_items=True, approximate_recommend=True,
                 method='hnsw', index_params=None, query_params=None,
                 random_state=None, *args, **kwargs):
        if index_params is None:
            index_params = {'M': 16, 'post': 0, 'efConstruction': 400}
        if query_params is None:
            query_params = {'ef': 90}

        self.similar_items_index = None
        self.recommend_index = None

        self.approximate_similar_items = approximate_similar_items
        self.approximate_recommend = approximate_recommend
        self.method = method

        self.index_params = index_params
        self.query_params = query_params

        super(NMSLibAlternatingLeastSquares, self).__init__(*args,
                                                            random_state=random_state,
                                                            **kwargs)

    def fit(self, Ciu, show_progress=True):
        # nmslib can be a little chatty when first imported, disable some of
        # the logging
        logging.getLogger('nmslib').setLevel(logging.WARNING)
        import nmslib

        # train the model
        super(NMSLibAlternatingLeastSquares, self).fit(Ciu, show_progress)

        # create index for similar_items
        if self.approximate_similar_items:
            log.debug("Building nmslib similar items index")
            self.similar_items_index = nmslib.init(
                method=self.method, space='cosinesimil')

            # there are some numerical instability issues here with
            # building a cosine index with vectors with 0 norms, hack around this
            # by just not indexing them
            norms = numpy.linalg.norm(self.item_factors, axis=1)
            ids = numpy.arange(self.item_factors.shape[0])

            # delete zero valued rows from the matrix
            item_factors = numpy.delete(self.item_factors, ids[norms == 0], axis=0)
            ids = ids[norms != 0]

            self.similar_items_index.addDataPointBatch(item_factors, ids=ids)
            self.similar_items_index.createIndex(self.index_params,
                                                 print_progress=show_progress)
            self.similar_items_index.setQueryTimeParams(self.query_params)

        # build up a separate index for the inner product (for recommend
        # methods)
        if self.approximate_recommend:
            log.debug("Building nmslib recommendation index")
            self.max_norm, extra = augment_inner_product_matrix(
                self.item_factors)
            self.recommend_index = nmslib.init(
                method='hnsw', space='cosinesimil')
            self.recommend_index.addDataPointBatch(extra)
            self.recommend_index.createIndex(self.index_params, print_progress=show_progress)
            self.recommend_index.setQueryTimeParams(self.query_params)

    def similar_items(self, itemid, N=10):
        if not self.approximate_similar_items:
            return super(NMSLibAlternatingLeastSquares, self).similar_items(itemid, N)

        neighbours, distances = self.similar_items_index.knnQuery(
            self.item_factors[itemid], N)
        return zip(neighbours, 1.0 - distances)

    def recommend(self, userid, user_items, N=10, filter_already_liked_items=True,
                  filter_items=None, recalculate_user=False):
        if not self.approximate_recommend:
            return super(NMSLibAlternatingLeastSquares,
                         self).recommend(userid, user_items, N=N,
                                         filter_items=filter_items,
                                         recalculate_user=recalculate_user)

        user = self._user_factor(userid, user_items, recalculate_user)

        # calculate the top N items, removing the users own liked items from
        # the results
        liked = set()
        if filter_already_liked_items:
            liked.update(user_items[userid].indices)
        if filter_items:
            liked.update(filter_items)
        count = N + len(liked)

        query = numpy.append(user, 0)
        ids, dist = self.recommend_index.knnQuery(query, count)

        # convert the distances from euclidean to cosine distance,
        # and then rescale the cosine distance to go back to inner product
        scaling = self.max_norm * numpy.linalg.norm(query)
        dist = scaling * (1.0 - dist)
        return list(itertools.islice((rec for rec in zip(ids, dist) if rec[0] not in liked), N))


class AnnoyAlternatingLeastSquares(AlternatingLeastSquares):

    """A version of the :class:`~implicit.als.AlternatingLeastSquares` model that uses an
    `Annoy <https://github.com/spotify/annoy>`_ index to calculate similar items and
    recommend items.

    Parameters
    ----------
    n_trees : int, optional
        The number of trees to use when building the Annoy index. More trees gives higher precision
        when querying.
    search_k : int, optional
        Provides a way to search more trees at runtime, giving the ability to have more accurate
        results at the cost of taking more time.
    approximate_similar_items : bool, optional
        whether or not to build an Annoy index for computing similar_items
    approximate_recommend : bool, optional
        whether or not to build an Annoy index for the recommend call
    random_state : int, RandomState or None, optional
        The random state for seeding the initial item and user factors.
        Default is None.

    Attributes
    ----------
    similar_items_index : annoy.AnnoyIndex
        Annoy index for looking up similar items in the cosine space formed by the latent
        item_factors

    recommend_index : annoy.AnnoyIndex
        Annoy index for looking up similar items in the inner product space formed by the latent
        item_factors
    """

    def __init__(self, approximate_similar_items=True, approximate_recommend=True,
                 n_trees=50, search_k=-1, random_state=None, *args, **kwargs):

        super(AnnoyAlternatingLeastSquares, self).__init__(*args,
                                                           random_state=random_state,
                                                           **kwargs)

        self.similar_items_index = None
        self.recommend_index = None

        self.approximate_similar_items = approximate_similar_items
        self.approximate_recommend = approximate_recommend

        self.n_trees = n_trees
        self.search_k = search_k

    def fit(self, Ciu, show_progress=True):
        # delay loading the annoy library in case its not installed here
        import annoy

        # train the model
        super(AnnoyAlternatingLeastSquares, self).fit(Ciu, show_progress)

        # build up an Annoy Index with all the item_factors (for calculating
        # similar items)
        if self.approximate_similar_items:
            log.debug("Building annoy similar items index")

            self.similar_items_index = annoy.AnnoyIndex(
                self.item_factors.shape[1], 'angular')
            for i, row in enumerate(self.item_factors):
                self.similar_items_index.add_item(i, row)
            self.similar_items_index.build(self.n_trees)

        # build up a separate index for the inner product (for recommend
        # methods)
        if self.approximate_recommend:
            log.debug("Building annoy recommendation index")
            self.max_norm, extra = augment_inner_product_matrix(self.item_factors)
            self.recommend_index = annoy.AnnoyIndex(extra.shape[1], 'angular')
            for i, row in enumerate(extra):
                self.recommend_index.add_item(i, row)
            self.recommend_index.build(self.n_trees)

    def similar_items(self, itemid, N=10):
        if not self.approximate_similar_items:
            return super(AnnoyAlternatingLeastSquares, self).similar_items(itemid, N)

        neighbours, dist = self.similar_items_index.get_nns_by_item(itemid, N,
                                                                    search_k=self.search_k,
                                                                    include_distances=True)
        # transform distances back to cosine from euclidean distance
        return zip(neighbours, 1 - (numpy.array(dist) ** 2) / 2)

    def recommend(self, userid, user_items, N=10, filter_already_liked_items=True,
                  filter_items=None, recalculate_user=False):
        if not self.approximate_recommend:
            return super(AnnoyAlternatingLeastSquares,
                         self).recommend(userid, user_items, N=N,
                                         filter_items=filter_items,
                                         recalculate_user=recalculate_user)

        user = self._user_factor(userid, user_items, recalculate_user)

        # calculate the top N items, removing the users own liked items from
        # the results
        liked = set()
        if filter_already_liked_items:
            liked.update(user_items[userid].indices)
        if filter_items:
            liked.update(filter_items)
        count = N + len(liked)

        query = numpy.append(user, 0)
        ids, dist = self.recommend_index.get_nns_by_vector(query, count, include_distances=True,
                                                           search_k=self.search_k)

        # convert the distances from euclidean to cosine distance,
        # and then rescale the cosine distance to go back to inner product
        scaling = self.max_norm * numpy.linalg.norm(query)
        dist = scaling * (1 - (numpy.array(dist) ** 2) / 2)
        return list(itertools.islice((rec for rec in zip(ids, dist) if rec[0] not in liked), N))


class FaissAlternatingLeastSquares(AlternatingLeastSquares):

    """ Speeds up the base :class:`~implicit.als.AlternatingLeastSquares` model by using
    `Faiss <https://github.com/facebookresearch/faiss>`_ to create approximate nearest neighbours
    indices of the latent factors.


    Parameters
    ----------
    nlist : int, optional
        The number of cells to use when building the Faiss index.
    nprobe : int, optional
        The number of cells to visit to perform a search.
    use_gpu : bool, optional
        Whether or not to enable run Faiss on the GPU. Requires faiss to have been
        built with GPU support.
    approximate_similar_items : bool, optional
        whether or not to build an Faiss index for computing similar_items
    approximate_recommend : bool, optional
        whether or not to build an Faiss index for the recommend call
    random_state : int, RandomState or None, optional
        The random state for seeding the initial item and user factors.
        Default is None.

    Attributes
    ----------
    similar_items_index : faiss.IndexIVFFlat
        Faiss index for looking up similar items in the cosine space formed by the latent
        item_factors

    recommend_index : faiss.IndexIVFFlat
        Faiss index for looking up similar items in the inner product space formed by the latent
        item_factors
    """

    def __init__(self, approximate_similar_items=True, approximate_recommend=True,
                 nlist=400, nprobe=20, use_gpu=implicit.cuda.HAS_CUDA, random_state=None,
                 *args, **kwargs):

        self.similar_items_index = None
        self.recommend_index = None

        self.approximate_similar_items = approximate_similar_items
        self.approximate_recommend = approximate_recommend

        # hyper-parameters for FAISS
        self.nlist = nlist
        self.nprobe = nprobe
        super(FaissAlternatingLeastSquares, self).__init__(*args,
                                                           random_state=random_state,
                                                           use_gpu=use_gpu, **kwargs)

    def fit(self, Ciu, show_progress=True):
        import faiss

        # train the model
        super(FaissAlternatingLeastSquares, self).fit(Ciu, show_progress)

        self.quantizer = faiss.IndexFlat(self.factors)

        if self.use_gpu:
            self.gpu_resources = faiss.StandardGpuResources()

        item_factors = self.item_factors.astype('float32')

        if self.approximate_recommend:
            log.debug("Building faiss recommendation index")

            # build up a inner product index here
            if self.use_gpu:
                index = faiss.GpuIndexIVFFlat(self.gpu_resources, self.factors, self.nlist,
                                              faiss.METRIC_INNER_PRODUCT)
            else:
                index = faiss.IndexIVFFlat(self.quantizer, self.factors, self.nlist,
                                           faiss.METRIC_INNER_PRODUCT)

            index.train(item_factors)
            index.add(item_factors)
            index.nprobe = self.nprobe
            self.recommend_index = index

        if self.approximate_similar_items:
            log.debug("Building faiss similar items index")

            # likewise build up cosine index for similar_items, using an inner product
            # index on normalized vectors`
            norms = numpy.linalg.norm(item_factors, axis=1)
            norms[norms == 0] = 1e-10

            normalized = (item_factors.T / norms).T.astype('float32')
            if self.use_gpu:
                index = faiss.GpuIndexIVFFlat(self.gpu_resources, self.factors, self.nlist,
                                              faiss.METRIC_INNER_PRODUCT)
            else:
                index = faiss.IndexIVFFlat(self.quantizer, self.factors, self.nlist,
                                           faiss.METRIC_INNER_PRODUCT)

            index.train(normalized)
            index.add(normalized)
            index.nprobe = self.nprobe
            self.similar_items_index = index

    def similar_items(self, itemid, N=10):
        if not self.approximate_similar_items or (self.use_gpu and N >= 1024):
            return super(FaissAlternatingLeastSquares, self).similar_items(itemid, N)

        factors = self.item_factors[itemid]
        factors /= numpy.linalg.norm(factors)
        (dist,), (ids,) = self.similar_items_index.search(factors.reshape(1, -1).astype('float32'),
                                                          N)
        return zip(ids, dist)

    def recommend(self, userid, user_items, N=10, filter_already_liked_items=True,
                  filter_items=None, recalculate_user=False):
        if not self.approximate_recommend:
            return super(FaissAlternatingLeastSquares,
                         self).recommend(userid, user_items, N=N,
                                         filter_items=filter_items,
                                         recalculate_user=recalculate_user)

        user = self._user_factor(userid, user_items, recalculate_user)

        # calculate the top N items, removing the users own liked items from
        # the results
        liked = set()
        if filter_already_liked_items:
            liked.update(user_items[userid].indices)
        if filter_items:
            liked.update(filter_items)
        count = N + len(liked)

        # the GPU variant of faiss doesn't support returning more than 1024 results.
        # fall back to the exact match when this happens
        if self.use_gpu and count >= 1024:
            return super(FaissAlternatingLeastSquares,
                         self).recommend(userid, user_items, N=N,
                                         filter_items=filter_items,
                                         recalculate_user=recalculate_user)

        # faiss expects multiple queries - convert query to a matrix
        # and results back to single vectors
        query = user.reshape(1, -1).astype('float32')
        (dist,), (ids,) = self.recommend_index.search(query, count)

        # convert the distances from euclidean to cosine distance,
        # and then rescale the cosine distance to go back to inner product
        return list(itertools.islice((rec for rec in zip(ids, dist) if rec[0] not in liked), N))
