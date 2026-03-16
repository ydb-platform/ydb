""" Base class for recommendation algorithms in this package """
# distutils: language = c++
# cython: language_level=3

import itertools
from abc import ABCMeta, abstractmethod
from tqdm.auto import tqdm
import numpy as np
import multiprocessing
from scipy.sparse import csr_matrix
import cython
from cython.parallel import prange
from math import ceil

# Define wrapper for C++ sorting function
cdef extern from "topnc.h":
    cdef void fargsort_c(float A[], int n_row, int m_row, int m_cols, int ktop, int B[]) nogil


class ModelFitError(Exception):
    pass


class RecommenderBase(object):
    """ Defines the interface that all recommendations models here expose """
    __metaclass__ = ABCMeta

    @abstractmethod
    def fit(self, item_users):
        """
        Trains the model on a sparse matrix of item/user/weight

        Parameters
        ----------
        item_user : csr_matrix
            A matrix of shape (number_of_items, number_of_users). The nonzero
            entries in this matrix are the items that are liked by each user.
            The values are how confident you are that the item is liked by the user.
        """
        pass

    @abstractmethod
    def recommend(self, userid, user_items,
                  N=10, filter_already_liked_items=True, filter_items=None, recalculate_user=False):
        """
        Recommends items for a user

        Calculates the N best recommendations for a user, and returns a list of itemids, score.

        Parameters
        ----------
        userid : int
            The userid to calculate recommendations for
        user_items : csr_matrix
            A sparse matrix of shape (number_users, number_items). This lets us look
            up the liked items and their weights for the user. This is used to filter out
            items that have already been liked from the output, and to also potentially
            calculate the best items for this user.
        N : int, optional
            The number of results to return
        filter_already_liked_items: bool, optional
            When true, don't return items present in the training set that were rated
            by the specificed user.
        filter_items : sequence of ints, optional
            List of extra item ids to filter out from the output
        recalculate_user : bool, optional
            When true, don't rely on stored user state and instead recalculate from the
            passed in user_items

        Returns
        -------
        list
            List of (itemid, score) tuples
        """
        pass

    @abstractmethod
    def rank_items(self, userid, user_items, selected_items, recalculate_user=False):
        """
        Rank given items for a user and returns sorted item list.

        Parameters
        ----------
        userid : int
            The userid to calculate recommendations for
        user_items : csr_matrix
            A sparse matrix of shape (number_users, number_items). This lets us look
            up the liked items and their weights for the user. This is used to filter out
            items that have already been liked from the output, and to also potentially
            calculate the best items for this user.
        selected_items : List of itemids
        recalculate_user : bool, optional
            When true, don't rely on stored user state and instead recalculate from the
            passed in user_items

        Returns
        -------
        list
            List of (itemid, score) tuples. it only contains items that appears in
            input parameter selected_items
        """
        pass

    @abstractmethod
    def similar_users(self, userid, N=10):
        """
        Calculates a list of similar users

        Parameters
        ----------
        userid : int
            The row id of the user to retrieve similar users for
        N : int, optional
            The number of similar users to return

        Returns
        -------
        list
            List of (userid, score) tuples
        """
        pass

    @abstractmethod
    def similar_items(self, itemid, N=10, react_users=None, recalculate_item=False):

        """
        Calculates a list of similar items

        Parameters
        ----------
        itemid : int
            The row id of the item to retrieve similar items for
        N : int, optional
            The number of similar items to return
        react_users : csr_matrix, optional
            A sparse matrix of shape (number_items, number_users). This lets us look
            up the reacted users and their weights for the item.
        recalculate_item : bool, optional
            When true, don't rely on stored item state and instead recalculate from the
            passed in react_users

        Returns
        -------
        list
            List of (itemid, score) tuples
        """
        pass


class MatrixFactorizationBase(RecommenderBase):
    """ MatrixFactorizationBase contains common functionality for recommendation models.

    Attributes
    ----------
    item_factors : ndarray
        Array of latent factors for each item in the training set
    user_factors : ndarray
        Array of latent factors for each user in the training set
     """
    def __init__(self):
        # learned parameters
        self.item_factors = None
        self.user_factors = None

        # cache of user, item norms (useful for calculating similar items)
        self._user_norms, self._item_norms = None, None

    def recommend(self, userid, user_items,
                  N=10, filter_already_liked_items=True, filter_items=None, recalculate_user=False):
        user = self._user_factor(userid, user_items, recalculate_user)

        liked = set()
        if filter_already_liked_items:
            liked.update(user_items[userid].indices)
        if filter_items:
            liked.update(filter_items)

        # calculate the top N items, removing the users own liked items from the results
        scores = self.item_factors.dot(user)

        count = N + len(liked)
        if count < len(scores):
            ids = np.argpartition(scores, -count)[-count:]
            best = sorted(zip(ids, scores[ids]), key=lambda x: -x[1])
        else:
            best = sorted(enumerate(scores), key=lambda x: -x[1])
        return list(itertools.islice((rec for rec in best if rec[0] not in liked), N))

    @cython.boundscheck(False)
    @cython.wraparound(False)
    @cython.nonecheck(False)
    def recommend_all(self, user_items, int N=10,
                      recalculate_user=False, filter_already_liked_items=True, filter_items=None,
                      int num_threads=0, show_progress=True, int batch_size=0,
                      int users_items_offset=0):
        """
        Recommends items for all users

        Calculates the N best recommendations for all users, and returns numpy ndarray of
        shape (number_users, N) with item's ids in reversed probability order

        Parameters
        ----------
        self : implicit.als.AlternatingLeastSquares
            The fitted recommendation model
        user_items : csr_matrix
            A sparse matrix of shape (number_users, number_items). This lets us look
            up the liked items and their weights for the user. This is used to filter out
            items that have already been liked from the output, and to also potentially
            calculate the best items for this user.
        N : int, optional
            The number of results to return
        recalculate_user : bool, optional
            When true, don't rely on stored user state and instead recalculate from the
            passed in user_items
        filter_already_liked_items : bool, optional
            This is used to filter out items that have already been liked from the user_items
        filter_items: list, optional
            List of item id's to exclude from recommendations for all users
        num_threads : int, optional
            The number of threads to use for sorting scores in parallel by users. Default is
            number of cores on machine
        show_progress : bool, optional
            Whether to show a progress bar
        batch_size : int, optional
            To optimise memory usage while matrix multiplication, users are separated into groups
            and scored iteratively. By default batch_size == num_threads * 100
        users_items_offset : int, optional
            Allow to pass a slice of user_items matrix to split calculations

        Returns
        -------
        numpy ndarray
            Array of (number_users, N) with item's ids in descending probability order
        """

        # Check N possibility
        if filter_already_liked_items:
            max_row_n = user_items.getnnz(axis=1).max()
            if max_row_n > user_items.shape[1] - N:
                raise ValueError(f"filter_already_liked_items:\
                cannot filter {max_row_n} and recommend {N} items\
                out of {user_items.shape[1]} available.")
        if filter_items:
            filter_items = list(set(filter_items))
            if len(filter_items) > user_items.shape[1] - N:
                raise ValueError(f"filter_items:\
                cannot filter {len(filter_items)} and recommend {N} items\
                out of {user_items.shape[1]} available.")

        if num_threads==0:
            num_threads=multiprocessing.cpu_count()

        if not isinstance(user_items, csr_matrix):
            user_items = user_items.tocsr()

        factors_items = self.item_factors.T

        cdef:
            int users_c = user_items.shape[0], items_c = user_items.shape[1]
            int batch = num_threads * 100 if batch_size==0 else batch_size
            int u_b, u_low, u_high, u_len, u
        A = np.zeros((batch, items_c), dtype=np.float32)
        cdef:
            int users_c_b = ceil(users_c / float(batch))
            float[:, ::1] A_mv = A
            float * A_mv_p = &A_mv[0, 0]
            int[:, ::1] B_mv = np.zeros((users_c, N), dtype=np.intc)
            int * B_mv_p = &B_mv[0, 0]

        progress = tqdm(total=users_c, disable=not show_progress)
        # Separate all users in batches
        for u_b in range(users_c_b):
            u_low = u_b * batch
            u_high = min([(u_b + 1) * batch, users_c])
            u_len = u_high - u_low
            # Prepare array with scores for batch of users
            users_factors = np.vstack([
                self._user_factor(u+users_items_offset, user_items, recalculate_user)
                for u
                in range(u_low, u_high, 1)
            ]).astype(np.float32)
            users_factors.dot(factors_items, out=A[:u_len])
            # Precalculate min if needed later
            if filter_already_liked_items or filter_items:
                A_min = np.amin(A)
            # Filter out items from user_items if needed
            if filter_already_liked_items:
                A[user_items[u_low:u_high].nonzero()] = A_min - 1
            # Filter out constant items
            if filter_items:
                A[:, filter_items] = A_min - 1
            # Sort array of scores in parallel
            for u in prange(u_len, nogil=True, num_threads=num_threads, schedule='dynamic'):
                fargsort_c(A_mv_p, u, batch * u_b + u, items_c, N, B_mv_p)
            progress.update(u_len)
        progress.close()
        return np.asarray(B_mv)

    def rank_items(self, userid, user_items, selected_items, recalculate_user=False):
        user = self._user_factor(userid, user_items, recalculate_user)

        # check selected items are  in the model
        if max(selected_items) >= user_items.shape[1] or min(selected_items) < 0:
            raise IndexError("Some of selected itemids are not in the model")

        item_factors = self.item_factors[selected_items]
        # calculate relevance scores of given items w.r.t the user
        scores = item_factors.dot(user)

        # return sorted results
        return sorted(zip(selected_items, scores), key=lambda x: -x[1])

    recommend.__doc__ = RecommenderBase.recommend.__doc__

    def _user_factor(self, userid, user_items, recalculate_user=False):
        if recalculate_user:
            return self.recalculate_user(userid, user_items)
        else:
            return self.user_factors[userid]

    def _item_factor(self, itemid, react_users, recalculate_item=False):
        if recalculate_item:
            return self.recalculate_item(itemid, react_users)
        else:
            return self.item_factors[itemid]

    def recalculate_user(self, userid, user_items):
        raise NotImplementedError("recalculate_user is not supported with this model")

    def recalculate_item(self, itemid, react_users):
        raise NotImplementedError("recalculate_item is not supported with this model")

    def similar_users(self, userid, N=10):
        factor = self.user_factors[userid]
        factors = self.user_factors
        norms = self.user_norms
        norm = norms[userid]
        return self._get_similarity_score(factor, norm, factors, norms, N)

    similar_users.__doc__ = RecommenderBase.similar_users.__doc__

    def similar_items(self, itemid, N=10, react_users=None, recalculate_item=False):
        factor = self._item_factor(itemid, react_users, recalculate_item)
        factors = self.item_factors
        norms = self.item_norms
        if recalculate_item:
            norm = np.linalg.norm(factor)
            norm = norm if norm != 0 else 1e-10
        else:
            norm = norms[itemid]
        return self._get_similarity_score(factor, norm, factors, norms, N)

    similar_items.__doc__ = RecommenderBase.similar_items.__doc__

    def _get_similarity_score(self, factor, norm, factors, norms, N):
        scores = factors.dot(factor) / (norm * norms)
        best = np.argpartition(scores, -N)[-N:]
        return sorted(zip(best, scores[best]), key=lambda x: -x[1])

    @property
    def user_norms(self):
        if self._user_norms is None:
            self._user_norms = np.linalg.norm(self.user_factors, axis=-1)
            # don't divide by zero in similar_items, replace with small value
            self._user_norms[self._user_norms == 0] = 1e-10
        return self._user_norms

    @property
    def item_norms(self):
        if self._item_norms is None:
            self._item_norms = np.linalg.norm(self.item_factors, axis=-1)
            # don't divide by zero in similar_items, replace with small value
            self._item_norms[self._item_norms == 0] = 1e-10
        return self._item_norms

    def _check_fit_errors(self):
        is_nan = np.any(np.isnan(self.user_factors), axis=None)
        is_nan |= np.any(np.isnan(self.item_factors), axis=None)
        if is_nan:
            raise ModelFitError('NaN encountered in factors')
