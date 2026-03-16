'''
Created on 16 Oct 2015

@author: Max Zwiessele
'''
import numpy as np
from ..model import Model
from ..core.observable_array import ObsAr
from ..param import Param
from ..caching import Cacher
from ..parameterized import Parameterized

class RidgeRegression(Model):
    '''
    Ridge regression with regularization.

    For any regularization to work we to gradient based optimization.
    '''
    def __init__(self, X, Y, regularizer=None, basis=None, name='ridge_regression'):
        '''
        :param array-like X: the inputs X of the regression problem
        :param array-like Y: the outputs Y
        :param :py:class:`paramz.examples.ridge_regression.Regularizer` regularizer: the regularizer to use
        :param str name: the name of this regression object
        '''
        super(RidgeRegression, self).__init__(name=name)
        assert X.ndim == 2, 'inputs need to be at least a column vector'
        assert Y.ndim == 2, 'inputs need to be at least a column vector'

        self.X = ObsAr(X)
        self.Y = ObsAr(Y)

        if basis is None:
            basis = Polynomial(1)
        self.basis = basis

        if regularizer is None:
            regularizer = Ridge(1)
        self.regularizer = regularizer

        self.regularizer.init(basis, X.shape[1])

        self.link_parameters(self.regularizer, self.basis)

    @property
    def weights(self):
        return self.regularizer.weights

    @property
    def degree(self):
        return self.basis.degree

    @property
    def _phi(self):
        return self.basis._basis

    def phi(self, Xpred, degrees=None):
        """
        Compute the design matrix for this model
        using the degrees given by the index array
        in degrees

        :param array-like Xpred: inputs to compute the design matrix for
        :param array-like degrees: array of degrees to use [default=range(self.degree+1)]
        :returns array-like phi: The design matrix [degree x #samples x #dimensions]
        """
        assert Xpred.shape[1] == self.X.shape[1], "Need to predict with same shape as training data."
        if degrees is None:
            degrees = range(self.basis.degree+1)
        tmp_phi = np.empty((len(degrees), Xpred.shape[0], Xpred.shape[1]))
        for i, w in enumerate(degrees):
            # Objective function
            tmpX = self._phi(Xpred, w)
            tmp_phi[i] = tmpX * self.weights[[w], :]
        return tmp_phi

    def parameters_changed(self):
        tmp_outer = 0.
        for i in range(self.degree+1):
            # Objective function
            tmp_X = self._phi(self.X, i)
            target_f = tmp_X.dot(self.weights[[i], :].T)
            tmp_outer += target_f

        tmp_outer = (self.Y-tmp_outer)
        for i in range(self.degree+1):
            tmp_X = self._phi(self.X, i)
            # gradient:
            # Note, that we updated the weights gradients
            # in the basis first. So here we update
            # and add in the gradient for the bound.
            self.weights.gradient[i] -= 2*(tmp_outer*tmp_X).sum(0)
        self._obj = (((tmp_outer)**2).sum() + self.regularizer.error.sum())

    def objective_function(self):
        return self._obj

    def predict(self, Xnew):
        tmp_outer = 0.
        for i in range(self.degree+1):
            # Objective function
            tmp_X = self._phi(Xnew, i)
            tmp_outer += tmp_X.dot(self.weights[[i], :].T)
        return tmp_outer



class Basis(Parameterized):
    def __init__(self, degree, name='basis'):
        """
        Basis class for computing the design matrix phi(X). The weights are held
        in the regularizer, so that this only represents the design matrix.
        """
        super(Basis, self).__init__(name=name)
        self.degree = degree
        # One way of setting up the caching is by the following, the
        # other is by the decorator.
        self._basis = Cacher(self.basis, self.degree+1, [], [])
        # If we do not set up the caching ourself, using the decorator, we
        # cannot set the limit of the cacher easily to the degree+1.
        # It could be done on runtime, by inspecting self.cache and setting
        # the limit of the right cacher, but that is more complex then necessary

    def basis(self, X, i):
        """
        Return the ith basis dimension.
        In the polynomial case, this is X**index.
        You can write your own basis function here, inheriting from this class
        and the gradients will still check.

        Note: i will be zero for the first degree. This means we
        have also a bias in the model, which makes the problem of having an explicit
        bias obsolete.
        """
        raise NotImplementedError('Implement the basis you want to optimize over.')

class Polynomial(Basis):
    def __init__(self, degree, name='polynomial'):
        super(Polynomial, self).__init__(degree, name)

    def basis(self, X, i):
        return X**i



class Regularizer(Parameterized):
    def __init__(self, lambda_, name='regularizer'):
        super(Regularizer, self).__init__(name=name)
        self.lambda_ = lambda_
        self._initialized = False

    def init(self, basis, input_dim):
        if self._initialized:
            raise RuntimeError("already initialized, please use new object.")
        weights = np.ones((basis.degree + 1, input_dim))*np.arange(basis.degree+1)[:,None]
        for i in range(basis.degree+1):
            weights[[i]] /= basis.basis(weights[[i]], i)

        if not isinstance(weights, Param):
            if weights.ndim == 1:
                weights = weights[:,None]
            weights = Param('weights', weights)
        else:
            assert weights.ndim == 2, 'weights needs to be at least a column vector'
        self.weights = weights
        self.link_parameter(weights)
        self._initialized = True
        self.update_error()

    def parameters_changed(self):
        if self._initialized:
            self.update_error()

    def update_error(self):
        raise NotImplementedError('Set the error `error` and gradient of weights in here')

class Lasso(Regularizer):
    def __init__(self, lambda_, name='Lasso'):
        super(Lasso, self).__init__(lambda_, name)

    def update_error(self):
        self.error = self.lambda_*np.sum(np.abs(self.weights), 1)
        self.weights.gradient[:] = self.lambda_*np.sign(self.weights)

class Ridge(Regularizer):
    def __init__(self, lambda_, name='Ridge'):
        super(Ridge, self).__init__(lambda_, name)

    def update_error(self):
        self.error = self.lambda_*np.sum(self.weights**2, 1)
        self.weights.gradient[:] = self.lambda_*2*self.weights
