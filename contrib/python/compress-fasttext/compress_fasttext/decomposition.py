import numpy as np

try:
    from sklearn.decomposition import TruncatedSVD
except ImportError:
    # SVD is not the core feature of this library, so we keep this dependency optional
    TruncatedSVD = None


class DecomposedMatrix:
    def __init__(self, compressed, components):
        self.compressed = compressed
        self.components = components

    def __getitem__(self, item):
        return np.dot(self.compressed[item], self.components)

    @property
    def shape(self):
        return self.compressed.shape[0], self.components.shape[1]

    @property
    def dtype(self):
        return self.components.dtype

    @classmethod
    def compress(cls, data, n_components=30, fp16=True):
        if not TruncatedSVD:
            raise ImportError('You need to install the `scikit-learn` package to perform matrix decomposition')
        model = TruncatedSVD(n_components=n_components)
        compressed = model.fit_transform(data)
        if fp16:
            compressed = compressed.astype(np.float16)
        return cls(compressed, model.components_)

    def __len__(self):
        return len(self.compressed)
