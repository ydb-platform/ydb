from thinc.api import Relu, Softmax, chain, concatenate
from thinc.model import Model
from thinc.types import Floats2d

n_hidden = 32
dropout = 0.2

model1: Model[Floats2d, Floats2d] = chain(
    Relu(nO=n_hidden, dropout=dropout), Relu(nO=n_hidden, dropout=dropout), Softmax()
)

model2: Model[Floats2d, Floats2d] = chain(
    Relu(nO=n_hidden, dropout=dropout), Relu(nO=n_hidden, dropout=dropout), Softmax()
)

model3: Model[Floats2d, Floats2d] = concatenate(*[model1, model2])
