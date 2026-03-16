from thinc.api import Relu, Softmax, add, chain, reduce_max

bad_model = chain(Relu(10), reduce_max(), Softmax())

bad_model2 = add(Relu(10), reduce_max(), Softmax())
