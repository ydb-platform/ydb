from thinc.api import Relu, Softmax, add, chain, reduce_max

good_model = chain(Relu(10), Relu(10), Softmax())
reveal_type(good_model)

good_model2 = add(Relu(10), Relu(10), Softmax())
reveal_type(good_model2)

bad_model_undetected = chain(Relu(10), Relu(10), reduce_max(), Softmax())
reveal_type(bad_model_undetected)

bad_model_undetected2 = add(Relu(10), Relu(10), reduce_max(), Softmax())
reveal_type(bad_model_undetected2)
