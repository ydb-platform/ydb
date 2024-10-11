# crutch script from yamaker for generating directory for given path, and put in this dir empty file yamaker_mock.dep
import os,sys

path = sys.argv[1]
if not os.path.exists(path):
    os.makedirs(path)
with open(os.path.join(path, "yamaker_mock.dep"), "w") as f:
    pass
