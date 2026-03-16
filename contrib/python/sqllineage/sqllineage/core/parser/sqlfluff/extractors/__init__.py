import importlib
import os
import pkgutil

# import each module so that BaseExtractor's __subclasses__ will work
for module in pkgutil.iter_modules([os.path.dirname(__file__)]):
    importlib.import_module(__name__ + "." + module.name)
