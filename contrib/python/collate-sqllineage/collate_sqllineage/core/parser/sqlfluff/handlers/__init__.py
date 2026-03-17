import importlib
import os
import pkgutil

# Later we'll use BaseHandler's __subclasses__ hook to call each subclass, for that to work, we'll need to make sure
# each module the subclass in is imported before calling the hook
for module in pkgutil.iter_modules([os.path.dirname(__file__)]):
    importlib.import_module(__name__ + "." + module.name)
