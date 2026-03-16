# Define the version number. This class is exec'd by setup.py to read
# the value without loading mf2py (loading mf2py is bad if its dependencies
# haven't been installed yet, which is common during setup)

import importlib.metadata

__version__ = importlib.metadata.metadata("mf2py")["Version"]
