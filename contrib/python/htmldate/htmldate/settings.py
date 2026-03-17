# pylint:disable-msg=E0611
"""
Listing a series of settings that are applied module-wide.
"""

from datetime import datetime

# Function cache
CACHE_SIZE: int = 8192

# Download
MAX_FILE_SIZE: int = 20000000

# Plausible dates
# earliest possible date to take into account (inclusive)
MIN_DATE: datetime = datetime(1995, 1, 1)

# set an upper limit to the number of candidates
MAX_POSSIBLE_CANDIDATES: int = 1000

CLEANING_LIST = [
    "applet",
    "audio",
    "canvas",
    "datalist",
    "embed",
    "frame",
    "frameset",
    "iframe",
    "label",
    "map",
    "math",
    "noframes",
    "object",
    "picture",
    "rdf",
    "svg",
    "track",
    "video",
]
# "figure", "input", "layer", "param", "source"
