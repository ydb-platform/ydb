import json

from opensfm import context
from opensfm import io
from library.python import resource

sensor_data = io.json_loads(resource.find('sensor_data'))

# Convert model types to lower cases for easier query
keys = [k.lower() for k in sensor_data.keys()]
values = sensor_data.values()
sensor_data = dict(zip(keys, values))
