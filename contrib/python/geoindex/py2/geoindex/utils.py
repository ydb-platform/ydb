from functools import partial
import operator

EARTH_RADIUS_KM = 6372.795
EARTH_RADIUS_MI = 3959.87122552164

km_to_mi = partial(operator.mul, 0.621371192)
mi_to_km = partial(operator.mul, 1.609344)
