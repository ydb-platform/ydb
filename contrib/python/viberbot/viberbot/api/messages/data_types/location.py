from future.utils import python_2_unicode_compatible


class LocationConsts(object):
	MAX_LONGITUDE = 180
	MIN_LONGITUDE = -180
	MAX_LATITUDE = 90
	MIN_LATITUDE = -90


class Location(object):
	def __init__(self, lat=None, lon=None):
		self._lat = lat
		self._lon = lon

	def from_dict(self, location):
		if 'lat' in location:
			self._lat = location['lat']
		if 'lon' in location:
			self._lon = location['lon']
		return self

	def to_dict(self):
		return {
			'lat': self._lat,
			'lon': self._lon
		}

	@property
	def latitude(self):
		return self._lat

	@property
	def longitude(self):
		return self._lon

	def validate(self):
		if self._lat is None or self._lon is None:
			return False
		if self._lat >= LocationConsts.MAX_LATITUDE or self._lat <= LocationConsts.MIN_LATITUDE:
			return False
		if self._lon >= LocationConsts.MAX_LONGITUDE or self._lon <= LocationConsts.MIN_LONGITUDE:
			return False
		return True

	def __eq__(self, other):
		return self._lat == other.latitude and self._lon == other.longitude

	@python_2_unicode_compatible
	def __str__(self):
		return u"Location[lat={0}, lon={1}]".format(self._lat, self._lon)
