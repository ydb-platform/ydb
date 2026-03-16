"""Modest database of more than a hundred world cities."""

import ephem
import json
import sys
from math import radians

_python3 = sys.version_info > (3,)
if _python3:
    from urllib.parse import urlencode
    from urllib.request import urlopen
else:
    from urllib import urlencode
    from urllib2 import urlopen

_city_data = {
    'London': ('51.5001524', '-0.1262362', 14.605533),  # United Kingdom
    'Paris': ('48.8566667', '2.3509871', 35.917042),  # France
    'New York': ('40.7143528', '-74.0059731', 9.775694),  # United States
    'Tokyo': ('35.6894875', '139.6917064', 37.145370),  # Japan
    'Chicago': ('41.8781136', '-87.6297982', 181.319290),  # United States
    'Frankfurt': ('50.1115118', '8.6805059', 106.258285),  # Germany
    'Hong Kong': ('22.396428', '114.109497', 321.110260),  # Hong Kong
    'Los Angeles': ('34.0522342', '-118.2436849', 86.847092),  # United States
    'Milan': ('45.4636889', '9.1881408', 122.246513),  # Italy
    'Singapore': ('1.352083', '103.819836', 57.821636),  # Singapore
    'San Francisco': ('37.7749295', '-122.4194155', 15.557819),  # United States
    'Sydney': ('-33.8599722', '151.2111111', 3.341026),  # Australia
    'Toronto': ('43.6525', '-79.3816667', 90.239403),  # Canada
    'Zurich': ('47.3833333', '8.5333333', 405.500916),  # Switzerland
    'Brussels': ('50.8503', '4.35171', 26.808620),  # Belgium
    'Madrid': ('40.4166909', '-3.7003454', 653.005005),  # Spain
    'Mexico City': ('19.4270499', '-99.1275711', 2228.146484),  # Mexico
    'Sao Paulo': ('-23.5489433', '-46.6388182', 760.344849),  # Brazil
    'Moscow': ('55.755786', '37.617633', 151.189835),  # Russian Federation
    'Seoul': ('37.566535', '126.9779692', 41.980915),  # South Korea
    'Amsterdam': ('52.3730556', '4.8922222', 14.975505),  # The Netherlands
    'Boston': ('42.3584308', '-71.0597732', 15.338848),  # United States
    'Caracas': ('10.491016', '-66.902061', 974.727417),  # Venezuela
    'Dallas': ('32.802955', '-96.769923', 154.140625),  # United States
    'Dusseldorf': ('51.2249429', '6.7756524', 43.204800),  # Germany
    'Geneva': ('46.2057645', '6.141593', 379.026245),  # Switzerland
    'Houston': ('29.7628844', '-95.3830615', 6.916622),  # United States
    'Jakarta': ('-6.211544', '106.845172', 10.218226),  # Indonesia
    'Johannesburg': ('-26.1704415', '27.9717606', 1687.251099),  # South Africa
    'Melbourne': ('-37.8131869', '144.9629796', 27.000000),  # Australia
    'Osaka': ('34.6937378', '135.5021651', 16.347811),  # Japan
    'Prague': ('50.0878114', '14.4204598', 191.103485),  # Czech Republic
    'Santiago': ('-33.4253598', '-70.5664659', 665.926880),  # Chile
    'Taipei': ('25.091075', '121.5598345', 32.288563),  # Taiwan
    'Washington': ('38.8951118', '-77.0363658', 7.119641),  # United States
    'Bangkok': ('13.7234186', '100.4762319', 4.090096),  # Thailand
    'Beijing': ('39.904214', '116.407413', 51.858883),  # China
    'Montreal': ('45.5088889', '-73.5541667', 16.620916),  # Canada
    'Rome': ('41.8954656', '12.4823243', 19.704413),  # Italy
    'Stockholm': ('59.3327881', '18.0644881', 25.595907),  # Sweden
    'Warsaw': ('52.2296756', '21.0122287', 115.027786),  # Poland
    'Atlanta': ('33.7489954', '-84.3879824', 319.949738),  # United States
    'Barcelona': ('41.387917', '2.1699187', 19.991053),  # Spain
    'Berlin': ('52.5234051', '13.4113999', 45.013939),  # Germany
    'Buenos Aires': ('-34.6084175', '-58.3731613', 40.544090),  # Argentina
    'Budapest': ('47.4984056', '19.0407578', 106.463295),  # Hungary
    'Copenhagen': ('55.693403', '12.583046', 6.726723),  # Denmark
    'Hamburg': ('53.5538148', '9.9915752', 5.104634),  # Germany
    'Istanbul': ('41.00527', '28.97696', 37.314278),  # Turkey
    'Kuala Lumpur': ('3.139003', '101.686855', 52.271698),  # Malaysia
    'Manila': ('14.5833333', '120.9666667', 3.041384),  # Philippines
    'Miami': ('25.7889689', '-80.2264393', 0.946764),  # United States
    'Minneapolis': ('44.9799654', '-93.2638361', 253.002655),  # United States
    'Munich': ('48.1391265', '11.5801863', 523.000000),  # Germany
    'Shanghai': ('31.230393', '121.473704', 15.904707),  # China
    'Athens': ('37.97918', '23.716647', 47.597061),  # Greece
    'Auckland': ('-36.8484597', '174.7633315', 21.000000),  # New Zealand
    'Dublin': ('53.344104', '-6.2674937', 8.214323),  # Ireland
    'Helsinki': ('60.1698125', '24.9382401', 7.153307),  # Finland
    'Luxembourg': ('49.815273', '6.129583', 305.747925),  # Luxembourg
    'Lyon': ('45.767299', '4.8343287', 182.810547),  # France
    'Mumbai': ('19.0176147', '72.8561644', 12.408822),  # India
    'New Delhi': ('28.635308', '77.22496', 213.999054),  # India
    'Philadelphia': ('39.952335', '-75.163789', 12.465688),  # United States
    'Rio de Janeiro': ('-22.9035393', '-43.2095869', 9.521935),  # Brazil
    'Tel Aviv': ('32.0599254', '34.7851264', 21.114218),  # Israel
    'Vienna': ('48.20662', '16.38282', 170.493149),  # Austria
    'Abu Dhabi': ('24.4666667', '54.3666667', 6.296038),  # United Arab Emirates
    'Almaty': ('43.255058', '76.912628', 785.522156),  # Kazakhstan
    'Birmingham': ('52.4829614', '-1.893592', 141.448563),  # United Kingdom
    'Bogota': ('4.5980556', '-74.0758333', 2614.037109),  # Colombia
    'Bratislava': ('48.1483765', '17.1073105', 155.813446),  # Slovakia
    'Brisbane': ('-27.4709331', '153.0235024', 28.163914),  # Australia
    'Bucharest': ('44.437711', '26.097367', 80.407768),  # Romania
    'Cairo': ('30.064742', '31.249509', 20.248013),  # Egypt
    'Cleveland': ('41.4994954', '-81.6954088', 198.879639),  # United States
    'Cologne': ('50.9406645', '6.9599115', 59.181450),  # Germany
    'Detroit': ('42.331427', '-83.0457538', 182.763428),  # United States
    'Dubai': ('25.2644444', '55.3116667', 8.029230),  # United Arab Emirates
    'Ho Chi Minh City': ('10.75918', '106.662498', 10.757121),  # Vietnam
    'Kiev': ('50.45', '30.5233333', 157.210175),  # Ukraine
    'Lima': ('-12.0433333', '-77.0283333', 154.333740),  # Peru
    'Lisbon': ('38.7070538', '-9.1354884', 2.880179),  # Portugal
    'Manchester': ('53.4807125', '-2.2343765', 57.892406),  # United Kingdom
    'Montevideo': ('-34.8833333', '-56.1666667', 45.005032),  # Uruguay
    'Oslo': ('59.9127263', '10.7460924', 10.502326),  # Norway
    'Rotterdam': ('51.924216', '4.481776', 2.766272),  # The Netherlands
    'Riyadh': ('24.6880015', '46.7224333', 613.475281),  # Saudi Arabia
    'Seattle': ('47.6062095', '-122.3320708', 53.505501),  # United States
    'Stuttgart': ('48.7771056', '9.1807688', 249.205185),  # Germany
    'The Hague': ('52.0698576', '4.2911114', 3.686689),  # The Netherlands
    'Vancouver': ('49.248523', '-123.1088', 70.145927),  # Canada
    'Adelaide': ('-34.9305556', '138.6205556', 49.098354),  # Australia
    'Antwerp': ('51.21992', '4.39625', 7.296879),  # Belgium
    'Arhus': ('56.162939', '10.203921', 26.879421),  # Denmark
    'Baltimore': ('39.2903848', '-76.6121893', 10.258920),  # United States
    'Bangalore': ('12.9715987', '77.5945627', 911.858398),  # India
    'Bologna': ('44.4942191', '11.3464815', 72.875923),  # Italy
    'Brazilia': ('-14.235004', '-51.92528', 259.063477),  # Brazil
    'Calgary': ('51.045', '-114.0572222', 1046.000000),  # Canada
    'Cape Town': ('-33.924788', '18.429916', 5.838447),  # South Africa
    'Colombo': ('6.927468', '79.848358', 9.969995),  # Sri Lanka
    'Columbus': ('39.9611755', '-82.9987942', 237.651932),  # United States
    'Dresden': ('51.0509912', '13.7336335', 114.032356),  # Germany
    'Edinburgh': ('55.9501755', '-3.1875359', 84.453995),  # United Kingdom
    'Genoa': ('44.4070624', '8.9339889', 35.418076),  # Italy
    'Glasgow': ('55.8656274', '-4.2572227', 38.046883),  # United Kingdom
    'Gothenburg': ('57.6969943', '11.9865', 15.986326),  # Sweden
    'Guangzhou': ('23.129163', '113.264435', 18.892920),  # China
    'Hanoi': ('21.0333333', '105.85', 20.009024),  # Vietnam
    'Kansas City': ('39.1066667', '-94.6763889', 274.249390),  # United States
    'Leeds': ('53.7996388', '-1.5491221', 47.762367),  # United Kingdom
    'Lille': ('50.6371834', '3.0630174', 28.139490),  # France
    'Marseille': ('43.2976116', '5.3810421', 24.785774),  # France
    'Richmond': ('37.542979', '-77.469092', 63.624462),  # United States
    'St. Petersburg': ('59.939039', '30.315785', 11.502971),  # Russian Federation
    'Tashkent': ('41.2666667', '69.2166667', 430.668427),  # Uzbekistan
    'Tehran': ('35.6961111', '51.4230556', 1180.595947),  # Iran
    'Tijuana': ('32.533489', '-117.018204', 22.712011),  # Mexico
    'Turin': ('45.0705621', '7.6866186', 234.000000),  # Italy
    'Utrecht': ('52.0901422', '5.1096649', 7.720881),  # The Netherlands
    'Wellington': ('-41.2924945', '174.7732353', 17.000000),  # New Zealand
    }

def city(name):
    try:
        data = _city_data[name]
    except KeyError:
        raise KeyError('Unknown city: %r' % (name,))
    o = ephem.Observer()
    o.name = name
    o.lat, o.lon, o.elevation = data
    o.compute_pressure()
    return o

def lookup(address):
    """DEPRECATED: Google, alas, no longer supports anonymous lat/lon lookup.

    Because looking up an address is really a problem in geography, not
    astronomy, PyEphem is not planning on repairing this routine.  Look
    for a good Python geolocation library if you need to turn strings
    into latitudes and longitudes.

    """
    raise NotImplementedError(lookup.__doc__.split(None, 1)[1].strip())

def lookup_with_geonames(q, username):
    """Given a string `q`, do a geonames lookup and return an Observer.

    Free geonames queries require registration with an email address
    at this url: https://www.geonames.org/login

    After registration, you also must enable the free webservices
    through your user account
    """
    parameters = urlencode({'q': q, 'username': username})
    url = 'http://api.geonames.org/searchJSON?' + parameters
    data = json.loads(urlopen(url).read().decode('utf-8'))
    if data['totalResultsCount'] == 0:
        raise ValueError('geonames cannot find a place named %r' % name)
    location = data['geonames'][0]
    parameters_elev = urlencode({'lat':location['lat'],
                                 'lng':location['lng'],
                                 'username':username})
    url_elev = 'http://api.geonames.org/srtm1JSON?' + parameters_elev
    data_elev = json.loads(urlopen(url_elev).read().decode('utf-8'))

    o = ephem.Observer()
    o.name = location['toponymName']
    o.lat = location['lat']
    o.lon = location['lng']
    o.elevation = data_elev['srtm1']
    o.compute_pressure()
    return o

