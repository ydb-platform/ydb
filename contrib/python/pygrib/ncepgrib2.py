__version__ = '2.0.3'
import g2clib
import struct
import string
import math
import functools
import warnings
import operator
from datetime import datetime
try:
    from StringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO

import numpy as np
from numpy import ma
try:
    import pyproj
except ImportError:
    try:
        from mpl_toolkits.basemap import pyproj
    except:
        raise ImportError("either pyproj or basemap required")

# Code Table 3.2: Shape of the Earth.
_earthparams={0:6367470.0,
1:'Spherical - radius specified in m by data producer',
2:(6378160.0,6356775.0),
3:'OblateSpheroid - major and minor axes specified in km by data producer',
4:(6378137.0,6356752.314),
5:'WGS84',
6:6371229.0,
7:'OblateSpheroid - major and minor axes specified in m by data producer',
8:6371200.0,
255:'Missing'}
for _n in range(192):
    if not _n in _earthparams: _earthparams[_n]='Reserved'
for _n in range(192,255):
    _earthparams[_n] = 'Reserved for local use'

_table0={1:('Melbourne (WMC)','ammc'),
2:('Melbourne - BMRC (WMC)',None),
3:('Melbourne (WMC)',None),
4:('Moscow (WMC)',None),
5:('Moscow (WMC)',None),
6:('Moscow (WMC)',None),
7:('US National Weather Service - NCEP (WMC)','kwbc'),
8:('US National Weather Service - NWSTG (WMC)',None),
9:('US National Weather Service - Other (WMC)',None),
10:('Cairo (RSMC/RAFC)',None),
11:('Cairo (RSMC/RAFC)',None),
12:('Dakar (RSMC/RAFC)',None),
13:('Dakar (RSMC/RAFC)',None),
14:('Nairobi (RSMC/RAFC)',None),
15:('Nairobi (RSMC/RAFC)',None),
16:('Casablanca',None),
17:('Tunis (RSMC)',None),
18:('Tunis-Casablanca (RSMC)',None),
19:('Tunis-Casablanca (RSMC)',None),
20:('Las Palmas (RAFC)',None),
21:('Algiers (RSMC)',None),
22:('ACMAD',None),
23:('Mozambique (NMC)',None),
24:('Pretoria (RSMC)',None),
25:('La Reunion (RSMC)',None),
26:('Khabarovsk (RSMC)',None),
27:('Khabarovsk (RSMC)',None),
28:('New Delhi (RSMC/RAFC)',None),
29:('New Delhi (RSMC/RAFC)',None),
30:('Novosibirsk (RSMC)',None),
31:('Novosibirsk (RSMC)',None),
32:('Tashkent (RSMC)',None),
33:('Jeddah (RSMC)',None),
34:('Japanese Meteorological Agency - Tokyo (RSMC)','rjtd'),
35:('Japanese Meteorological Agency - Tokyo (RSMC)',None),
36:('Bankok',None),
37:('Ulan Bator',None),
38:('Beijing (RSMC)','babj'),
39:('Beijing (RSMC)',None),
40:('Korean Meteorological Administration - Seoul','rksl'),
41:('Buenos Aires (RSMC/RAFC)',None),
42:('Buenos Aires (RSMC/RAFC)',None),
43:('Brasilia (RSMC/RAFC)',None),
44:('Brasilia (RSMC/RAFC)',None),
45:('Santiago',None),
46:('Brazilian Space Agency - INPE','sbsj'),
47:('Columbia (NMC)',None),
48:('Ecuador (NMC)',None),
49:('Peru (NMC)',None),
50:('Venezuela (NMC)',None),
51:('Miami (RSMC/RAFC)',None),
52:('Tropical Prediction Center (NHC), Miami (RSMC)',None),
53:('Canadian Meteorological Service - Montreal (RSMC)',None),
54:('Canadian Meteorological Service - Montreal (RSMC)','cwao'),
55:('San Francisco',None),
56:('ARINC Center',None),
57:('U.S. Air Force - Global Weather Center',None),
58:('US Navy - Fleet Numerical Oceanography Center','fnmo'),
59:('NOAA Forecast Systems Lab, Boulder CO',None),
60:('National Center for Atmospheric Research (NCAR), Boulder, CO',None),
61:('Service ARGOS - Landover, MD, USA',None),
62:('US Naval Oceanographic Office',None),
63:('Reserved',None),
64:('Honolulu',None),
65:('Darwin (RSMC)',None),
66:('Darwin (RSMC)',None),
67:('Melbourne (RSMC)',None),
68:('Reserved',None),
69:('Wellington (RSMC/RAFC)',None),
70:('Wellington (RSMC/RAFC)',None),
71:('Nadi (RSMC)',None),
72:('Singapore',None),
73:('Malaysia (NMC)',None),
74:('U.K. Met Office - Exeter (RSMC)','egrr'),
75:('U.K. Met Office - Exeter (RSMC)',None),
76:('Moscow (RSMC/RAFC)',None),
77:('Reserved',None),
78:('Offenbach (RSMC)','edzw'),
79:('Offenbach (RSMC)',None),
80:('Rome (RSMC)','cnmc'),
81:('Rome (RSMC)',None),
82:('Norrkoping',None),
83:('Norrkoping',None),
84:('French Weather Service - Toulouse','lfpw'),
85:('French Weather Service - Toulouse','lfpw'),
86:('Helsinki',None),
87:('Belgrade',None),
88:('Oslo',None),
89:('Prague',None),
90:('Episkopi',None),
91:('Ankara',None),
92:('Frankfurt/Main (RAFC)',None),
93:('London (WAFC)',None),
94:('Copenhagen',None),
95:('Rota',None),
96:('Athens',None),
97:('European Space Agency (ESA)',None),
98:('European Center for Medium-Range Weather Forecasts (RSMC)','ecmf'),
99:('De BiltNone), Netherlands',None),
100:('Brazzaville',None),
101:('Abidjan',None),
102:('Libyan Arab Jamahiriya (NMC)',None),
103:('Madagascar (NMC)',None),
104:('Mauritius (NMC)',None),
105:('Niger (NMC)',None),
106:('Seychelles (NMC)',None),
107:('Uganda (NMC)',None),
108:('Tanzania (NMC)',None),
109:('Zimbabwe (NMC)',None),
110:('Hong-Kong',None),
111:('Afghanistan (NMC)',None),
112:('Bahrain (NMC)',None),
113:('Bangladesh (NMC)',None),
114:('Bhutan (NMC)',None),
115:('Cambodia (NMC)',None),
116:("Democratic People's Republic of Korea (NMC)",None),
117:('Islamic Republic of Iran (NMC)',None),
118:('Iraq (NMC)',None),
119:('Kazakhstan (NMC)',None),
120:('Kuwait (NMC)',None),
121:('Kyrgyz Republic (NMC)',None),
122:("Lao People's Democratic Republic (NMC)",None),
123:('MacaoNone), China',None),
124:('Maldives (NMC)',None),
125:('Myanmar (NMC)',None),
126:('Nepal (NMC)',None),
127:('Oman (NMC)',None),
128:('Pakistan (NMC)',None),
129:('Qatar (NMC)',None),
130:('Republic of Yemen (NMC)',None),
131:('Sri Lanka (NMC)',None),
132:('Tajikistan (NMC)',None),
133:('Turkmenistan (NMC)',None),
134:('United Arab Emirates (NMC)',None),
135:('Uzbekistan (NMC)',None),
136:('Socialist Republic of Viet Nam (NMC)',None),
137:('Reserved',None),
138:('Reserved',None),
139:('Reserved',None),
140:('Bolivia (NMC)',None),
141:('Guyana (NMC)',None),
142:('Paraguay (NMC)',None),
143:('Suriname (NMC)',None),
144:('Uruguay (NMC)',None),
145:('French Guyana',None),
146:('Brazilian Navy Hydrographic Center',None),
147:('Reserved',None),
148:('Reserved',None),
149:('Reserved',None),
150:('Antigua and Barbuda (NMC)',None),
151:('Bahamas (NMC)',None),
152:('Barbados (NMC)',None),
153:('Belize (NMC)',None),
154:('British Caribbean Territories Center',None),
155:('San Jose',None),
156:('Cuba (NMC)',None),
157:('Dominica (NMC)',None),
158:('Dominican Republic (NMC)',None),
159:('El Salvador (NMC)',None),
160:('US NOAA/NESDIS',None),
161:('US NOAA Office of Oceanic and Atmospheric Research',None),
162:('Guatemala (NMC)',None),
163:('Haiti (NMC)',None),
164:('Honduras (NMC)',None),
165:('Jamaica (NMC)',None),
166:('Mexico',None),
167:('Netherlands Antilles and Aruba (NMC)',None),
168:('Nicaragua (NMC)',None),
169:('Panama (NMC)',None),
170:('Saint Lucia (NMC)',None),
171:('Trinidad and Tobago (NMC)',None),
172:('French Departments',None),
173:('Reserved',None),
174:('Reserved',None),
175:('Reserved',None),
176:('Reserved',None),
177:('Reserved',None),
178:('Reserved',None),
179:('Reserved',None),
180:('Reserved',None),
181:('Reserved',None),
182:('Reserved',None),
183:('Reserved',None),
184:('Reserved',None),
185:('Reserved',None),
186:('Reserved',None),
187:('Reserved',None),
188:('Reserved',None),
189:('Reserved',None),
190:('Cook Islands (NMC)',None),
191:('French Polynesia (NMC)',None),
192:('Tonga (NMC)',None),
193:('Vanuatu (NMC)',None),
194:('Brunei (NMC)',None),
195:('Indonesia (NMC)',None),
196:('Kiribati (NMC)',None),
197:('Federated States of Micronesia (NMC)',None),
198:('New Caledonia (NMC)',None),
199:('Niue',None),
200:('Papua New Guinea (NMC)',None),
201:('Philippines (NMC)',None),
202:('Samoa (NMC)',None),
203:('Solomon Islands (NMC)',None),
204:('Reserved',None),
205:('Reserved',None),
206:('Reserved',None),
207:('Reserved',None),
208:('Reserved',None),
209:('Reserved',None),
210:('Frascati',None),
211:('Lanion',None),
212:('Lisboa',None),
213:('Reykjavik',None),
214:('Madrid','lemm'),
215:('Zurich',None),
216:('Service ARGOS - ToulouseNone), FR',None),
217:('Bratislava',None),
218:('Budapest',None),
219:('Ljubljana',None),
220:('Warsaw',None),
221:('Zagreb',None),
222:('Albania (NMC)',None),
223:('Armenia (NMC)',None),
224:('Austria (NMC)',None),
225:('Azerbaijan (NMC)',None),
226:('Belarus (NMC)',None),
227:('Belgium (NMC)',None),
228:('Bosnia and Herzegovina (NMC)',None),
229:('Bulgaria (NMC)',None),
230:('Cyprus (NMC)',None),
231:('Estonia (NMC)',None),
232:('Georgia (NMC)',None),
233:('Dublin',None),
234:('Israel (NMC)',None),
235:('Jordan (NMC)',None),
236:('Latvia (NMC)',None),
237:('Lebanon (NMC)',None),
238:('Lithuania (NMC)',None),
239:('Luxembourg',None),
240:('Malta (NMC)',None),
241:('Monaco',None),
242:('Romania (NMC)',None),
243:('Syrian Arab Republic (NMC)',None),
244:('The former Yugoslav Republic of Macedonia (NMC)',None),
245:('Ukraine (NMC)',None),
246:('Republic of Moldova',None),
247:('Reserved',None),
248:('Reserved',None),
249:('Reserved',None),
250:('Reserved',None),
251:('Reserved',None),
252:('Reserved',None),
253:('Reserved',None),
254:('EUMETSAT Operations Center',None),
255:('Missing Value',None)}

def _dec2bin(val, maxbits = 8):
    """
    A decimal to binary converter. Returns bits in a list.
    """
    retval = []
    for i in range(maxbits - 1, -1, -1):
        bit = int(val / (2 ** i))
        val = (val % (2 ** i))
        retval.append(bit)
    return retval

def _putieeeint(r):
    """convert a float to a IEEE format 32 bit integer"""
    ra = np.array([r],'f')
    ia = np.empty(1,'i')
    g2clib.rtoi_ieee(ra,ia)
    return ia[0]

def _getieeeint(i):
    """convert an IEEE format 32 bit integer to a float"""
    ia = np.array([i],'i')
    ra = np.empty(1,'f')
    g2clib.itor_ieee(ia,ra)
    return ra[0]

def _isString(string):
    """Test if string is a string like object if not return 0 """
    try: string + ''
    except: return 0
    else: return 1

class Grib2Message:
    """
 Class for accessing data in a GRIB Edition 2 message.

 The L{Grib2Decode} function returns a list of these class instances,
 one for each grib message in the file.

 When a class instance is created, metadata in the GRIB2 file
 is decoded and used to set various instance variables.

 @ivar bitmap_indicator_flag: flag to indicate whether a bit-map is used (0 for yes, 255 for no).
 @ivar data_representation_template: data representation template from section 5.
 @ivar data_representation_template_number: data representation template number
 from section 5
 (U{Table 5.0
 <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table5-0.shtml>})
 @ivar has_local_use_section:  True if grib message contains a local use
 section. If True the actual local use section is contained in the
 C{_local_use_section} instance variable, as a raw byte string.
 @ivar discipline_code: product discipline code for grib message
 (U{Table 0.0
 <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table0-0.shtml>}).
 @ivar earthRmajor: major (equatorial) earth radius.
 @ivar earthRminor: minor (polar) earth radius.
 @ivar grid_definition_info: grid definition section information from section 3.
  See L{Grib2Encode.addgrid} for details.
 @ivar grid_definition_template: grid definition template from section 3.
 @ivar grid_definition_template_number: grid definition template number from section 3
 (U{Table 3.1
 <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table3-1.shtml>}).
 @ivar gridlength_in_x_direction: x (or longitudinal) direction grid length.
 @ivar gridlength_in_y_direction: y (or latitudinal) direction grid length.
 @ivar identification_section: data from identification section (section 1).
  See L{Grib2Encode.__init__} for details.
 @ivar latitude_first_gridpoint: latitude of first grid point on grid.
 @ivar latitude_last_gridpoint: latitude of last grid point on grid.
 @ivar longitude_first_gridpoint: longitude of first grid point on grid.
 @ivar longitude_last_gridpoint: longitude of last grid point on grid.
 @ivar originating_center: name of national/international originating center.
 @ivar center_wmo_code: 4 character wmo code for originating center.
 @ivar scanmodeflags: scanning mode flags from Table 3.4
 (U{Table 3.4
 <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table3-4.shtml>}).

  - bit 1:

    0 - Points in the first row or column scan in the +i (+x) direction

    1 - Points in the first row or column scan in the -i (-x) direction

  - bit 2:

    0 - Points in the first row or column scan in the -j (-y) direction

    1 - Points in the first row or column scan in the +j (+y) direction

  - bit 3:

    0 - Adjacent points in the i (x) direction are consecutive (row-major order).

    1 - Adjacent points in the j (y) direction are consecutive (column-major order).

  - bit 4:

    0 - All rows scan in the same direction

    1 - Adjacent rows scan in the opposite direction

 @ivar number_of_data_points_to_unpack: total number of data points in grib message.
 @ivar points_in_x_direction: number of points in the x (longitudinal) direction.
 @ivar points_in_y_direction: number of points in the y (latitudinal) direction.
 @ivar product_definition_template: product definition template from section 4.
 @ivar product_definition_template_number: product definition template number from section 4
 (U{Table 4.0
 <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table4-0.shtml>}).
 @ivar shape_of_earth: string describing the shape of the earth (e.g. 'Oblate Spheroid', 'Spheroid').
 @ivar spectral_truncation_parameters:  pentagonal truncation parameters that describe the
 spherical harmonic truncation (only relevant for grid_definition_template_numbers 50-52).
 For triangular truncation, all three of these numbers are the same.
 @ivar latitude_of_southern_pole: the geographic latitude in degrees of the southern
 pole of the coordinate system (for rotated lat/lon or gaussian grids).
 @ivar longitude_of_southern_pole: the geographic longitude in degrees of the southern
 pole of the coordinate system (for rotated lat/lon or gaussian grids).
 @ivar angle_of_pole_rotation: The angle of rotation in degrees about the new
 polar axis (measured clockwise when looking from the southern to the northern pole)
 of the coordinate system. For rotated lat/lon or gaussian grids.
 @ivar missing_value: primary missing value (for data_representation_template_numbers
 2 and 3).
 @ivar missing_value2: secondary missing value (for data_representation_template_numbers
 2 and 3).
 @ivar proj4_: instance variables with this prefix are used to set the map projection
 parameters for U{PROJ.4<http://proj.maptools.org>}.
    """
    def __init__(self,**kwargs):
        """
 create a Grib2Decode class instance given a GRIB Edition 2 filename.

 (used by L{Grib2Decode} function - not directly called by user)
        """
        for k,v in kwargs.items():
            setattr(self,k,v)
        # grid information
        gdsinfo = self.grid_definition_info
        gdtnum = self.grid_definition_template_number
        gdtmpl = self.grid_definition_template
        reggrid = gdsinfo[2] == 0 # gdsinfo[2]=0 means regular 2-d grid
        # shape of the earth.
        if gdtnum not in [50,51,52,1200]:
            earthR = _earthparams[gdtmpl[0]]
            if earthR == 'Reserved': earthR = None
        else:
            earthR = None
        if _isString(earthR) and (earthR.startswith('Reserved') or earthR=='Missing'):
            self.shape_of_earth = earthR
            self.earthRminor = None
            self.earthRmajor = None
        elif _isString(earthR) and earthR.startswith('Spherical'):
            self.shape_of_earth = earthR
            scaledearthR = gdtmpl[2]
            earthRscale = gdtmpl[1]
            self.earthRmajor = math.pow(10,-earthRscale)*scaledearthR
            self.earthRminor = self.earthRmajor
        elif _isString(earthR) and earthR.startswith('OblateSpheroid'):
            self.shape_of_earth = earthR
            scaledearthRmajor = gdtmpl[4]
            earthRmajorscale = gdtmpl[3]
            self.earthRmajor = math.pow(10,-earthRmajorscale)*scaledearthRmajor
            self.earthRmajor = self.earthRmajor*1000. # convert to m from km
            scaledearthRminor = gdtmpl[6]
            earthRminorscale = gdtmpl[5]
            self.earthRminor = math.pow(10,-earthRminorscale)*scaledearthRminor
            self.earthRminor = self.earthRminor*1000. # convert to m from km
        elif _isString(earthR) and earthR.startswith('WGS84'):
            self.shape_of_earth = earthR
            self.earthRmajor = 6378137.0
            self.earthRminor = 6356752.3142
        elif isinstance(earthR,tuple):
            self.shape_of_earth = 'OblateSpheroid'
            self.earthRmajor = earthR[0]
            self.earthRminor = earthR[1]
        else:
            if earthR is not None:
                self.shape_of_earth = 'Spherical'
                self.earthRmajor = earthR
                self.earthRminor = self.earthRmajor
        if reggrid and gdtnum not in [50,51,52,53,100,120,1000,1200]:
            self.points_in_x_direction = gdtmpl[7]
            self.points_in_y_direction = gdtmpl[8]
        if not reggrid and gdtnum == 40: # 'reduced' gaussian grid.
            self.points_in_y_direction = gdtmpl[8]
        if gdtnum in [0,1,203,205,32768,32769]: # regular or rotated lat/lon grid
            scalefact = float(gdtmpl[9])
            divisor = float(gdtmpl[10])
            if scalefact == 0: scalefact = 1.
            if divisor <= 0: divisor = 1.e6
            self.latitude_first_gridpoint = scalefact*gdtmpl[11]/divisor
            self.longitude_first_gridpoint = scalefact*gdtmpl[12]/divisor
            self.latitude_last_gridpoint = scalefact*gdtmpl[14]/divisor
            self.longitude_last_gridpoint = scalefact*gdtmpl[15]/divisor
            self.gridlength_in_x_direction = scalefact*gdtmpl[16]/divisor
            self.gridlength_in_y_direction = scalefact*gdtmpl[17]/divisor
            if self.latitude_first_gridpoint > self.latitude_last_gridpoint:
                self.gridlength_in_y_direction = -self.gridlength_in_y_direction
            if self.longitude_first_gridpoint > self.longitude_last_gridpoint:
                self.gridlength_in_x_direction = -self.gridlength_in_x_direction
            self.scanmodeflags = _dec2bin(gdtmpl[18])[0:4]
            if gdtnum == 1:
                self.latitude_of_southern_pole = scalefact*gdtmpl[19]/divisor
                self.longitude_of_southern_pole = scalefact*gdtmpl[20]/divisor
                self.angle_of_pole_rotation = gdtmpl[21]
        elif gdtnum == 10: # mercator
            self.latitude_first_gridpoint = gdtmpl[9]/1.e6
            self.longitude_first_gridpoint = gdtmpl[10]/1.e6
            self.latitude_last_gridpoint = gdtmpl[13]/1.e6
            self.longitude_last_gridpoint = gdtmpl[14]/1.e6
            self.gridlength_in_x_direction = gdtmpl[17]/1.e3
            self.gridlength_in_y_direction= gdtmpl[18]/1.e3
            self.proj4_lat_ts = gdtmpl[12]/1.e6
            self.proj4_lon_0 = 0.5*(self.longitude_first_gridpoint+self.longitude_last_gridpoint)
            self.proj4_proj = 'merc'
            self.scanmodeflags = _dec2bin(gdtmpl[15])[0:4]
        elif gdtnum == 20: # stereographic
            projflag = _dec2bin(gdtmpl[16])[0]
            self.latitude_first_gridpoint = gdtmpl[9]/1.e6
            self.longitude_first_gridpoint = gdtmpl[10]/1.e6
            self.proj4_lat_ts = gdtmpl[12]/1.e6
            if projflag == 0:
                self.proj4_lat_0 = 90
            elif projflag == 1:
                self.proj4_lat_0 = -90
            else:
                raise ValueError('Invalid projection center flag = %s'%projflag)
            self.proj4_lon_0 = gdtmpl[13]/1.e6
            self.gridlength_in_x_direction = gdtmpl[14]/1000.
            self.gridlength_in_y_direction = gdtmpl[15]/1000.
            self.proj4_proj = 'stere'
            self.scanmodeflags = _dec2bin(gdtmpl[17])[0:4]
        elif gdtnum == 30: # lambert conformal
            self.latitude_first_gridpoint = gdtmpl[9]/1.e6
            self.longitude_first_gridpoint = gdtmpl[10]/1.e6
            self.gridlength_in_x_direction = gdtmpl[14]/1000.
            self.gridlength_in_y_direction = gdtmpl[15]/1000.
            self.proj4_lat_1 = gdtmpl[18]/1.e6
            self.proj4_lat_2 = gdtmpl[19]/1.e6
            self.proj4_lat_0 = gdtmpl[12]/1.e6
            self.proj4_lon_0 = gdtmpl[13]/1.e6
            self.proj4_proj = 'lcc'
            self.scanmodeflags = _dec2bin(gdtmpl[17])[0:4]
        elif gdtnum == 31: # albers equal area.
            self.latitude_first_gridpoint = gdtmpl[9]/1.e6
            self.longitude_first_gridpoint = gdtmpl[10]/1.e6
            self.gridlength_in_x_direction = gdtmpl[14]/1000.
            self.gridlength_in_y_direction = gdtmpl[15]/1000.
            self.proj4_lat_1 = gdtmpl[18]/1.e6
            self.proj4_lat_2 = gdtmpl[19]/1.e6
            self.proj4_lat_0 = gdtmpl[12]/1.e6
            self.proj4_lon_0 = gdtmpl[13]/1.e6
            self.proj4_proj = 'aea'
            self.scanmodeflags = _dec2bin(gdtmpl[17])[0:4]
        elif gdtnum == 40 or gdtnum == 41: # gaussian grid.
            scalefact = float(gdtmpl[9])
            divisor = float(gdtmpl[10])
            if scalefact == 0: scalefact = 1.
            if divisor <= 0: divisor = 1.e6
            self.points_between_pole_and_equator = gdtmpl[17]
            self.latitude_first_gridpoint = scalefact*gdtmpl[11]/divisor
            self.longitude_first_gridpoint = scalefact*gdtmpl[12]/divisor
            self.latitude_last_gridpoint = scalefact*gdtmpl[14]/divisor
            self.longitude_last_gridpoint = scalefact*gdtmpl[15]/divisor
            if reggrid:
                self.gridlength_in_x_direction = scalefact*gdtmpl[16]/divisor
                if self.longitude_first_gridpoint > self.longitude_last_gridpoint:
                    self.gridlength_in_x_direction = -self.gridlength_in_x_direction
            self.scanmodeflags = _dec2bin(gdtmpl[18])[0:4]
            if gdtnum == 41:
                self.latitude_of_southern_pole = scalefact*gdtmpl[19]/divisor
                self.longitude_of_southern_pole = scalefact*gdtmpl[20]/divisor
                self.angle_of_pole_rotation = gdtmpl[21]
        elif gdtnum == 50: # spectral coefficients.
            self.spectral_truncation_parameters = (gdtmpl[0],gdtmpl[1],gdtmpl[2])
            self.scanmodeflags = [None,None,None,None] # doesn't apply
        elif gdtnum == 90: # near-sided vertical perspective satellite projection
            self.proj4_lat_0 = gdtmpl[9]/1.e6
            self.proj4_lon_0 = gdtmpl[10]/1.e6
            self.proj4_h = self.earthRmajor * (gdtmpl[18]/1.e6)
            dx = gdtmpl[12]
            dy = gdtmpl[13]
            # if lat_0 is equator, it's a geostationary view.
            if self.proj4_lat_0 == 0.: # if lat_0 is equator, it's a
                self.proj4_proj = 'geos'
            # general case of 'near-side perspective projection' (untested)
            else:
                self.proj4_proj = 'nsper'
                msg = """
only geostationary perspective is supported.
lat/lon values returned by grid method may be incorrect."""
                warnings.warn(msg)
            # latitude of horizon on central meridian
            lonmax = 90.-(180./np.pi)*np.arcsin(self.earthRmajor/self.proj4_h)
            # longitude of horizon on equator
            latmax = 90.-(180./np.pi)*np.arcsin(self.earthRminor/self.proj4_h)
            # truncate to nearest thousandth of a degree (to make sure
            # they aren't slightly over the horizon)
            latmax = int(1000*latmax)/1000.
            lonmax = int(1000*lonmax)/1000.
            # h is measured from surface of earth at equator.
            self.proj4_h = self.proj4_h - self.earthRmajor
            # width and height of visible projection
            P = pyproj.Proj(proj=self.proj4_proj,\
                            a=self.earthRmajor,b=self.earthRminor,\
                            lat_0=0,lon_0=0,h=self.proj4_h)
            x1,y1 = P(0.,latmax); x2,y2 = P(lonmax,0.)
            width = 2*x2; height = 2*y1
            self.gridlength_in_x_direction = width/dx
            self.gridlength_in_y_direction = height/dy
            self.scanmodeflags = _dec2bin(gdtmpl[16])[0:4]
        elif gdtnum == 110: # azimuthal equidistant.
            self.proj4_lat_0 = gdtmpl[9]/1.e6
            self.proj4_lon_0 = gdtmpl[10]/1.e6
            self.gridlength_in_x_direction = gdtmpl[12]/1000.
            self.gridlength_in_y_direction = gdtmpl[13]/1000.
            self.proj4_proj = 'aeqd'
            self.scanmodeflags = _dec2bin(gdtmpl[15])[0:4]
        elif gdtnum == 204: # curvilinear orthogonal
            self.scanmodeflags = _dec2bin(gdtmpl[18])[0:4]
        # missing value.
        drtnum = self.data_representation_template_number
        drtmpl = self.data_representation_template
        if (drtnum == 2 or drtnum == 3) and drtmpl[6] != 0:
            self.missing_value = _getieeeint(drtmpl[7])
            if drtmpl[6] == 2:
                self.missing_value2 = _getieeeint(drtmpl[8])

    def __repr__(self):
        strings = []
        # python 3 keys() is an iterator that can't be sorted
        keys = list(self.__dict__.keys())
        keys.sort()
        for k in keys:
            if not k.startswith('_'):
                strings.append('%s = %s\n'%(k,self.__dict__[k]))
        return ''.join(strings)

    def data(self,fill_value=9.9692099683868690e+36,masked_array=True,expand=True,order=None):
        """
 returns an unpacked data grid.  Can also be accomplished with L{values}
 property.

 @keyword fill_value: missing or masked data is filled with this value
 (default 9.9692099683868690e+36).

 @keyword masked_array: if True, return masked array if there is bitmap
 for missing or masked data (default True).

 @keyword expand:  if True (default), ECMWF 'reduced' gaussian grids are
 expanded to regular gaussian grids.

 @keyword order: if 1, linear interpolation is used for expanding reduced
 gaussian grids.  if 0, nearest neighbor interpolation is used. Default
 is 0 if grid has missing or bitmapped values, 1 otherwise.

 @return: C{B{data}}, a float32 numpy regular or masked array
 with shape (nlats,lons) containing the requested grid.
        """
        # make sure scan mode is supported.
        # if there is no 'scanmodeflags', then grid is not supported.
        from redtoreg import _redtoreg
        if not hasattr(self,'scanmodeflags'):
            raise ValueError('unsupported grid definition template number %s'%self.grid_definition_template_number)
        else:
            if self.scanmodeflags[2]:
                storageorder='F'
            else:
                storageorder='C'
        bitmapflag = self.bitmap_indicator_flag
        drtnum = self.data_representation_template_number
        drtmpl = self.data_representation_template
        # default order=0 is missing values or bitmap exists.
        if order is None:
            if ((drtnum == 3 or drtnum == 2) and drtmpl[6] != 0) or bitmapflag == 0:
                order = 0
            else:
                order = 1
        try:
            f = open(self._grib_filename,'rb')
        # self._grib_filename can be a grib message binary string
        # if it is, ValueError is returned in python 3.5 (issue #24)
        except (ValueError,TypeError,IOError):
            f = StringIO(self._grib_filename)
        f.seek(self._grib_message_byteoffset)
        gribmsg = f.read(self._grib_message_length)
        f.close()
        gdtnum = self.grid_definition_template_number
        gdtmpl = self.grid_definition_template
        ndpts = self.number_of_data_points_to_unpack
        gdsinfo = self.grid_definition_info
        ngrdpts = gdsinfo[1]
        ipos = self._section7_byte_offset
        fld1=g2clib.unpack7(gribmsg,gdtnum,gdtmpl,drtnum,drtmpl,ndpts,ipos,np.empty,storageorder=storageorder)
        # apply bitmap.
        if bitmapflag == 0:
            bitmap=self._bitmap
            fld = fill_value*np.ones(ngrdpts,'f')
            np.put(fld,np.nonzero(bitmap),fld1)
            if masked_array:
                fld = ma.masked_values(fld,fill_value)
        # missing values instead of bitmap
        elif masked_array and hasattr(self,'missing_value'):
            if hasattr(self, 'missing_value2'):
                mask = np.logical_or(fld1 == self.missing_value, fld1 == self.missing_value2)
            else:
                mask = fld1 == self.missing_value
            fld = ma.array(fld1,mask=mask)
        else:
            fld = fld1
        nx = None; ny = None
        if hasattr(self,'points_in_x_direction'):
            nx = self.points_in_x_direction
        if hasattr(self,'points_in_y_direction'):
            ny = self.points_in_y_direction
        if nx is not None and ny is not None: # rectangular grid.
            if ma.isMA(fld):
                fld = ma.reshape(fld,(ny,nx))
            else:
                fld = np.reshape(fld,(ny,nx))
        else:
            if gdsinfo[2] and gdtnum == 40: # ECMWF 'reduced' global gaussian grid.
                if expand:
                    nx = 2*ny
                    lonsperlat = self.grid_definition_list
                    if ma.isMA(fld):
                        fld = ma.filled(fld)
                        fld = _redtoreg(nx, lonsperlat.astype(np.long),\
                                fld.astype(np.double), fill_value)
                        fld = ma.masked_values(fld,fill_value)
                    else:
                        fld = _redtoreg(nx, lonsperlat.astype(np.long),\
                                fld.astype(np.double), fill_value)
        # check scan modes for rect grids.
        if nx is not None and ny is not None:
            # rows scan in the -x direction (so flip)
            #if self.scanmodeflags[0]:
            #    fldsave = fld.astype('f') # casting makes a copy
            #    fld[:,:] = fldsave[:,::-1]
            # columns scan in the -y direction (so flip)
            #if not self.scanmodeflags[1]:
            #    fldsave = fld.astype('f') # casting makes a copy
            #    fld[:,:] = fldsave[::-1,:]
            # adjacent rows scan in opposite direction.
            # (flip every other row)
            if self.scanmodeflags[3]:
                fldsave = fld.astype('f') # casting makes a copy
                fld[1::2,:] = fldsave[1::2,::-1]
        return fld

    values = property(data)

    def latlons(self):
        """alias for L{grid}"""
        return self.grid()

    def grid(self):
        """
 return lats,lons (in degrees) of grid.
 currently can handle reg. lat/lon, global gaussian, mercator, stereographic,
 lambert conformal, albers equal-area, space-view and azimuthal
 equidistant grids.  L{latlons} method does the same thing.

 @return: C{B{lats},B{lons}}, float32 numpy arrays
 containing latitudes and longitudes of grid (in degrees).
        """
        gdsinfo = self.grid_definition_info
        gdtnum = self.grid_definition_template_number
        gdtmpl = self.grid_definition_template
        reggrid = gdsinfo[2] == 0 # gdsinfo[2]=0 means regular 2-d grid
        projparams = {}
        projparams['a']=self.earthRmajor
        projparams['b']=self.earthRminor
        if gdtnum == 0: # regular lat/lon grid
            lon1, lat1 = self.longitude_first_gridpoint, self.latitude_first_gridpoint
            lon2, lat2 = self.longitude_last_gridpoint, self.latitude_last_gridpoint
            delon = self.gridlength_in_x_direction
            delat = self.gridlength_in_y_direction
            lats = np.arange(lat1,lat2+delat,delat)
            lons = np.arange(lon1,lon2+delon,delon)
            # flip if scan mode says to.
            #if self.scanmodeflags[0]:
            #    lons = lons[::-1]
            #if not self.scanmodeflags[1]:
            #    lats = lats[::-1]
            projparams['proj'] = 'cyl'
            lons,lats = np.meshgrid(lons,lats) # make 2-d arrays.
        elif gdtnum == 40: # gaussian grid (only works for global!)
            try:
                from pygrib import gaulats
            except:
                raise ImportError("pygrib required to compute Gaussian lats/lons")
            lon1, lat1 = self.longitude_first_gridpoint, self.latitude_first_gridpoint
            lon2, lat2 = self.longitude_last_gridpoint, self.latitude_last_gridpoint
            nlats = self.points_in_y_direction
            if not reggrid: # ECMWF 'reduced' gaussian grid.
                nlons = 2*nlats
                delon = 360./nlons
            else:
                nlons = self.points_in_x_direction
                delon = self.gridlength_in_x_direction
            lons = np.arange(lon1,lon2+delon,delon)
            # compute gaussian lats (north to south)
            lats = gaulats(nlats)
            if lat1 < lat2:  # reverse them if necessary
                lats = lats[::-1]
            # flip if scan mode says to.
            #if self.scanmodeflags[0]:
            #    lons = lons[::-1]
            #if not self.scanmodeflags[1]:
            #    lats = lats[::-1]
            projparams['proj'] = 'cyl'
            lons,lats = np.meshgrid(lons,lats) # make 2-d arrays
        # mercator, lambert conformal, stereographic, albers equal area, azimuthal equidistant
        elif gdtnum in [10,20,30,31,110]:
            nx = self.points_in_x_direction
            ny = self.points_in_y_direction
            dx = self.gridlength_in_x_direction
            dy = self.gridlength_in_y_direction
            lon1, lat1 = self.longitude_first_gridpoint, self.latitude_first_gridpoint
            if gdtnum == 10: # mercator.
                projparams['lat_ts']=self.proj4_lat_ts
                projparams['proj']=self.proj4_proj
                projparams['lon_0']=self.proj4_lon_0
                pj = pyproj.Proj(projparams)
                llcrnrx, llcrnry = pj(lon1,lat1)
                x = llcrnrx+dx*np.arange(nx)
                y = llcrnry+dy*np.arange(ny)
                x, y = np.meshgrid(x, y)
                lons, lats = pj(x, y, inverse=True)
            elif gdtnum == 20:  # stereographic
                projparams['lat_ts']=self.proj4_lat_ts
                projparams['proj']=self.proj4_proj
                projparams['lat_0']=self.proj4_lat_0
                projparams['lon_0']=self.proj4_lon_0
                pj = pyproj.Proj(projparams)
                llcrnrx, llcrnry = pj(lon1,lat1)
                x = llcrnrx+dx*np.arange(nx)
                y = llcrnry+dy*np.arange(ny)
                x, y = np.meshgrid(x, y)
                lons, lats = pj(x, y, inverse=True)
            elif gdtnum in [30,31]: # lambert, albers
                projparams['lat_1']=self.proj4_lat_1
                projparams['lat_2']=self.proj4_lat_2
                projparams['proj']=self.proj4_proj
                projparams['lon_0']=self.proj4_lon_0
                pj = pyproj.Proj(projparams)
                llcrnrx, llcrnry = pj(lon1,lat1)
                x = llcrnrx+dx*np.arange(nx)
                y = llcrnry+dy*np.arange(ny)
                x, y = np.meshgrid(x, y)
                lons, lats = pj(x, y, inverse=True)
            elif gdtnum == 110: # azimuthal equidistant
                projparams['proj']=self.proj4_proj
                projparams['lat_0']=self.proj4_lat_0
                projparams['lon_0']=self.proj4_lon_0
                pj = pyproj.Proj(projparams)
                llcrnrx, llcrnry = pj(lon1,lat1)
                x = llcrnrx+dx*np.arange(nx)
                y = llcrnry+dy*np.arange(ny)
                x, y = np.meshgrid(x, y)
                lons, lats = pj(x, y, inverse=True)
        elif gdtnum == 90: # satellite projection.
            nx = self.points_in_x_direction
            ny = self.points_in_y_direction
            dx = self.gridlength_in_x_direction
            dy = self.gridlength_in_y_direction
            projparams['proj']=self.proj4_proj
            projparams['lon_0']=self.proj4_lon_0
            projparams['lat_0']=self.proj4_lat_0
            projparams['h']=self.proj4_h
            pj = pyproj.Proj(projparams)
            x = dx*np.indices((ny,nx),'f')[1,:,:]
            x = x - 0.5*x.max()
            y = dy*np.indices((ny,nx),'f')[0,:,:]
            y = y - 0.5*y.max()
            lons, lats = pj(x,y,inverse=True)
            # set lons,lats to 1.e30 where undefined
            abslons = np.fabs(lons); abslats = np.fabs(lats)
            lons = np.where(abslons < 1.e20, lons, 1.e30)
            lats = np.where(abslats < 1.e20, lats, 1.e30)
        else:
            raise ValueError('unsupported grid')
        self.projparams = projparams
        return lats.astype('f'), lons.astype('f')

def Grib2Decode(filename,gribmsg=False):
    """
 Read the contents of a GRIB2 file.

 @param filename: name of GRIB2 file (default, gribmsg=False) or binary string
 representing a grib message (if gribmsg=True).

 @return:  a list of L{Grib2Message} instances representing all of the
 grib messages in the file.  Messages with multiple fields are split
 into separate messages (so that each L{Grib2Message} instance contains
 just one data field). The metadata in each GRIB2 message can be
 accessed via L{Grib2Message} instance variables, the actual data
 can be read using L{Grib2Message.data}, and the lat/lon values of the grid
 can be accesses using L{Grib2Message.grid}. If there is only one grib
 message, just the L{Grib2Message} instance is returned, instead of a list
 with one element.
    """
    if gribmsg:
        f = StringIO(filename)
    else:
        f = open(filename,'rb')
    nmsg = 0
    # loop over grib messages, read section 0, get entire grib message.
    disciplines = []
    startingpos = []
    msglen = []
    while 1:
        # find next occurence of string 'GRIB' (or EOF).
        nbyte = f.tell()
        while 1:
            f.seek(nbyte)
            start = f.read(4).decode('ascii','ignore')
            if start == '' or start == 'GRIB': break
            nbyte = nbyte + 1
        if start == '': break # at EOF
        # otherwise, start (='GRIB') contains indicator message (section 0)
        startpos = f.tell()-4
        f.seek(2,1)  # next two octets are reserved
        # get discipline info.
        disciplines.append(struct.unpack('>B',f.read(1))[0])
        # check to see it's a grib edition 2 file.
        vers = struct.unpack('>B',f.read(1))[0]
        if vers != 2:
            raise IOError('not a GRIB2 file (version number %d)' % vers)
        lengrib = struct.unpack('>q',f.read(8))[0]
        msglen.append(lengrib)
        startingpos.append(startpos)
        # read in entire grib message.
        f.seek(startpos)
        gribmsg = f.read(lengrib)
        # make sure the message ends with '7777'
        end = gribmsg[-4:lengrib].decode('ascii','ignore')
        if end != '7777':
           raise IOError('partial GRIB message (no "7777" at end)')
        # do next message.
        nmsg=nmsg+1
    # if no grib messages found, nmsg is still 0 and it's not GRIB.
    if nmsg==0:
       raise IOError('not a GRIB file')
    # now for each grib message, find number of fields.
    numfields = []
    f.seek(0) # rewind file.
    for n in range(nmsg):
        f.seek(startingpos[n])
        gribmsg = f.read(msglen[n])
        pos = 0
        numflds = 0
        while 1:
            if gribmsg[pos:pos+4].decode('ascii','ignore') == 'GRIB':
                sectnum = 0
                lensect = 16
            elif gribmsg[pos:pos+4].decode('ascii','ignore') == '7777':
                break
            else:
                lensect = struct.unpack('>i',gribmsg[pos:pos+4])[0]
                sectnum = struct.unpack('>B',gribmsg[pos+4:pos+5])[0]
                if sectnum == 4: numflds=numflds+1
                #if sectnum == 2: numlocal=numlocal+1
            pos = pos + lensect
            #print sectnum,lensect,pos
        #print n+1,len(gribmsg),numfields,numlocal
        numfields.append(numflds)
    # decode each section in grib message (sections 1 and above).
    gdtnum = [] # grid defn template number from sxn 3
    gdtmpl = [] # grid defn template from sxn 3
    gdeflist = [] # optional grid definition list from sxn 3
    gdsinfo = [] # grid definition section info from sxn3
    pdtmpl = [] # product defn template from sxn 4
    pdtnum = [] # product defn template number from sxn 4
    coordlist = [] # vertical coordinate info from sxn 4
    drtmpl = [] # data representation template from sxn 5
    drtnum = [] # data representation template number from sxn 5
    ndpts = [] # number of data points to be unpacked (from sxn 5)
    bitmapflag = [] # bit-map indicator flag from sxn 6
    bitmap = [] # bitmap from sxn 6.
    pos7 = [] # byte offset for section 7.
    localsxn = [] # local use sections.
    msgstart = [] # byte offset in file for message start.
    msglength = [] # length of the message in bytes.
    message = [] # the actual grib message.
    identsect = [] # identification section (section 1).
    discipline = [] # discipline code.
    for n in range(nmsg):
        spos = startingpos[n]
        lengrib = msglen[n]
        #gribmsg = gribmsgs[n]
        f.seek(spos)
        gribmsg = f.read(lengrib)
        discipl = disciplines[n]
        lensect0 = 16
        # get length of section 1 and section number.
        #lensect1 = struct.unpack('>i',gribmsg[lensect0:lensect0+4])[0]
        #sectnum1 = struct.unpack('>B',gribmsg[lensect0+4])[0]
        #print 'sectnum1, lensect1 = ',sectnum1,lensect1
        # unpack section 1, octets 1-21 (13 parameters).  This section
        # can occur only once per grib message.
        #idsect,pos = _unpack1(gribmsg,lensect0) # python version
        idsect,pos = g2clib.unpack1(gribmsg,lensect0,np.empty) # c version
        # loop over rest of sections in message.
        gdtnums = []
        gdtmpls = []
        gdeflists = []
        gdsinfos = []
        pdtmpls = []
        coordlists = []
        pdtnums = []
        drtmpls = []
        drtnums = []
        ndptslist = []
        bitmapflags = []
        bitmaps = []
        sxn7pos = []
        localsxns = []
        while 1:
            # check to see if this is the end of the message.
            if gribmsg[pos:pos+4].decode('ascii','ignore') == '7777': break
            lensect = struct.unpack('>i',gribmsg[pos:pos+4])[0]
            sectnum = struct.unpack('>B',gribmsg[pos+4:pos+5])[0]
            # section 2, local use section.
            if sectnum == 2:
                # "local use section", used by NDFD to store WX
                # strings.  This section is returned as a raw
                # bytestring for further dataset-specific parsing,
                # not as a numpy array.
                localsxns.append(gribmsg[pos+5:pos+lensect])
                pos = pos + lensect
            # section 3, grid definition section.
            elif sectnum == 3:
                gds,gdtempl,deflist,pos = g2clib.unpack3(gribmsg,pos,np.empty)
                gdtnums.append(gds[4])
                gdtmpls.append(gdtempl)
                gdeflists.append(deflist)
                gdsinfos.append(gds)
            # section, product definition section.
            elif sectnum == 4:
                pdtempl,pdtn,coordlst,pos = g2clib.unpack4(gribmsg,pos,np.empty)
                pdtmpls.append(pdtempl)
                coordlists.append(coordlst)
                pdtnums.append(pdtn)
            # section 5, data representation section.
            elif sectnum == 5:
                drtempl,drtn,npts,pos = g2clib.unpack5(gribmsg,pos,np.empty)
                drtmpls.append(drtempl)
                drtnums.append(drtn)
                ndptslist.append(npts)
            # section 6, bit-map section.
            elif sectnum == 6:
                bmap,bmapflag = g2clib.unpack6(gribmsg,gds[1],pos,np.empty)
                #bitmapflag = struct.unpack('>B',gribmsg[pos+5])[0]
                if bmapflag == 0:
                    bitmaps.append(bmap.astype('b'))
                # use last defined bitmap.
                elif bmapflag == 254:
                    bmapflag = 0
                    for bmp in bitmaps[::-1]:
                        if bmp is not None: bitmaps.append(bmp)
                else:
                    bitmaps.append(None)
                bitmapflags.append(bmapflag)
                pos = pos + lensect
            # section 7, data section (nothing done here,
            # data unpacked when getfld method is invoked).
            else:
                if sectnum != 7:
                   msg = 'unknown section = %i' % sectnum
                   raise ValueError(msg)
                sxn7pos.append(pos)
                pos = pos + lensect
        # extend by repeating last value for all remaining fields.
        gdtnum.append(_repeatlast(numfields[n],gdtnums))
        gdtmpl.append(_repeatlast(numfields[n],gdtmpls))
        gdeflist.append(_repeatlast(numfields[n],gdeflists))
        gdsinfo.append(_repeatlast(numfields[n],gdsinfos))
        pdtmpl.append(_repeatlast(numfields[n],pdtmpls))
        pdtnum.append(_repeatlast(numfields[n],pdtnums))
        coordlist.append(_repeatlast(numfields[n],coordlists))
        drtmpl.append(_repeatlast(numfields[n],drtmpls))
        drtnum.append(_repeatlast(numfields[n],drtnums))
        ndpts.append(_repeatlast(numfields[n],ndptslist))
        bitmapflag.append(_repeatlast(numfields[n],bitmapflags))
        bitmap.append(_repeatlast(numfields[n],bitmaps))
        pos7.append(_repeatlast(numfields[n],sxn7pos))
        if len(localsxns) == 0:
            localsxns = [None]
        localsxn.append(_repeatlast(numfields[n],localsxns))
        msgstart.append(_repeatlast(numfields[n],[spos]))
        msglength.append(_repeatlast(numfields[n],[lengrib]))
        identsect.append(_repeatlast(numfields[n],[idsect]))
        discipline.append(_repeatlast(numfields[n],[discipl]))

    gdtnum = _flatten(gdtnum)
    gdtmpl = _flatten(gdtmpl)
    gdeflist = _flatten(gdeflist)
    gdsinfo = _flatten(gdsinfo)
    pdtmpl = _flatten(pdtmpl)
    pdtnum = _flatten(pdtnum)
    coordlist = _flatten(coordlist)
    drtmpl = _flatten(drtmpl)
    drtnum = _flatten(drtnum)
    ndpts = _flatten(ndpts)
    bitmapflag = _flatten(bitmapflag)
    bitmap = _flatten(bitmap)
    pos7 = _flatten(pos7)
    localsxn = _flatten(localsxn)
    msgstart = _flatten(msgstart)
    msglength = _flatten(msglength)
    identsect = _flatten(identsect)
    discipline = _flatten(discipline)

    gribs = []
    for n in range(len(msgstart)):
        kwargs = {}
        kwargs['originating_center']=_table0[identsect[n][0]][0]
        wmo_code = _table0[identsect[n][0]][1]
        if wmo_code is not None:
            kwargs['center_wmo_code']=wmo_code
        kwargs['grid_definition_template_number']=gdtnum[n]
        kwargs['grid_definition_template']=gdtmpl[n]
        if gdeflist[n].size > 0:
            kwargs['grid_definition_list']=gdeflist[n]
        kwargs['grid_definition_info']=gdsinfo[n]
        kwargs['discipline_code']=discipline[n]
        kwargs['product_definition_template_number']=pdtnum[n]
        kwargs['product_definition_template']=pdtmpl[n]
        kwargs['data_representation_template_number']=drtnum[n]
        kwargs['data_representation_template']=drtmpl[n]
        if coordlist[n].size > 0:
            kwargs['extra_vertical_coordinate_info']=coordlist[n]
        kwargs['number_of_data_points_to_unpack']=ndpts[n]
        kwargs['bitmap_indicator_flag']=bitmapflag[n]
        if bitmap[n] is not []:
            kwargs['_bitmap']=bitmap[n]
        kwargs['_section7_byte_offset']=pos7[n]
        kwargs['_grib_message_byteoffset']=msgstart[n]
        kwargs['_grib_message_length']=msglength[n]
        kwargs['_grib_filename']=filename
        kwargs['identification_section']=identsect[n]
        kwargs['_grib_message_number']=n+1
        if localsxn[n] is not None:
            kwargs['has_local_use_section'] = True
            kwargs['_local_use_section']=localsxn[n]
        else:
            kwargs['has_local_use_section'] = False
        gribs.append(Grib2Message(**kwargs))
    f.close()
    if len(gribs) == 1:
        return gribs[0]
    else:
        return gribs

def dump(filename, grbs):
    """
 write the given L{Grib2Message} instances to a grib file.

 @param filename: file to write grib data to.
 @param grbs: a list of L{Grib2Message} instances.
    """
    gribfile = open(filename,'wb')
    for grb in grbs:
        try:
            f = open(grb._grib_filename,'rb')
        except TypeError:
            f = StringIO(grb._grib_filename)
        f.seek(grb._grib_message_byteoffset)
        gribmsg = f.read(grb._grib_message_length)
        f.close()
        gribfile.write(gribmsg)
    gribfile.close()

# private methods and functions below here.

def _getdate(idsect):
    """return yyyy,mm,dd,min,ss from section 1"""
    yyyy=idsect[5]
    mm=idsect[6]
    dd=idsect[7]
    hh=idsect[8]
    min=idsect[9]
    ss=idsect[10]
    return yyyy,mm,dd,hh,min,ss

def _unpack1(gribmsg,pos):
    """unpack section 1 given starting point in bytes
    used to test pyrex interface to g2_unpack1"""
    idsect = []
    pos = pos + 5
    idsect.append(struct.unpack('>h',gribmsg[pos:pos+2])[0])
    pos = pos + 2
    idsect.append(struct.unpack('>h',gribmsg[pos:pos+2])[0])
    pos = pos + 2
    idsect.append(struct.unpack('>B',gribmsg[pos:pos+1])[0])
    pos = pos + 1
    idsect.append(struct.unpack('>B',gribmsg[pos:pos+1])[0])
    pos = pos + 1
    idsect.append(struct.unpack('>B',gribmsg[pos:pos+1])[0])
    pos = pos + 1
    idsect.append(struct.unpack('>h',gribmsg[pos:pos+2])[0])
    pos = pos + 2
    idsect.append(struct.unpack('>B',gribmsg[pos:pos+1])[0])
    pos = pos + 1
    idsect.append(struct.unpack('>B',gribmsg[pos:pos+1])[0])
    pos = pos + 1
    idsect.append(struct.unpack('>B',gribmsg[pos:pos+1])[0])
    pos = pos + 1
    idsect.append(struct.unpack('>B',gribmsg[pos:pos+1])[0])
    pos = pos + 1
    idsect.append(struct.unpack('>B',gribmsg[pos:pos+1])[0])
    pos = pos + 1
    idsect.append(struct.unpack('>B',gribmsg[pos:pos+1])[0])
    pos = pos + 1
    idsect.append(struct.unpack('>B',gribmsg[pos:pos+1])[0])
    pos = pos + 1
    return np.array(idsect,'i'),pos

def _repeatlast(numfields,listin):
    """repeat last item in listin, until len(listin) = numfields"""
    if len(listin) < numfields:
        last = listin[-1]
        for n in range(len(listin),numfields):
            listin.append(last)
    return listin

def _flatten(lst):
    return functools.reduce(operator.add,lst)


class Grib2Encode:
    """
 Class for encoding data into a GRIB2 message.
  - Creating a class instance (L{__init__}) initializes the message and adds
    sections 0 and 1 (the indicator and identification sections),
  - method L{addgrid} adds a grid definition (section 3) to the messsage.
  - method L{addfield} adds sections 4-7 to the message (the product
    definition, data representation, bitmap and data sections).
  - method L{end} adds the end section (section 8) and terminates the message.


 A GRIB Edition 2 message is a machine independent format for storing
 one or more gridded data fields.  Each GRIB2 message consists of the
 following sections:
  - SECTION 0: Indicator Section - only one per message
  - SECTION 1: Identification Section - only one per message
  - SECTION 2: (Local Use Section) - optional
  - SECTION 3: Grid Definition Section
  - SECTION 4: Product Definition Section
  - SECTION 5: Data Representation Section
  - SECTION 6: Bit-map Section
  - SECTION 7: Data Section
  - SECTION 8: End Section

 Sequences of GRIB sections 2 to 7, 3 to 7, or sections 4 to 7 may be repeated
 within a single GRIB message.  All sections within such repeated sequences
 must be present and shall appear in the numerical order noted above.
 Unrepeated sections remain in effect until redefined.

 Note:  Writing section 2 (the 'local use section') is
 not yet supported.

 @ivar msg: A binary string containing the GRIB2 message.
 After the message has been terminated by calling
 the L{end} method, this string can be written to a file.
    """

    def __init__(self, discipline, idsect):
        """
 create a Grib2Enecode class instance given the GRIB2 discipline
 parameter and the identification section (sections 0 and 1).

 The GRIB2 message is stored as a binary string in instance variable L{msg}.

 L{addgrid}, L{addfield} and L{end} class methods must be called to complete
 the GRIB2 message.

 @param discipline:  Discipline or GRIB Master Table Number (Code Table 0.0).
 (0 for meteorlogical, 1 for hydrological, 2 for land surface, 3 for space,
 10 for oceanographic products).

 @param idsect:  Sequence containing identification section (section 1).
  - idsect[0]=Id of orginating centre (Common Code
    U{Table C-1<http://www.nws.noaa.gov/tg/GRIB_C1.htm>})
  - idsect[1]=Id of orginating sub-centre (local table)
  - idsect[2]=GRIB Master Tables Version Number (Code
    U{Table 1.0
    <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table1-0.shtml>})
  - idsect[3]=GRIB Local Tables Version Number (Code
    U{Table 1.1
    <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table1-1.shtml>})
  - idsect[4]=Significance of Reference Time (Code
    U{Table 1.2
    <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table1-2.shtml>})
  - idsect[5]=Reference Time - Year (4 digits)
  - idsect[6]=Reference Time - Month
  - idsect[7]=Reference Time - Day
  - idsect[8]=Reference Time - Hour
  - idsect[9]=Reference Time - Minute
  - idsect[10]=Reference Time - Second
  - idsect[11]=Production status of data (Code
    U{Table 1.3
    <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table1-3.shtml>})
  - idsect[12]=Type of processed data (Code
    U{Table
    1.4<http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table1-4.shtml>})
        """
        self.msg,msglen=g2clib.grib2_create(np.array([discipline,2],np.int32),np.array(idsect,np.int32))

    def addgrid(self,gdsinfo,gdtmpl,deflist=None):
        """
 Add a grid definition section (section 3) to the GRIB2 message.

 @param gdsinfo: Sequence containing information needed for the grid definition section.
  - gdsinfo[0] = Source of grid definition (see Code
    U{Table 3.0
    <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table3-0.shtml>})
  - gdsinfo[1] = Number of grid points in the defined grid.
  - gdsinfo[2] = Number of octets needed for each additional grid points defn.
    Used to define number of points in each row for non-reg grids (=0 for
    regular grid).
  - gdsinfo[3] = Interp. of list for optional points defn (Code
    U{Table 3.11
    <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table3-11.shtml>})
  - gdsinfo[4] = Grid Definition Template Number (Code
    U{Table 3.1
    <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table3-1.shtml>})

 @param gdtmpl: Contains the data values for the specified Grid Definition
 Template ( NN=gdsinfo[4] ).  Each element of this integer
 array contains an entry (in the order specified) of Grid
 Definition Template 3.NN

 @param deflist: (Used if gdsinfo[2] != 0)  Sequence containing the
 number of grid points contained in each row (or column)
 of a non-regular grid.
        """
        if deflist is not None:
            dflist = np.array(deflist,'i')
        else:
            dflist = None
        self.scanmodeflags = None
        gdtnum = gdsinfo[4]
        if gdtnum in [0,1,2,3,40,41,42,43,44,203,205,32768,32769]:
            self.scanmodeflags = _dec2bin(gdtmpl[18])[0:4]
        elif gdtnum == 10: # mercator
            self.scanmodeflags = _dec2bin(gdtmpl[15])[0:4]
        elif gdtnum == 20: # stereographic
            self.scanmodeflags = _dec2bin(gdtmpl[17])[0:4]
        elif gdtnum == 30: # lambert conformal
            self.scanmodeflags = _dec2bin(gdtmpl[17])[0:4]
        elif gdtnum == 31: # albers equal area.
            self.scanmodeflags = _dec2bin(gdtmpl[17])[0:4]
        elif gdtnum == 90: # near-sided vertical perspective satellite projection
            self.scanmodeflags = _dec2bin(gdtmpl[16])[0:4]
        elif gdtnum == 110: # azimuthal equidistant.
            self.scanmodeflags = _dec2bin(gdtmpl[15])[0:4]
        elif gdtnum == 120:
            self.scanmodeflags = _dec2bin(gdtmpl[6])[0:4]
        elif gdtnum == 204: # curvilinear orthogonal
            self.scanmodeflags = _dec2bin(gdtmpl[18])[0:4]
        elif gdtnum in [1000,1100]:
            self.scanmodeflags = _dec2bin(gdtmpl[12])[0:4]
        self.msg,msglen=g2clib.grib2_addgrid(self.msg,np.array(gdsinfo,'i'),np.array(gdtmpl,'i'),dflist)

    def addfield(self,pdtnum,pdtmpl,drtnum,drtmpl,field,coordlist=None):
        """
 Add a product definition section, data representation section,
 bitmap section and data section to the GRIB2 message (sections 4-7).
 Must be called after grid definition section is created with L{addgrid}.

 @param pdtnum: Product Definition Template Number (see Code U{Table
 4.0<http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table4-0.shtml>})

 @param pdtmpl: Sequence with the data values for the specified Product Definition
 Template (N=pdtnum).  Each element of this integer
 array contains an entry (in the order specified) of Product
 Definition Template 4.N

 @param drtnum: Data Representation Template Number (see Code
 U{Table 5.0
 <http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_table5-0.shtml>})

 @param drtmpl: Sequence with the data values for the specified Data Representation
 Template (N=drtnum).  Each element of this integer
 array contains an entry (in the order specified) of Data
 Representation Template 5.N
 Note that some values in this template (eg. reference
 values, number of bits, etc...) may be changed by the
 data packing algorithms.
 Use this to specify scaling factors and order of
 spatial differencing, if desired.

 @param field:  numpy array of data points to pack.
 If field is a masked array, then a bitmap is created from
 the mask.

 @param coordlist: Sequence containing floating point values intended to document
 the vertical discretization with model data
 on hybrid coordinate vertical levels. Default None.
        """
        if not hasattr(self,'scanmodeflags'):
            raise ValueError('addgrid must be called before addfield')
        # reorder array to be consistent with
        # specified scan order.
        if self.scanmodeflags is not None:
            #if self.scanmodeflags[0]:
            ## rows scan in the -x direction (so flip)
            #    fieldsave = field.astype('f') # casting makes a copy
            #    field[:,:] = fieldsave[:,::-1]
            ## columns scan in the -y direction (so flip)
            #if not self.scanmodeflags[1]:
            #    fieldsave = field.astype('f') # casting makes a copy
            #    field[:,:] = fieldsave[::-1,:]
            # adjacent rows scan in opposite direction.
            # (flip every other row)
            if self.scanmodeflags[3]:
                fieldsave = field.astype('f') # casting makes a copy
                field[1::2,:] = fieldsave[1::2,::-1]
        fld = field.astype('f')
        if ma.isMA(field):
            bmap = 1 - np.ravel(field.mask.astype('i'))
            bitmapflag  = 0
        else:
            bitmapflag = 255
            bmap = None
        if coordlist is not None:
            crdlist = np.array(coordlist,'f')
        else:
            crdlist = None
        self.msg,msglen=g2clib.grib2_addfield(self.msg,pdtnum,np.array(pdtmpl,'i'),crdlist,drtnum,np.array(drtmpl,'i'),np.ravel(fld),bitmapflag,bmap)

    def end(self):
        """
 Add an end section (section 8) to the GRIB2 message.
 A GRIB2 message is not complete without an end section.
 Once an end section is added, the GRIB2 message can be
 output to a file.
        """
        self.msg,msglen=g2clib.grib2_end(self.msg)
