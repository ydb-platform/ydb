"""
Grafana unit formats
(https://github.com/grafana/grafana/blob/main/packages/grafana-data/src/valueFormats/categories.ts)

To use:
from grafanalib import formatunits as UNITS

format = UNITS.BYTES
"""

NO_FORMAT = 'none'
NONE_FORMAT = 'none'
NUMBER_FORMAT = 'none'
STRING_FORMAT = 'string'
PERCENT_UNIT = 'percentunit'
PERCENT_FORMAT = 'percent'
SHORT = 'short'
HUMIDITY = 'humidity'     # %H
DECIBEL = 'dB'
HEXADECIMAL_OX = 'hex0x'  # 0x
HEXADECIMAL = 'hex'
SCI_NOTATION = 'sci'
LOCAL_FORMAT = 'locale'
PIXELS = 'pixel'
# Acceleration
METERS_SEC_2 = 'accMS2'  # m/sec²
FEET_SEC_2 = 'accFS2'    # f/sec²
G_UNIT = 'accG'          # g
# Angle
DEGREES = 'degree'      # °
RADIANS = 'radian'      # rad
GRADIAN = 'grad'        # grad
ARC_MINUTES = 'arcmin'  # arcmin
ARC_SECONDS = 'arcsec'  # arcsec
# Area
SQUARE_METERS = 'areaM2'  # m²
SQUARE_FEET = 'areaF2'    # ft²
SQUARE_MILES = 'areaMI2'  # mi²
# Computation
FLOPS_PER_SEC = 'flops'         # FLOP/s
MEGA_FLOPS_PER_SEC = 'mflops'   # MFLOP/s
GIGA_FLOPS_PER_SEC = 'gflops'   # GFLOP/s
TERA_FLOPS_PER_SEC = 'tflops'   # TFLOP/s
PETA_FLOPS_PER_SEC = 'pflops'   # PFLOP/s
EXA_FLOPS_PER_SEC = 'eflops'    # EFLOP/s
ZETTA_FLOPS_PER_SEC = 'zflops'  # ZFLOP/s
YOTTA_FLOPS_PER_SEC = 'yflops'  # YFLOP/s
# Concentration
PARTS_PER_MILLION = 'ppm'                       # ppm
PARTS_PER_BILLION = 'conppb'                    # ppb
NANO_GRAM_PER_CUBIC_METER = 'conngm3'           # ng/m³
NANO_GRAM_PER_NORMAL_CUBIC_METER = 'conngNm3'   # ng/Nm³
MICRO_GRAM_PER_CUBIC_METER = 'conμgm3'          # μg/m³
MICRO_GRAM_PER_NORMAL_CUBIC_METER = 'conμgNm3'  # μg/Nm³
MILLI_GRAM_PER_CUBIC_METER = 'conmgm3'          # mg/m³
MILLI_GRAM_PER_NORMAL_CUBIC_METER = 'conmgNm3'  # mg/Nm³
GRAM_PER_CUBIC_METER = 'congm3'                 # g/m³
GRAM_PER_NORMAL_CUBIC_METER = 'congNm3'         # g/Nm³
MILLI_GRAM_PER_DECI_LITRE = 'conmgdL'           # mg/dL
MILLI_MOLES_PER_LITRE = 'conmmolL'              # mmol/L
# Currency
DOLLARS = 'currencyUSD'             # $
POUNDS = 'currencyGBP'              # £
EURO = 'currencyEUR'                # €
YEN = 'currencyJPY'                 # ¥
RUBLES = 'currencyRUB'              # ₽
HRYVNIAS = 'currencyUAH'            # ₴
REAL = 'currencyBRL'                # R$
DANISH_KRONE = 'currencyDKK'        # kr
ICELANDIC_KRONA = 'currencyISK'     # kr
NORWEGIAN_KRONE = 'currencyNOK'     # kr
SWEDISH_KORNA = 'currencySEK'       # kr
CZECH_KORUNA = 'currencyCZK'        # czk
SWISS_FRANC = 'currencyCHF'         # CHF
POLISH_ZLOTY = 'currencyPLN'        # PLN
BITCOIN = 'currencyBTC'             # ฿
MILLI_BITCOIN = 'currencymBTC'      # mBTC
MICRO_BITCOIN = 'currencyμBTC'      # μBTC
SOUTH_AFRICAN_RAND = 'currencyZAR'  # R
INDIAN_RUPEE = 'currencyINR'        # ₹
SOUTH_KOREAN_WON = 'currencyKRW'    # ₩
INDONESIAN_RUPIAH = 'currencyIDR'   # Rp
PHILIPPINE_PESO = 'currencyPHP'     # PHP
# Data
BYTES_IEC = 'bytes'
BYTES = 'decbytes'          # B
BITS_IEC = 'bits'
BITS = 'decbits'
KIBI_BYTES = 'kbytes'       # KiB
KILO_BYTES = 'deckbytes'    # kB
MEBI_BYTES = 'mbytes'       # MiB
MEGA_BYTES = 'decmbytes'    # MB
GIBI_BYTES = 'gbytes'       # GiB
GIGA_BYTES = 'decgbytes'    # GB
TEBI_BYTES = 'tbytes'       # TiB
TERA_BYTES = 'dectbytes'    # TB
PEBI_BYTES = 'pbytes'       # PiB
PETA_BYTES = 'decpbytes'    # PB
# Data Rate
PACKETS_SEC = 'pps'      # p/s

BYTES_SEC_IEC = 'binBps'  # B/s
KIBI_BYTES_SEC = 'KiBs'   # KiB/s
MEBI_BYTES_SEC = 'MiBs'   # MiB/s
GIBI_BYTES_SEC = 'GiBs'   # GiB/s
TEBI_BYTES_SEC = 'TiBs'   # TiB/s
PEBI_BYTES_SEC = 'PiBs'   # PB/s

BYTES_SEC = 'Bps'        # B/s
KILO_BYTES_SEC = 'KBs'   # kB/s
MEGA_BYTES_SEC = 'MBs'   # MB/s
GIGA_BYTES_SEC = 'GBs'   # GB/s
TERA_BYTES_SEC = 'TBs'   # TB/s
PETA_BYTES_SEC = 'PBs'   # PB/s

BITS_SEC_IEC = 'binbps'   # b/s
KIBI_BITS_SEC = 'Kibits'  # Kib/s
MEBI_BITS_SEC = 'Mibits'  # Mib/s
GIBI_BITS_SEC = 'Gibits'  # Gib/s
TEBI_BITS_SEC = 'Tibits'  # Tib/s
PEBI_BITS_SEC = 'Pibits'  # Pib/s

BITS_SEC = 'bps'         # b/s
KILO_BITS_SEC = 'Kbits'  # kb/s
MEGA_BITS_SEC = 'Mbits'  # Mb/s
GIGA_BITS_SEC = 'Gbits'  # Gb/s
TERA_BITS_SEC = 'Tbits'  # Tb/s
PETA_BITS_SEC = 'Pbits'  # Pb/s
# Date & Time
DATE_TIME_ISO = 'dateTimeAsIso'
DATE_TIME_ISO_TODAY = 'dateTimeAsIsoNoDateIfToday'
DATE_TIME_US = 'dateTimeAsUS'
DATE_TIME_US_TODAY = 'dateTimeAsUSNoDateIfToday'
DATE_TIME_LOCAL = 'dateTimeAsLocal'
DATE_TIME_LOCAL_TODAY = 'dateTimeAsLocalNoDateIfToday'
DATE_TIME_DEFAULT = 'dateTimeAsSystem'
DATE_TIME_FROM_NOW = 'dateTimeFromNow'
# Energy
WATT = 'watt'          # W
KILO_WATT = 'kwatt'    # kW
MEGA_WATT = 'megwatt'  # MW
GIGA_WATT = 'gwatt'    # GW
MILLI_WATT = 'mwatt'   # mW
WATT_SQUARE_METER = 'Wm2'      # W/m²
VOLT_AMPERE = 'voltamp'        # VA
KILO_VOLT_AMPERE = 'kvoltamp'  # kVA
VAR = 'voltampreact'           # VAR
KILO_VAR = 'kvoltampreact'     # kVAR
WATT_HOUR = 'watth'            # Wh
WATT_HOUR_KILO = 'watthperkg'  # Wh/kg
KILO_WATT_HOUR = 'kwatth'      # kWh
KILO_WATT_MIN = 'kwattm'       # kWm
AMPERE_HOUR = 'amph'           # Ah
KILO_AMPERE_HR = 'kamph'       # kAh
MILLI_AMPER_HOUR = 'mamph'     # mAh
JOULE = 'joule'        # J
ELECTRON_VOLT = 'ev'   # eV
AMPERE = 'amp'         # A
KILO_AMPERE = 'kamp'   # kA
MILLI_AMPERE = 'mamp'  # mA
VOLT = 'volt'          # V
KILO_VOLT = 'kvolt'    # kV
MILLI_VOLT = 'mvolt'   # mV
DECIBEL_MILLI_WATT = 'dBm'  # dBm
OHM = 'ohm'        # Ω
KILO_OHM = 'kohm'  # kΩ
MEGA_OHM = 'Mohm'  # MΩ
FARAD = 'farad'         # F
MICRO_FARAD = 'µfarad'  # µF
NANO_FARAD = 'nfarad'   # nF
PICO_FARAD = 'pfarad'   # pF
FEMTO_FARAD = 'ffarad'  # fF
HENRY = 'henry'         # H
MILLI_HENRY = 'mhenry'  # mH
MICRO_HENRY = 'µhenry'  # µH
LUMENS = 'lumens'       # Lm
# Flow
GALLONS_PER_MIN = 'flowgpm'       # gpm
CUBIC_METERS_PER_SEC = 'flowcms'  # cms
CUBIC_FEET_PER_SEC = 'flowcfs'    # cfs
CUBIC_FEET_PER_MIN = 'flowcfm'    # cfm
LITRES_PER_HOUR = 'litreh'        # L/h
LITRES_PER_MIN = 'flowlpm'        # L/min
MILLI_LITRE_PER_MIN = 'flowmlpm'  # mL/min
LUX = 'lux'                       # lx
# Force
NEWTON_METERS = 'forceNm'        # Nm
KILO_NEWTON_METERS = 'forcekNm'  # kNm
NEWTONS = 'forceN'               # N
KILO_NEWTONS = 'forcekN'         # kN
# Hash Rate
HASHES_PER_SEC = 'Hs'        # H/s
KILO_HASHES_PER_SEC = 'KHs'  # kH/s
MEGA_HASHES_PER_SEC = 'MHs'  # MH/s
GIGA_HASHES_PER_SEC = 'GHs'  # GH/s
TERA_HASHES_PER_SEC = 'THs'  # TH/s
PETA_HASHES_PER_SEC = 'PHs'  # PH/s
EXA_HASHES_PER_SEC = 'EHs'   # EH/s
# Mass
MILLI_GRAM = 'massmg'  # mg
GRAM = 'massg'         # g
POUND = 'masslb'       # lb
KILO_GRAM = 'masskg'   # kg
METRIC_TON = 'masst'   # t
# Length
MILLI_METER = 'lengthmm'  # mm
INCH = 'lengthin'         # in
METER = 'lengthm'         # m
KILO_METER = 'lengthkm'   # km
FEET = 'lengthft'         # ft
MILE = 'lengthmi'         # mi
# Pressure
MILLI_BARS = 'pressurembar'       # mBar,
BARS = 'pressurebar'              # Bar,
KILO_BARS = 'pressurekbar'        # kBar,
PASCALS = 'pressurepa'            # Pa
HECTO_PASCALS = 'pressurehpa'     # hPa
KILO_PASCALS = 'pressurekpa'      # kPa
INCHES_OF_MERCURY = 'pressurehg'  # "Hg
PSI = 'pressurepsi'               # psi
# Radiation
BECQUEREL = 'radbq'       # Bq
CURIE = 'radci'           # Ci
GRAY = 'radgy'            # Gy
RAD = 'radrad'            # rad
MICROSIEVERT = 'radusv'   # µSv
MILLI_SIEVERT = 'radmsv'  # mSv
SIEVERT = 'radsv'         # Sv
REM = 'radrem'            # rem
EXPOSURE = 'radexpckg'    # C/kg
ROENTGEN = 'radr'         # R
MICRO_SIEVERT_PER_HOUR = 'radusvh'  # µSv/h
MILLI_SIEVERT_PER_HOUR = 'radmsvh'  # mSv/h
SIEVERT_PER_HOUR = 'radsvh'         # Sv/h
# Rotational Speed
RPM = 'rotrpm'                  # rpm
HERTZ_ROTATION = 'rothz'        # Hz
RADS_PER_SEC = 'rotrads'        # rad/s
DEGREES_PER_SECOND = 'rotdegs'  # °/s
# Temperature
CELSUIS = 'celsius'       # °C
FARENHEIT = 'fahrenheit'  # °F
KELVIN = 'kelvin'         # K
# Time
HERTZ = 'hertz'  # Hz
NANO_SECONDS = 'ns'   # ns
MICRO_SECONDS = 'µs'  # µs
MILLI_SECONDS = 'ms'  # ms
SECONDS = 's'         # s
MINUTES = 'm'         # m
HOURS = 'h'           # h
DAYS = 'd'            # d
DURATION_MILLI_SECONDS = 'dtdurationms'  # ms
DURATION_SECONDS = 'dtdurations'         # s
HH_MM_SS = 'dthms'        # hh:mm:ss
D_HH_MM_SS = 'dtdhms'     # d hh:mm:ss
TIME_TICKS = 'timeticks'  # s/100
CLOCK_MSEC = 'clockms'    # ms
CLOCK_SEC = 'clocks'      # s
# Throughput
COUNTS_PER_SEC = 'cps'      # cps
OPS_PER_SEC = 'ops'         # ops
REQUESTS_PER_SEC = 'reqps'  # rps
READS_PER_SEC = 'rps'       # rps
WRITES_PER_SEC = 'wps'      # wps
IO_OPS_PER_SEC = 'iops'     # iops
COUNTS_PER_MIN = 'cpm'      # cpm
OPS_PER_MIN = 'opm'         # opm
READS_PER_MIN = 'rpm'       # rpm
WRITES_PER_MIN = 'wpm'      # wpm
# Velocity
METERS_PER_SEC = 'velocityms'        # m/s
KILO_METERS_PER_SEC = 'velocitykmh'  # km/h
MILES_PER_HOUR = 'velocitymph'       # mph
KNOTS = 'velocityknot'               # kn
# Volume
MILLI_LITRE = 'mlitre'       # mL
LITRE = 'litre'              # L
CUBIC_METER = 'm3'           # m³
NORMAL_CUBIC_METER = 'Nm3'   # Nm³
CUBIC_DECI_METER = 'dm3'     # dm³
GALLONS = 'gallons'          # g
# Boolean
TRUE_FALSE = 'bool'      # True/False
YES_NO = 'bool_yes_no'   # Yes/No
ON_OFF = 'bool_on_off'   # On/Off
