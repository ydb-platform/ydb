__all__ = ['Units']

from typing import Optional


class Units:
    # As defined in ISO-14229:2006 Annex C
    class Prefixs:
        class Prefix:
            id: int
            name: str
            symbol: str
            description: Optional[str]

            def __init__(self, id: int, name: str, symbol: str, description: Optional[str] = None):
                self.name = name
                self.id = id
                self.symbol = symbol
                self.description = description

            def __str__(self) -> str:
                return self.name

            def __repr__(self) -> str:
                desc = "(%s) " % self.description if self.description is not None else ""
                return "<UDS Unit prefix : %s[%s] %swith ID=%d at %08x>" % (self.name, self.symbol, desc, self.id, id(self))

        exa = Prefix(id=0x40, name='exa', symbol='E', description='10e18')
        peta = Prefix(id=0x41, name='peta', symbol='P', description='10e15')
        tera = Prefix(id=0x42, name='tera', symbol='T', description='10e12')
        giga = Prefix(id=0x43, name='giga', symbol='G', description='10e9')
        mega = Prefix(id=0x44, name='mega', symbol='M', description='10e6')
        kilo = Prefix(id=0x45, name='kilo', symbol='k', description='10e3')
        hecto = Prefix(id=0x46, name='hecto', symbol='h', description='10e2')
        deca = Prefix(id=0x47, name='deca', symbol='da', description='10e1')
        deci = Prefix(id=0x48, name='deci', symbol='d', description='10e-1')
        centi = Prefix(id=0x49, name='centi', symbol='c', description='10e-2')
        milli = Prefix(id=0x4A, name='milli', symbol='m', description='10e-3')
        micro = Prefix(id=0x4B, name='micro', symbol='m', description='10e-6')
        nano = Prefix(id=0x4C, name='nano', symbol='n', description='10e-9')
        pico = Prefix(id=0x4D, name='pico', symbol='p', description='10e-12')
        femto = Prefix(id=0x4E, name='femto', symbol='f', description='10e-15')
        atto = Prefix(id=0x4F, name='atto', symbol='a', description='10e-18')

    class Unit:
        id: int
        name: str
        symbol: str
        description: Optional[str]

        def __init__(self, id: int, name: str, symbol: str, description: Optional[str] = None):
            self.id = id
            self.name = name
            self.symbol = symbol
            self.description = description

        def __str__(self):
            return self.name

        def __repr__(self):
            desc = "(unit of %s) " % self.description if self.description is not None else ""
            return "<UDS Unit : %s[%s] %swith ID=%d at %08x>" % (self.name, self.symbol, desc, self.id, id(self))

    no_unit = Unit(id=0x00, name='no unit', symbol='-', description='-')
    meter = Unit(id=0x01, name='meter', symbol='m', description='length')
    foor = Unit(id=0x02, name='foot', symbol='ft', description='length')
    inch = Unit(id=0x03, name='inch', symbol='in', description='length')
    yard = Unit(id=0x04, name='yard', symbol='yd', description='length')
    english_mile = Unit(id=0x05, name='mile (English)', symbol='mi', description='length')
    gram = Unit(id=0x06, name='gram', symbol='g', description='mass')
    metric_ton = Unit(id=0x07, name='ton (metric)', symbol='t', description='mass')
    second = Unit(id=0x08, name='second', symbol='s', description='time')
    minute = Unit(id=0x09, name='minute', symbol='min', description='time')
    hour = Unit(id=0x0A, name='hour', symbol='h', description='time')
    day = Unit(id=0x0B, name='day', symbol='d', description='time')
    year = Unit(id=0x0C, name='year', symbol='y', description='time')
    ampere = Unit(id=0x0D, name='ampere', symbol='A', description='current')
    volt = Unit(id=0x0E, name='volt', symbol='V', description='voltage')
    coulomb = Unit(id=0x0F, name='coulomb', symbol='C', description='electric charge')
    ohm = Unit(id=0x10, name='ohm', symbol='W', description='resistance')
    farad = Unit(id=0x11, name='farad', symbol='F', description='capacitance')
    henry = Unit(id=0x12, name='henry', symbol='H', description='inductance')
    siemens = Unit(id=0x13, name='siemens', symbol='S', description='electric conductance')
    weber = Unit(id=0x14, name='weber', symbol='Wb', description='magnetic flux')
    tesla = Unit(id=0x15, name='tesla', symbol='T', description='magnetic flux density')
    kelvin = Unit(id=0x16, name='kelvin', symbol='K', description='thermodynamic temperature')
    Celsius = Unit(id=0x17, name='Celsius', symbol='°C', description='thermodynamic temperature')
    Fahrenheit = Unit(id=0x18, name='Fahrenheit', symbol='°F', description='thermodynamic temperature')
    candela = Unit(id=0x19, name='candela', symbol='cd', description='luminous intensity')
    radian = Unit(id=0x1A, name='radian', symbol='rad', description='plane angle')
    degree = Unit(id=0x1B, name='degree', symbol='°', description='plane angle')
    hertz = Unit(id=0x1C, name='hertz', symbol='Hz', description='frequency')
    joule = Unit(id=0x1D, name='joule', symbol='J', description='energy')
    Newton = Unit(id=0x1E, name='Newton', symbol='N', description='force')
    kilopond = Unit(id=0x1F, name='kilopond', symbol='kp', description='force')
    pound = Unit(id=0x20, name='pound force', symbol='lbf', description='force')
    watt = Unit(id=0x21, name='watt', symbol='W', description='power')
    horse = Unit(id=0x22, name='horse power (metric)', symbol='hk', description='power')
    horse = Unit(id=0x23, name='horse power(UK and US)', symbol='hp', description='power')
    Pascal = Unit(id=0x24, name='Pascal', symbol='Pa', description='pressure')
    bar = Unit(id=0x25, name='bar', symbol='bar', description='pressure')
    atmosphere = Unit(id=0x26, name='atmosphere', symbol='atm', description='pressure')
    psi = Unit(id=0x27, name='pound force per square inch', symbol='psi', description='pressure')
    becqerel = Unit(id=0x28, name='becqerel', symbol='Bq', description='radioactivity')
    lumen = Unit(id=0x29, name='lumen', symbol='lm', description='light flux')
    lux = Unit(id=0x2A, name='lux', symbol='lx', description='illuminance')
    liter = Unit(id=0x2B, name='liter', symbol='l', description='volume')
    gallon = Unit(id=0x2C, name='gallon (British)', symbol='-', description='volume')
    gallon = Unit(id=0x2D, name='gallon (US liq)', symbol='-', description='volume')
    cubic = Unit(id=0x2E, name='cubic inch', symbol='cu in', description='volume')
    meter_per_sec = Unit(id=0x2F, name='meter per seconds', symbol='m/s', description='speed')
    kmh = Unit(id=0x30, name='kilometre per hour', symbol='km/h', description='speed')
    mph = Unit(id=0x31, name='mile per hour', symbol='mph', description='speed')
    rps = Unit(id=0x32, name='revolutions per second', symbol='rps', description='angular velocity')
    rpm = Unit(id=0x33, name='revolutions per minute', symbol='rpm', description='angular velocity')
    counts = Unit(id=0x34, name='counts', symbol='-', description='-')
    percent = Unit(id=0x35, name='percent', symbol='%', description='-')
    mg_per_stroke = Unit(id=0x36, name='milligram per stroke', symbol='mg/stroke', description='mass per engine stroke')
    meter_per_sec2 = Unit(id=0x37, name='meter per square seconds', symbol='m/s2', description='acceleration')
    Nm = Unit(id=0x38, name='Newton meter', symbol='Nm', description='moment')
    liter_per_min = Unit(id=0x39, name='liter per minute', symbol='l/min', description='flow')
    watt_per_meter2 = Unit(id=0x3A, name='watt per square meter', symbol='W/m2', description='intensity')
    bar_per_sec = Unit(id=0x3B, name='bar per second', symbol='bar/s', description='pressure change')
    radians_per_sec = Unit(id=0x3C, name='radians per second', symbol='rad/s', description='angular velocity')
    radians = Unit(id=0x3D, name='radians square second', symbol='rad/s2', description='angular acceleration')
    kilogram_per_meter2 = Unit(id=0x3E, name='kilogram per square meter', symbol='kg/m2', description='-')
    date1 = Unit(id=0x50, name='Date1', symbol='-', description='Year-Month-Day')
    date2 = Unit(id=0x51, name='Date2', symbol='-', description='Day/Month/Year')
    date3 = Unit(id=0x52, name='Date3', symbol='-', description='Month/Day/Year')
    week = Unit(id=0x53, name='week', symbol='W', description='calendar week')
    time1 = Unit(id=0x54, name='Time1', symbol='-', description='UTC Hour/Minute/Second')
    time2 = Unit(id=0x55, name='Time2', symbol='-', description='Hour/Minute/Second')
    datetime1 = Unit(id=0x56, name='DateAndTime1', symbol='-', description='Second/Minute/Hour/Day/Month/Year')
    datetime2 = Unit(id=0x57, name='DateAndTime2', symbol='-', description='Second/Minute/Hour/Day/Month/Year/Local minute offset/Localhour offset')
    datetime3 = Unit(id=0x58, name='DateAndTime3', symbol='-', description='Second/Minute/Hour/Month/Day/Year')
    datetime4 = Unit(id=0x59, name='DateAndTime4', symbol='-', description='Second/Minute/Hour/Month/Day/Year/Local minute offset/Localhour offset')
