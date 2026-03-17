# -*- coding: utf-8 -*-


# PyMeeus: Python module implementing astronomical algorithms.
# Copyright (C) 2018  Dagoberto Salazar
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


from math import pi

from pymeeus.base import TOL
from pymeeus.Angle import Angle


# Angle class

def test_angle_constructor():
    """Tests the constructor of Angle class"""

    a = Angle(23.44694444)
    assert abs(a._deg - 23.44694444) < TOL, \
        "ERROR: 1st constructor test, degrees value doesn't match"

    b = Angle(-23.44694444)
    assert abs(b._deg - (-23.44694444)) < TOL, \
        "ERROR: 2nd constructor test, degrees value doesn't match"

    c = Angle(383.44694444)
    assert abs(c._deg - 23.44694444) < TOL, \
        "ERROR: 3rd constructor test, degrees value doesn't match"

    d = Angle(-383.44694444)
    assert abs(d._deg - (-23.44694444)) < TOL, \
        "ERROR: 4th constructor test, degrees value doesn't match"

    e = Angle(-23.0, 26.0, 48.999983999)
    assert abs(e._deg - (-23.44694444)) < TOL, \
        "ERROR: 5th constructor test, degrees value doesn't match"

    f = Angle(23.0, -30.0)
    assert abs(f._deg - (-23.5)) < TOL, \
        "ERROR: 6th constructor test, degrees value doesn't match"

    g = Angle((-23.0, 26.0, 48.999983999))
    assert abs(g._deg - (-23.44694444)) < TOL, \
        "ERROR: 7th constructor test, degrees value doesn't match"

    h = Angle([-23.0, 26.0, 48.999983999])
    assert abs(h._deg - (-23.44694444)) < TOL, \
        "ERROR: 8th constructor test, degrees value doesn't match"

    i = Angle(1.0, radians=True)
    assert abs(i._deg - 57.29577951308232) < TOL, \
        "ERROR: 9th constructor test, degrees value doesn't match"

    j = Angle((23.0, 26.0, 48.999983999, -1.0))
    assert abs(j._deg - (-23.44694444)) < TOL, \
        "ERROR: 10th constructor test, degrees value doesn't match"

    k = Angle(23.0, 26.0, 48.999983999, -7.4)
    assert abs(k._deg - (-23.44694444)) < TOL, \
        "ERROR: 11th constructor test, degrees value doesn't match"

    m = Angle([23.0, -26.0, 48.999983999, -4.5])
    assert abs(m._deg - (-23.44694444)) < TOL, \
        "ERROR: 12th constructor test, degrees value doesn't match"


def test_angle_set_radians():
    """Tests the set_radians() method of Angle class"""

    a = Angle()

    a.set_radians(pi)                               # Input is in radians
    assert abs(a() - 180.0) < TOL, \
        "ERROR: 1st set_radians() test, degrees value doesn't match"


def test_angle_set_ra():
    """Tests the set_ra() method of Angle class"""

    a = Angle()

    a.set_ra(9.248833333333)                        # Input is in RA
    assert abs(a._deg - 138.7325) < TOL, \
        "ERROR: 1st set_ra() test, degrees value doesn't match"

    a.set_ra(-9.248833333333)
    assert abs(a._deg - (-138.7325)) < TOL, \
        "ERROR: 2nd set_ra() test, degrees value doesn't match"

    a.set_ra(9, 14, 55.8)
    assert abs(a._deg - 138.7325) < TOL, \
        "ERROR: 3rd set_ra() test, degrees value doesn't match"

    a.set_ra((9, 14, 55.8, -1.0))
    assert abs(a._deg - (-138.7325)) < TOL, \
        "ERROR: 4th set_ra() test, degrees value doesn't match"


def test_angle_deg2dms():
    """Tests deg2dms() static method of Angle class"""

    (d, m, s, sign) = Angle.deg2dms(23.44694444)
    assert abs(d - 23.0) < TOL, \
        "ERROR: In 1st deg2dms() test, degrees value doesn't match"
    assert abs(m - 26.0) < TOL, \
        "ERROR: In 1st deg2dms() test, minutes value doesn't match"
    assert abs(s - 48.9999839999999) < TOL, \
        "ERROR: In 1st deg2dms() test, seconds value doesn't match"
    assert abs(sign - 1.0) < TOL, \
        "ERROR: In 1st deg2dms() test, sign value doesn't match"

    (d, m, s, sign) = Angle.deg2dms(-23.44694444)
    assert abs(d - 23.0) < TOL, \
        "ERROR: In 2nd deg2dms() test, degrees value doesn't match"
    assert abs(m - 26.0) < TOL, \
        "ERROR: In 2nd deg2dms() test, minutes value doesn't match"
    assert abs(s - 48.9999839999999) < TOL, \
        "ERROR: In 2nd deg2dms() test, seconds value doesn't match"
    assert abs(sign - (-1.0)) < TOL, \
        "ERROR: In 2nd deg2dms() test, sign value doesn't match"


def test_angle_dms2deg():
    """Tests dms2deg() static method of Angle class"""

    d = Angle.dms2deg(23.0, 26.0, 48.999984)
    assert abs(d - 23.44694444) < TOL, \
        "ERROR: In 1st dms2deg() test, degrees value doesn't match"

    d = Angle.dms2deg(-23.0, 26.0, 48.999984)
    assert abs(d - (-23.44694444)) < TOL, \
        "ERROR: In 2nd dms2deg() test, degrees value doesn't match"

    d = Angle.dms2deg(0.0, -26.0, 48.999984)
    assert abs(d - (-0.44694444)) < TOL, \
        "ERROR: In 3rd dms2deg() test, degrees value doesn't match"


def test_angle_reduce_deg():
    """Tests reduce_deg() static method of Angle class"""

    d = Angle.reduce_deg(745.67)
    assert abs(d - 25.67) < TOL, \
        "ERROR: In 1st reduce_deg() test, degrees value doesn't match"

    d = Angle.reduce_deg(-360.86)
    assert abs(d - (-0.86)) < TOL, \
        "ERROR: In 2nd reduce_deg() test, degrees value doesn't match"


def test_angle_reduce_dms():
    """Tests reduce_dms() static method of Angle class"""

    (d, m, s, sign) = Angle.reduce_dms(383.0, 26.0, 48.999984)
    assert abs(d - 23.0) < TOL, \
        "ERROR: In 1st reduce_dms() test, degrees value doesn't match"

    assert abs(m - 26.0) < TOL, \
        "ERROR: In 1st reduce_dms() test, minutes value doesn't match"

    assert abs(s - 48.999984) < TOL, \
        "ERROR: In 1st reduce_dms() test, seconds value doesn't match"

    assert abs(sign - 1.0) < TOL, \
        "ERROR: In 1st reduce_dms() test, sign value doesn't match"

    (d, m, s, sign) = Angle.reduce_dms(-1103.6, 86.5, 168.84)
    assert abs(d - 25.0) < TOL, \
        "ERROR: In 2nd reduce_dms() test, degrees value doesn't match"

    assert abs(m - 5.0) < TOL, \
        "ERROR: In 2nd reduce_dms() test, minutes value doesn't match"

    assert abs(s - 18.8399999997) < TOL, \
        "ERROR: In 2nd reduce_dms() test, seconds value doesn't match"

    assert abs(sign - (-1.0)) < TOL, \
        "ERROR: In 2nd reduce_dms() test, sign value doesn't match"

    (d, m, s, sign) = Angle.reduce_dms(0.0, -206.71)
    assert abs(d - 3.0) < TOL, \
        "ERROR: In 3rd reduce_dms() test, degrees value doesn't match"

    assert abs(m - 26.0) < TOL, \
        "ERROR: In 3rd reduce_dms() test, minutes value doesn't match"

    assert abs(s - 42.6) < TOL, \
        "ERROR: In 3rd reduce_dms() test, seconds value doesn't match"

    assert abs(sign - (-1.0)) < TOL, \
        "ERROR: In 3rd reduce_dms() test, sign value doesn't match"


def test_angle_dms_str():
    """Tests dms_str() method of Angle class"""
    a = Angle(0, -46.25, 0.0)

    result = a.dms_str()
    assert result == "-46' 15.0''", \
        "ERROR: In 1st dms_str() test, the output value doesn't match"

    result = a.dms_str(False)
    assert result == "0:-46:15.0", \
        "ERROR: In 2nd dms_str() test, the output value doesn't match"


def test_angle_ra_str():
    """Tests ra_str() method of Angle class"""
    a = Angle(138.75)

    result = a.ra_str()
    assert result == "9h 15' 0.0''", \
        "ERROR: In 1st ra_str() test, the output value doesn't match"

    result = a.ra_str(False)
    assert result == "9:15:0.0", \
        "ERROR: In 2nd ra_str() test, the output value doesn't match"


def test_angle_get_ra():
    """Tests get_ra() method of Angle class"""
    a = Angle(138.75)

    assert abs(a.get_ra() - 9.25) < TOL, \
        "ERROR: In 1st get_ra() test, the output value doesn't match"


def test_angle_call():
    """Tests the __call__() method of Angle class"""

    type_ok = False
    a = Angle(40, -46.25, 0.0)
    if isinstance(a(), (int, float)):       # Test the returned type
        type_ok = True

    assert type_ok, "ERROR: In 1st __call__() test, type doesn't match"

    assert abs(a() - (-40.770833333333)) < TOL, \
        "ERROR: In 2nd __call__() test, degrees value doesn't match"


def test_angle_str():
    """Tests the __str__() method of Angle class"""

    type_ok = False
    a = Angle(40, -46.5, 0.0)
    if isinstance(a.__str__(), str):                  # Test the returned type
        type_ok = True

    assert type_ok, "ERROR: In 1st __str__() test, type doesn't match"

    assert a.__str__() == "-40.775", \
        "ERROR: In 2nd __str__() test, degrees value doesn't match"


def test_angle_rad():
    """Tests the rad() method of Angle class"""
    a = Angle(180.0)

    assert abs(a.rad() - pi) < TOL, \
        "ERROR: In 1st rad() test, radians value doesn't match"


def test_angle_to_positive():
    """Tests the to_positive() method"""
    a = Angle(-87.32)
    b = Angle(87.32)

    assert abs(a.to_positive()() - 272.68) < TOL, \
        "ERROR: In 1st to_positive() test, value doesn't match"

    assert abs(b.to_positive()() - 87.32) < TOL, \
        "ERROR: In 2nd to_positive() test, value doesn't match"


def test_angle_ne():
    """Tests the 'is not equal' operator of Angles"""
    # NOTE: Test 'is not equal' also tests 'is equal' operator
    # Default tolerance for Angles is 1E-10
    a = Angle(152.7)
    b = Angle(152.7000000001)

    assert (a != b), \
        "ERROR: In 1st __ne__() test, Angles are different but taken as equal"

    a = Angle(-13, 30)
    b = Angle(-13.50000000001)

    assert not (a != b), \
        "ERROR: In 2nd __ne__() test, Angles are equal but taken as different"


def test_angle_ge():
    """Tests the 'is greater or equal' operator of Angles"""
    # NOTE: Test of 'is greater or equal' also test 'is less than' operator
    a = Angle(152.7)
    b = Angle(152.70000001)

    assert not (a >= b), \
        "ERROR: In 1st __ge__() test, Angles values don't match operator"

    a = Angle(-13, 30)
    b = Angle(-13.5)

    assert (a >= b), \
        "ERROR: In 2nd __ne__() test, Angles values don't match operator"


def test_angle_le():
    """Tests the 'is less or equal' operator of Angles"""
    # NOTE: Test of 'is less or equal' also test 'is greater than' operator
    a = Angle(152.7)
    b = Angle(152.70000001)

    assert (a <= b), \
        "ERROR: In 1st __le__() test, Angles values don't match operator"

    a = Angle(-13, 30)
    b = Angle(-13.5)

    assert (a <= b), \
        "ERROR: In 2nd __le__() test, Angles values don't match operator"


def test_angle_neg():
    """Tests the negation of Angles"""
    a = Angle(152.7)

    b = -a

    assert abs(b() - (-152.7)) < TOL, \
        "ERROR: In 1st __neg__() test, degrees value doesn't match"

    a = Angle(-13, 30)
    b = -a

    assert abs(b() - 13.5) < TOL, \
        "ERROR: In 2nd __neg__() test, degrees value doesn't match"


def test_angle_abs():
    """Tests the absolute value of Angles"""
    a = Angle(152.7)

    b = abs(a)

    assert abs(b() - 152.7) < TOL, \
        "ERROR: In 1st __abs__() test, degrees value doesn't match"

    a = Angle(-13, 30)
    b = abs(a)

    assert abs(b() - 13.5) < TOL, \
        "ERROR: In 2nd __abs__() test, degrees value doesn't match"


def test_angle_mod():
    """Tests the module of Angles"""
    a = Angle(152.7)

    b = a % 50

    assert abs(b() - 2.7) < TOL, \
        "ERROR: In 1st __mod__() test, degrees value doesn't match"

    a = Angle(-13, 30)
    b = a % 7

    assert abs(b() - (-6.5)) < TOL, \
        "ERROR: In 2nd __mod__() test, degrees value doesn't match"


def test_angle_add():
    """Tests the addition between Angles"""
    a = Angle(180.0)
    b = Angle(13, 30)
    c = a + b

    assert abs(c() - 193.5) < TOL, \
        "ERROR: In 1st __add__() test, degrees value doesn't match"

    b.set(-13, 30)
    c = a + b

    assert abs(c() - 166.5) < TOL, \
        "ERROR: In 2nd __add__() test, degrees value doesn't match"

    c = a + 11.5

    assert abs(c() - 191.5) < TOL, \
        "ERROR: In 3rd __add__() test, degrees value doesn't match"


def test_angle_sub():
    """Tests the subtraction between Angles"""
    a = Angle(180.0)
    b = Angle(13, 30)
    c = a - b

    assert abs(c() - 166.5) < TOL, \
        "ERROR: In 1st __sub__() test, degrees value doesn't match"

    b.set(-13, 30)
    c = a - b

    assert abs(c() - 193.5) < TOL, \
        "ERROR: In 2nd __sub__() test, degrees value doesn't match"

    c = a - 11.5

    assert abs(c() - 168.5) < TOL, \
        "ERROR: In 3rd __sub__() test, degrees value doesn't match"


def test_angle_mul():
    """Tests the multiplication between Angles"""
    a = Angle(150.0)
    b = Angle(5.0)
    c = a * b

    assert abs(c() - 30.0) < TOL, \
        "ERROR: In 1st __mul__() test, degrees value doesn't match"

    b.set(-5.0)
    c = a * b

    assert abs(c() - (-30.0)) < TOL, \
        "ERROR: In 2nd __mul__() test, degrees value doesn't match"

    c = a * 2.5

    assert abs(c() - 15.0) < TOL, \
        "ERROR: In 3rd __mul__() test, degrees value doesn't match"


def test_angle_div():
    """Tests the division between Angles"""
    # NOTE: This also tests method self.__truediv__()
    a = Angle(150.0)
    b = Angle(6.0)
    c = a / b

    assert abs(c() - 25.0) < TOL, \
        "ERROR: In 1st __div__() test, degrees value doesn't match"

    b.set(-6.0)
    c = a / b

    assert abs(c() - (-25.0)) < TOL, \
        "ERROR: In 2nd __div__() test, degrees value doesn't match"

    c = a / 1.5

    assert abs(c() - 100.0) < TOL, \
        "ERROR: In 3rd __div__() test, degrees value doesn't match"


def test_angle_pow():
    """Tests the power operation in Angles"""
    a = Angle(13, 30)
    b = a ** 3

    assert abs(b() - 300.375) < TOL, \
        "ERROR: In 1st __pow__() test, degrees value doesn't match"

    b.set(-2.0)
    c = a ** b

    assert abs(c() - 0.005486968449931413) < TOL, \
        "ERROR: In 2nd __pow__() test, degrees value doesn't match"


def test_angle_imod():
    """Tests the accumulative module between Angles"""
    a = Angle(152.7)

    a %= 50

    assert abs(a() - 2.7) < TOL, \
        "ERROR: In 1st __imod__() test, degrees value doesn't match"

    a = Angle(-13, 30)
    a %= 7

    assert abs(a() - (-6.5)) < TOL, \
        "ERROR: In 2nd __imod__() test, degrees value doesn't match"


def test_angle_iadd():
    """Tests the accumulative addition between Angles"""
    a = Angle(180.0)
    b = Angle(13, 30)
    a += b

    assert abs(a() - 193.5) < TOL, \
        "ERROR: In 1st __iadd__() test, degrees value doesn't match"

    b.set(-10, 30)
    a += b

    assert abs(a() - 183.0) < TOL, \
        "ERROR: In 2nd __iadd__() test, degrees value doesn't match"

    a += 37.5

    assert abs(a() - 220.5) < TOL, \
        "ERROR: In 3rd __iadd__() test, degrees value doesn't match"


def test_angle_isub():
    """Tests the accumulative subtraction between Angles"""
    a = Angle(180.0)
    b = Angle(13, 30)
    a -= b

    assert abs(a() - 166.5) < TOL, \
        "ERROR: In 1st __isub__() test, degrees value doesn't match"

    b.set(-10, 30)
    a -= b

    assert abs(a() - 177.0) < TOL, \
        "ERROR: In 2nd __isub__() test, degrees value doesn't match"

    a -= 37.5

    assert abs(a() - 139.5) < TOL, \
        "ERROR: In 3rd __isub__() test, degrees value doesn't match"


def test_angle_imul():
    """Tests the accumulative multiplication between Angles"""
    a = Angle(150.0)
    b = Angle(5.0)
    a *= b

    assert abs(a() - 30.0) < TOL, \
        "ERROR: In 1st __imul__() test, degrees value doesn't match"

    b.set(-5.0)
    a *= b

    assert abs(a() - (-150.0)) < TOL, \
        "ERROR: In 2nd __imul__() test, degrees value doesn't match"

    a *= 2.5

    assert abs(a() - (-15.0)) < TOL, \
        "ERROR: In 3rd __imul__() test, degrees value doesn't match"


def test_angle_idiv():
    """Tests the accumulative division between Angles"""
    # NOTE: This also tests method self.__itruediv__()
    a = Angle(150.0)
    b = Angle(6.0)
    a /= b

    assert abs(a() - 25.0) < TOL, \
        "ERROR: In 1st __idiv__() test, degrees value doesn't match"

    b.set(-20.0)
    a /= b

    assert abs(a() - (-1.25)) < TOL, \
        "ERROR: In 2nd __idiv__() test, degrees value doesn't match"

    a /= 1.5

    assert abs(a() - (-0.833333333333333)) < TOL, \
        "ERROR: In 3rd __idiv__() test, degrees value doesn't match"


def test_angle_ipow():
    """Tests the accumulative power operation in Angles"""
    a = Angle(13, 30)
    a **= 3

    assert abs(a() - 300.375) < TOL, \
        "ERROR: In 1st __ipow__() test, degrees value doesn't match"

    b = Angle(-2.0)
    a **= b

    assert abs(a() - 1.108338532999e-05) < TOL, \
        "ERROR: In 2nd __ipow__() test, degrees value doesn't match"


def test_angle_rmod():
    """Tests the module operation between Angles by the right"""
    a = Angle(25.0)
    b = Angle(163.0)
    c = a.__rmod__(b)

    assert abs(c() - 13.0) < TOL, \
        "ERROR: In 1st __rmod__() test, degrees value doesn't match"

    b.set(-78.0)
    c = a.__rmod__(b)

    assert abs(c() - (-3.0)) < TOL, \
        "ERROR: In 2nd __rmod__() test, degrees value doesn't match"

    c = 31.5 % a

    assert abs(c() - 6.5) < TOL, \
        "ERROR: In 3rd __rmod__() test, degrees value doesn't match"


def test_angle_radd():
    """Tests the addition between Angles by the right"""
    a = Angle(180.0)
    b = Angle(13, 30)
    c = a.__radd__(b)

    assert abs(c() - 193.5) < TOL, \
        "ERROR: In 1st __radd__() test, degrees value doesn't match"

    b.set(-13, 30)
    c = a.__radd__(b)

    assert abs(c() - 166.5) < TOL, \
        "ERROR: In 2nd __radd__() test, degrees value doesn't match"

    c = 11.5 + a

    assert abs(c() - 191.5) < TOL, \
        "ERROR: In 3rd __radd__() test, degrees value doesn't match"


def test_angle_rsub():
    """Tests the subtraction between Angles by the right"""
    a = Angle(180.0)
    b = Angle(-13, 30)
    c = a.__rsub__(b)

    assert abs(c() - (-193.5)) < TOL, \
        "ERROR: In 1st __rsub__() test, degrees value doesn't match"

    b.set(13, 30)
    c = a.__rsub__(b)

    assert abs(c() - (-166.5)) < TOL, \
        "ERROR: In 2nd __rsub__() test, degrees value doesn't match"

    c = 11.5 - a

    assert abs(c() - (-168.5)) < TOL, \
        "ERROR: In 3rd __rsub__() test, degrees value doesn't match"


def test_angle_rmul():
    """Tests the multiplication between Angles by the right"""
    a = Angle(150.0)
    b = Angle(5.0)
    c = a.__rmul__(b)

    assert abs(c() - 30.0) < TOL, \
        "ERROR: In 1st __rmul__() test, degrees value doesn't match"

    b.set(-5.0)
    c = a.__rmul__(b)

    assert abs(c() - (-30.0)) < TOL, \
        "ERROR: In 2nd __rmul__() test, degrees value doesn't match"

    c = 2.5 * a

    assert abs(c() - 15.0) < TOL, \
        "ERROR: In 3rd __rmul__() test, degrees value doesn't match"


def test_angle_rdiv():
    """Tests the division between Angles by the right"""
    a = Angle(150.0)
    b = Angle(5.0)
    c = b.__rdiv__(a)

    assert abs(c() - 30.0) < TOL, \
        "ERROR: In 1st __rdiv__() test, degrees value doesn't match"

    b.set(-5.0)
    a = b.__rdiv__(c)

    assert abs(a() - (-6.0)) < TOL, \
        "ERROR: In 2nd __rdiv__() test, degrees value doesn't match"

    c = -24.0 / a

    assert abs(c() - 4.0) < TOL, \
        "ERROR: In 3rd __rdiv__() test, degrees value doesn't match"


def test_angle_rpow():
    """Tests the power operation between Angles by the right"""
    a = Angle(15.0)
    b = Angle(3.0)
    c = b.__rpow__(a)

    assert abs(c() - 135.0) < TOL, \
        "ERROR: In 1st __rpow__() test, degrees value doesn't match"

    b.set(-2.0)
    a = b.__rpow__(c)

    assert abs(a() - 5.48697e-05) < TOL, \
        "ERROR: In 2nd __rpow__() test, degrees value doesn't match"

    c = -10.0 ** b

    assert abs(c() - (-0.01)) < TOL, \
        "ERROR: In 3rd __rpow__() test, degrees value doesn't match"


def test_angle_float():
    """Tests the 'float()' operation on Angles"""
    a = Angle(15, 30)

    assert abs(float(a) - 15.5) < TOL, \
        "ERROR: In 1st __float__() test, degrees value doesn't match"


def test_angle_int():
    """Tests the 'int()' operation on Angles"""
    a = Angle(15, 30)

    assert abs(int(a) - 15) < TOL, \
        "ERROR: In 1st __int__() test, degrees value doesn't match"


def test_angle_round():
    """Tests the 'round()' operation on Angles"""
    a = Angle(1.0, radians=True)

    # NOTE: The 'float(round(x))' hack makes this test work in Python 2 and 3
    assert abs(float(round(a)) - 57.0) < TOL, \
        "ERROR: In 1st __round__() test, degrees value doesn't match"

    assert abs(float(round(a, 3)) - 57.296) < TOL, \
        "ERROR: In 2nd __round__() test, degrees value doesn't match"

    assert abs(float(round(a, 7)) - 57.2957795) < TOL, \
        "ERROR: In 3rd __round__() test, degrees value doesn't match"
