""" _curves.py
specify some additional curves that OpenSSL provides but cryptography doesn't explicitly expose
"""

from cryptography import utils

from cryptography.hazmat.primitives.asymmetric import ec

from cryptography.hazmat.bindings.openssl.binding import Binding

__all__ = tuple()

# TODO: investigate defining additional curves using EC_GROUP_new_curve
#       https://wiki.openssl.org/index.php/Elliptic_Curve_Cryptography#Defining_Curves


def _openssl_get_supported_curves():
    if hasattr(_openssl_get_supported_curves, '_curves'):
        return _openssl_get_supported_curves._curves

    # use cryptography's cffi bindings to get an array of curve names
    b = Binding()
    cn = b.lib.EC_get_builtin_curves(b.ffi.NULL, 0)
    cs = b.ffi.new('EC_builtin_curve[]', cn)
    b.lib.EC_get_builtin_curves(cs, cn)

    # store the result so we don't have to do all of this every time
    curves = { b.ffi.string(b.lib.OBJ_nid2sn(c.nid)).decode('utf-8') for c in cs }
    # Ed25519 and X25519 are always present in cryptography>=2.6
    # The python cryptography lib provides a different interface for these curves,
    # so they are handled differently in the ECDHPriv/Pub and EdDSAPriv/Pub classes
    curves |= {'X25519', 'ed25519'}
    _openssl_get_supported_curves._curves = curves
    return curves


def use_legacy_cryptography_decorator():
    """
    The decorator utils.register_interface was removed in version 38.0.0. Keep using it
    if the decorator exists, inherit from `ec.EllipticCurve` otherwise.
    """
    return hasattr(utils, "register_interface") and callable(utils.register_interface)


if use_legacy_cryptography_decorator():
    @utils.register_interface(ec.EllipticCurve)
    class BrainpoolP256R1(object):
        name = 'brainpoolP256r1'
        key_size = 256


    @utils.register_interface(ec.EllipticCurve)  # noqa: E303
    class BrainpoolP384R1(object):
        name = 'brainpoolP384r1'
        key_size = 384


    @utils.register_interface(ec.EllipticCurve)  # noqa: E303
    class BrainpoolP512R1(object):
        name = 'brainpoolP512r1'
        key_size = 512


    @utils.register_interface(ec.EllipticCurve)  # noqa: E303
    class X25519(object):
        name = 'X25519'
        key_size = 256


    @utils.register_interface(ec.EllipticCurve)  # noqa: E303
    class Ed25519(object):
        name = 'ed25519'
        key_size = 256
else:
    class BrainpoolP256R1(ec.EllipticCurve):
        name = 'brainpoolP256r1'
        key_size = 256


    class BrainpoolP384R1(ec.EllipticCurve):  # noqa: E303
        name = 'brainpoolP384r1'
        key_size = 384


    class BrainpoolP512R1(ec.EllipticCurve):  # noqa: E303
        name = 'brainpoolP512r1'
        key_size = 512


    class X25519(ec.EllipticCurve):  # noqa: E303
        name = 'X25519'
        key_size = 256


    class Ed25519(ec.EllipticCurve):  # noqa: E303
        name = 'ed25519'
        key_size = 256


# add these curves to the _CURVE_TYPES list
for curve in [BrainpoolP256R1, BrainpoolP384R1, BrainpoolP512R1, X25519, Ed25519]:
    if curve.name not in ec._CURVE_TYPES and curve.name in _openssl_get_supported_curves():
        ec._CURVE_TYPES[curve.name] = curve
