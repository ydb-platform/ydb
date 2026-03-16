PY3_LIBRARY()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/flatbuffers
)
PY_SRCS(
    NAMESPACE pregen_lib
    MyGame/Sample/Color.py
    MyGame/Sample/Equipment.py
    MyGame/Sample/Monster.py
    MyGame/Sample/Vec3.py
    MyGame/Sample/Weapon.py
    __init__.py
)
NO_LINT() # flatc output does not follow arcadia style
END()
