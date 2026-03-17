PY3TEST()

DATA(sbr://5697989922)

PEERDIR(
    contrib/python/lxml
    contrib/python/pycollada
)

NO_LINT()

SRCDIR(
    contrib/python/pycollada/collada/tests
)

TEST_SRCS(
    test_asset.py
    test_camera.py
    test_collada.py
    test_geometry.py
    test_iteration.py
    test_light.py
    test_lineset.py
    test_material.py
    test_scene.py
    test_source.py
)

END()
