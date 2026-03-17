PY3TEST()

PEERDIR(
    contrib/python/arrow
    contrib/python/Flask-Admin
    contrib/python/Flask-WTF
    contrib/python/Flask-SQLAlchemy
    contrib/python/SQLAlchemy-Utils
    contrib/python/peewee
    contrib/python/wtf-peewee
    contrib/python/Pillow
    contrib/python/flask-mongoengine
    contrib/python/mongoengine
    contrib/python/sqlalchemy/sqlalchemy-2
)

NO_LINT()

SRCDIR(contrib/python/Flask-Admin/flask_admin/tests)

TEST_SRCS(
    __init__.py
    fileadmin/__init__.py
    fileadmin/test_fileadmin.py
#    fileadmin/test_fileadmin_azure.py
    geoa/__init__.py
#    geoa/test_basic.py
    mock.py
    mongoengine/__init__.py
#    mongoengine/test_basic.py
    peeweemodel/__init__.py
    peeweemodel/test_basic.py
    pymongo/__init__.py
#    pymongo/test_basic.py
    sqla/__init__.py
    sqla/test_basic.py
#    sqla/test_form_rules.py
#    sqla/test_inlineform.py
#    sqla/test_multi_pk.py
#    sqla/test_postgres.py
#    sqla/test_translation.py
    test_base.py
    test_form_upload.py
    test_helpers.py
    test_model.py
    test_tools.py
)

RESOURCE_FILES(
    PREFIX contrib/python/Flask-Admin/flask_admin/tests/
    data/copyleft.gif
    data/copyleft.jpeg
    data/copyleft.jpg
    data/copyleft.png
    data/copyleft.tiff
    fileadmin/files/dummy.txt
    sqla/templates/another_macro.html
    sqla/templates/macro.html
    templates/method.html
    templates/mock.html
)

END()
