from flask_wtf._compat import FlaskWTFDeprecationWarning


def test_deprecated_html5(recwarn):
    __import__('flask_wtf.html5')
    w = recwarn.pop(FlaskWTFDeprecationWarning)
    assert 'wtforms.fields.html5' in str(w.message)
