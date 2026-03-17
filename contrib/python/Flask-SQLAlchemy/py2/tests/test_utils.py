import mock

from flask_sqlalchemy import utils


class TestSQLAlchemyVersion:
    def test_parse_version(self):
        assert utils.parse_version('1.2.3') == (1, 2, 3)
        assert utils.parse_version('1.2') == (1, 2, 0)
        assert utils.parse_version('1') == (1, 0, 0)

    @mock.patch.object(utils, 'sqlalchemy')
    def test_sqlalchemy_version(self, m_sqlalchemy):
        m_sqlalchemy.__version__ = '1.3'

        assert not utils.sqlalchemy_version('<', '1.3')
        assert not utils.sqlalchemy_version('>', '1.3')
        assert utils.sqlalchemy_version('<=', '1.3')
        assert utils.sqlalchemy_version('==', '1.3')
        assert utils.sqlalchemy_version('>=', '1.3')

        m_sqlalchemy.__version__ = '1.2.99'

        assert utils.sqlalchemy_version('<', '1.3')
        assert not utils.sqlalchemy_version('>', '1.3')
        assert utils.sqlalchemy_version('<=', '1.3')
        assert not utils.sqlalchemy_version('==', '1.3')
        assert not utils.sqlalchemy_version('>=', '1.3')
