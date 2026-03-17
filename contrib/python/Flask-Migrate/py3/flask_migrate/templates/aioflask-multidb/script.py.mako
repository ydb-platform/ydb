<%!
import re

%>"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()

<%
    from flask import current_app
    bind_names = []
    if current_app.config.get('SQLALCHEMY_BINDS') is not None:
        bind_names = list(current_app.config['SQLALCHEMY_BINDS'].keys())
    else:
        get_bind_names = getattr(current_app.extensions['migrate'].db, 'bind_names', None)
        if get_bind_names:
            bind_names = get_bind_names()
    db_names = [''] + bind_names
%>

## generate an "upgrade_<xyz>() / downgrade_<xyz>()" function
## for each database name in the ini file.

% for db_name in db_names:

def upgrade_${db_name}():
    ${context.get("%s_upgrades" % db_name, "pass")}


def downgrade_${db_name}():
    ${context.get("%s_downgrades" % db_name, "pass")}

% endfor
