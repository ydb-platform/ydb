import pytest
from werkzeug.exceptions import NotFound

import flask_sqlalchemy as fsa


def test_basic_pagination():
    p = fsa.Pagination(None, 1, 20, 500, [])
    assert p.page == 1
    assert not p.has_prev
    assert p.has_next
    assert p.total == 500
    assert p.pages == 25
    assert p.next_num == 2
    assert list(p.iter_pages()) == [1, 2, 3, 4, 5, None, 24, 25]
    p.page = 10
    assert list(p.iter_pages()) == [1, 2, None, 8, 9, 10, 11, 12, 13, 14, None, 24, 25]


def test_pagination_pages_when_0_items_per_page():
    p = fsa.Pagination(None, 1, 0, 500, [])
    assert p.pages == 0


def test_query_paginate(app, db, Todo):
    with app.app_context():
        db.session.add_all([Todo('', '') for _ in range(100)])
        db.session.commit()

    @app.route('/')
    def index():
        p = Todo.query.paginate()
        return '{0} items retrieved'.format(len(p.items))

    c = app.test_client()
    # request default
    r = c.get('/')
    assert r.status_code == 200
    # request args
    r = c.get('/?per_page=10')
    assert r.data.decode('utf8') == '10 items retrieved'

    with app.app_context():
        # query default
        p = Todo.query.paginate()
        assert p.total == 100


def test_query_paginate_more_than_20(app, db, Todo):
    with app.app_context():
        db.session.add_all(Todo('', '') for _ in range(20))
        db.session.commit()

    assert len(Todo.query.paginate(max_per_page=10).items) == 10


def test_paginate_min(app, db, Todo):
    with app.app_context():
        db.session.add_all(Todo(str(x), '') for x in range(20))
        db.session.commit()

    assert Todo.query.paginate(error_out=False, page=-1).items[0].title == '0'
    assert len(Todo.query.paginate(error_out=False, per_page=0).items) == 0
    assert len(Todo.query.paginate(error_out=False, per_page=-1).items) == 20

    with pytest.raises(NotFound):
        Todo.query.paginate(page=0)

    with pytest.raises(NotFound):
        Todo.query.paginate(per_page=-1)
