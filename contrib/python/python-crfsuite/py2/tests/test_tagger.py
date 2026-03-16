# -*- coding: utf-8 -*-
from __future__ import absolute_import
import pytest
from pycrfsuite import Tagger, Trainer, ItemSequence


def test_open_close_labels(model_filename, yseq):
    tagger = Tagger()

    with pytest.raises(ValueError):
        # tagger should be closed, so labels() method should fail here
        labels = tagger.labels()

    with tagger.open(model_filename):
        labels = tagger.labels()
    assert set(labels) == set(yseq)

    with pytest.raises(ValueError):
        # tagger should be closed, so labels() method should fail here
        labels = tagger.labels()


def test_open_non_existing():
    tagger = Tagger()
    with pytest.raises(IOError):
        tagger.open('foo')


#def test_open_invalid():
#    tagger = Tagger()
#    with pytest.raises(ValueError):
#        tagger.open(__file__)


def test_open_invalid_small(tmpdir):
    tmp = tmpdir.join('tmp.txt')
    tmp.write(b'foo')
    tagger = Tagger()
    with pytest.raises(ValueError):
        tagger.open(str(tmp))


def test_open_invalid_small_with_correct_signature(tmpdir):
    tmp = tmpdir.join('tmp.txt')
    tmp.write(b"lCRFfoo")
    tagger = Tagger()
    with pytest.raises(ValueError):
        tagger.open(str(tmp))


@pytest.mark.xfail(reason="see https://github.com/chokkan/crfsuite/pull/24",
                   run=False)
def test_open_invalid_with_correct_signature(tmpdir):
    tmp = tmpdir.join('tmp.txt')
    tmp.write(b"lCRFfoo"*100)
    tagger = Tagger()
    with pytest.raises(ValueError):
        tagger.open(str(tmp))


def test_open_inmemory(model_bytes, xseq, yseq):
    with Tagger().open_inmemory(model_bytes) as tagger:
        assert tagger.tag(xseq) == yseq


def test_open_inmemory_invalid():
    tagger = Tagger()
    with pytest.raises(ValueError):
        tagger.open_inmemory(b'')

    with pytest.raises(ValueError):
        tagger.open_inmemory(b'lCRFabc')


@pytest.mark.xfail(reason="see https://github.com/scrapinghub/python-crfsuite/issues/28",
                   run=False)
def test_tag_not_opened(xseq):
    tagger = Tagger()
    with pytest.raises(Exception):
        tagger.tag(xseq)


def test_tag(model_filename, xseq, yseq):
    with Tagger().open(model_filename) as tagger:
        assert tagger.tag(xseq) == yseq


def test_tag_item_sequence(model_filename, xseq, yseq):
    with Tagger().open(model_filename) as tagger:
        assert tagger.tag(ItemSequence(xseq)) == yseq


def test_tag_string_lists(model_filename, xseq, yseq):
    with Tagger().open(model_filename) as tagger:
        # Working with lists is supported,
        # but if we discard weights the results become different
        data = [x.keys() for x in xseq]
        assert tagger.tag(data) != yseq


def test_tag_bools(model_filename, xseq, yseq):
    with Tagger().open(model_filename) as tagger:
        # Some values are bools:
        # True <=> 1.0; False <=> 0.0
        data = [
            dict((k, bool(v) if v==0 or v==1 else v) for (k, v) in x.items())
            for x in xseq
        ]
        assert tagger.tag(data) == yseq


def test_tag_formats(tmpdir, xseq, yseq):
    # make all coefficients 1 and check that results are the same
    model_filename = str(tmpdir.join('model.crfsuite'))
    xseq = [dict((key, 1) for key in x) for x in xseq]

    trainer = Trainer()
    trainer.set('c2', 1e-6)  # make sure model overfits
    trainer.append(xseq, yseq)
    trainer.train(model_filename)

    with Tagger().open(model_filename) as tagger:
        assert tagger.tag(xseq) == yseq

    # strings
    with Tagger().open(model_filename) as tagger:
        data = [x.keys() for x in xseq]
        assert tagger.tag(data) == yseq


@pytest.mark.xfail()
@pytest.mark.parametrize("bad_seq", [
    'foo',
    ['foo'],            # should be a list of lists of strings
    # [[{'foo': 1.0}]],   # should be a list of dicts  # Arcadia XXX - Test crashed if Py_DEBUG is enabled
])
def test_tag_invalid_feature_format(model_filename, bad_seq):
    with Tagger().open(model_filename) as tagger:
        with pytest.raises(ValueError):
            tagger.tag(bad_seq)


def test_tag_probability(model_filename, xseq, yseq):
    with Tagger().open(model_filename) as tagger:
        res = tagger.tag(xseq)
        prob = tagger.probability(res)
        prob2 = tagger.probability([yseq[0]]*len(yseq))
        assert prob > prob2
        assert 0 < prob < 1
        assert 0 < prob2 < 1


def test_dump(tmpdir, model_filename):
    with Tagger().open(model_filename) as tagger:
        dump_filename = str(tmpdir.join("dump.txt"))
        tagger.dump(dump_filename)

        with open(dump_filename, 'rb') as f:
            res = f.read().decode('utf8')
            assert 'LABELS = {' in res
            assert u'солнце:не светит --> rainy:' in res

    # it shouldn't segfault on a closed tagger
    with pytest.raises(RuntimeError):
        tagger.dump(dump_filename)


def test_info(model_filename):
    with Tagger().open(model_filename) as tagger:
        res = tagger.info()

        assert res.transitions[('sunny', 'sunny')] > res.transitions[('sunny', 'rainy')]
        assert res.state_features[('walk', 'sunny')] > res.state_features[('walk', 'rainy')]
        assert (u'солнце:не светит', u'rainy') in res.state_features
        assert res.header['num_labels'] == '2'
        assert set(res.labels.keys()) == set(['sunny', 'rainy'])
        assert set(res.attributes.keys()) == set(['shop', 'walk', 'clean', u'солнце:не светит'])

    # it shouldn't segfault on a closed tagger
    with pytest.raises(RuntimeError):
        tagger.info()


def test_append_strstr_dicts(tmpdir):
    trainer = Trainer()
    trainer.append(
        [{'foo': 'bar'}, {'baz': False}, {'foo': 'bar', 'baz': True}, {'baz': 0.2}],
        ['spam', 'egg', 'spam', 'spam']
    )
    model_filename = str(tmpdir.join('model.crfsuite'))
    trainer.train(model_filename)

    with Tagger().open(model_filename) as tagger:
        info = tagger.info()
        assert set(info.attributes.keys()) == set(['foo:bar', 'baz'])
        assert info.state_features[('foo:bar', 'spam')] > 0


def test_append_nested_dicts(tmpdir):
    trainer = Trainer()
    trainer.append(
        [
            {
                "foo": {
                    "bar": "baz",
                    "spam": 0.5,
                    "egg": ["x", "y"],
                    "ham": {"x": -0.5, "y": -0.1}
                },
            },
            {
                "foo": {
                    "bar": "ham",
                    "spam": -0.5,
                    "ham": set(["x", "y"])
                },
            },
        ],
        ['first', 'second']
    )
    model_filename = str(tmpdir.join('model.crfsuite'))
    trainer.train(model_filename)

    with Tagger().open(model_filename) as tagger:
        info = tagger.info()
        assert set(info.attributes.keys()) == set([
            'foo:bar:baz',
            'foo:spam',
            'foo:egg:x',
            'foo:egg:y',
            'foo:ham:x',
            'foo:ham:y',
            'foo:bar:ham',
        ])

        for feat in ['foo:bar:baz', 'foo:spam', 'foo:egg:x', 'foo:egg:y']:
            assert info.state_features[(feat, 'first')] > 0
            assert info.state_features.get((feat, 'second'), 0) <= 0

        for feat in ['foo:bar:ham', 'foo:ham:x', 'foo:ham:y']:
            assert info.state_features[(feat, 'second')] > 0
            assert info.state_features.get((feat, 'first'), 0) <= 0
