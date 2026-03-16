from pytest import mark, raises

from wand.api import library
from wand.image import Image


def expire(image):
    """Expire image's sequence cache."""
    image.sequence.instances = []


def test_length(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        assert len(img.sequence) == 4


def test_validate_position_error(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        with raises(TypeError):
            img.sequence.validate_position(0.001)


def test_validate_slice_error(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        with raises(ValueError):
            img.sequence.validate_slice(slice(0, 10, 3))


def test_getitem(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        size = img.size
        assert size == img.sequence[img.sequence.current_index].size
        assert img.sequence[0].size == (32, 32)
        assert img.sequence[1].size == (16, 16)
        assert img.sequence[2].size == (32, 32)
        assert img.sequence[3].size == (16, 16)
        with raises(IndexError):
            img.sequence[4]
        assert img.sequence[-1].size == (16, 16)
        assert img.sequence[-2].size == (32, 32)
        assert img.sequence[-3].size == (16, 16)
        assert img.sequence[-4].size == (32, 32)
        with raises(IndexError):
            img.sequence[-5]
        assert img.size == size


def test_setitem(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as imga:
        with Image(filename=str(fx_asset.joinpath('google.ico'))) as imgg:
            imga.sequence[2] = imgg
        assert len(imga.sequence) == 4
        assert imga.sequence[2].size == (16, 16)
        expire(imga)
        assert imga.sequence[2].size == (16, 16)
        with raises(TypeError):
            imga.sequence[3] = 0xDEADBEEF


def test_delitem(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        detached = img.sequence[0]
        del img.sequence[0]
        assert len(img.sequence) == 3
        assert img.sequence[0] is not detached
        assert img.sequence[0].size == (16, 16)
        expire(img)
        assert len(img.sequence) == 3
        assert img.sequence[0] is not detached
        assert img.sequence[0].size == (16, 16)
        assert img.sequence[1].size == (32, 32)
        assert img.sequence[2].size == (16, 16)


def test_delitem_not_loaded(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        del img.sequence[1]
        assert len(img.sequence) == 3


slices = {
    'to_end': slice(2, None, None),
    'from_first': slice(None, 2, None),
    'from_back': slice(-2, None, None),
    'to_back': slice(None, -2, None),
    'middle': slice(1, 3, None),
    'from_overflow': slice(10, None, None),
    'to_overflow': slice(None, 10, None)
}

slice_items = list(sorted(slices.items(), key=lambda k: k[0]))


@mark.parametrize(('slice_name', 'slice_'), slice_items)
def test_getitem_slice(slice_name, slice_, fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        assert list(img.sequence[slice_]) == list(img.sequence)[slice_]


@mark.parametrize(('slice_name', 'slice_'), slice_items)
def test_setitem_slice(slice_name, slice_, fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as imga:
        instances = list(imga.sequence)
        with Image(filename=str(fx_asset.joinpath('github.ico'))) as imgg:
            instances[slice_] = imgg.sequence
            imga.sequence[slice_] = imgg.sequence
            assert instances == list(imga.sequence)
            expire(imga)
            assert instances == list(imga.sequence)


@mark.parametrize(('slice_name', 'slice_'), slice_items)
def test_delitem_slice(slice_name, slice_, fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        instances = list(img.sequence)
        del instances[slice_]
        del img.sequence[slice_]
        assert list(img.sequence) == instances
        expire(img)
        assert list(img.sequence) == instances


def test_iterator(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        container_size = img.sequence[img.sequence.current_index].size
        actual = []
        expected = [(32, 32), (16, 16), (32, 32), (16, 16)]
        for i in img.sequence:
            actual.append(i.size)
            assert img.size == container_size
        assert actual == expected


def test_append(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as imga:
        with Image(filename=str(fx_asset.joinpath('google.ico'))) as imgg:
            imga.sequence.append(imgg)
            assert imga.sequence[4] == imgg.sequence[0]
            expire(imga)
            assert imga.sequence[4] == imgg.sequence[0]
        assert len(imga.sequence) == 5
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as imga:
        with Image(filename=str(fx_asset.joinpath('github.ico'))) as imgg:
            imga.sequence.append(imgg)
            assert imga.sequence[4] == imgg.sequence[0]
            expire(imga)
            assert imga.sequence[4] == imgg.sequence[0]
        assert len(imga.sequence) == 5
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as imga:
        with Image(filename=str(fx_asset.joinpath('github.ico'))) as imgg:
            imga.sequence.append(imgg.sequence[1])
            assert imga.sequence[4] == imgg.sequence[1]
            expire(imga)
            assert imga.sequence[4] == imgg.sequence[1]
        assert len(imga.sequence) == 5


def test_insert(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as imga:
        instances = [imga.sequence[i] for i in range(2, 4)]
        assert len(imga.sequence) == 4
        with Image(filename=str(fx_asset.joinpath('github.ico'))) as imgg:
            imga.sequence.insert(2, imgg)
            assert imga.sequence[2] == imgg.sequence[0]
            assert len(imga.sequence) == 5
            for i, instance in enumerate(instances):
                assert instance == imga.sequence[3 + i]
            expire(imga)
            assert imga.sequence[2] == imgg.sequence[0]
        for i, instance in enumerate(instances):
            assert instance == imga.sequence[3 + i]
        with raises(TypeError):
            imga.sequence.insert(0xDEADBEEF)


def test_insert_first(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as imga:
        assert len(imga.sequence) == 4
        with Image(filename=str(fx_asset.joinpath('github.ico'))) as imgg:
            imga.sequence.insert(0, imgg)
            assert imga.sequence[0] == imgg.sequence[0]
            expire(imga)
            assert imga.sequence[0] == imgg.sequence[0]
        assert len(imga.sequence) == 5


def test_extend(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as a:
        length = len(a.sequence)
        with Image(filename=str(fx_asset.joinpath('github.ico'))) as b:
            a.sequence.extend(list(b.sequence)[::-1])
            assert a.sequence[length] == b.sequence[1]
            assert a.sequence[length + 1] == b.sequence[0]
            expire(a)
            assert a.sequence[length] == b.sequence[1]
            assert a.sequence[length + 1] == b.sequence[0]
        assert len(a.sequence) == 6
        with raises(TypeError):
            a.sequence.extend([0xDEADBEEF])


def test_extend_sequence(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as a:
        length = len(a.sequence)
        with Image(filename=str(fx_asset.joinpath('github.ico'))) as b:
            a.sequence.extend(b.sequence)
            for i in range(2):
                assert a.sequence[length + i] == b.sequence[i]
            expire(a)
            for i in range(2):
                assert a.sequence[length + i] == b.sequence[i]
        assert len(a.sequence) == 6


@mark.parametrize('how_many', range(2, 5))
def test_extend_offset(fx_asset, how_many):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as a:
        instances = list(a.sequence)
        with Image(filename=str(fx_asset.joinpath('github.ico'))) as b:
            added = list(b.sequence)[::-1] + [b.sequence[0]] * (how_many - 2)
            a.sequence.extend(added, 2)
            instances[2:2] = added
            assert list(a.sequence) == instances
            expire(a)
            assert list(a.sequence) == instances
        assert len(a.sequence) == 4 + how_many


def test_extend_offset_sequence(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as a:
        instances = list(a.sequence)
        with Image(filename=str(fx_asset.joinpath('github.ico'))) as b:
            a.sequence.extend(b.sequence, 2)
            instances[2:2] = list(b.sequence)
            assert list(a.sequence) == instances
            expire(a)
            assert list(a.sequence) == instances
        assert len(a.sequence) == 6


def test_extend_first(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as a:
        instances = list(a.sequence)
        with Image(filename=str(fx_asset.joinpath('github.ico'))) as b:
            a.sequence.extend(list(b.sequence)[::-1], 0)
            instances[:0] = list(b.sequence)[::-1]
            assert list(a.sequence) == instances
            expire(a)
            assert list(a.sequence) == instances
        assert len(a.sequence) == 6


def test_extend_first_sequence(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as a:
        instances = list(a.sequence)
        with Image(filename=str(fx_asset.joinpath('github.ico'))) as b:
            a.sequence.extend(b.sequence, 0)
            instances[:0] = list(b.sequence)
            assert list(a.sequence) == instances
            expire(a)
            assert list(a.sequence) == instances
        assert len(a.sequence) == 6


def test_container(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        for single in img.sequence:
            assert single.container is img


def test_index(fx_asset):
    with Image(filename=str(fx_asset.joinpath('nocomments.gif'))) as img:
        for i, single in enumerate(img.sequence):
            assert single.index == i
        del img.sequence[0]
        for i, single in enumerate(img.sequence):
            assert single.index == i
    with Image(filename=str(fx_asset.joinpath('nocomments.gif'))) as img:
        del img.sequence[2]
        for i, single in enumerate(img.sequence):
            assert single.index == i


@mark.parametrize(('cmp_name', 'f'), [
    ('equals', lambda i: i),  # identity
    ('hash_equals', hash),
    ('signature_equals', lambda i: i.signature)
])
def test_equals(cmp_name, f, fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as a:
        with Image(filename=str(fx_asset.joinpath('apple.ico'))) as b:
            assert f(a) == f(b)
            assert f(a.sequence[0]) == f(b.sequence[0])
            assert f(a) != f(b.sequence[1])
            assert f(a.sequence[0]) != f(b.sequence[1])
            assert f(a.sequence[1]) == f(b.sequence[1])


def test_clone(fx_asset):
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        with img.sequence[2].clone() as single:
            assert single.wand != img.wand
            assert len(single.sequence) == 1
            assert len(list(single.sequence)) == 1
            assert single.size == img.sequence[2].size


def test_changes_reflected_back(fx_asset):
    """Changes on each single image should be reflected back to
    the container image.

    """
    with Image(filename=str(fx_asset.joinpath('apple.ico'))) as img:
        with img.sequence[3] as single:
            single.resize(32, 32)
            assert single.size == (32, 32)
            img.sequence.instances[3] = None  # to make new instance
            uncommitted = img.sequence[3]
            assert uncommitted.size == (16, 16)
        img.sequence.instances[3] = None
        with img.sequence[3] as committed:
            assert committed.size == (32, 32)


def test_delay(fx_asset):
    fpath = str(fx_asset.joinpath('nocomments-delay-100.gif'))
    with Image(filename=fpath) as img:
        for s in img.sequence:
            assert s.delay == 100


def test_set_delay(fx_asset):
    with Image(filename=str(fx_asset.joinpath('nocomments.gif'))) as img:
        with img.sequence[2] as frame:
            assert frame.delay == 0
            frame.delay = 10
        with img.sequence.index_context(2):
            assert library.MagickGetImageDelay(img.wand) == 10
        with raises(TypeError):
            img.sequence[2].delay = 0.0001
        with raises(ValueError):
            img.sequence[2].delay = -1
