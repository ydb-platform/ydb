import pytest  # noqa
import colorcet as cc

def test_bokeh_palettes_available():
    assert len(cc.palette.items()) == 210
    assert len(cc.palette_n.items()) == 34

def test_bokeh_palette_is_a_list():
    assert isinstance(cc.blues, list)
    assert len(cc.blues) == 256
    assert cc.blues[0] == '#f0f0f0'
    assert cc.blues[-1] == '#3a7bb1'

def test_bokeh_palette_glasbey_do_not_start_with_bw():
    for name in cc.all_original_names(group='glasbey'):
        cmap = cc.palette[name]
        assert isinstance(cmap, list)
        assert len(cmap) == 256
        assert {cmap[0], cmap[1]} != {'#00000', '#ffffff'}
