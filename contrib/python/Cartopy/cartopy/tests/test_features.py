# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import pytest

import cartopy.feature as cfeature


small_extent = (-6, -8, 56, 59)
medium_extent = (-20, 20, 20, 60)
large_extent = (-40, 40, 0, 80)

auto_scaler = cfeature.AdaptiveScaler('110m', (('50m', 50), ('10m', 15)))

auto_land = cfeature.NaturalEarthFeature('physical', 'land', auto_scaler)


class TestFeatures:
    def test_change_scale(self):
        # Check that features can easily be retrieved with a different
        # scale.
        new_lakes = cfeature.LAKES.with_scale('10m')
        assert new_lakes.scale == '10m'
        assert new_lakes.kwargs == cfeature.LAKES.kwargs
        assert new_lakes.category == cfeature.LAKES.category
        assert new_lakes.name == cfeature.LAKES.name

    def test_scale_from_extent(self):
        # Check that scaler.scale_from_extent returns the appropriate
        # scales.
        small_scale = auto_land.scaler.scale_from_extent(small_extent)
        medium_scale = auto_land.scaler.scale_from_extent(medium_extent)
        large_scale = auto_land.scaler.scale_from_extent(large_extent)
        assert small_scale == '10m'
        assert medium_scale == '50m'
        assert large_scale == '110m'

    def test_intersecting_geometries_small(self, monkeypatch):
        # Patch so we don't actually try to download anything.
        monkeypatch.setattr(auto_land, 'geometries', lambda: [])
        # Check that intersecting_geometries will set the scale to
        # '10m' when the extent is small and autoscale is True.
        auto_land.intersecting_geometries(small_extent)
        assert auto_land.scale == '10m'

    def test_intersecting_geometries_medium(self, monkeypatch):
        # Patch so we don't actually try to download anything.
        monkeypatch.setattr(auto_land, 'geometries', lambda: [])
        # Check that intersecting_geometries will set the scale to
        # '50m' when the extent is medium and autoscale is True.
        auto_land.intersecting_geometries(medium_extent)
        assert auto_land.scale == '50m'

    def test_intersecting_geometries_large(self, monkeypatch):
        # Patch so we don't actually try to download anything.
        monkeypatch.setattr(auto_land, 'geometries', lambda: [])
        # Check that intersecting_geometries will set the scale to
        # '110m' when the extent is large and autoscale is True.
        auto_land.intersecting_geometries(large_extent)
        assert auto_land.scale == '110m'


def test_bad_ne_scale():
    with pytest.raises(ValueError, match='not a valid Natural Earth scale'):
        cfeature.NaturalEarthFeature('physical', 'land', '30m')
