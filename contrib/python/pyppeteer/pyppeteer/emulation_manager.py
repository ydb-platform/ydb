#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Emulation Manager module."""

from pyppeteer import helper
from pyppeteer.connection import CDPSession


class EmulationManager(object):
    """EmulationManager class."""

    def __init__(self, client: CDPSession) -> None:
        """Make new emulation manager."""
        self._client = client
        self._emulatingMobile = False
        self._hasTouch = False

    async def emulateViewport(self, viewport: dict) -> bool:
        """Evaluate viewport."""
        options = dict()
        mobile = viewport.get('isMobile', False)
        options['mobile'] = mobile
        if 'width' in viewport:
            options['width'] = helper.get_positive_int(viewport, 'width')
        if 'height' in viewport:
            options['height'] = helper.get_positive_int(viewport, 'height')

        options['deviceScaleFactor'] = viewport.get('deviceScaleFactor', 1)
        if viewport.get('isLandscape'):
            options['screenOrientation'] = {'angle': 90,
                                            'type': 'landscapePrimary'}
        else:
            options['screenOrientation'] = {'angle': 0,
                                            'type': 'portraitPrimary'}
        hasTouch = viewport.get('hasTouch', False)

        await self._client.send('Emulation.setDeviceMetricsOverride', options)
        await self._client.send('Emulation.setTouchEmulationEnabled', {
            'enabled': hasTouch,
            'configuration': 'mobile' if mobile else 'desktop'
        })

        reloadNeeded = (self._emulatingMobile != mobile or
                        self._hasTouch != hasTouch)

        self._emulatingMobile = mobile
        self._hasTouch = hasTouch
        return reloadNeeded
