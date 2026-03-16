# Copyright (c) 2014-2024, Emmanuel Blot <emmanuel.blot@free.fr>
# Copyright (c) 2016, Emmanuel Bouaziz <ebouaziz@free.fr>
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

"""USB Helpers"""

import sys
from importlib import import_module
from string import printable as printablechars
from threading import RLock
from typing import (Any, Dict, List, NamedTuple, Optional, Sequence, Set,
                    TextIO, Type, Tuple, Union)
from urllib.parse import SplitResult, urlsplit, urlunsplit
from usb.backend import IBackend
from usb.core import Device as UsbDevice, USBError
from usb.util import dispose_resources, get_string as usb_get_string
from .misc import to_int

# pylint: disable=broad-except

UsbDeviceDescriptor = NamedTuple('UsbDeviceDescriptor',
                                 (('vid', int),
                                  ('pid', int),
                                  ('bus', Optional[int]),
                                  ('address', Optional[int]),
                                  ('sn', Optional[str]),
                                  ('index', Optional[int]),
                                  ('description', Optional[str])))
"""USB Device descriptor are used to report known information about a FTDI
   compatible device, and as a device selection filter

   * vid: vendor identifier, 16-bit integer
   * pid: product identifier, 16-bit integer
   * bus: USB bus identifier, host dependent integer
   * address: USB address identifier on a USB bus, host dependent integer
   * sn: serial number, string
   * index: integer, can be used to descriminate similar devices
   * description: device description, as a string

   To select a device, use None for unknown fields

   .. note::

     * Always prefer serial number to other identification methods if available
     * Prefer bus/address selector over index
"""

UsbDeviceKey = Union[Tuple[int, int, int, int], Tuple[int, int]]
"""USB device indentifier on the system.

   This is used as USB device identifiers on the host. On proper hosts,
   this is a (bus, address, vid, pid) 4-uple. On stupid hosts (such as M$Win),
   it may be degraded to (vid, pid) 2-uple.
"""


class UsbToolsError(Exception):
    """UsbTools error."""


class UsbTools:
    """Helpers to obtain information about connected USB devices."""

    # Supported back ends, in preference order
    BACKENDS = ('usb.backend.libusb1', 'usb.backend.libusb0')

    # Need to maintain a list of reference USB devices, to circumvent a
    # limitation in pyusb that prevents from opening several times the same
    # USB device. The following dictionary used bus/address/vendor/product keys
    # to track (device, refcount) pairs
    Lock = RLock()
    Devices = {}  # (bus, address, vid, pid): (usb.core.Device, refcount)
    UsbDevices = {}  # (vid, pid): {usb.core.Device}
    UsbApi = None

    @classmethod
    def find_all(cls, vps: Sequence[Tuple[int, int]],
                 nocache: bool = False) -> \
            List[Tuple[UsbDeviceDescriptor, int]]:
        """Find all devices that match the specified vendor/product pairs.

           :param vps: a sequence of 2-tuple (vid, pid) pairs
           :param bool nocache: bypass cache to re-enumerate USB devices on
                                the host
           :return: a list of 2-tuple (UsbDeviceDescriptor, interface count)
        """
        with cls.Lock:
            devs = set()
            for vid, pid in vps:
                # TODO optimize useless loops
                devs.update(UsbTools._find_devices(vid, pid, nocache))
            devices = set()
            for dev in devs:
                ifcount = max(cfg.bNumInterfaces for cfg in dev)
                # TODO: handle / is serial number strings
                sernum = UsbTools.get_string(dev, dev.iSerialNumber)
                description = UsbTools.get_string(dev, dev.iProduct)
                descriptor = UsbDeviceDescriptor(dev.idVendor, dev.idProduct,
                                                 dev.bus, dev.address,
                                                 sernum, None, description)
                devices.add((descriptor, ifcount))
            return list(devices)

    @classmethod
    def flush_cache(cls, ):
        """Flush the FTDI device cache.

           It is highly recommanded to call this method a FTDI device is
           unplugged/plugged back since the last enumeration, as the device
           may appear on a different USB location each time it is plugged
           in.

           Failing to clear out the cache may lead to USB Error 19:
           ``Device may have been disconnected``.
        """
        with cls.Lock:
            cls.UsbDevices.clear()

    @classmethod
    def get_device(cls, devdesc: UsbDeviceDescriptor) -> UsbDevice:
        """Find a previously open device with the same vendor/product
           or initialize a new one, and return it.

           If several FTDI devices of the same kind (vid, pid) are connected
           to the host, either index or serial argument should be used to
           discriminate the FTDI device.

           index argument is not a reliable solution as the host may enumerate
           the USB device in random order. serial argument is more reliable
           selector and should always be prefered.

           Some FTDI devices support several interfaces/ports (such as FT2232H,
           FT4232H and FT4232HA). The interface argument selects the FTDI port
           to use, starting from 1 (not 0).

           :param devdesc: Device descriptor that identifies the device by
                           constraints.
           :return: PyUSB device instance
        """
        with cls.Lock:
            if devdesc.index or devdesc.sn or devdesc.description:
                dev = None
                if not devdesc.vid:
                    raise ValueError('Vendor identifier is required')
                devs = cls._find_devices(devdesc.vid, devdesc.pid)
                if devdesc.description:
                    devs = [dev for dev in devs if
                            UsbTools.get_string(dev, dev.iProduct) ==
                            devdesc.description]
                if devdesc.sn:
                    devs = [dev for dev in devs if
                            UsbTools.get_string(dev, dev.iSerialNumber) ==
                            devdesc.sn]
                if devdesc.bus is not None and devdesc.address is not None:
                    devs = [dev for dev in devs if
                            (devdesc.bus == dev.bus and
                             devdesc.address == dev.address)]
                if isinstance(devs, set):
                    # there is no guarantee the same index with lead to the
                    # same device. Indexing should be reworked
                    devs = list(devs)
                try:
                    dev = devs[devdesc.index or 0]
                except IndexError as exc:
                    raise IOError("No such device") from exc
            else:
                devs = cls._find_devices(devdesc.vid, devdesc.pid)
                dev = list(devs)[0] if devs else None
            if not dev:
                raise IOError('Device not found')
            try:
                devkey = (dev.bus, dev.address, devdesc.vid, devdesc.pid)
                if None in devkey[0:2]:
                    raise AttributeError('USB backend does not support bus '
                                         'enumeration')
            except AttributeError:
                devkey = (devdesc.vid, devdesc.pid)
            if devkey not in cls.Devices:
                # only change the active configuration if the active one is
                # not the first. This allows other libusb sessions running
                # with the same device to run seamlessly.
                try:
                    config = dev.get_active_configuration()
                    setconf = config.bConfigurationValue != 1
                except USBError:
                    setconf = True
                if setconf:
                    try:
                        dev.set_configuration()
                    except USBError:
                        pass
                cls.Devices[devkey] = [dev, 1]
            else:
                cls.Devices[devkey][1] += 1
            return cls.Devices[devkey][0]

    @classmethod
    def release_device(cls, usb_dev: UsbDevice):
        """Release a previously open device, if it not used anymore.

           :param usb_dev: a previously instanciated USB device instance
        """
        # Lookup for ourselves in the class dictionary
        with cls.Lock:
            # pylint: disable=unnecessary-dict-index-lookup
            for devkey, (dev, refcount) in cls.Devices.items():
                if dev == usb_dev:
                    # found
                    if refcount > 1:
                        # another interface is open, decrement
                        cls.Devices[devkey][1] -= 1
                    else:
                        # last interface in use, release
                        dispose_resources(cls.Devices[devkey][0])
                        del cls.Devices[devkey]
                    break

    @classmethod
    def release_all_devices(cls, devclass: Optional[Type] = None) -> int:
        """Release all open devices.

           :param devclass: optional class to only release devices of one type
           :return: the count of device that have been released.
        """
        with cls.Lock:
            remove_devs = set()
            # pylint: disable=consider-using-dict-items
            for devkey in cls.Devices:
                if devclass:
                    dev = cls._get_backend_device(cls.Devices[devkey][0])
                    if dev is None or not isinstance(dev, devclass):
                        continue
                dispose_resources(cls.Devices[devkey][0])
                remove_devs.add(devkey)
            for devkey in remove_devs:
                del cls.Devices[devkey]
            return len(remove_devs)

    @classmethod
    def list_devices(cls, urlstr: str,
                     vdict: Dict[str, int],
                     pdict: Dict[int, Dict[str, int]],
                     default_vendor: int) -> \
            List[Tuple[UsbDeviceDescriptor, int]]:
        """List candidates that match the device URL pattern.

           :see: :py:meth:`show_devices` to generate the URLs from the
                 candidates list

           :param url: the URL to parse
           :param vdict: vendor name map of USB vendor ids
           :param pdict: vendor id map of product name map of product ids
           :param default_vendor: default vendor id
           :return: list of (UsbDeviceDescriptor, interface)
        """
        urlparts = urlsplit(urlstr)
        if not urlparts.path:
            raise UsbToolsError('URL string is missing device port')
        candidates, _ = cls.enumerate_candidates(urlparts, vdict, pdict,
                                                 default_vendor)
        return candidates

    @classmethod
    def parse_url(cls, urlstr: str, scheme: str,
                  vdict: Dict[str, int],
                  pdict: Dict[int, Dict[str, int]],
                  default_vendor: int) -> Tuple[UsbDeviceDescriptor, int]:
        """Parse a device specifier URL.

           :param url: the URL to parse
           :param scheme: scheme to match in the URL string (scheme://...)
           :param vdict: vendor name map of USB vendor ids
           :param pdict: vendor id map of product name map of product ids
           :param default_vendor: default vendor id
           :return: UsbDeviceDescriptor, interface

           ..note:

              URL syntax:

                  protocol://vendor:product[:serial|:index|:bus:addr]/interface
        """
        urlparts = urlsplit(urlstr)
        if scheme != urlparts.scheme:
            raise UsbToolsError(f'Invalid URL: {urlstr}')
        try:
            if not urlparts.path:
                raise UsbToolsError('URL string is missing device port')
            path = urlparts.path.strip('/')
            if path == '?' or (not path and urlstr.endswith('?')):
                report_devices = True
                interface = -1
            else:
                interface = to_int(path)
                report_devices = False
        except (IndexError, ValueError) as exc:
            raise UsbToolsError(f'Invalid device URL: {urlstr}') from exc
        candidates, idx = cls.enumerate_candidates(urlparts, vdict, pdict,
                                                   default_vendor)
        if report_devices:
            UsbTools.show_devices(scheme, vdict, pdict, candidates)
            raise SystemExit(candidates and
                             'Please specify the USB device' or
                             'No USB-Serial device has been detected')
        if idx is None:
            if len(candidates) > 1:
                raise UsbToolsError(f"{len(candidates)} USB devices match URL "
                                    f"'{urlstr}'")
            idx = 0
        try:
            desc, _ = candidates[idx]
            vendor, product = desc[:2]
        except IndexError:
            raise UsbToolsError(f'No USB device matches URL {urlstr}') \
                from None
        if not vendor:
            cvendors = {candidate[0] for candidate in candidates}
            if len(cvendors) == 1:
                vendor = cvendors.pop()
        if vendor not in pdict:
            vstr = '0x{vendor:04x}' if vendor is not None else '?'
            raise UsbToolsError(f'Vendor ID {vstr} not supported')
        if not product:
            cproducts = {candidate[1] for candidate in candidates
                         if candidate[0] == vendor}
            if len(cproducts) == 1:
                product = cproducts.pop()
        if product not in pdict[vendor].values():
            pstr = '0x{vendor:04x}' if product is not None else '?'
            raise UsbToolsError(f'Product ID {pstr} not supported')
        devdesc = UsbDeviceDescriptor(vendor, product, desc.bus, desc.address,
                                      desc.sn, idx, desc.description)
        return devdesc, interface

    @classmethod
    def enumerate_candidates(cls, urlparts: SplitResult,
                             vdict: Dict[str, int],
                             pdict: Dict[int, Dict[str, int]],
                             default_vendor: int) -> \
            Tuple[List[Tuple[UsbDeviceDescriptor, int]], Optional[int]]:
        """Enumerate USB device URLs that match partial URL and VID/PID
           criteria.

           :param urlpart: splitted device specifier URL
           :param vdict: vendor name map of USB vendor ids
           :param pdict: vendor id map of product name map of product ids
           :param default_vendor: default vendor id
           :return: list of (usbdev, iface), parsed index if any
        """
        specifiers = urlparts.netloc.split(':')
        plcomps = specifiers + [''] * 2
        try:
            plcomps[0] = vdict.get(plcomps[0], plcomps[0])
            if plcomps[0]:
                vendor = to_int(plcomps[0])
            else:
                vendor = None
            product_ids = pdict.get(vendor, None)
            if not product_ids:
                product_ids = pdict[default_vendor]
            plcomps[1] = product_ids.get(plcomps[1], plcomps[1])
            if plcomps[1]:
                try:
                    product = to_int(plcomps[1])
                except ValueError as exc:
                    raise UsbToolsError(f'Product {plcomps[1]} is not '
                                        f'referenced') from exc
            else:
                product = None
        except (IndexError, ValueError) as exc:
            raise UsbToolsError(f'Invalid device URL: '
                                f'{urlunsplit(urlparts)}') from exc
        sernum = None
        idx = None
        bus = None
        address = None
        locators = specifiers[2:]
        if len(locators) > 1:
            try:
                bus = int(locators[0], 16)
                address = int(locators[1], 16)
            except ValueError as exc:
                raise UsbToolsError(f'Invalid bus/address: '
                                    f'{":".join(locators)}') from exc
        else:
            if locators and locators[0]:
                try:
                    devidx = to_int(locators[0])
                    if devidx > 255:
                        raise ValueError()
                    idx = devidx
                    if idx:
                        idx = devidx-1
                except ValueError:
                    sernum = locators[0]
        candidates = []
        vendors = [vendor] if vendor else set(vdict.values())
        vps = set()
        for vid in vendors:
            products = pdict.get(vid, [])
            for pid in products:
                vps.add((vid, products[pid]))
        devices = cls.find_all(vps)
        if sernum:
            if sernum not in [dev.sn for dev, _ in devices]:
                raise UsbToolsError(f'No USB device with S/N {sernum}')
        for desc, ifcount in devices:
            if vendor and vendor != desc.vid:
                continue
            if product and product != desc.pid:
                continue
            if sernum and sernum != desc.sn:
                continue
            if bus is not None:
                if bus != desc.bus or address != desc.address:
                    continue
            candidates.append((desc, ifcount))
        return candidates, idx

    @classmethod
    def show_devices(cls, scheme: str,
                     vdict: Dict[str, int],
                     pdict: Dict[int, Dict[str, int]],
                     devdescs: Sequence[Tuple[UsbDeviceDescriptor, int]],
                     out: Optional[TextIO] = None):
        """Show supported devices. When the joker url ``scheme://*/?`` is
           specified as an URL, it generates a list of connected USB devices
           that match the supported USB devices. It can be used to provide the
           end-user with a list of valid URL schemes.

           :param scheme: scheme to match in the URL string (scheme://...)
           :param vdict: vendor name map of USB vendor ids
           :param pdict: vendor id map of product name map of product ids
           :param devdescs: candidate devices
           :param out: output stream, none for stdout
        """
        if not devdescs:
            return
        if not out:
            out = sys.stdout
        devstrs = cls.build_dev_strings(scheme, vdict, pdict, devdescs)
        max_url_len = max(len(url) for url, _ in devstrs)
        print('Available interfaces:', file=out)
        for url, desc in devstrs:
            print(f'  {url:{max_url_len}s}  {desc}', file=out)
        print('', file=out)

    @classmethod
    def build_dev_strings(cls, scheme: str,
                          vdict: Dict[str, int],
                          pdict: Dict[int, Dict[str, int]],
                          devdescs: Sequence[Tuple[UsbDeviceDescriptor,
                                                   int]]) -> \
            List[Tuple[str, str]]:
        """Build URL and device descriptors from UsbDeviceDescriptors.

           :param scheme: protocol part of the URLs to generate
           :param vdict: vendor name map of USB vendor ids
           :param pdict: vendor id map of product name map of product ids
           :param devdescs: USB devices and interfaces
           :return: list of (url, descriptors)
        """
        indices = {}  # Dict[Tuple[int, int], int]
        descs = []
        for desc, ifcount in sorted(devdescs):
            ikey = (desc.vid, desc.pid)
            indices[ikey] = indices.get(ikey, 0) + 1
            # try to find a matching string for the current vendor
            vendors = []
            # fallback if no matching string for the current vendor is found
            vendor = f'{desc.vid:04x}'
            for vidc in vdict:
                if vdict[vidc] == desc.vid:
                    vendors.append(vidc)
            if vendors:
                vendors.sort(key=len)
                vendor = vendors[0]
            # try to find a matching string for the current vendor
            # fallback if no matching string for the current product is found
            product = f'{desc.pid:04x}'
            try:
                products = []
                productids = pdict[desc.vid]
                for prdc in productids:
                    if productids[prdc] == desc.pid:
                        products.append(prdc)
                if products:
                    product = products[0]
            except KeyError:
                pass
            for port in range(1, ifcount+1):
                fmt = '%s://%s/%d'
                parts = [vendor, product]
                sernum = desc.sn
                if not sernum:
                    sernum = ''
                if [c for c in sernum if c not in printablechars or c == '?']:
                    serial = f'{indices[ikey]}'
                else:
                    serial = sernum
                if serial:
                    parts.append(serial)
                elif desc.bus is not None and desc.address is not None:
                    parts.append(f'{desc.bus:x}')
                    parts.append(f'{desc.address:x}')
                # the description may contain characters that cannot be
                # emitted in the output stream encoding format
                try:
                    url = fmt % (scheme, ':'.join(parts), port)
                except Exception:
                    url = fmt % (scheme, ':'.join([vendor, product, '???']),
                                 port)
                try:
                    if desc.description:
                        description = f'({desc.description})'
                    else:
                        description = ''
                except Exception:
                    description = ''
                descs.append((url, description))
        return descs

    @classmethod
    def get_string(cls, device: UsbDevice, stridx: int) -> str:
        """Retrieve a string from the USB device, dealing with PyUSB API breaks

           :param device: USB device instance
           :param stridx: the string identifier
           :return: the string read from the USB device
        """
        if cls.UsbApi is None:
            # pylint: disable=import-outside-toplevel
            import inspect
            args, _, _, _ = \
                inspect.signature(UsbDevice.read).parameters
            if (len(args) >= 3) and args[1] == 'length':
                cls.UsbApi = 1
            else:
                cls.UsbApi = 2
        try:
            if cls.UsbApi == 2:
                return usb_get_string(device, stridx)
            return usb_get_string(device, 64, stridx)
        except UnicodeDecodeError:
            # do not abort if EEPROM data is somewhat incoherent
            return ''

    @classmethod
    def find_backend(cls) -> IBackend:
        """Try to find and load an PyUSB backend.

           ..note:: There is no need to call this method for regular usage.

           :return: PyUSB backend
        """
        with cls.Lock:
            return cls._load_backend()

    @classmethod
    def _find_devices(cls, vendor: int, product: int,
                      nocache: bool = False) -> Set[UsbDevice]:
        """Find a USB device and return it.

           This code re-implements the usb.core.find() method using a local
           cache to avoid calling several times the underlying LibUSB and the
           system USB calls to enumerate the available USB devices. As these
           calls are time-hungry (about 1 second/call), the enumerated devices
           are cached. It consumes a bit more memory but dramatically improves
           start-up time.
           Hopefully, this kludge is temporary and replaced with a better
           implementation from PyUSB at some point.

           :param vendor: USB vendor id
           :param product: USB product id
           :param bool nocache: bypass cache to re-enumerate USB devices on
                                the host
           :return: a set of USB device matching the vendor/product identifier
                    pair
        """
        backend = cls._load_backend()
        vidpid = (vendor, product)
        if nocache or (vidpid not in cls.UsbDevices):
            # not freed until Python runtime completion
            # enumerate_devices returns a generator, so back up the
            # generated device into a list. To save memory, we only
            # back up the supported devices
            devs = set()
            vpdict = {}  # Dict[int, List[int]]
            vpdict.setdefault(vendor, [])
            vpdict[vendor].append(product)
            for dev in backend.enumerate_devices():
                # pylint: disable=no-member
                device = UsbDevice(dev, backend)
                if device.idVendor in vpdict:
                    products = vpdict[device.idVendor]
                    if products and (device.idProduct not in products):
                        continue
                    devs.add(device)
            if sys.platform == 'win32':
                # ugly kludge for a boring OS:
                # on Windows, the USB stack may enumerate the very same
                # devices several times: a real device with N interface
                # appears also as N device with as single interface.
                # We only keep the "device" that declares the most
                # interface count and discard the "virtual" ones.
                filtered_devs = {}
                for dev in devs:
                    vid = dev.idVendor
                    pid = dev.idProduct
                    ifc = max(cfg.bNumInterfaces for cfg in dev)
                    k = (vid, pid, dev.bus, dev.address)
                    if k not in filtered_devs:
                        filtered_devs[k] = dev
                    else:
                        fdev = filtered_devs[k]
                        fifc = max(cfg.bNumInterfaces for cfg in fdev)
                        if fifc < ifc:
                            filtered_devs[k] = dev
                devs = set(filtered_devs.values())
            cls.UsbDevices[vidpid] = devs
        return cls.UsbDevices[vidpid]

    @classmethod
    def _get_backend_device(cls, device: UsbDevice) -> Any:
        """Return the backend implementation of a device.

           :param device: the UsbDevice (usb.core.Device)
           :return: the implementation of any
        """
        try:
            # pylint: disable=protected-access
            # need to access private member _ctx of PyUSB device
            # (resource manager) until PyUSB #302 is addressed
            return device._ctx.dev
            # pylint: disable=protected-access
        except AttributeError:
            return None

    @classmethod
    def _load_backend(cls) -> IBackend:
        backend = None  # Optional[IBackend]
        for candidate in cls.BACKENDS:
            mod = import_module(candidate)
            backend = mod.get_backend()
            if backend is not None:
                return backend
        raise ValueError('No backend available')
