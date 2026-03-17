# This file is part of Androguard.
#
# Copyright (C) 2012/2013, Anthony Desnos <desnos at t0t0.fr>
# All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from pyaxmlparser import bytecode
import collections
from struct import unpack
from pyaxmlparser.arscutil import ARSCHeader, ARSCResTablePackage, \
    ARSCResTypeSpec, ARSCResType, ARSCResTableEntry, ARSCResTableConfig
from pyaxmlparser.stringblock import StringBlock
import pyaxmlparser.constants as const
from pyaxmlparser.utils import complexToFloat
from xml.sax.saxutils import escape

log = logging.getLogger("pyaxmlparser.arscparser")


class ARSCParser(object):
    """
    Parser for resource.arsc files
    """
    def __init__(self, raw_buff):
        self.analyzed = False
        self._resolved_strings = None
        self.buff = bytecode.BuffHandle(raw_buff)

        self.header = ARSCHeader(self.buff)
        # TODO: assert header type
        self.packageCount = unpack('<I', self.buff.read(4))[0]

        self.packages = {}
        self.values = {}
        self.resource_values = collections.defaultdict(collections.defaultdict)
        self.resource_configs = collections.defaultdict(lambda: collections.defaultdict(set))
        self.resource_keys = collections.defaultdict(
            lambda: collections.defaultdict(collections.defaultdict))
        self.stringpool_main = None

        # skip to the start of the first chunk data, skipping trailing header bytes
        self.buff.set_idx(self.header.start + self.header.header_size)

        # Gives the offset inside the file of the end of this chunk
        data_end = self.header.start + self.header.size

        while self.buff.get_idx() <= data_end - ARSCHeader.SIZE:
            res_header = ARSCHeader(self.buff)

            if res_header.start + res_header.size > data_end:
                # this inner chunk crosses the boundary of the table chunk
                break

            if res_header.type == const.RES_STRING_POOL_TYPE and not self.stringpool_main:
                self.stringpool_main = StringBlock(self.buff, res_header)

            elif res_header.type == const.RES_TABLE_PACKAGE_TYPE:
                assert len(self.packages) < self.packageCount, "Got more packages than expected"

                current_package = ARSCResTablePackage(self.buff, res_header)
                package_name = current_package.get_name()
                package_data_end = res_header.start + res_header.size

                self.packages[package_name] = []

                # After the Header, we have the resource type symbol table
                self.buff.set_idx(current_package.header.start + current_package.typeStrings)
                type_sp_header = ARSCHeader(self.buff)
                assert type_sp_header.type == const.RES_STRING_POOL_TYPE, \
                    "Expected String Pool header, got %x" % type_sp_header.type
                mTableStrings = StringBlock(self.buff, type_sp_header)

                # Next, we should have the resource key symbol table
                self.buff.set_idx(current_package.header.start + current_package.keyStrings)
                key_sp_header = ARSCHeader(self.buff)
                assert key_sp_header.type == const.RES_STRING_POOL_TYPE, \
                    "Expected String Pool header, got %x" % key_sp_header.type
                mKeyStrings = StringBlock(self.buff, key_sp_header)

                # Add them to the dict of read packages
                self.packages[package_name].append(current_package)
                self.packages[package_name].append(mTableStrings)
                self.packages[package_name].append(mKeyStrings)

                pc = PackageContext(current_package, self.stringpool_main,
                                    mTableStrings, mKeyStrings)

                # skip to the first header in this table package chunk
                # FIXME is this correct? We have already read the first two sections!
                # self.buff.set_idx(res_header.start + res_header.header_size)
                # this looks more like we want: (???)
                self.buff.set_idx(res_header.start + res_header.header_size + type_sp_header.size + key_sp_header.size)

                # Read all other headers
                while self.buff.get_idx() <= package_data_end - ARSCHeader.SIZE:
                    pkg_chunk_header = ARSCHeader(self.buff)
                    log.debug("Found a header: {}".format(pkg_chunk_header))
                    if pkg_chunk_header.start + pkg_chunk_header.size > package_data_end:
                        # we are way off the package chunk; bail out
                        break

                    self.packages[package_name].append(pkg_chunk_header)

                    if pkg_chunk_header.type == const.RES_TABLE_TYPE_SPEC_TYPE:
                        self.packages[package_name].append(ARSCResTypeSpec(self.buff, pc))

                    elif pkg_chunk_header.type == const.RES_TABLE_TYPE_TYPE:
                        a_res_type = ARSCResType(self.buff, pc)
                        self.packages[package_name].append(a_res_type)
                        self.resource_configs[package_name][a_res_type].add(a_res_type.config)

                        log.debug("Config: {}".format(a_res_type.config))

                        entries = []
                        for i in range(0, a_res_type.entryCount):
                            current_package.mResId = current_package.mResId & 0xffff0000 | i
                            entries.append((unpack('<i', self.buff.read(4))[0], current_package.mResId))

                        self.packages[package_name].append(entries)

                        for entry, res_id in entries:
                            if self.buff.end():
                                break

                            if entry != -1:
                                ate = ARSCResTableEntry(self.buff, res_id, pc)
                                self.packages[package_name].append(ate)
                                if ate.is_weak():
                                    # FIXME we are not sure how to implement the FLAG_WEAk!
                                    # We saw the following: There is just a single Res_value after the ARSCResTableEntry
                                    # and then comes the next ARSCHeader.
                                    # Therefore we think this means all entries are somehow replicated?
                                    # So we do some kind of hack here. We set the idx to the entry again...
                                    # Now we will read all entries!
                                    # Not sure if this is a good solution though
                                    self.buff.set_idx(ate.start)
                    elif pkg_chunk_header.type == const.RES_TABLE_LIBRARY_TYPE:
                        log.warning("RES_TABLE_LIBRARY_TYPE chunk is not supported")
                    else:
                        # FIXME: silently skip other chunk types
                        pass

                    # skip to the next chunk
                    self.buff.set_idx(pkg_chunk_header.start + pkg_chunk_header.size)

            # move to the next resource chunk
            self.buff.set_idx(res_header.start + res_header.size)

    def _analyse(self):
        if self.analyzed:
            return

        self.analyzed = True

        for package_name in self.packages:
            self.values[package_name] = {}

            nb = 3
            while nb < len(self.packages[package_name]):
                header = self.packages[package_name][nb]
                if isinstance(header, ARSCHeader):
                    if header.type == const.RES_TABLE_TYPE_TYPE:
                        a_res_type = self.packages[package_name][nb + 1]

                        locale = a_res_type.config.get_language_and_region()

                        c_value = self.values[package_name].setdefault(locale, {"public": []})

                        entries = self.packages[package_name][nb + 2]
                        nb_i = 0
                        for entry, res_id in entries:
                            if entry != -1:
                                ate = self.packages[package_name][nb + 3 + nb_i]

                                self.resource_values[ate.mResId][a_res_type.config] = ate
                                self.resource_keys[package_name][a_res_type.get_type()][ate.get_value()] = ate.mResId

                                if ate.get_index() != -1:
                                    c_value["public"].append(
                                        (a_res_type.get_type(), ate.get_value(),
                                         ate.mResId))

                                if a_res_type.get_type() not in c_value:
                                    c_value[a_res_type.get_type()] = []

                                if a_res_type.get_type() == "string":
                                    c_value["string"].append(
                                        self.get_resource_string(ate))

                                elif a_res_type.get_type() == "id":
                                    if not ate.is_complex():
                                        c_value["id"].append(
                                            self.get_resource_id(ate))

                                elif a_res_type.get_type() == "bool":
                                    if not ate.is_complex():
                                        c_value["bool"].append(
                                            self.get_resource_bool(ate))

                                elif a_res_type.get_type() == "integer":
                                    c_value["integer"].append(
                                        self.get_resource_integer(ate))

                                elif a_res_type.get_type() == "color":
                                    c_value["color"].append(
                                        self.get_resource_color(ate))

                                elif a_res_type.get_type() == "dimen":
                                    c_value["dimen"].append(
                                        self.get_resource_dimen(ate))

                                nb_i += 1
                        nb += 3 + nb_i - 1  # -1 to account for the nb+=1 on the next line
                nb += 1

    def get_resource_string(self, ate):
        return [ate.get_value(), ate.get_key_data()]

    def get_resource_id(self, ate):
        x = [ate.get_value()]
        if ate.key.get_data() == 0:
            x.append("false")
        elif ate.key.get_data() == 1:
            x.append("true")
        return x

    def get_resource_bool(self, ate):
        x = [ate.get_value()]
        if ate.key.get_data() == 0:
            x.append("false")
        elif ate.key.get_data() == -1:
            x.append("true")
        return x

    def get_resource_integer(self, ate):
        return [ate.get_value(), ate.key.get_data()]

    def get_resource_color(self, ate):
        entry_data = ate.key.get_data()
        return [
            ate.get_value(),
            "#%02x%02x%02x%02x" % (
                ((entry_data >> 24) & 0xFF),
                ((entry_data >> 16) & 0xFF),
                ((entry_data >> 8) & 0xFF),
                (entry_data & 0xFF))
        ]

    def get_resource_dimen(self, ate):
        try:
            return [
                ate.get_value(), "%s%s" % (
                    complexToFloat(ate.key.get_data()),
                    const.DIMENSION_UNITS[ate.key.get_data() & const.COMPLEX_UNIT_MASK])
            ]
        except IndexError:
            log.debug("Out of range dimension unit index for %s: %s" % (
                complexToFloat(ate.key.get_data()),
                ate.key.get_data() & const.COMPLEX_UNIT_MASK))
            return [ate.get_value(), ate.key.get_data()]

    # FIXME
    def get_resource_style(self, ate):
        return ["", ""]

    def get_packages_names(self):
        """
        Retrieve a list of all package names, which are available
        in the given resources.arsc.
        """
        return list(self.packages.keys())

    def get_locales(self, package_name):
        """
        Retrieve a list of all available locales in a given packagename.

        :param package_name: the package name to get locales of
        """
        self._analyse()
        return list(self.values[package_name].keys())

    def get_types(self, package_name, locale='\x00\x00'):
        """
        Retrieve a list of all types which are available in the given
        package and locale.

        :param package_name: the package name to get types of
        :param locale: the locale to get types of (default: '\x00\x00')
        """
        self._analyse()
        return list(self.values[package_name][locale].keys())

    def get_public_resources(self, package_name, locale='\x00\x00'):
        """
        Get the XML (as string) of all resources of type 'public'.

        The public resources table contains the IDs for each item.

        :param package_name: the package name to get the resources for
        :param locale: the locale to get the resources for (default: '\x00\x00')
        """

        self._analyse()

        buff = '<?xml version="1.0" encoding="utf-8"?>\n'
        buff += '<resources>\n'

        try:
            for i in self.values[package_name][locale]["public"]:
                buff += '<public type="%s" name="%s" id="0x%08x" />\n' % (
                    i[0], i[1], i[2])
        except KeyError:
            pass

        buff += '</resources>\n'

        return buff.encode('utf-8')

    def get_string_resources(self, package_name, locale='\x00\x00'):
        """
        Get the XML (as string) of all resources of type 'string'.

        Read more about string resources:
        https://developer.android.com/guide/topics/resources/string-resource.html

        :param package_name: the package name to get the resources for
        :param locale: the locale to get the resources for (default: '\x00\x00')
        """
        self._analyse()

        buff = '<?xml version="1.0" encoding="utf-8"?>\n'
        buff += '<resources>\n'

        try:
            for i in self.values[package_name][locale]["string"]:
                if any(map(i[1].__contains__, '<&>')):
                    value = '<![CDATA[%s]]>' % i[1]
                else:
                    value = i[1]
                buff += '<string name="%s">%s</string>\n' % (i[0], value)
        except KeyError:
            pass

        buff += '</resources>\n'

        return buff.encode('utf-8')

    def get_strings_resources(self):
        """
        Get the XML (as string) of all resources of type 'string'.
        This is a combined variant, which has all locales and all package names
        stored.
        """
        self._analyse()

        buff = '<?xml version="1.0" encoding="utf-8"?>\n'

        buff += "<packages>\n"
        for package_name in self.get_packages_names():
            buff += "<package name=\"%s\">\n" % package_name

            for locale in self.get_locales(package_name):
                buff += "<locale value=%s>\n" % repr(locale)

                buff += '<resources>\n'
                try:
                    for i in self.values[package_name][locale]["string"]:
                        buff += '<string name="%s">%s</string>\n' % (i[0], escape(i[1]))
                except KeyError:
                    pass

                buff += '</resources>\n'
                buff += '</locale>\n'

            buff += "</package>\n"

        buff += "</packages>\n"

        return buff.encode('utf-8')

    def get_id_resources(self, package_name, locale='\x00\x00'):
        """
        Get the XML (as string) of all resources of type 'id'.

        Read more about ID resources:
        https://developer.android.com/guide/topics/resources/more-resources.html#Id

        :param package_name: the package name to get the resources for
        :param locale: the locale to get the resources for (default: '\x00\x00')
        """
        self._analyse()

        buff = '<?xml version="1.0" encoding="utf-8"?>\n'
        buff += '<resources>\n'

        try:
            for i in self.values[package_name][locale]["id"]:
                if len(i) == 1:
                    buff += '<item type="id" name="%s"/>\n' % (i[0])
                else:
                    buff += '<item type="id" name="%s">%s</item>\n' % (i[0],
                                                                       escape(i[1]))
        except KeyError:
            pass

        buff += '</resources>\n'

        return buff.encode('utf-8')

    def get_bool_resources(self, package_name, locale='\x00\x00'):
        """
        Get the XML (as string) of all resources of type 'bool'.

        Read more about bool resources:
        https://developer.android.com/guide/topics/resources/more-resources.html#Bool

        :param package_name: the package name to get the resources for
        :param locale: the locale to get the resources for (default: '\x00\x00')
        """
        self._analyse()

        buff = '<?xml version="1.0" encoding="utf-8"?>\n'
        buff += '<resources>\n'

        try:
            for i in self.values[package_name][locale]["bool"]:
                buff += '<bool name="%s">%s</bool>\n' % (i[0], i[1])
        except KeyError:
            pass

        buff += '</resources>\n'

        return buff.encode('utf-8')

    def get_integer_resources(self, package_name, locale='\x00\x00'):
        """
        Get the XML (as string) of all resources of type 'integer'.

        Read more about integer resources:
        https://developer.android.com/guide/topics/resources/more-resources.html#Integer

        :param package_name: the package name to get the resources for
        :param locale: the locale to get the resources for (default: '\x00\x00')
        """
        self._analyse()

        buff = '<?xml version="1.0" encoding="utf-8"?>\n'
        buff += '<resources>\n'

        try:
            for i in self.values[package_name][locale]["integer"]:
                buff += '<integer name="%s">%s</integer>\n' % (i[0], i[1])
        except KeyError:
            pass

        buff += '</resources>\n'

        return buff.encode('utf-8')

    def get_color_resources(self, package_name, locale='\x00\x00'):
        """
        Get the XML (as string) of all resources of type 'color'.

        Read more about color resources:
        https://developer.android.com/guide/topics/resources/more-resources.html#Color

        :param package_name: the package name to get the resources for
        :param locale: the locale to get the resources for (default: '\x00\x00')
        """
        self._analyse()

        buff = '<?xml version="1.0" encoding="utf-8"?>\n'
        buff += '<resources>\n'

        try:
            for i in self.values[package_name][locale]["color"]:
                buff += '<color name="%s">%s</color>\n' % (i[0], i[1])
        except KeyError:
            pass

        buff += '</resources>\n'

        return buff.encode('utf-8')

    def get_dimen_resources(self, package_name, locale='\x00\x00'):
        """
        Get the XML (as string) of all resources of type 'dimen'.

        Read more about Dimension resources:
        https://developer.android.com/guide/topics/resources/more-resources.html#Dimension

        :param package_name: the package name to get the resources for
        :param locale: the locale to get the resources for (default: '\x00\x00')
        """
        self._analyse()

        buff = '<?xml version="1.0" encoding="utf-8"?>\n'
        buff += '<resources>\n'

        try:
            for i in self.values[package_name][locale]["dimen"]:
                buff += '<dimen name="%s">%s</dimen>\n' % (i[0], i[1])
        except KeyError:
            pass

        buff += '</resources>\n'

        return buff.encode('utf-8')

    def get_id(self, package_name, rid, locale='\x00\x00'):
        """
        Returns the tuple (resource_type, resource_name, resource_id)
        for the given resource_id.

        :param package_name: package name to query
        :param rid: the resource_id
        :param locale: specific locale
        :return: tuple of (resource_type, resource_name, resource_id)
        """
        self._analyse()

        try:
            for i in self.values[package_name][locale]["public"]:
                if i[2] == rid:
                    return i
        except KeyError:
            pass
        return None, None, None

    class ResourceResolver(object):
        """
        Resolves resources by ID
        """
        def __init__(self, android_resources, config=None):
            self.resources = android_resources
            self.wanted_config = config

        def resolve(self, res_id):
            result = []
            self._resolve_into_result(result, res_id, self.wanted_config)
            return result

        def _resolve_into_result(self, result, res_id, config):
            configs = self.resources.get_res_configs(res_id, config)
            if configs:
                for config, ate in configs:
                    self.put_ate_value(result, ate, config)

        def put_ate_value(self, result, ate, config):
            if ate.is_complex():
                complex_array = []
                result.append((config, complex_array))
                for _, item in ate.item.items:
                    self.put_item_value(complex_array, item, config, complex_=True)
            else:
                self.put_item_value(result, ate.key, config, complex_=False)

        def put_item_value(self, result, item, config, complex_):
            if item.is_reference():
                res_id = item.get_data()
                if res_id:
                    self._resolve_into_result(
                        result,
                        item.get_data(),
                        self.wanted_config)
            else:
                if complex_:
                    result.append(item.format_value())
                else:
                    result.append((config, item.format_value()))

    def get_resolved_res_configs(self, rid, config=None):
        resolver = ARSCParser.ResourceResolver(self, config)
        return resolver.resolve(rid)

    def get_resolved_strings(self):
        self._analyse()
        if self._resolved_strings:
            return self._resolved_strings

        r = {}
        for package_name in self.get_packages_names():
            r[package_name] = {}
            k = {}

            for locale in self.values[package_name]:
                v_locale = locale
                if v_locale == '\x00\x00':
                    v_locale = 'DEFAULT'

                r[package_name][v_locale] = {}

                try:
                    for i in self.values[package_name][locale]["public"]:
                        if i[0] == 'string':
                            r[package_name][v_locale][i[2]] = None
                            k[i[1]] = i[2]
                except KeyError:
                    pass

                try:
                    for i in self.values[package_name][locale]["string"]:
                        if i[0] in k:
                            r[package_name][v_locale][k[i[0]]] = i[1]
                except KeyError:
                    pass

        self._resolved_strings = r
        return r

    def get_res_configs(self, rid, config=None, fallback=True):
        """
        Return the resources found with the ID `rid` and select
        the right one based on the configuration, or return all if no configuration was set.

        But we try to be generous here and at least try to resolve something:
        This method uses a fallback to return at least one resource (the first one in the list)
        if more than one items are found and the default config is used and no default entry could be found.

        This is usually a bad sign (i.e. the developer did not follow the android documentation:
        https://developer.android.com/guide/topics/resources/localization.html#failing2)
        In practise an app might just be designed to run on a single locale and thus only has those locales set.

        You can disable this fallback behaviour, to just return exactly the given result.

        :param rid: resource id as int
        :param config: a config to resolve from, or None to get all results
        :param fallback: Enable the fallback for resolving default configuration (default: True)
        :return: a list of ARSCResTableConfig: ARSCResTableEntry
        """
        self._analyse()

        if not rid:
            raise ValueError("'rid' should be set")
        if not isinstance(rid, int):
            raise ValueError("'rid' must be an int")

        if rid not in self.resource_values:
            log.info("The requested rid could not be found in the resources.")
            return []

        res_options = self.resource_values[rid]
        if len(res_options) > 1 and config:
            if config in res_options:
                return [(config, res_options[config])]
            elif fallback and config == ARSCResTableConfig.default_config():
                log.warning("No default resource config could be found for the given rid, using fallback!")
                return [list(self.resource_values[rid].items())[0]]
            else:
                return []
        else:
            return list(res_options.items())

    def get_string(self, package_name, name, locale='\x00\x00'):
        self._analyse()

        try:
            for i in self.values[package_name][locale]["string"]:
                if i[0] == name:
                    return i
        except KeyError:
            return None

    def get_res_id_by_key(self, package_name, resource_type, key):
        try:
            return self.resource_keys[package_name][resource_type][key]
        except KeyError:
            return None

    def get_items(self, package_name):
        self._analyse()
        return self.packages[package_name]

    def get_type_configs(self, package_name, type_name=None):
        if package_name is None:
            package_name = self.get_packages_names()[0]
        result = collections.defaultdict(list)

        for res_type, configs in list(self.resource_configs[package_name].items()):
            if res_type.get_package_name() == package_name and (
                            type_name is None or res_type.get_type() == type_name):
                result[res_type.get_type()].extend(configs)

        return result

    @staticmethod
    def parse_id(name):
        """
        Resolves an id from a binary XML file in the form "@[package:]DEADBEEF"
        and returns a tuple of package name and resource id.
        If no package name was given, i.e. the ID has the form "@DEADBEEF",
        the package name is set to None.

        Raises a ValueError if the id is malformed.

        :param name: the string of the resource, as in the binary XML file
        :return: a tuple of (resource_id, package_name).
        """

        if not name.startswith('@'):
            raise ValueError("Not a valid resource ID, must start with @: '{}'".format(name))

        # remove @
        name = name[1:]

        package = None
        if ':' in name:
            package, res_id = name.split(':', 1)
        else:
            res_id = name

        if len(res_id) != 8:
            raise ValueError("Numerical ID is not 8 characters long: '{}'".format(res_id))

        try:
            return int(res_id, 16), package
        except ValueError:
            raise ValueError("ID is not a hex ID: '{}'".format(res_id))

    def get_resource_xml_name(self, r_id, package=None):
        """
        Returns the XML name for a resource, including the package name if package is None.
        A full name might look like `@com.example:string/foobar`
        Otherwise the name is only looked up in the specified package and is returned without
        the package name.
        The same example from about without the package name will read as `@string/foobar`.

        If the ID could not be found, `None` is returned.

        A description of the XML name can be found here:
        https://developer.android.com/guide/topics/resources/providing-resources#ResourcesFromXml

        :param r_id: numerical ID if the resource
        :param package: package name
        :return: XML name identifier
        """
        if package:
            resource, name, i_id = self.get_id(package, r_id)
            if not i_id:
                return None
            return "@{}/{}".format(resource, name)
        else:
            for p in self.get_packages_names():
                r, n, i_id = self.get_id(p, r_id)
                if i_id:
                    # found the resource in this package
                    package = p
                    resource = r
                    name = n
                    break
            if not package:
                return None
            else:
                return "@{}:{}/{}".format(package, resource, name)


class PackageContext(object):
    def __init__(self, current_package, stringpool_main, mTableStrings,
                 mKeyStrings):
        self.stringpool_main = stringpool_main
        self.mTableStrings = mTableStrings
        self.mKeyStrings = mKeyStrings
        self.current_package = current_package

    def get_mResId(self):
        return self.current_package.mResId

    def set_mResId(self, mResId):
        self.current_package.mResId = mResId

    def get_package_name(self):
        return self.current_package.get_name()
