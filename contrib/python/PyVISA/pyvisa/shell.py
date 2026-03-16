#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""Shell for interactive testing.

This file is taken from the Lantz Project.

:copyright: (c) 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: BSD, see LICENSE for more details.

"""

import cmd
from typing import List, Tuple

from . import ResourceManager, VisaIOError, attributes, constants
from .thirdparty import prettytable

# TODO providing a way to list/use constants would be nice


class VisaShell(cmd.Cmd):
    """Shell for interactive testing."""

    intro: str = "\nWelcome to the VISA shell. Type help or ? to list commands.\n"
    prompt: str = "(visa) "

    use_rawinput: bool = True

    def __init__(self, library_path: str = ""):
        super().__init__()
        self.resource_manager = ResourceManager(library_path)
        self.default_prompt = self.prompt

        #: Resource list (used for autocomplete)
        #: Store a tuple with the name and the alias.
        self.resources: List[Tuple[str, str]] = []

        #: Resource in use
        #: pyvisa.resources.Resource
        self.current = None

        self.py_attr: List[str] = []
        self.vi_attr: List[str] = []

    def do_list(self, args):
        """List all connected resources."""

        try:
            resources = self.resource_manager.list_resources_info()
        except Exception as e:
            print(e)
        else:
            self.resources = []
            for ndx, (resource_name, value) in enumerate(resources.items()):
                if not args:
                    print("({0:2d}) {1}".format(ndx, resource_name))
                    if value.alias:
                        print("     alias: {}".format(value.alias))

                self.resources.append((resource_name, value.alias or None))

    def do_open(self, args):
        """Open resource by number, resource name or alias: open 3"""

        if not args:
            print("A resource name must be specified.")
            return

        if self.current:
            print(
                "You can only open one resource at a time. Please close the current one first."
            )
            return

        if args.isdigit():
            try:
                args = self.resources[int(args)][0]
            except IndexError:
                print('Not a valid resource number. Use the command "list".')
                return

        try:
            self.current = self.resource_manager.open_resource(args)
            print(
                "{} has been opened.\n"
                'You can talk to the device using "write", "read" or "query".\n'
                "The default end of message is added to each message.".format(args)
            )

            self.py_attr = []
            self.vi_attr = []
            for attr in getattr(self.current, "visa_attributes_classes", ()):
                if attr.py_name:
                    self.py_attr.append(attr.py_name)
                self.vi_attr.append(attr.visa_name)

            self.prompt = "(open) "
        except Exception as e:
            print(e)

    def complete_open(self, text, line, begidx, endidx):
        """Provide completion on open."""
        if not self.resources:
            self.do_list("do not print")
        return [item[0] for item in self.resources if item[0].startswith(text)] + [
            item[1] for item in self.resources if item[1] and item[1].startswith(text)
        ]

    def do_close(self, args):
        """Close resource in use."""

        if not self.current:
            print('There are no resources in use. Use the command "open".')
            return

        try:
            self.current.close()
        except Exception as e:
            print(e)
        else:
            print("The resource has been closed.")
            self.current = None
            self.prompt = self.default_prompt

    def do_query(self, args):
        """Query resource in use: query *IDN?"""

        if not self.current:
            print('There are no resources in use. Use the command "open".')
            return

        try:
            print("Response: {}".format(self.current.query(args)))
        except Exception as e:
            print(e)

    def do_read(self, args):
        """Receive from the resource in use."""

        if not self.current:
            print('There are no resources in use. Use the command "open".')
            return

        try:
            print(self.current.read())
        except Exception as e:
            print(e)

    def do_write(self, args):
        """Send to the resource in use: send *IDN?"""

        if not self.current:
            print('There are no resources in use. Use the command "open".')
            return

        try:
            self.current.write(args)
        except Exception as e:
            print(e)

    def do_timeout(self, args):
        """Get or set timeout (in ms) for resource in use.

        Get timeout:

            timeout

        Set timeout:

            timeout <mstimeout>

        """

        if not self.current:
            print('There are no resources in use. Use the command "open".')
            return

        args = args.strip()

        if not args:
            try:
                print("Timeout: {}ms".format(self.current.timeout))
            except Exception as e:
                print(e)
        else:
            args = args.split(" ")
            try:
                self.current.timeout = float(args[0])
                print("Done")
            except Exception as e:
                print(e)

    def print_attribute_list(self):
        """Print the supported attribute list."""
        p = prettytable.PrettyTable(("VISA name", "Constant", "Python name", "val"))
        for attr in getattr(self.current, "visa_attributes_classes", ()):
            try:
                val = self.current.get_visa_attribute(attr.attribute_id)
            except VisaIOError as e:
                val = e.abbreviation
            except Exception as e:
                val = str(e)
                if len(val) > 10:
                    val = val[:10] + "..."
            p.add_row((attr.visa_name, attr.attribute_id, attr.py_name, val))

        print(p.get_string(sortby="VISA name"))

    def do_attr(self, args):
        """Get or set the state for a visa attribute.

        List all attributes:

            attr

        Get an attribute state:

            attr <name>

        Set an attribute state:

            attr <name> <state>
        """

        if not self.current:
            print('There are no resources in use. Use the command "open".')
            return

        args = args.strip()

        if not args:
            self.print_attribute_list()
            return

        args = args.split(" ")

        if len(args) > 2:
            print(
                "Invalid syntax, use `attr <name>` to get; or `attr <name> <value>` to set"
            )
            return

        if len(args) == 1:
            # Get a given attribute
            attr_name = args[0]
            if attr_name.startswith("VI_"):
                try:
                    print(
                        self.current.get_visa_attribute(getattr(constants, attr_name))
                    )
                except Exception as e:
                    print(e)
            else:
                try:
                    print(getattr(self.current, attr_name))
                except Exception as e:
                    print(e)
            return

        # Set the specified attribute value
        attr_name, attr_state = args[0], args[1]
        if attr_name.startswith("VI_"):
            try:
                attributeId = getattr(constants, attr_name)
                attr = attributes.AttributesByID[attributeId]
                datatype = attr.visa_type
                retcode = None
                if datatype == "ViBoolean":
                    if attr_state == "True":
                        attr_state = True
                    elif attr_state == "False":
                        attr_state = False
                    else:
                        retcode = (
                            constants.StatusCode.error_nonsupported_attribute_state
                        )
                elif datatype in [
                    "ViUInt8",
                    "ViUInt16",
                    "ViUInt32",
                    "ViInt8",
                    "ViInt16",
                    "ViInt32",
                ]:
                    try:
                        attr_state = int(attr_state)
                    except ValueError:
                        retcode = (
                            constants.StatusCode.error_nonsupported_attribute_state
                        )
                if not retcode:
                    retcode = self.current.set_visa_attribute(attributeId, attr_state)
                if retcode:
                    print("Error {}".format(str(retcode)))
                else:
                    print("Done")
            except Exception as e:
                print(e)
        else:
            print("Setting Resource Attributes by python name is not yet supported.")
            return

    def complete_attr(self, text, line, begidx, endidx):
        """Provide completion for the attr command."""
        return [item for item in self.py_attr if item.startswith(text)] + [
            item for item in self.vi_attr if item.startswith(text)
        ]

    def do_termchar(self, args):
        """Get or set termination character for resource in use.
        <termchar> can be one of: CR, LF, CRLF, NUL or None.
        None is used to disable termination character

        Get termination character:

            termchar

        Set termination character read or read+write:

            termchar <termchar> [<termchar>]

        """

        if not self.current:
            print('There are no resources in use. Use the command "open".')
            return

        args = args.strip()

        if not args:
            try:
                charmap = {"\r": "CR", "\n": "LF", "\r\n": "CRLF", "\0": "NUL"}
                chr = self.current.read_termination
                if chr in charmap:
                    chr = charmap[chr]
                chw = self.current.write_termination
                if chw in charmap:
                    chw = charmap[chw]
                print("Termchar read: {} write: {}".format(chr, chw))
            except Exception as e:
                print(e)

        args = args.split(" ")

        if len(args) > 2:
            print(
                "Invalid syntax, use `termchar <termchar>` to set both "
                "read_termination and write_termination to the same value, or "
                "`termchar <read_termchar> <write_termchar>` to use distinct values."
            )
        else:
            charmap = {
                "CR": "\r",
                "LF": "\n",
                "CRLF": "\r\n",
                "NUL": "\0",
                "None": None,
            }
            chr = args[0]
            chw = args[0 if len(args) == 1 else 1]
            if chr in charmap and chw in charmap:
                try:
                    self.current.read_termination = charmap[chr]
                    self.current.write_termination = charmap[chw]
                    print("Done")
                except Exception as e:
                    print(e)
            else:
                print("use CR, LF, CRLF, NUL or None to set termchar")
                return

    def do_exit(self, arg):
        """Exit the shell session."""

        if self.current:
            self.current.close()
        self.resource_manager.close()
        del self.resource_manager
        return True

    def do_EOF(self, arg):
        """Handle an EOF."""
        return True


def main(library_path=""):
    """Main entry point to start the shell."""
    VisaShell(library_path).cmdloop()
