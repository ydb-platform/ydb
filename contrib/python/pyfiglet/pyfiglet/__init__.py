#!/usr/bin/env python
#-*- encoding: utf-8 -*-

"""
Python FIGlet adaption
"""

from __future__ import print_function, unicode_literals, annotations
from typing import Any, Optional

import itertools
import importlib.resources
import os
import pathlib
import re
import shutil
import sys
import zipfile
from optparse import OptionParser

from .version import __version__

__author__ = 'Peter Waller <p@pwaller.net>'
__copyright__ = """
The MIT License (MIT)
Copyright © 2007-2018
  Christopher Jones <cjones@insub.org>
  Stefano Rivera <stefano@rivera.za.net>
  Peter Waller <p@pwaller.net>
  And various contributors (see git history).

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
"""


DEFAULT_FONT = 'standard'

COLOR_CODES = {'BLACK': 30, 'RED': 31, 'GREEN': 32, 'YELLOW': 33, 'BLUE': 34, 'MAGENTA': 35, 'CYAN': 36, 'LIGHT_GRAY': 37,
               'DEFAULT': 39, 'DARK_GRAY': 90, 'LIGHT_RED': 91, 'LIGHT_GREEN': 92, 'LIGHT_YELLOW': 93, 'LIGHT_BLUE': 94,
               'LIGHT_MAGENTA': 95, 'LIGHT_CYAN': 96, 'WHITE': 97, 'RESET': 0
}

RESET_COLORS = b'\033[0m'

if sys.platform == 'win32':
    SHARED_DIRECTORY = os.path.join(os.environ["APPDATA"], "pyfiglet")
else:
    SHARED_DIRECTORY = '/usr/local/share/pyfiglet/'


def figlet_format(text:str, font:str=DEFAULT_FONT, **kwargs:Any):
    fig = Figlet(font, **kwargs)
    return fig.renderText(text)


def print_figlet(text:str, font:str=DEFAULT_FONT, colors:str=":", **kwargs:Any):
    ansiColors = parse_color(colors)
    if ansiColors:
        sys.stdout.write(ansiColors)

    print(figlet_format(text, font, **kwargs))

    if ansiColors:
        sys.stdout.write(RESET_COLORS.decode('UTF-8', 'replace'))
        sys.stdout.flush()


class FigletError(Exception):
    def __init__(self, error:str):
        self.error = error

    def __str__(self):
        return self.error

class CharNotPrinted(FigletError):
    """
    Raised when the width is not sufficient to print a character
    """

class FontNotFound(FigletError):
    """
    Raised when a font can't be located
    """


class FontError(FigletError):
    """
    Raised when there is a problem parsing a font file
    """


class InvalidColor(FigletError):
    """
    Raised when the color passed is invalid
    """


class FigletFont(object):
    """
    This class represents the currently loaded font, including
    meta-data about how it should be displayed by default
    """

    reMagicNumber = re.compile(r'^[tf]lf2.')
    reEndMarker = re.compile(r'(.)\s*$')

    def __init__(self, font:str=DEFAULT_FONT):
        self.font = font

        self.comment = ''
        self.chars: dict[int, list[str]] = {}
        self.width: dict[int, int] = {}
        self.data = self.preloadFont(font)
        self.loadFont()

    @classmethod
    def preloadFont(cls, font:str):
        """
        Load font data if exist
        """
        # Find a plausible looking font file.
        data = None
        font_path = None
        for extension in ('tlf', 'flf'):
            fn = '%s.%s' % (font, extension)
            path = importlib.resources.files('pyfiglet.fonts').joinpath(fn)
            if path.exists():   # type: ignore
                font_path = path
                break
            else:
                for location in ("./", SHARED_DIRECTORY):
                    full_name = os.path.join(location, fn)
                    if os.path.isfile(full_name):
                        font_path = pathlib.Path(full_name)
                        break

        # Unzip the first file if this file/stream looks like a ZIP file.
        if font_path:
            with font_path.open('rb') as f:
                if zipfile.is_zipfile(f):
                    with zipfile.ZipFile(f) as zip_file:
                        zip_font = zip_file.open(zip_file.namelist()[0])
                        data = zip_font.read()
                else:
                    # ZIP file check moves the current file pointer - reset to start of file.
                    f.seek(0)
                    data = f.read()

        # Return the decoded data (if any).
        if data:
            return data.decode('UTF-8', 'replace')
        else:
            raise FontNotFound(font)

    @classmethod
    def isValidFont(cls, font:str):
        if not font.endswith(('.flf', '.tlf')):
            return False
        f = None
        full_file = os.path.join(SHARED_DIRECTORY, font)
        if os.path.isfile(font):
            f = open(font, 'rb')
        elif os.path.isfile(full_file):
            f = open(full_file, 'rb')
        else:
            f = importlib.resources.files('pyfiglet.fonts').joinpath(font).open('rb')

        if zipfile.is_zipfile(f):
            # If we have a match, the ZIP file spec says we should just read the first file in the ZIP.
            with zipfile.ZipFile(f) as zip_file:
                zip_font = zip_file.open(zip_file.namelist()[0])
                header = zip_font.readline().decode('UTF-8', 'replace')
        else:
            # ZIP file check moves the current file pointer - reset to start of file.
            f.seek(0)
            header = f.readline().decode('UTF-8', 'replace')

        f.close()

        return cls.reMagicNumber.search(header)

    @classmethod
    def getFonts(cls):
        all_files = importlib.resources.files('pyfiglet.fonts').iterdir()
        if os.path.isdir(SHARED_DIRECTORY):
             all_files = itertools.chain(all_files, pathlib.Path(SHARED_DIRECTORY).iterdir())
        return [font.name.split('.', 2)[0] for font
                in all_files
                if font.is_file() and cls.isValidFont(font.name)]

    @classmethod
    def infoFont(cls, font:str, short:bool=False):
        """
        Get information of font
        """
        data = FigletFont.preloadFont(font)
        infos: list[str] = []
        reStartMarker = re.compile(r"""
            ^(FONT|COMMENT|FONTNAME_REGISTRY|FAMILY_NAME|FOUNDRY|WEIGHT_NAME|
              SETWIDTH_NAME|SLANT|ADD_STYLE_NAME|PIXEL_SIZE|POINT_SIZE|
              RESOLUTION_X|RESOLUTION_Y|SPACING|AVERAGE_WIDTH|
              FONT_DESCENT|FONT_ASCENT|CAP_HEIGHT|X_HEIGHT|FACE_NAME|FULL_NAME|
              COPYRIGHT|_DEC_|DEFAULT_CHAR|NOTICE|RELATIVE_).*""", re.VERBOSE)
        reEndMarker = re.compile(r'^.*[@#$]$')
        for line in data.splitlines()[0:100]:
            if (cls.reMagicNumber.search(line) is None
                    and reStartMarker.search(line) is None
                    and reEndMarker.search(line) is None):
                infos.append(line)
        return '\n'.join(infos) if not short else infos[0]

    @staticmethod
    def installFonts(file_name:str):
        """
        Install the specified font file to this system.
        """
        if hasattr(importlib.resources.files('pyfiglet'), 'resolve'):
            # Figlet looks like a standard directory - so lets use that to install new fonts.
            location = str(importlib.resources.files('pyfiglet.fonts'))
        else:
            # Figlet is installed using a zipped resource - don't try to upload to it.
            location = SHARED_DIRECTORY

        print("Installing {} to {}".format(file_name, location))

        # Make sure the required destination directory exists
        if not os.path.exists(location):
            os.makedirs(location)

        # Copy the font definitions - unpacking any zip files as needed.
        if os.path.splitext(file_name)[1].lower() == ".zip":
            # Ignore any structure inside the ZIP file.
            with zipfile.ZipFile(file_name) as zip_file:
                for font in zip_file.namelist():
                    font_file = os.path.basename(font)
                    if not font_file:
                        continue
                    with zip_file.open(font) as src:
                        with open(os.path.join(location, font_file), "wb") as dest:
                            shutil.copyfileobj(src, dest)
        else:
            shutil.copy(file_name, location)

    def loadFont(self):
        """
        Parse loaded font data for the rendering engine to consume
        """
        try:
            # Remove any unicode line splitting characters other
            # than CRLF - to match figlet line parsing
            data = re.sub(r"[\u0085\u2028\u2029]", " ", self.data)

            # Parse first line of file, the header
            data = data.splitlines()

            header = data.pop(0)
            if self.reMagicNumber.search(header) is None:
                raise FontError('%s is not a valid figlet font' % self.font)

            header = self.reMagicNumber.sub('', header)
            header = header.split()

            if len(header) < 6:
                raise FontError('malformed header for %s' % self.font)

            hardBlank = header[0]
            height, baseLine, maxLength, oldLayout, commentLines = map( # type: ignore
                int, header[1:6])
            printDirection = fullLayout = None

            # these are all optional for backwards compat
            if len(header) > 6:
                printDirection = int(header[6])
            if len(header) > 7:
                fullLayout = int(header[7])

            # if the new layout style isn't available,
            # convert old layout style. backwards compatibility
            if fullLayout is None:
                if oldLayout == 0:
                    fullLayout = 64
                elif oldLayout < 0:
                    fullLayout = 0
                else:
                    fullLayout = (oldLayout & 31) | 128

            # Some header information is stored for later, the rendering
            # engine needs to know this stuff.
            self.height = height
            self.hardBlank = hardBlank
            self.printDirection = printDirection
            self.smushMode = fullLayout

            # Strip out comment lines
            for i in range(0, commentLines):
                self.comment += data.pop(0)

            def __char(data: list[str]) -> tuple[int, list[str]]:
                """
                Function loads one character in the internal array from font
                file content
                """
                end = None
                width = 0
                chars: list[str] = []
                for _ in range(0, height):
                    line = data.pop(0)
                    if end is None:
                        end = self.reEndMarker.search(line).group(1)  # type: ignore
                        end = re.compile(re.escape(end) + r'{1,2}\s*$')

                    line = end.sub('', line)

                    if len(line) > width:
                        width = len(line)
                    chars.append(line)
                return width, chars

            # Load ASCII standard character set (32 - 127).
            # Don't skip space definition as later rendering pipeline will
            # ignore all missing chars and space is critical for the line
            # breaking logic.
            for i in range(32, 127):
                width, letter = __char(data)
                if i == 32 or ''.join(letter) != '':
                    self.chars[i] = letter
                    self.width[i] = width

            # Load German Umlaute - the follow directly after standard character 127
            if data:
                for i in 'ÄÖÜäöüß':
                    width, letter = __char(data)
                    if ''.join(letter) != '':
                        self.chars[ord(i)] = letter
                        self.width[ord(i)] = width

            # Load ASCII extended character set
            while data:
                line = data.pop(0).strip()
                i = line.split(' ', 1)[0]
                if (i == ''):
                    continue
                hex_match = re.search('^0x', i, re.IGNORECASE)
                if hex_match is not None:
                    i = int(i, 16)
                    width, letter = __char(data)
                    if ''.join(letter) != '':
                        self.chars[i] = letter
                        self.width[i] = width

        except Exception as e:
            raise FontError('problem parsing %s font: %s' % (self.font, e))

    def __str__(self):
        return '<FigletFont object: %s>' % self.font


unicode_string = type(''.encode('ascii').decode('ascii'))


class FigletString(unicode_string):
    """
    Rendered figlet font
    """

    # translation map for reversing ascii art / -> \, etc.
    __reverse_map__ = (
        '\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f'
        '\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f'
        ' !"#$%&\')(*+,-.\\'
        '0123456789:;>=<?'
        '@ABCDEFGHIJKLMNO'
        'PQRSTUVWXYZ]/[^_'
        '`abcdefghijklmno'
        'pqrstuvwxyz}|{~\x7f'
        '\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f'
        '\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f'
        '\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf'
        '\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf'
        '\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf'
        '\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf'
        '\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef'
        '\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff')

    # translation map for flipping ascii art ^ -> v, etc.
    __flip_map__ = (
        '\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f'
        '\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f'
        ' !"#$%&\'()*+,-.\\'
        '0123456789:;<=>?'
        '@VBCDEFGHIJKLWNO'
        'bQbSTUAMXYZ[/]v-'
        '`aPcdefghijklwno'
        'pqrstu^mxyz{|}~\x7f'
        '\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f'
        '\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f'
        '\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf'
        '\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf'
        '\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf'
        '\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf'
        '\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef'
        '\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff')

    def reverse(self) -> FigletString:
        out: list[str] = []
        for row in self.splitlines():
            out.append(row.translate(self.__reverse_map__)[::-1])

        return self.newFromList(out)

    def flip(self) -> FigletString:
        out: list[str] = []
        for row in self.splitlines()[::-1]:
            out.append(row.translate(self.__flip_map__))

        return self.newFromList(out)

    # doesn't do self.strip() because it could remove leading whitespace on first line of the font
    # doesn't do row.strip() because it could remove empty lines within the font character
    def strip_surrounding_newlines(self) -> str:
        out: list[str] = []
        chars_seen = False
        for row in self.splitlines():
            # if the row isn't empty or if we're in the middle of the font character, add the line.
            if row.strip() != "" or chars_seen:
                chars_seen = True
                out.append(row)

        # rstrip to get rid of the trailing newlines
        return self.newFromList(out).rstrip()

    def normalize_surrounding_newlines(self):
        return '\n' + self.strip_surrounding_newlines() + '\n'

    def newFromList(self, list: list[str]):
        return FigletString('\n'.join(list) + '\n')


class FigletRenderingEngine(object):
    """
    This class handles the rendering of a FigletFont,
    including smushing/kerning/justification/direction
    """

    def __init__(self, base:Optional[Figlet]=None):
        self.base = base

    def render(self, text:str):
        """
        Render an ASCII text string in figlet
        """
        builder = FigletBuilder(text,
                                self.base.Font,
                                self.base.direction,
                                self.base.width,
                                self.base.justify)

        while builder.isNotFinished():
            builder.addCharToProduct()
            builder.goToNextChar()

        return builder.returnProduct()

class FigletProduct(object):
    """
    This class stores the internal build part of
    the ascii output string
    """
    def __init__(self):
        self.queue: list[list[str]] = list()
        self.buffer_string = ""

    def append(self, buffer: list[str]):
        self.queue.append(buffer)

    def getString(self):
        return FigletString(self.buffer_string)


class FigletBuilder(object):
    """
    Represent the internals of the build process
    """
    def __init__(self, text:str, font:FigletFont, direction:str, width:int, justify:str):

        self.text = list(map(ord, list(text)))
        self.direction = direction
        self.width = width
        self.font = font
        self.justify = justify

        self.iterator: int = 0
        self.maxSmush: int = 0
        self.newBlankRegistered = False

        self.curCharWidth: int = 0
        self.prevCharWidth: int = 0
        self.currentTotalWidth: int = 0

        self.blankMarkers: list[tuple[list[str], int]] = list()
        self.product = FigletProduct()
        self.buffer = ['' for i in range(self.font.height)]

        # constants.. lifted from figlet222
        self.SM_EQUAL = 1    # smush equal chars (not hardblanks)
        self.SM_LOWLINE = 2    # smush _ with any char in hierarchy
        self.SM_HIERARCHY = 4    # hierarchy: |, /\, [], {}, (), <>
        self.SM_PAIR = 8    # hierarchy: [ + ] -> |, { + } -> |, ( + ) -> |
        self.SM_BIGX = 16    # / + \ -> X, > + < -> X
        self.SM_HARDBLANK = 32    # hardblank + hardblank -> hardblank
        self.SM_KERN = 64
        self.SM_SMUSH = 128

    # builder interface

    def addCharToProduct(self):
        curChar = self.getCurChar()

        # if the character is a newline, we flush the buffer
        if self.text[self.iterator] == ord("\n"):
                self.blankMarkers.append(([row for row in self.buffer], self.iterator))
                self.handleNewLine()
                return None

        if curChar is None:
            return
        if self.width < self.getCurWidth():
            raise CharNotPrinted("Width is not enough to print this character")
        self.curCharWidth = self.getCurWidth()
        self.maxSmush = self.currentSmushAmount(curChar)

        self.currentTotalWidth = len(self.buffer[0]) + self.curCharWidth - self.maxSmush

        if self.text[self.iterator] == ord(' '):
            self.blankMarkers.append(([row for row in self.buffer], self.iterator))

        if self.text[self.iterator] == ord('\n'):
            self.blankMarkers.append(([row for row in self.buffer], self.iterator))
            self.handleNewLine()

        if (self.currentTotalWidth >= self.width):
            self.handleNewLine()
        else:
            for row in range(0, self.font.height):
                self.addCurCharRowToBufferRow(curChar, row)


        self.prevCharWidth = self.curCharWidth

    def goToNextChar(self):
        self.iterator += 1

    def returnProduct(self):
        """
        Returns the output string created by formatProduct
        """
        if self.buffer[0] != '':
            self.flushLastBuffer()
        self.formatProduct()
        return self.product.getString()

    def isNotFinished(self):
        ret = self.iterator < len(self.text)
        return ret

    # private

    def flushLastBuffer(self):
        self.product.append(self.buffer)

    def formatProduct(self):
        """
        This create the output string representation from
        the internal representation of the product
        """
        string_acc = ''
        for buffer in self.product.queue:
            buffer = self.justifyString(self.justify, buffer)
            string_acc += self.replaceHardblanks(buffer)
        self.product.buffer_string = string_acc

    def getCharAt(self, i: int) -> Optional[list[str]]:
        if i < 0 or i >= len(list(self.text)):
            return None
        c = self.text[i]

        if c not in self.font.chars:
            return None
        else:
            return self.font.chars[c]

    def getCharWidthAt(self, i: int) -> Optional[int]:
        if i < 0 or i >= len(self.text):
            return None
        c = self.text[i]
        if c not in self.font.chars:
            return None
        else:
            return self.font.width[c]

    def getCurChar(self) -> Optional[list[str]]:
        return self.getCharAt(self.iterator)

    def getCurWidth(self):
        return self.getCharWidthAt(self.iterator)

    def getLeftSmushedChar(self, i:int, addLeft):
        idx = len(addLeft) - self.maxSmush + i
        if idx >= 0 and idx < len(addLeft):
            left = addLeft[idx]
        else:
            left = ''
        return left, idx

    def currentSmushAmount(self, curChar: list[str]):
        return self.smushAmount(self.buffer, curChar)

    def updateSmushedCharInLeftBuffer(self, addLeft, idx, smushed):
        l = list(addLeft)
        if idx < 0 or idx > len(l):
            return addLeft
        l[idx] = smushed
        addLeft = ''.join(l)
        return addLeft

    def smushRow(self, curChar:list[str], row:int):
        addLeft = self.buffer[row]
        addRight = curChar[row]

        if self.direction == 'right-to-left':
            addLeft, addRight = addRight, addLeft

        for i in range(0, self.maxSmush):
            left, idx = self.getLeftSmushedChar(i, addLeft)
            right = addRight[i]
            smushed = self.smushChars(left=left, right=right)
            addLeft = self.updateSmushedCharInLeftBuffer(addLeft, idx, smushed)
        return addLeft, addRight

    def addCurCharRowToBufferRow(self, curChar:list[str], row:int):
        addLeft, addRight = self.smushRow(curChar, row)
        self.buffer[row] = addLeft + addRight[self.maxSmush:]

    def cutBufferCommon(self):
        self.currentTotalWidth = 0
        self.buffer = ['' for i in range(self.font.height)]
        self.blankMarkers = list()
        self.prevCharWidth = 0
        curChar = self.getCurChar()
        if curChar is None:
            return
        self.maxSmush = self.currentSmushAmount(curChar)

    def cutBufferAtLastBlank(self, saved_buffer, saved_iterator:int):
        self.product.append(saved_buffer)
        self.iterator = saved_iterator
        self.cutBufferCommon()

    def cutBufferAtLastChar(self):
        self.product.append(self.buffer)
        self.iterator -= 1
        self.cutBufferCommon()

    def blankExist(self, last_blank:int):
        return last_blank != -1

    def getLastBlank(self):
        try:
            saved_buffer, saved_iterator = self.blankMarkers.pop()
        except IndexError:
            return -1,-1
        return (saved_buffer, saved_iterator)

    def handleNewLine(self):
        saved_buffer, saved_iterator = self.getLastBlank()
        if self.blankExist(saved_iterator):
            self.cutBufferAtLastBlank(saved_buffer, saved_iterator)
        else:
            self.cutBufferAtLastChar()

    def justifyString(self, justify:str, buffer:list[str]):
        if justify == 'right':
            for row in range(0, self.font.height):
                buffer[row] = (
                        ' ' * (self.width - len(buffer[row]) - 1)
                        ) + buffer[row]
        elif justify == 'center':
            for row in range(0, self.font.height):
                buffer[row] = (
                        ' ' * int((self.width - len(buffer[row])) / 2)
                        ) + buffer[row]
        return buffer

    def replaceHardblanks(self, buffer:list[str]):
        string = '\n'.join(buffer) + '\n'
        string = string.replace(self.font.hardBlank, ' ')
        return string

    def smushAmount(self, buffer:list[str]=[], curChar:list[str]=[]):
        """
        Calculate the amount of smushing we can do between this char and the
        last If this is the first char it will throw a series of exceptions
        which are caught and cause appropriate values to be set for later.

        This differs from C figlet which will just get bogus values from
        memory and then discard them after.
        """
        if (self.font.smushMode & (self.SM_SMUSH | self.SM_KERN)) == 0:
            return 0

        maxSmush = self.curCharWidth
        for row in range(0, self.font.height):
            lineLeft = buffer[row]
            lineRight = curChar[row]
            if self.direction == 'right-to-left':
                lineLeft, lineRight = lineRight, lineLeft

            # Only strip ascii space to match figlet exactly.
            linebd = len(lineLeft.rstrip(' ')) - 1
            if linebd < 0:
                linebd = 0

            if linebd < len(lineLeft):
                ch1 = lineLeft[linebd]
            else:
                linebd = 0
                ch1 = ''

            # Only strip ascii space to match figlet exactly.
            charbd = len(lineRight) - len(lineRight.lstrip(' '))
            if charbd < len(lineRight):
                ch2 = lineRight[charbd]
            else:
                charbd = len(lineRight)
                ch2 = ''

            amt = charbd + len(lineLeft) - 1 - linebd

            if ch1 == '' or ch1 == ' ':
                amt += 1
            elif (ch2 != ''
                    and self.smushChars(left=ch1, right=ch2) is not None):
                amt += 1

            if amt < maxSmush:
                maxSmush = amt

        return maxSmush

    def smushChars(self, left:str='', right:str=''):
        """
        Given 2 characters which represent the edges rendered figlet
        fonts where they would touch, see if they can be smushed together.
        Returns None if this cannot or should not be done.
        """
        # Don't use isspace because this also matches unicode chars that figlet
        # treats as hard breaks
        if left == ' ':
            return right
        if right == ' ':
            return left

        # Disallows overlapping if previous or current char has a width of 1 or
        # zero
        if (self.prevCharWidth < 2) or (self.curCharWidth < 2):
            return

        # kerning only
        if (self.font.smushMode & self.SM_SMUSH) == 0:
            return

        # smushing by universal overlapping
        if (self.font.smushMode & 63) == 0:
            # Ensure preference to visiable characters.
            if left == self.font.hardBlank:
                return right
            if right == self.font.hardBlank:
                return left

            # Ensures that the dominant (foreground)
            # fig-character for overlapping is the latter in the
            # user's text, not necessarily the rightmost character.
            if self.direction == 'right-to-left':
                return left
            else:
                return right

        if self.font.smushMode & self.SM_HARDBLANK:
            if (left == self.font.hardBlank
                    and right == self.font.hardBlank):
                return left

        if (left == self.font.hardBlank
                or right == self.font.hardBlank):
            return

        if self.font.smushMode & self.SM_EQUAL:
            if left == right:
                return left

        smushes = ()

        if self.font.smushMode & self.SM_LOWLINE:
            smushes += (('_', r'|/\[]{}()<>'),)

        if self.font.smushMode & self.SM_HIERARCHY:
            smushes += (
                ('|', r'/\[]{}()<>'),
                (r'\/', '[]{}()<>'),
                ('[]', '{}()<>'),
                ('{}', '()<>'),
                ('()', '<>'),
            )

        for a, b in smushes:
            if left in a and right in b:
                return right
            if right in a and left in b:
                return left

        if self.font.smushMode & self.SM_PAIR:
            for pair in [left+right, right+left]:
                if pair in ['[]', '{}', '()']:
                    return '|'

        if self.font.smushMode & self.SM_BIGX:
            if (left == '/') and (right == '\\'):
                return '|'
            if (right == '/') and (left == '\\'):
                return 'Y'
            if (left == '>') and (right == '<'):
                return 'X'
        return


class Figlet(object):
    """
    Main figlet class.
    """

    def __init__(self, font:str=DEFAULT_FONT, direction:str='auto', justify:str='auto',
                 width:int=80) -> None:
        self.font = font
        self._direction = direction
        self._justify = justify
        self.width = width
        self.setFont()
        self.engine = FigletRenderingEngine(base=self)

    def setFont(self, **kwargs:str) -> None:
        if 'font' in kwargs:
            self.font = kwargs['font']

        self.Font = FigletFont(font=self.font)

    @property
    def direction(self):
        if self._direction == 'auto':
            direction = self.Font.printDirection
            if direction == 0:
                return 'left-to-right'
            elif direction == 1:
                return 'right-to-left'
            else:
                return 'left-to-right'

        else:
            return self._direction

    @property
    def justify(self):
        if self._justify == 'auto':
            if self.direction == 'left-to-right':
                return 'left'
            elif self.direction == 'right-to-left':
                return 'right'

        else:
            return self._justify

    def renderText(self, text:str) -> FigletString:
        # wrapper method to engine
        return self.engine.render(text)

    def getFonts(self) -> list[str]:
        return self.Font.getFonts()


def color_to_ansi(color:str, isBackground:bool):
    if not color:
        return ''
    color = color.upper()
    if color.count(';') > 0 and color.count(';') != 2:
        raise InvalidColor('Specified color \'{}\' not a valid color in R;G;B format')
    elif color.count(';') == 0 and color not in COLOR_CODES:
        raise InvalidColor('Specified color \'{}\' not found in ANSI COLOR_CODES list'.format(color))

    if color in COLOR_CODES:
        ansiCode = COLOR_CODES[color]
        if isBackground:
            ansiCode += 10
    else:
        ansiCode = 48 if isBackground else 38
        ansiCode = '{};2;{}'.format(ansiCode, color)

    return '\033[{}m'.format(ansiCode)


def parse_color(color:str):
    foreground, _, background = color.partition(":")
    ansiForeground = color_to_ansi(foreground, isBackground=False)
    ansiBackground = color_to_ansi(background, isBackground=True)
    return ansiForeground + ansiBackground


def main():
    parser = OptionParser(version=__version__,
                          usage='%prog [options] [text..]')
    parser.add_option('-f', '--font', default=DEFAULT_FONT,
                      help='font to render with (default: %default)',
                      metavar='FONT')
    parser.add_option('-D', '--direction', type='choice',
                      choices=('auto', 'left-to-right', 'right-to-left'),
                      default='auto', metavar='DIRECTION',
                      help='set direction text will be formatted in '
                           '(default: %default)')
    parser.add_option('-j', '--justify', type='choice',
                      choices=('auto', 'left', 'center', 'right'),
                      default='auto', metavar='SIDE',
                      help='set justification, defaults to print direction')
    parser.add_option('-w', '--width', type='int', default=80, metavar='COLS',
                      help='set terminal width for wrapping/justification '
                           '(default: %default)')
    parser.add_option('-r', '--reverse', action='store_true', default=False,
                      help='shows mirror image of output text')
    parser.add_option('-n', '--normalize-surrounding-newlines', action='store_true', default=False,
                      help='output has one empty line before and after')
    parser.add_option('-s', '--strip-surrounding-newlines', action='store_true', default=False,
                      help='removes empty leading and trailing lines')                      
    parser.add_option('-F', '--flip', action='store_true', default=False,
                      help='flips rendered output text over')
    parser.add_option('-l', '--list_fonts', action='store_true', default=False,
                      help='show installed fonts list')
    parser.add_option('-i', '--info_font', action='store_true', default=False,
                      help='show font\'s information, use with -f FONT')
    parser.add_option('-L', '--load', default=None,
                      help='load and install the specified font definition')
    parser.add_option('-c', '--color', default=':',
                      help='''prints text with passed foreground color,
                            --color=foreground:background
                            --color=:background\t\t\t # only background
                            --color=foreground | foreground:\t # only foreground
                            --color=list\t\t\t # list all colors
                            COLOR = list[COLOR] | [0-255];[0-255];[0-255] (RGB)''')
    opts, args = parser.parse_args()

    if opts.list_fonts:
        print('\n'.join(sorted(FigletFont.getFonts())))
        exit(0)

    if opts.color == 'list':
        print('[0-255];[0-255];[0-255] # RGB\n' + '\n'.join((sorted(COLOR_CODES.keys()))))
        exit(0)

    if opts.info_font:
        print(FigletFont.infoFont(opts.font))
        exit(0)

    if opts.load:
        FigletFont.installFonts(opts.load)
        exit(0)

    if len(args) == 0:
        parser.print_help()
        return 1

    if sys.version_info < (3,):
        args = [arg.decode('UTF-8') for arg in args]

    text = ' '.join(args)

    try:
        f = Figlet(
            font=opts.font, direction=opts.direction,
            justify=opts.justify, width=opts.width,
        )
    except FontNotFound as err:
        print(f"pyfiglet error: requested font {opts.font!r} not found.")
        return 1

    r = f.renderText(text)
    if opts.reverse:
        r = r.reverse()
    if opts.flip:
        r = r.flip()
    if opts.strip_surrounding_newlines:
        r = r.strip_surrounding_newlines()
    elif opts.normalize_surrounding_newlines:
        r = r.normalize_surrounding_newlines()

    if sys.version_info > (3,):
        # Set stdout to binary mode
        sys.stdout = sys.stdout.detach()

    ansiColors = parse_color(opts.color)
    if ansiColors:
        sys.stdout.write(ansiColors.encode('UTF-8'))

    sys.stdout.write(r.encode('UTF-8'))
    sys.stdout.write(b'\n')

    if ansiColors:
        sys.stdout.write(RESET_COLORS)

    return 0


if __name__ == '__main__':
    sys.exit(main())
