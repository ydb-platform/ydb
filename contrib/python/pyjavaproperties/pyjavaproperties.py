#! /usr/bin/env python

"""
A Python replacement for java.util.Properties class
This is modelled as closely as possible to the Java original.

Created - Anand B Pillai <abpillai@gmail.com>
"""

from __future__ import print_function
import sys,os
import re
import time

class Properties(object):
    """ A Python replacement for java.util.Properties """

    def __init__(self, props=None, verbose=False):

        # Note: We don't take a default properties object
        # as argument yet

        # Dictionary of properties.
        self._props = {}
        # Dictionary of properties with 'pristine' keys
        # This is used for dumping the properties to a file
        # using the 'store' method
        self._origprops = {}
        self._keyorder = []
        # Dictionary mapping keys from property
        # dictionary to pristine dictionary
        self._keymap = {}

        # All regexs moved here
        self.othercharre = re.compile(r'(?<!\\)(\s*\=)|(?<!\\)(\s*\:)')
        self.othercharre2 = re.compile(r'(\s*\=)|(\s*\:)')
        self.bspacere = re.compile(r'\\(?!\s$)')
        self.wspacere = re.compile(r'(?<![\\\=\:])(\s)')
        self.wspacere2 = re.compile(r'(?<![\\])(\s)')
        # Reference to previous properties like {propertykey}
        self.refre =  re.compile('{.+?}')
        # Debug flag
        self._verbose = verbose

    def __str__(self):
        s='{'
        for key,value in self._props.items():
            s = ''.join((s,key,'=',value,', '))

        s=''.join((s[:-2],'}'))
        return s

    def _debug(self, msg):
        print(msg) if self._verbose else None

    def __parse(self, lines):
        """ Parse a list of lines and create
        an internal property dictionary """

        # Every line in the file must consist of either a comment
        # or a key-value pair. A key-value pair is a line consisting
        # of a key which is a combination of non-white space characters
        # The separator character between key-value pairs is a '=',
        # ':' or a whitespace character not including the newline.
        # If the '=' or ':' characters are found, in the line, even
        # keys containing whitespace chars are allowed.

        # A line with only a key according to the rules above is also
        # fine. In such case, the value is considered as the empty string.
        # In order to include characters '=' or ':' in a key or value,
        # they have to be properly escaped using the backslash character.

        # Some examples of valid key-value pairs:
        #
        # key     value
        # key=value
        # key:value
        # key     value1,value2,value3
        # key     value1,value2,value3 \
        #         value4, value5
        # key
        # This key= this value
        # key = value1 value2 value3

        # Any line that starts with a '#' or '!' is considerered a comment
        # and skipped. Also any trailing or preceding whitespaces
        # are removed from the key/value.

        # This is a line parser. It parses the
        # contents like by line.

        lineno=0
        i = iter(lines)

        for line in i:
            lineno += 1
            line = line.strip()
            # Skip null lines
            if not line: continue
            # Skip lines which are comments
            if line[0] in ('#','!'): continue
            # Some flags
            escaped=False
            # Position of first separation char
            sepidx = -1
            # A flag for performing wspace re check
            flag = 0
            # Check for valid space separation
            # First obtain the max index to which we
            # can search.
            m = self.othercharre.search(line)
            if m:
                first, last = m.span()
                start, end = 0, first
                flag = 1
                wspacere = self.wspacere
            else:
                if self.othercharre2.search(line):
                    # Check if either '=' or ':' is present
                    # in the line. If they are then it means
                    # they are preceded by a backslash.

                    # This means, we need to modify the
                    # wspacere a bit, not to look for
                    # : or = characters.
                    wspacere = self.wspacere2

                start, end = 0, len(line)

            m2 = wspacere.search(line, start, end)
            if m2:
                # Means we need to split by space.
                first, last = m2.span()
                sepidx = first
            elif m:
                # No matching wspace char found, need
                # to split by either '=' or ':'
                first, last = m.span()
                sepidx = last - 1

            # If the last character is a backslash
            # it has to be preceded by a space in which
            # case the next line is read as part of the
            # same property
            while line[-1] == '\\':
                # Read next line
                nextline = next(i)
                nextline = nextline.strip()
                lineno += 1
                # This line will become part of the value
                line = line[:-1] + nextline

            # Now split to key,value according to separation char
            if sepidx != -1:
                key, value = line[:sepidx], line[sepidx+1:]
            else:
                key,value = line,''
            self._keyorder.append(key)
            self.process_pair(key, value)

    def process_pair(self, key, value):
        """ Process a (key, value) pair """

        oldkey = key
        oldvalue = value

        # Create key intelligently
        keyparts = self.bspacere.split(key)

        strippable = False
        lastpart = keyparts[-1]

        if lastpart.find('\\ ') != -1:
            keyparts[-1] = lastpart.replace('\\','')

        # If no backspace is found at the end, but empty
        # space is found, strip it
        elif lastpart and lastpart[-1] == ' ':
            strippable = True

        key = ''.join(keyparts)
        if strippable:
            key = key.strip()
            oldkey = oldkey.strip()

        oldvalue = self.unescape(oldvalue)
        value = self.unescape(value)

        # Patch from N B @ ActiveState
        refs = self.refre.findall(value)
        # To take care of referenced properties

        for ref in refs:
            ref_key = ref[1:-1]
            if ref_key not in self._props:
                self._debug('Error: referenced key {} does not exist!'.format(ref_key))
                continue

            # Replace with value of referred key - just once
            ref_val = self._props[ref_key]
            self._debug('Overwriting value with referenced key {} value => {}'.format(ref_key, ref_val))
            value = value.replace(ref, ref_val, 1)

        self._props[key] = value.strip()

        # Check if an entry exists in pristine keys
        if key in self._keymap:
            oldkey = self._keymap.get(key)
            self._origprops[oldkey] = oldvalue.strip()
        else:
            self._origprops[oldkey] = oldvalue.strip()
            # Store entry in keymap
            self._keymap[key] = oldkey

        if key not in self._keyorder:
            self._keyorder.append(key)

    def escape(self, value):
        """ Escaping some chars """
        # Java escapes the '=' and ':' in the value
        # string with backslashes in the store method.
        # So let us do the same.
        newvalue = value.replace(':','\\:')
        newvalue = newvalue.replace('=','\\=')

        return newvalue

    def unescape(self, value):
        """ Unescaping some chars """

        # Reverse of escape
        newvalue = value.replace('\\:',':')
        newvalue = newvalue.replace('\\=','=')

        return newvalue

    def load(self, stream):
        """ Load properties from an open file stream """

        with stream as fp:
            lines = fp.readlines()
            self.__parse(lines)

    def getProperty(self, key):
        """ Return a property for the given key """

        return self._props.get(key)

    def setProperty(self, key, value):
        """ Set the property for the given key """

        if type(key) is str and type(value) is str:
            self.process_pair(key, value)
        else:
            raise TypeError('both key and value should be strings!')

    def propertyNames(self):
        """ Return an iterator over all the keys of the property
        dictionary, i.e the names of the properties """

        return self._props.keys()

    def list(self, out=sys.stdout):
        """ Prints a listing of the properties to the
        stream 'out' which defaults to the standard output """

        out.write('-- listing properties --\n')
        for key,value in self._props.items():
            out.write(''.join((key,'=',value,'\n')))

    def store(self, out, header="", timestamp=True):
        """ Write the properties list to the stream 'out' along
        with the optional 'header' """


        with out as fp:
            if header: out.write(''.join(('#',header,'\n')))
            # Write timestamp
            if timestamp:
                tstamp = time.strftime('%a %b %d %H:%M:%S %Z %Y', time.localtime())
                out.write(''.join(('#',tstamp,'\n')))

            # Write properties from the pristine dictionary
            for prop in self._keyorder:
                if prop in self._origprops:
                    val = self._origprops[prop]
                    out.write(''.join((prop,'=',self.escape(val),'\n')))

    def __contains__(self, key):
        return key in self._props

    def getPropertyDict(self):
        return self._props

    def __getitem__(self, name):
        """ To support direct dictionary like access """

        return self.getProperty(name)

    def __setitem__(self, name, value):
        """ To support direct dictionary like access """

        self.setProperty(name, value)

    def __getattr__(self, name):
        """ For attributes not found in self, redirect
        to the properties dictionary """

        try:
            return self.__dict__[name]
        except KeyError as e:
            if hasattr(self._props,name):
                return getattr(self._props, name)

if __name__=="__main__":
    p = Properties()
    p.load(open('testdata/complex.properties'))
    p.list()
    print(p)
    print(p.items())
    print(p['name3'])
    p['name3'] = 'changed = value'
    print(p['name3'])
    p['new key'] = 'new value'
    p.store(open('test.properties','w'))
