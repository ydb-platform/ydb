# VObject 
[![PyPI version](https://badge.fury.io/py/vobject.svg)](https://pypi.python.org/pypi/vobject)
[![PyPI downloads](https://img.shields.io/pypi/dm/vobject.svg)](https://pypi.python.org/pypi/vobject)
[![Build](https://github.com/py-vobject/vobject/actions/workflows/test.yml/badge.svg)](https://github.com/py-vobject/vobject/actions/workflows/test.yml)
[![License](https://img.shields.io/pypi/l/vobject.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

VObject is intended to be a full-featured Python package for parsing and 
generating vCard and vCalendar files.

Currently, iCalendar files are supported and well tested. vCard 3.0 files are 
supported, and all data should be imported, but only a few components are 
understood in a sophisticated way. 

The [Calendar Server](http://calendarserver.org/) team has added VAVAILABILITY 
support to VObject's iCalendar parsing. 

Please report bugs and issues directly on [GitHub](https://github.com/py-vobject/vobject/issues).

VObject is licensed under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).

Useful scripts included with VObject:

* [ics_diff](https://github.com/py-vobject/vobject/blob/master/vobject/ics_diff.py): order is irrelevant in iCalendar files, return a diff of meaningful changes between icalendar files
* [change_tz](https://github.com/py-vobject/vobject/blob/master/vobject/change_tz.py): Take an iCalendar file with events in the wrong timezone, change all events or just UTC events into one of the timezones **pytz** supports. Requires [pytz](https://pypi.python.org/pypi/pytz/).

# History
VObject was originally developed in concert with the Open Source Application 
Foundation's _Chandler_ project by Jeffrey Harris.  Maintenance was later
passed to [Sameen Karim](https://github.com/skarim) and 
[Will Percival](https://github.com/wpercy) at [Eventable](https://github.com/eventable).
After several years of inactivity, the project was revived under a dedicated
GitHub organization, with new volunteers.

**Please note**: the original repository at [eventable/vobject](https://github.com/eventable/vobject/) 
is _unmaintained_.  This project forked the latest code from that repository, after
attempts to revive the existing project with new maintainers were unsuccessful.

Many thanks to [all the contributors](https://github.com/py-vobject/vobject/blob/master/ACKNOWLEDGEMENTS.txt) for their dedication and support.

# Installation

To install with [pip](https://pypi.python.org/pypi/pip), run:

```
pip install vobject
```


Or download the package and run:

```
python setup.py install
```

VObject requires Python 2.7 or higher, along with the [dateutil](https://pypi.python.org/pypi/python-dateutil/) and [six](https://pypi.python.org/pypi/six) packages.


# Running tests

To run all tests, use:

```
python tests.py
```


# Usage

## iCalendar

#### Creating iCalendar objects

VObject has a basic datastructure for working with iCalendar-like
syntaxes.  Additionally, it defines specialized behaviors for many of
the commonly used iCalendar objects.

To create an object that already has a behavior defined, run:

```
>>> import vobject
>>> cal = vobject.newFromBehavior('vcalendar')
>>> cal.behavior
<class 'vobject.icalendar.VCalendar2_0'>
```

Convenience functions exist to create iCalendar and vCard objects:

```
>>> cal = vobject.iCalendar()
>>> cal.behavior
<class 'vobject.icalendar.VCalendar2_0'>
>>> card = vobject.vCard()
>>> card.behavior
<class 'vobject.vcard.VCard3_0'>
```

Once you have an object, you can use the add method to create
children:

```
>>> cal.add('vevent')
<VEVENT| []>
>>> cal.vevent.add('summary').value = "This is a note"
>>> cal.prettyPrint()
 VCALENDAR
    VEVENT
       SUMMARY: This is a note
```

Note that summary is a little different from vevent, it's a
ContentLine, not a Component.  It can't have children, and it has a
special value attribute.

ContentLines can also have parameters.  They can be accessed with
regular attribute names with _param appended:

```
>>> cal.vevent.summary.x_random_param = 'Random parameter'
>>> cal.prettyPrint()
 VCALENDAR
    VEVENT
       SUMMARY: This is a note
       params for  SUMMARY:
          X-RANDOM ['Random parameter']
```

There are a few things to note about this example

  * The underscore in x_random is converted to a dash (dashes are
    legal in iCalendar, underscores legal in Python)
  * X-RANDOM's value is a list.

If you want to access the full list of parameters, not just the first,
use &lt;paramname&gt;_paramlist:

```
>>> cal.vevent.summary.x_random_paramlist
['Random parameter']
>>> cal.vevent.summary.x_random_paramlist.append('Other param')
>>> cal.vevent.summary
<SUMMARY{'X-RANDOM': ['Random parameter', 'Other param']}This is a note>
```

Similar to parameters, If you want to access more than just the first child of a Component, you can access the full list of children of a given name by appending _list to the attribute name:

```
>>> cal.add('vevent').add('summary').value = "Second VEVENT"
>>> for ev in cal.vevent_list:
...     print ev.summary.value
This is a note
Second VEVENT
```

The interaction between the del operator and the hiding of the
underlying list is a little tricky, del cal.vevent and del
cal.vevent_list both delete all vevent children:

```
>>> first_ev = cal.vevent
>>> del cal.vevent
>>> cal
<VCALENDAR| []>
>>> cal.vevent = first_ev
```

VObject understands Python's datetime module and tzinfo classes.

```
>>> import datetime
>>> utc = vobject.icalendar.utc
>>> start = cal.vevent.add('dtstart')
>>> start.value = datetime.datetime(2006, 2, 16, tzinfo = utc)
>>> first_ev.prettyPrint()
     VEVENT
        DTSTART: 2006-02-16 00:00:00+00:00
        SUMMARY: This is a note
        params for  SUMMARY:
           X-RANDOM ['Random parameter', 'Other param']
```

Components and ContentLines have serialize methods:

```
>>> cal.vevent.add('uid').value = 'Sample UID'
>>> icalstream = cal.serialize()
>>> print icalstream
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//PYVOBJECT//NONSGML Version 1//EN
BEGIN:VEVENT
UID:Sample UID
DTSTART:20060216T000000Z
SUMMARY;X-RANDOM=Random parameter,Other param:This is a note
END:VEVENT
END:VCALENDAR
```

Observe that serializing adds missing required lines like version and
prodid.  A random UID would be generated, too, if one didn't exist.

If dtstart's tzinfo had been something other than UTC, an appropriate
vtimezone would be created for it.


#### Parsing iCalendar objects

To parse one top level component from an existing iCalendar stream or
string, use the readOne function:

```
>>> parsedCal = vobject.readOne(icalstream)
>>> parsedCal.vevent.dtstart.value
datetime.datetime(2006, 2, 16, 0, 0, tzinfo=tzutc())
```

Similarly, readComponents is a generator yielding one top level component at a time from a stream or string.

```
>>> vobject.readComponents(icalstream).next().vevent.dtstart.value
datetime.datetime(2006, 2, 16, 0, 0, tzinfo=tzutc())
```

More examples can be found in source code doctests.


## vCards

#### Creating vCard objects

Making vCards proceeds in much the same way. Note that the 'N' and 'FN'
attributes are required.

```
>>> j = vobject.vCard()
>>> j.add('n')
 <N{}    >
>>> j.n.value = vobject.vcard.Name( family='Harris', given='Jeffrey' )
>>> j.add('fn')
 <FN{}>
>>> j.fn.value ='Jeffrey Harris'
>>> j.add('email')
 <EMAIL{}>
>>> j.email.value = 'jeffrey@osafoundation.org'
>>> j.email.type_param = 'INTERNET'
>>> j.add('org')
 <ORG{}>
>>> j.org.value = ['Open Source Applications Foundation']
>>> j.prettyPrint()
 VCARD
    ORG: ['Open Source Applications Foundation']
    EMAIL: jeffrey@osafoundation.org
    params for  EMAIL:
       TYPE ['INTERNET']
    FN: Jeffrey Harris
    N:  Jeffrey  Harris
```

serializing will add any required computable attributes (like 'VERSION')

```
>>> j.serialize()
'BEGIN:VCARD\r\nVERSION:3.0\r\nEMAIL;TYPE=INTERNET:jeffrey@osafoundation.org\r\nFN:Jeffrey Harris\r\nN:Harris;Jeffrey;;;\r\nORG:Open Source Applications Foundation\r\nEND:VCARD\r\n'
>>> j.prettyPrint()
 VCARD
    ORG: Open Source Applications Foundation
    VERSION: 3.0
    EMAIL: jeffrey@osafoundation.org
    params for  EMAIL:
       TYPE ['INTERNET']
    FN: Jeffrey Harris
    N:  Jeffrey  Harris 
```

#### Parsing vCard objects

```
>>> s = """
... BEGIN:VCARD
... VERSION:3.0
... EMAIL;TYPE=INTERNET:jeffrey@osafoundation.org
... EMAIL;TYPE=INTERNET:jeffery@example.org
... ORG:Open Source Applications Foundation
... FN:Jeffrey Harris
... N:Harris;Jeffrey;;;
... END:VCARD
... """
>>> v = vobject.readOne( s )
>>> v.prettyPrint()
 VCARD
    ORG: Open Source Applications Foundation
    VERSION: 3.0
    EMAIL: jeffrey@osafoundation.org
    params for  EMAIL:
       TYPE [u'INTERNET']
    FN: Jeffrey Harris
    N:  Jeffrey  Harris
>>> v.n.value.family
u'Harris'
>>> v.email_list
[<EMAIL{'TYPE': ['INTERNET']}jeffrey@osafoundation.org>,
 <EMAIL{'TYPE': ['INTERNET']}jeffery@example.org>]
```

Just like with iCalendar example above readComponents will yield a generator from a stream or string containing multiple vCards objects.
```
>>> vobject.readComponents(vCardStream).next().email.value
'jeffrey@osafoundation.org'
```
