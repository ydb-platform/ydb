<!--
# Copyright 2021-2022 Jetperch LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->

[![main](https://github.com/jetperch/jls/actions/workflows/packaging.yml/badge.svg?branch=main)](https://github.com/jetperch/jls/actions/workflows/packaging.yml)


# JLS

Welcome to the [Joulescope®](https://www.joulescope.com) File Format project.
The goal of this project is to provide performant data storage for huge, 
simultaneous, one-dimensional signals. This repository contains:

* The JLS file format specification
* The implementation in C
* Language bindings for Python


## Features

* Cross-platform
  * Microsoft Windows x64
  * Apple macOS x64
  * Apple macOS ARM
  * Linux x64
  * Linux aarch64 (ARM 64-bit).  Supports Raspberry Pi 4.
* Support for multiple, simultaneous data sources
* Support for multiple, simultaneous signal waveforms
* Fixed sample rate signals (FSR)
  * Handles missing samples gracefully (interpolate) 🔜
  * Multiple data types including:
    - Floating point: f32, f64
    - Unsigned integers: u1, u4, u8, u16, u24, u32, u64 
    - Signed integers: i4, i8, i16, i24, i32, i64
    - Fixed-point, signed integers (same bit sizes as signed integers)
    - Boolean (digital) 1-bit signals = u1
* Variable sample rate (VSR) signals 🔜
* Fast read performance
  * Signal Summaries
    * "Zoomed out" view with mean, min, max, standard deviation
    * Provides fast waveform load without any additional processing steps
  * Automatic load by summary level
  * Fast seek, next, previous access
* Sample ID to Wall-clock time (UTC) for FSR signals
* Annotations
  * Global VSR annotations
  * Signal annotations, timestamped to sample_id for FSR and UTC time for VSR
  * Support for text, marker, and user-defined (text, binary, JSON)
* User data
  * Arbitrary data included in the same file
  * Support for text, binary, and JSON
* Reliability
  * Integrated integrity checks using CRC32C
  * File data still accessible in the case of improper program termination 🔜
  * Uncorrupted data is still accessible in presence of file corruption 🔜
  * Write once, except for indices and the doubly-linked list pointers
* Compression options 🔜
  * lossless 🔜
  * lossy 🔜
  * lossy with downsampling below threshold 🔜
  * Support level 0 DATA not written (only INDEX & SUMMARY) 🔜

Items marked with 🔜 are under development and coming soon.
Items marked with ⏳ are planned for future release.

As of June 2023, the JLS v2 file structure is well-defined and stable.
However, the compression storage formats are not yet defined and
corrupted file recovery is not yet implemented.


## Why JLS?

The world is already full of file formats, and we would rather not create 
another one.  However, we could not identify a solution that met these
requirements.  [HDF5](https://www.hdfgroup.org/solutions/hdf5/) meets the
large storage requirements, but not the reliability and rapid load requirements.
The [Saleae binary export file format v2](https://support.saleae.com/faq/technical-faq/binary-export-format-logic-2)
is also not suitable since it buffers stores single, contiguous blocks.
[Sigrok v2](https://sigrok.org/wiki/File_format:Sigrok/v2) is similar.
The [Sigrok v3](https://sigrok.org/wiki/File_format:Sigrok/v3) format
(under development as of June 2023) is better in that it stores sequences of
"packets" containing data blocks, but it still will does not allow for
fast seek or summaries.

Timeseries databases, such as [InfluxDB](https://www.influxdata.com/), are 
powerful tools.  However, they are not well-designed for fast sample-rate
data.

Media containers are another option, especially the ISO base media file format
used by MPEG4 and many others:
  * [ISO/IEC 14496-14:2020 Specification](https://www.iso.org/standard/79110.html)
  * [Overview](https://mpeg.chiariglione.org/standards/mpeg-4/iso-base-media-file-format)

However, the standard does not include the ability to store the signal summaries
and our specific signal types.  While we could add these features, these formats
are already complicated, greatly reducing the advantage of repurposing them.


## Why JLS v2?

This file format is based upon JLS v1 designed for
[pyjoulescope](https://github.com/jetperch/pyjoulescope) and used by the
[Joulescope](https://www.joulescope.com/) test instrument.  We leveraged
the lessons learned from v1 to make v2 better, faster, and more extensible.

The JLS v1 format has been great for the Joulescope ecosystem and has
accomplished the objective of long data captures (days) with fast
sampling rates (MHz).  However, it now has a long list of issues including:

- Inflexible storage format (always current, voltage, power, current range, GPI0, GPI1).
- Unable to store from multiple sources.
- Unable to store other sources and signals.
- No annotation support: 
  [41](https://github.com/jetperch/pyjoulescope_ui/issues/41),
  [93](https://github.com/jetperch/pyjoulescope_ui/issues/93).
- Inflexible user data support.
- Inconsistent performance across sampling rates, zoom levels, and file sizes: 
  [48](https://github.com/jetperch/pyjoulescope_ui/issues/48),
  [103](https://github.com/jetperch/pyjoulescope_ui/issues/103).
- Unable to correlate sample times with UTC:
  [55](https://github.com/jetperch/pyjoulescope_ui/issues/55).

The JLS v2 file format addressed all of these issues, dramatically 
improved performance, and added new capabilities, such as signal compression.


## How?

At its lowest layer, JLS is an enhanced 
[tag-length-value](https://en.wikipedia.org/wiki/Type-length-value) (TLV)
format. TLV files form the foundation of many reliable image and video formats, 
including MPEG4 and PNG.  The enhanced header contains additional fields
to speed navigation and improve reliability.  The JLS file format calls 
each TLV a **chunk**.  The enhanced tag-length component the **chunk header**
or simply **header**.  The file also contains a **file header**, not to be 
confused with the **chunk header**.  A **chunk** may have zero payload length,
in which case the next header follows immediately.  Otherwise, a 
**chunk** consists of a **header** followed by a **payload**. 

The JLS file format defines **sources** that produce data.  The file allows
the application to clearly define and label the source.  Each source
can have any number of associated signals.

**Signals** are 1-D sequences of values over time consisting of a single,
fixed data type.  Each signal can have multiple **tracks** that contain
data associated with that signal. The JLS file supports two signal types: 
fixed sample rate (FSR) and variable sample rate (VSR).  FSR signals
store their sample data in the FSR track using FSR_DATA and FSR_SUMMARY.
FSR time is denoted by samples using timestamp.  FSR signals also support:

* Sample time to UTC time mapping using the UTC track.
* Annotations with the ANNOTATION track. 

VSR signals store their sample data in the VSR track.  VSR signals
specify time in UTC (wall-clock time).  VSR signals also
support annotations with the ANNOTATION track.
The JLS file format supports VSR signals that only use the 
ANNOTATION track and not the VSR track.  Such signals are commonly 
used to store UART text data where each line contains a UTC timestamp. 

Signals support DATA chunks and SUMMARY chunks.
The DATA chunks store the actual sample data.  The SUMMARY chunks
store the reduced statistics, where each statistic entry represents
multiple samples.  FSR tracks store the mean, min, max, 
and standard deviation.  Although standard deviation requires the
writer to compute the square root, standard deviation keeps the
same units and bit depth requirements as the other fields.  Variance
requires twice the bit size for integer types since it is squared.

Before each SUMMARY chunk, the JLS file will contain the INDEX chunk
which contains the starting time and offset for each chunk that 
contributed to the summary.  This SUMMARY chunk enables fast O(log n)
navigation of the file.  For FSR tracks, the starting time is 
calculated rather than stored for each entry.

The JLS file format design supports SUMMARY of SUMMARY.  It supports
the DATA and up to 15 layers of SUMMARIES.  timestamp is given as a
64-bit integer, which allows each summary to include only 20 samples
and still support the full 64-bit integer timestamp space.  In practice, the
first level summary increases a single value to 4 values, so summary
steps are usually 50 or more.

Many applications, including the Joulescope UI, prioritize read performance,
especially visualizing the waveform quickly following open, 
over write performance.   Waiting to scan through a 1 TB file is not a 
valid option.  The reader opens the file and scans for sources and signals.
The application can then quickly load the highest summary of summaries 
for every signal of interest.  The application can very quickly display this
data, and then start to retrieve more detailed information as requested.


## Example file structure

```
sof
header
USER_DATA(0, NULL)    // Required, point to first real user_data chunk
SOURCE_DEF(0)         // Required, internal, reserved for global annotations
SIGNAL_DEF(0, 0.VSR)  // Required, internal, reserved for global annotations
TRACK_DEF(0.VSR)
TRACK_HEAD(0.VSR)
TRACK_DEF(0.ANNO)
TRACK_HEAD(0.ANNO)
SOURCE_DEF(1)         // input device 1
SIGNAL_DEF(1, 1, FSR) // our signal, like "current" or "voltage"
TRACK_DEF(1.FSR)
TRACK_HEAD(1.FSR)
TRACK_DEF(1.ANNO)
TRACK_HEAD(1.ANNO)
TRACK_DEF(1.UTC)
TRACK_HEAD(1.UTC)
USER_DATA           // just because
TRACK_DATA(1.FSR)
TRACK_DATA(1.FSR)
TRACK_DATA(1.FSR)
TRACK_DATA(1.FSR)
TRACK_INDEX(1.FSR, lvl=0)
TRACK_SUMMARY(1.FSR, lvl=1)
TRACK_DATA(1.FSR)
TRACK_DATA(1.FSR)
TRACK_DATA(1.FSR)
TRACK_DATA(1.FSR)
TRACK_INDEX(1.FSR, lvl=0)
TRACK_SUMMARY(1.FSR, lvl=1)
TRACK_DATA(1.FSR)
TRACK_DATA(1.FSR)
TRACK_DATA(1.FSR)
TRACK_DATA(1.FSR)
TRACK_INDEX(1.FSR, lvl=0)
TRACK_SUMMARY(1.FSR, lvl=1)
TRACK_INDEX(1.FSR, lvl=1)
TRACK_SUMMARY(1.FSR, lvl=2)
USER_DATA           // just because
END
eof
```

Note that TRACK_HEAD(1.FSR) points to the first TRACK_INDEX(1.FSR, lvl=0) and
TRACK_INDEX(1.FSR, lvl=1). 
Each TRACK_DATA(1.FSR) is in a doubly-linked list with its next and previous
neighbors.  Each TRACK_INDEX(1.FSR, lvl=0) is likewise in a separate doubly-linked
list, and the payload of each TRACK_INDEX points to the summarized TRACK_DATA
instances.  TRACK_INDEX(1.FSR, lvl=1) points to each TRACK_INDEX(1.FSR, lvl=0) instance.
As more data is added, the TRACK_INDEX(1.FSR, lvl=1) will also get added to
the INDEX chunks at the same level.


## Resources

* [source code](https://github.com/jetperch/jls)
* [documentation](https://jls.readthedocs.io/en/latest/)
* [pypi](https://pypi.org/project/pyjls/)
* [Joulescope](https://www.joulescope.com/) (Joulescope web store)
* [forum](https://forum.joulescope.com/)


## References

* JLS v1: 
  [lower-layer](https://github.com/jetperch/pyjoulescope/blob/master/joulescope/datafile.py),
  [upper-layer](https://github.com/jetperch/pyjoulescope/blob/master/joulescope/data_recorder.py).
* [Sigrok/v3](https://sigrok.org/wiki/File_format:Sigrok/v3), which shares
  many of the same motivations.
* Tag-length-value: [Wikipedia](https://en.wikipedia.org/wiki/Type-length-value).
* Doubly linked list: [Wikipedia](https://en.wikipedia.org/wiki/Doubly_linked_list).


## License

This project is Copyright © 2017-2023 Jetperch LLC and licensed under the
permissive [Apache 2.0 License](./LICENSE).
