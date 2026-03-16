<!--
# Copyright 2014-2023 Jetperch LLC
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

[![packaging](https://github.com/jetperch/joulescope_driver/actions/workflows/packaging.yml/badge.svg)](https://github.com/jetperch/joulescope_driver/actions/workflows/packaging.yml)


# Joulescope Driver

Welcome to the Joulescope™ Driver project.
[Joulescope](https://www.joulescope.com) is an affordable, precision DC energy
analyzer that enables you to build better products.

This user-space C library communicates with Joulescope products to configure 
operation and receive data.  The first-generation driver introduced in 2019 was
written in Python.  While Python proved to be a very flexible language enabling
many user scripts, it was difficult to support other languages.  
This second-generation driver launched in 2022 addresses several issues
with the first-generation python driver including:

1. Improved event-driven API based upon PubSub for easier integration with 
   user interfaces and other complicated software packages.
2. Improved portability for easier language bindings.
3. Improved performance.

For more information, see:

* [source code](https://github.com/jetperch/joulescope_driver)
* [documentation](https://joulescope-driver.readthedocs.io/en/latest/)
* [pypi](https://pypi.org/project/pyjoulescope-driver/)
* [Joulescope](https://www.joulescope.com/) (Joulescope web store)
* [jls](https://github.com/jetperch/jls) (Joulescope file format)
* [forum](https://forum.joulescope.com/)


## Python Installation

The python bindings work with Python 3.9 and later.
To use the python bindings, ensure that you have a compatible version
of python installed on your host computer.  Then:

    python -m pip install pyjoulescope_driver

For Ubuntu, you will also need to [install the udev rules](#ubuntu-2204-lts).

You can then run the pyjoulescope_driver python entry points:

    python -m pyjoulescope_driver --help
    python -m pyjoulescope_driver scan
    python -m pyjoulescope_driver info
    python -m pyjoulescope_driver info * --verbose

Note that you may need to change "python" to "python3" or the full path.  
You can also use a python
[virtual environment](https://docs.python.org/3/tutorial/venv.html).


## Building

Ensure that your computer has a development environment including CMake.  


### Windows

Install cmake and your favorite build toolchain such as 
Visual Studio, mingw64, wsl, ninja.

### macOS

For macOS, install homebrew, then:

    brew install pkgconfig python3


### Ubuntu 22.04 LTS

For Ubuntu:

    sudo apt install cmake build-essential ninja-build libudev-dev

You will also need to install the udev rules. If you are using a modern Linux distribution managed by systemd, using tag-based rules is most likely the right choice:

    $ wget https://raw.githubusercontent.com/jetperch/joulescope_driver/main/72-joulescope.rules
    $ sudo cp 72-joulescope.rules /etc/udev/rules.d/
    $ sudo udevadm control --reload-rules

If your system is not managed by systemd or your user is not assigned a proper login seat (as may be the case when logging in via SSH), using group-based rules is necessery (note that the plugdev group must exist/be created and your user must belong to it):

    $ wget https://raw.githubusercontent.com/jetperch/joulescope_driver/main/99-joulescope.rules
    $ sudo cp 99-joulescope.rules /etc/udev/rules.d/
    $ sudo udevadm control --reload-rules


### Common

    cd {your/repos/joulescope_driver}
    mkdir build && cd build
    cmake ..
    cmake --build . && ctest .

This package includes a command-line tool, jsdrv:

    jsdrv --help
    jsdrv scan


### Build python bindings

Install a compatible version of Python 3.9 or later.  To install
the pyjoulescope_driver dependencies:

    cd {your/repos/joulescope_driver}
    python -m pip install -U requirements.txt

You should then be able to build the native bindings:

    python setup.py build_ext --inplace

You can build the package using isolation:

    python -m build

Depending upon your system configuration, you may need to replace
"python" with "python3" or the full path to your desired python installation.

On Windows, you may be prompted to install the 
[Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/).
