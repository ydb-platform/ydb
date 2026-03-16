C-Blosc libraries come with Python-Blosc wheels
===============================================

Starting on version 1.21.0, C-Blosc binary libraries can easily be installed from Python-Blosc (>= 1.10) wheels:

.. code-block:: console

        $ pip install blosc                                                                             (base)
        Collecting blosc
          Downloading blosc-1.10.0-cp37-cp37m-macosx_10_9_x86_64.whl (2.2 MB)
             |████████████████████████████████| 2.2 MB 4.7 MB/s
        Installing collected packages: blosc
          Attempting uninstall: blosc
            Found existing installation: blosc 1.10.0
            Uninstalling blosc-1.10.0:
              Successfully uninstalled blosc-1.10.0
        Successfully installed blosc-1.10.0

As a result, one can easily update to the latest version of C-Blosc binaries without the need to manually compile the thing.  Following are instructions on how to use the libraries in wheels for different platforms.


Compiling C files with Blosc wheels on Windows
----------------------------------------------

- The wheels for Windows have been produced with the Microsoft MSVC compiler, so we recommend that you use it too.  You can get it for free at: https://visualstudio.microsoft.com/es/downloads/.

- In order to check that the MSVC command line is set up correctly, enter ``cl`` in the command prompt window and verify that the output looks something like this:

.. code-block:: console

    > cl
    Microsoft (R) C/C++ Optimizing Compiler Version 19.00.24245 for x64
    Copyright (C) Microsoft Corporation.  All rights reserved.

    usage: cl [ option... ] filename... [ /link linkoption... ]

- Now, install the wheels:

.. code-block:: console

    > pip install blosc
    Collecting blosc
      Using cached blosc-1.10.0-cp37-cp37m-win_amd64.whl (1.5 MB)
    Installing collected packages: blosc
    Successfully installed blosc-1.10.0

- Make the compiler available. Its typical installation location uses to be `C:\\Program files (x86)\\Microsoft Visual Studio`, so change your current directory there. Then, to set up the build architecture environment you can open a command prompt window in the `VC\\Auxiliary\\Build` subdirectory and execute `vcvarsall.bat x64` if your architecture is 64 bits or `vcvarsall.bat x86` if it is 32 bits.

- You will need to know the path where the Blosc wheel has installed its files.  For this we will use the `dir /s` command (but you can use your preferred location method):

.. code-block:: console

    > dir /s c:\blosc.lib
     Volume in drive C is OS
     Volume Serial Number is 7A21-A5D5

     Directory of c:\Users\user\miniconda3\Lib

    14/12/2020  09:56             7.022 blosc.lib
                   1 File(s)          7.022 bytes

         Total list files:
                   1 File(s)          7.022 bytes
                   0 dirs  38.981.902.336 free bytes

- The output shows the path of blosc.lib in your system, but we are rather interested in the parent one:

.. code-block:: console

    > set WHEEL_DIR=c:\Users\user\miniconda3

- Now, it is important to copy the library `blosc.dll` to C:\\Windows\\System32 directory, so it can be found by the executable when it is necessary.

- Finally, to compile C files using Blosc libraries, enter this command:

.. code-block:: console

    > cl <file_name>.c <path_of_blosc.lib> /Ox /Fe<file_name>.exe /I<path_of_blosc.h> /MT /link/NODEFAULTLIB:MSVCRT

- For instance, in the case of blosc "examples/simple.c":

.. code-block:: console

    > cl simple.c %WHEEL_DIR%\lib\blosc.lib /Ox /Fesimple.exe /I%WHEEL_DIR%\include /MT /link/NODEFAULTLIB:MSVCRT

    Microsoft (R) C/C++ Optimizing Compiler Version 19.10.25017 for x86
    Copyright (C) Microsoft Corporation.  All rights reserved.

    simple.c
    Microsoft (R) Incremental Linker Version 14.10.25017.0
    Copyright (C) Microsoft Corporation.  All rights reserved.

    /out:simple.exe
    simple.obj
    /NODEFAULTLIB:MSVCRT
    .\miniconda3\lib\blosc.lib

- And you can run your program:

.. code-block:: console

    > simple
    Blosc version info: 1.20.1 ($Date:: 2020-09-08 #$)
    Compression: 4000000 -> 37816 (105.8x)
    Decompression successful!
    Successful roundtrip!

- Rejoice!


Compiling C files with Blosc wheels on Linux
--------------------------------------------

- Install the wheels:

.. code-block:: console

    $ pip install blosc
    Collecting blosc
      Using cached blosc-1.10.0-cp37-cp37m-manylinux2010_x86_64.whl (2.2 MB)
    Installing collected packages: blosc
    Successfully installed blosc-1.10.0

- Find the path where blosc wheel has installed its files:

.. code-block:: console

    $ find / -name libblosc.so 2>/dev/null
    /home/soscar/miniconda3/lib/libblosc.so

- The output shows the path of libblosc.so, but we are rather interested in the parent one:

.. code-block:: console

    $ WHEEL_DIR=/home/soscar/miniconda3

- To compile C files using blosc you only need to enter the commands:

.. code-block:: console

    $ export LD_LIBRARY_PATH=<path_of_libblosc.so>
    $ gcc <file_name>.c -I<path_of_blosc.h> -o <file_name> -L<path_of_libblosc.so> -lblosc

- For instance, let's compile blosc's "examples/many_compressors.c":

.. code-block:: console

    $ export LD_LIBRARY_PATH=$WHEEL_DIR/lib   # note that you need the LD_LIBRARY_PATH env variable
    $ gcc many_compressors.c -I$WHEEL_DIR/include -o many_compressors -L$WHEEL_DIR/lib -lblosc

- Run your program:

.. code-block:: console

    $ ./many_compressors
    Blosc version info: 1.20.1 ($Date:: 2020-09-08 #$)
    Using 4 threads (previously using 1)
    Using blosclz compressor
    Compression: 4000000 -> 37816 (105.8x)
    Successful roundtrip!
    Using lz4 compressor
    Compression: 4000000 -> 37938 (105.4x)
    Successful roundtrip!
    Using lz4hc compressor
    Compression: 4000000 -> 27165 (147.2x)
    Successful roundtrip!

- Rejoice!


Compiling C files with Blosc wheels on MacOS
--------------------------------------------

- Install the wheels:

.. code-block:: console

        $ pip install blosc                                                                             (base)
        Collecting blosc
          Downloading blosc-1.10.0-cp37-cp37m-macosx_10_9_x86_64.whl (2.2 MB)
             |████████████████████████████████| 2.2 MB 4.7 MB/s
        Installing collected packages: blosc
          Attempting uninstall: blosc
            Found existing installation: blosc 1.10.0
            Uninstalling blosc-1.10.0:
              Successfully uninstalled blosc-1.10.0
        Successfully installed blosc-1.10.0

- Find the path where blosc wheel has installed its files:

.. code-block:: console

    $ find / -name libblosc.dylib 2>/dev/null
    /home/soscar/miniconda3/lib/libblosc.dylib

- The output shows the path of libblosc.dylib, but we are rather interested in the parent one:

.. code-block:: console

    $ WHEEL_DIR=/home/soscar/miniconda3

- To compile C files using blosc you only need to enter the commands:

.. code-block:: console

    $ export LD_LIBRARY_PATH=<path_of_libblosc.dylib>
    $ clang <file_name>.c -I<path_of_blosc.h> -o <file_name> -L<path_of_libblosc.dylib> -lblosc

- For instance, let's compile blosc's "examples/many_compressors.c":

.. code-block:: console

    $ export LD_LIBRARY_PATH=$WHEEL_DIR/lib   # note that you need the LD_LIBRARY_PATH env variable
    $ clang many_compressors.c -I$WHEEL_DIR/include -o many_compressors -L$WHEEL_DIR/lib -lblosc

- Run your program:

.. code-block:: console

    $ ./many_compressors
    Blosc version info: 1.20.1 ($Date:: 2020-09-08 #$)
    Using 4 threads (previously using 1)
    Using blosclz compressor
    Compression: 4000000 -> 37816 (105.8x)
    Successful roundtrip!
    Using lz4 compressor
    Compression: 4000000 -> 37938 (105.4x)
    Successful roundtrip!
    Using lz4hc compressor
    Compression: 4000000 -> 27165 (147.2x)
    Successful roundtrip!

- Rejoice!
