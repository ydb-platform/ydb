This project has been ported to Windows.  A working solution file
exists in this directory:
    sparsehash.sln

You can load this solution file into either VC++ 7.1 (Visual Studio
2003) or VC++ 8.0 (Visual Studio 2005) -- in the latter case, it will
automatically convert the files to the latest format for you.

When you build the solution, it will create a number of
unittests,which you can run by hand (or, more easily, under the Visual
Studio debugger) to make sure everything is working properly on your
system.  The binaries will end up in a directory called "debug" or
"release" in the top-level directory (next to the .sln file).

Note that these systems are set to build in Debug mode by default.
You may want to change them to Release mode.

I have little experience with Windows programming, so there may be
better ways to set this up than I've done!  If you run across any
problems, please post to the google-sparsehash Google Group, or report
them on the sparsehash Google Code site:
   http://groups.google.com/group/google-sparsehash
   http://code.google.com/p/sparsehash/issues/list

-- craig
