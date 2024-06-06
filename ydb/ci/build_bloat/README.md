# Build bloat

A tool used to visualize clang's compilation profiles.

![screenshot](screenshot.png)

It helps to answer the following questions:
- which dirs/files has longest impact on build time
- which headers has longest impact on build time

## How to use

You have to generate compilation profiles using `-ftime-trace` as compilation flag. 

With `ya` tool it can be done with folowing comand:

`ya make <your usual arguments> --output=~/some_output_build_dir -DCOMPILER_TIME_TRACE --add-result=.json`

After build is done run:

`ya tool python3 main.py --build-dir ~/some_output_build_dir --html-dir-cpp html_cpp_impact --html-dir-headers html_headers_impact`

And open `html_cpp_impact/index.html`, `html_headers_impact/index.html`
