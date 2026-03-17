pkgs: attrs: with pkgs; rec {
  version = "4.12.0";

  opencv-contrib = fetchFromGitHub {
    owner = "opencv";
    repo = "opencv_contrib";
    rev = version;
    hash = "sha256-3tbscRFryjCynIqh0OWec8CUjXTeIDxOGJkHTK2aIao=";
  };

  # Bring only necessary opencv-contrib modules
  buildContrib = false;
  preConfigure = "";
  postUnpack = ''
     cp -r ${opencv-contrib}/modules/superres source/modules/
     cp -r ${opencv-contrib}/modules/optflow source/modules/
     cp -r ${opencv-contrib}/modules/ximgproc source/modules/
     cp -r ${opencv-contrib}/modules/cudaarithm source/modules/
     cp -r ${opencv-contrib}/modules/cudafilters source/modules/
     cp -r ${opencv-contrib}/modules/cudaimgproc source/modules/
     cp -r ${opencv-contrib}/modules/cudalegacy source/modules/
     cp -r ${opencv-contrib}/modules/cudaoptflow source/modules/
     cp -r ${opencv-contrib}/modules/cudawarping source/modules/
     cp -r ${opencv-contrib}/modules/cudev source/modules/
     chmod -R a+rw source/modules
  '';

  patches = [
    ./autogen.patch
  ];

  src = fetchFromGitHub {
    owner  = "opencv";
    repo   = "opencv";
    rev    = version;
    hash = "sha256-TZdEeZyBY3vCI53g4VDMzl3AASMuXAZKrSH/+XlxR7c=";
  };

  buildInputs = [
    zlib
    pcre
    boost
    eigen
    gflags
    protobuf
    python
    libjpeg
    libpng
    libtiff
    libwebp
    opencl-headers
    openjpeg
    openblas
    cudatoolkit
  ];

  cmakeFlags = [
    "-DBUILD_TESTS=OFF"
    "-DBUILD_PERF_TESTS=OFF"
    "-DBUILD_opencv_apps=OFF"
    "-DBUILD_opencv_dnn=OFF"
    "-DBUILD_opencv_gapi=OFF"
    "-DBUILD_opencv_python2=ON"
    "-DENABLE_PRECOMPILED_HEADERS=OFF"
    "-DWITH_FFMPEG=OFF"
    "-DWITH_GSTREAMER=OFF"
    "-DWITH_IPP=OFF"
    "-DWITH_ITT=OFF"
    "-DWITH_JASPER=OFF"
    "-DWITH_OPENCL=ON"
    "-DWITH_OPENEXR=OFF"
    "-DWITH_QUIRC=ON"
    "-DWITH_CUDA=ON"
    "-DWITH_IMGCODEC_GIF=ON"
  ];
}
