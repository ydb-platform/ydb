pkgs: attrs: with pkgs; rec {
  pname = "ceres-solver";
  version = "2.2.0";

  src = fetchFromGitHub {
    owner = "ceres-solver";
    repo = "ceres-solver";
    rev = "${version}";
    sha256 = "sha256-5SdHXcgwTlkDIUuyOQgD8JlAElk7aEWcFo/nyeOgN/k=";
  };

  patches = [
    ./commit-da34da3d-fix-issue-1104.patch
  ];

  cmakeFlags = [
    "-DLAPACK=OFF"
    "-DSUITESPARSE=OFF"
    "-DCXSPARSE=OFF"
    "-DEIGENSPARSE=ON"
    "-DSCHUR_SPECIALIZATIONS=OFF"
    "-DGFLAGS=OFF"
    "-DOPENMP=OFF"
    "-DCXX11_THREADS=ON"
    "-DCXX11=ON"
    "-DBUILD_EXAMPLES=OFF"
    "-DBUILD_BENCHMARKS=OFF"
  ];
}
