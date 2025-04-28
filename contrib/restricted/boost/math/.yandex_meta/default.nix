self: super: with self; {
  boost_math = stdenv.mkDerivation rec {
    pname = "boost_math";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "math";
      rev = "boost-${version}";
      hash = "sha256-6IPYRwmircWMOa7lz8ftD8zORCwVNRdAkWmIjYj8xNU=";
    };
  };
}
