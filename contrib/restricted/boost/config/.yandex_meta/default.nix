self: super: with self; {
  boost_config = stdenv.mkDerivation rec {
    pname = "boost_config";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "config";
      rev = "boost-${version}";
      hash = "sha256-aPvWkk6F0boHvhQsNRn/Dafkvq5CUuL+xW4diiG/HWU=";
    };
  };
}
