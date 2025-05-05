self: super: with self; {
  boost_tuple = stdenv.mkDerivation rec {
    pname = "boost_tuple";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "tuple";
      rev = "boost-${version}";
      hash = "sha256-12PmUCrHtXziGp0e9EsoTml3mBhTbHlBv71UHT8l5Yc=";
    };
  };
}
