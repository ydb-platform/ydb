self: super: with self; {
  boost_iterator = stdenv.mkDerivation rec {
    pname = "boost_iterator";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "iterator";
      rev = "boost-${version}";
      hash = "sha256-V30bjYRd2/Cyehhyy9g5P99Cz/3I3pvHWe+CPDoHPEU=";
    };
  };
}
