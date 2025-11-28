self: super: with self; {
  boost_regex = stdenv.mkDerivation rec {
    pname = "boost_regex";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "regex";
      rev = "boost-${version}";
      hash = "sha256-sovIecm5wZw1ZiX2KiBE643MROAHC1f+SbthxpGW09Q=";
    };
  };
}
