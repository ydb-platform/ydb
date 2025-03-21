self: super: with self; {
  boost_fusion = stdenv.mkDerivation rec {
    pname = "boost_fusion";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "fusion";
      rev = "boost-${version}";
      hash = "sha256-oTJ2laEBpjx3MMS9QKFg9aW6LlfLH7JWcx9LABBmrgI=";
    };
  };
}
