self: super: with self; {
  boost_fusion = stdenv.mkDerivation rec {
    pname = "boost_fusion";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "fusion";
      rev = "boost-${version}";
      hash = "sha256-uEuHtw9lYPEokOzutgQLBoVCSZ8t+lpAXmsaLS4vCSU=";
    };
  };
}
