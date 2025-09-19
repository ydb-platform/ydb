self: super: with self; {
  boost_container_hash = stdenv.mkDerivation rec {
    pname = "boost_container_hash";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "container_hash";
      rev = "boost-${version}";
      hash = "sha256-lKxh8nwQSIFX+lFAWJlUmUwxt2xM9678PFbkolkg2SE=";
    };
  };
}
