self: super: with self; {
  boost_container_hash = stdenv.mkDerivation rec {
    pname = "boost_container_hash";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "container_hash";
      rev = "boost-${version}";
      hash = "sha256-jfRtfQ7/IEgH3ZQv0qNu+ZYX8RFttwARb8G/jjKS6us=";
    };
  };
}
