self: super: with self; {
  boost_intrusive = stdenv.mkDerivation rec {
    pname = "boost_intrusive";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "intrusive";
      rev = "boost-${version}";
      hash = "sha256-n/rDG+VhBRy5cn0Bvhs4IdhWe6PMVhiT9QYUybJhBoU=";
    };
  };
}
