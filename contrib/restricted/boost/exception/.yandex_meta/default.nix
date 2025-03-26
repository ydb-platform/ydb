self: super: with self; {
  boost_exception = stdenv.mkDerivation rec {
    pname = "boost_exception";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "exception";
      rev = "boost-${version}";
      hash = "sha256-2ySIcMOeWlE7y8hIXdJfxgAqJiSIO+QXyzqN0u8IvC4=";
    };
  };
}
