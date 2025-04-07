self: super: with self; {
  boost_bind = stdenv.mkDerivation rec {
    pname = "boost_bind";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "bind";
      rev = "boost-${version}";
      hash = "sha256-Eu/inCn4rlpkHd6LFu/N9CtzxwM4IoLms2RPvJgF11Q=";
    };
  };
}
