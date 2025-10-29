self: super: with self; {
  boost_bind = stdenv.mkDerivation rec {
    pname = "boost_bind";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "bind";
      rev = "boost-${version}";
      hash = "sha256-xdT8F7JuinEWbY5EOey21oUTjvF/7cPsu1tD31oEVus=";
    };
  };
}
