self: super: with self; {
  boost_static_assert = stdenv.mkDerivation rec {
    pname = "boost_static_assert";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "static_assert";
      rev = "boost-${version}";
      hash = "sha256-J5OUm8Ou74N5cOnaOSinmGk+YPpolWpfWCnaGlvUZUY=";
    };
  };
}
