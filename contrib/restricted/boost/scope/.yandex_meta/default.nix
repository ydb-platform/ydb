self: super: with self; {
  boost_scope = stdenv.mkDerivation rec {
    pname = "boost_scope";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "scope";
      rev = "boost-${version}";
      hash = "sha256-g0C1ZRprYg5ZF5kmSCDIAWAhlqHMs0/tbKkMUHcfHc4=";
    };
  };
}
