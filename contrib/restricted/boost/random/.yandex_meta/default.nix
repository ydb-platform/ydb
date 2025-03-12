self: super: with self; {
  boost_random = stdenv.mkDerivation rec {
    pname = "boost_random";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "random";
      rev = "boost-${version}";
      hash = "sha256-Ijx3BNCUGSlqpXdlSDhGYYqW7NFjWR8srszLw5Ef3r8=";
    };
  };
}
