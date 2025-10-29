self: super: with self; {
  boost_winapi = stdenv.mkDerivation rec {
    pname = "boost_winapi";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "winapi";
      rev = "boost-${version}";
      hash = "sha256-bqgQoKsXrXj3GeHdE0TM34VCXPkydLrUlXEhWb1lwiw=";
    };
  };
}
