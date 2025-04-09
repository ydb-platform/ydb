self: super: with self; {
  boost_winapi = stdenv.mkDerivation rec {
    pname = "boost_winapi";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "winapi";
      rev = "boost-${version}";
      hash = "sha256-6K3opuoL0D4DkoicUEpTM2H3FRZZwPIi+ouDXN821JQ=";
    };
  };
}
