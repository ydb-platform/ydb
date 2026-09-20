self: super: with self; {
  boost_system = stdenv.mkDerivation rec {
    pname = "boost_system";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "system";
      rev = "boost-${version}";
      hash = "sha256-gNGvVFY2lYxBQMNkpxxxMK0JeqGcRal4ejXQ1j7RcoI=";
    };
  };
}
