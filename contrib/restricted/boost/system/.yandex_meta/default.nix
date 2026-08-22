self: super: with self; {
  boost_system = stdenv.mkDerivation rec {
    pname = "boost_system";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "system";
      rev = "boost-${version}";
      hash = "sha256-dQT6rp68Ve+N7wBy0PwW2cKNcBtCwcSW8Ir9aP6+Y7o=";
    };
  };
}
