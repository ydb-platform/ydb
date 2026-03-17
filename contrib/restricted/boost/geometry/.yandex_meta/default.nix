self: super: with self; {
  boost_geometry = stdenv.mkDerivation rec {
    pname = "boost_geometry";
    version = "1.86.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "geometry";
      rev = "boost-${version}";
      hash = "sha256-HnQWHogAiXHsj+jrkjn5SoX7FNOyEjw0f9Aht+HojlI=";
    };
  };
}
