self: super: with self; rec {
  version = "20250814.0";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-6Ro7miql9+wcArsOKTjlyDSyD91rmmPsIfO5auk9kiI=";
  };

  patches = [];
}
