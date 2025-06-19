self: super: with self; rec {
  version = "20250512.0";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-Tuw1Py+LQdXS+bizXsduPjjEU5YIAVFvL+iJ+w8JoSU=";
  };

  patches = [];
}
