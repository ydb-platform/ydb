self: super: with self; rec {
  version = "20260107.0";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-bHzFFanaRgO8D4xOy/fHqUrRhrAFcYCybBzVebgleEU=";
  };

  patches = [];
}
