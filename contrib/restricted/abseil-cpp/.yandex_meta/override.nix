self: super: with self; rec {
  version = "20250127.1";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-QTywqQCkyGFpdbtDBvUwz9bGXxbJs/qoFKF6zYAZUmQ=";
  };

  patches = [];
}
