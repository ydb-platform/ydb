self: super: with self; rec {
  version = "20250127.0";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-uOgUtF8gaEgcxFK9WAoAhv4GoS8P23IoUxHZZVZdpPk=";
  };

  patches = [];
}
