self: super: with self; rec {
  version = "20240722.1";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-ir4hG2VIPv3se7JfWqCM/siLqFEFkmhMW/IGCocy6Pc=";
  };

  patches = [];
}
