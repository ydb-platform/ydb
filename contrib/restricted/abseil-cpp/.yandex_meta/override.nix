self: super: with self; rec {
  version = "20260107.1";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-TJT2Kzc64zI42FAbbGWP3Sshh1dU/D/AtEpgZrrhebg=";
  };

  patches = [];
}
