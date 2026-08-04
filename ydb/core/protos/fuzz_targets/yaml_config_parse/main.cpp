#include <ydb/library/fyamlcpp/fyamlcpp.h>

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  TString yamlConfig(reinterpret_cast<const char*>(data), size);

  try {
    auto document = NKikimr::NFyaml::TDocument::Parse(yamlConfig);
    auto node = document.Root();
    (void)node;
  } catch (...) {
    // Ignored
  }

  return 0;
}
